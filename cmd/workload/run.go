package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/transport"
	"golang.org/x/sync/errgroup"
)

type runFlags struct {
	configPath  string
	keyspace int
	workers  int
	ops      int
	rwRatio  float64
	delRatio float64
	txnRatio float64
	seed     int
	outPath  string
}

func getRunFlags() runFlags {
	var f runFlags
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	fs.StringVar(&f.configPath, "config", "", "path to cluster config file")
	fs.IntVar(&f.keyspace, "keys", 100, "size of the keyspace")
	fs.IntVar(&f.workers, "workers", 5, "number of worker go routines")
	fs.IntVar(&f.ops, "ops", 1000, "number of ops to perform in total")
	fs.Float64Var(&f.rwRatio, "rw-ration", 0.5, "read-write ration (0 -> all writes, 1 -> all reads)")
	fs.Float64Var(&f.delRatio, "del-ration", 0.1, "out of writes, how many deletes vs puts should happen")
	fs.Float64Var(&f.txnRatio, "txn-ration", 0.0, "out of writes, how many txn's should happen")
	fs.IntVar(&f.seed, "seed", 0, "random seed")
	fs.StringVar(&f.outPath, "out", "workload.log", "output path to write the workload")

	if err := fs.Parse(os.Args[2:]); err != nil {
		log.Fatal(err)
	}

	if f.configPath == "" {
		log.Fatal("config path is required")
	}

	if f.seed == 0 {
		f.seed = time.Now().Nanosecond()
	}

	if f.rwRatio < 0 || f.rwRatio > 1 {
		log.Fatal("rw-ratio must be between 0 and 1")
	}
	if f.delRatio < 0 || f.delRatio > 1 {
		log.Fatal("del-ratio must be between 0 and 1")
	}
	if f.txnRatio < 0 || f.txnRatio > 1 {
		log.Fatal("txn-ratio must be between 0 and 1")
	}
	if f.rwRatio+f.delRatio+f.txnRatio > 1 {
		log.Fatal("rw-ratio + del-ratio + txn-ratio must be <= 1")
	}
	if f.workers <= 0 {
		log.Fatal("workers must be > 0")
	}
	if f.ops <= 0 {
		log.Fatal("ops must be > 0")
	}

	return f
}

type opType string

const (
	opGet opType = "get"
	opPut opType = "put"
	opDel opType = "del"
	opTxn opType = "txn"
)

type op struct {
	opType opType
	key    string
	value  []byte
	rev    int64
}

type logEntry struct {
	// identity
	SeqID    int64 `json:"seq_id"` // monotonically increasing, assigned by the log
	WorkerID int   `json:"worker_id"`
	TsNs     int64 `json:"ts_ns"` // unix nanoseconds at time of response

	// what was sent
	OpType   opType `json:"op_type"`
	Key      string `json:"key"`
	Value    []byte `json:"value,omitempty"`     // nil for get/delete
	GetValue []byte `json:"get_value,omitempty"` // only for get requests

	// what came back
	StatusCode int    `json:"status_code"`
	ErrMsg     string `json:"err_msg,omitempty"` // network-level error, not HTTP error

	// response payload — only populated on 200
	ResultRev int64  `json:"result_rev,omitempty"` // header.revision from response
	RaftIndex uint64 `json:"raft_index,omitempty"`
	RaftTerm  uint64 `json:"raft_term,omitempty"`
	NodeID    string `json:"node_id,omitempty"` // which node actually served it

	// classification — set by the worker, not by check
	// "ok" | "expected_error" | "election_gap" | "unexpected_error"
	Class string `json:"class"`
}

type workloadLog struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
	seq atomic.Int64
}

func NewWorkloadLog(path string) (*workloadLog, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create workload log: %w", err)
	}
	return &workloadLog{f: f, enc: json.NewEncoder(f)}, nil
}

func (l *workloadLog) Record(e logEntry) error {
	e.SeqID = l.seq.Add(1)
	e.TsNs = time.Now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.enc.Encode(e) // \n automatically
}

func (l *workloadLog) Close() error {
	return l.f.Close()
}

type response struct {
	StatusCode int
	Body       []byte
	Result     *command.Result
}

func worker(ctx context.Context, id int, quota int, gen *opGen, client *client, wlog *workloadLog) {
	q := 0
	for q < quota {
		select {
		case <-ctx.Done():
			return
		default:
		}
		q++

		op := gen.Next()
		entry := logEntry{
			WorkerID: id,
			OpType:   op.opType,
			Key:      op.key,
			Value:    op.value,
		}

		var resp *response
		var err error

		switch op.opType {
		case opPut:
			resp, err = client.put(ctx, op.key, string(op.value))
		case opGet:
			resp, err = client.get(ctx, op.key, 0)
		case opDel:
			resp, err = client.delete(ctx, op.key)
		}

		if err != nil {
			// network-level failure
			ready, _ := client.readyPeer(ctx)
			entry.ErrMsg = err.Error()
			entry.StatusCode = 0
			if !ready {
				entry.Class = "election_gap"
			} else {
				entry.Class = "unexpected_error"
			}
			wlog.Record(entry)
			continue
		}

		entry.StatusCode = resp.StatusCode

		switch {
		case resp.StatusCode == http.StatusOK && resp.Result != nil:
			entry.ResultRev = resp.Result.Header.Revision
			entry.RaftIndex = resp.Result.Header.RaftIndex
			entry.RaftTerm = resp.Result.Header.RaftTerm
			entry.NodeID = resp.Result.Header.NodeID
			entry.Class = "ok"

		case resp.StatusCode == http.StatusTemporaryRedirect:
			entry.Class = "expected_error"

		case resp.StatusCode >= 500:
			ready, _ := client.readyPeer(ctx)
			if !ready {
				entry.Class = "election_gap"
			} else {
				entry.Class = "unexpected_error"
			}

		default:
			entry.Class = "unexpected_error"
		}

		wlog.Record(entry)
	}
}

type opGen struct {
	rng        *rand.Rand
	keyspace   int
	readCutoff float64
	delCutoff  float64
	txnCutoff  float64
}

func newOpGen(seed int, keyspace int, rwRatio, deleteRatio, txnRatio float64) *opGen {
	writeFraction := 1.0 - rwRatio
	readCutoff := rwRatio
	delCutoff := readCutoff + writeFraction*deleteRatio
	txnCutoff := delCutoff + writeFraction*txnRatio

	return &opGen{
		rng:        rand.New(rand.NewSource(int64(seed))),
		keyspace:   keyspace,
		readCutoff: readCutoff,
		delCutoff:  delCutoff,
		txnCutoff:  txnCutoff,
	}
}

func (g *opGen) fork(workerID int) *opGen {
	childSeed := g.rng.Int63() ^ int64(workerID*2654435761) // cheap hash mix
	return &opGen{
		rng:        rand.New(rand.NewSource(childSeed)),
		keyspace:   g.keyspace,
		readCutoff: g.readCutoff,
		delCutoff:  g.delCutoff,
		txnCutoff:  g.txnCutoff,
	}
}

func (g *opGen) Next() op {
	key := g.randomKey()
	roll := g.rng.Float64() // single roll decides everything

	switch {
	case roll < g.readCutoff:
		return op{opType: opGet, key: key}

	case roll < g.delCutoff:
		return op{opType: opDel, key: key}

	case roll < g.txnCutoff:
		return g.randomTxn()

	default:
		return op{
			opType: opPut,
			key:    key,
			value:  g.randomValue(),
		}
	}
}

func (g *opGen) randomKey() string {
	return fmt.Sprintf("key-%04d", g.rng.Intn(g.keyspace))
}

func (g *opGen) randomValue() []byte {
	b := make([]byte, 8) // TODO: move value size into flags
	g.rng.Read(b)
	return b
}

// simple CAS
func (g *opGen) randomTxn() op {
	a := g.randomKey()
	b := g.randomKey()
	return op{
		opType: opTxn,
		key:    a,
		value:  []byte(b),
	}
}

type client struct {
	peers     []config.Peer
	leaderIdx int
	mu        sync.RWMutex
	http      *http.Client
}

func newClient(peers []config.Peer) *client {
	return &client{
		peers: peers,
		http: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

var errLeaderUnknown = errors.New("redirected, but no leader found")

func (c *client) put(ctx context.Context, key, value string) (*response, error) {
	cmd := command.PutCmd{
		Key:   []byte(key),
		Value: []byte(value),
	}
	return c.do(ctx, http.MethodPost, transport.UriKvUri+"/put", cmd)
}

func (c *client) get(ctx context.Context, key string, rev int64) (*response, error) {
	cmd := command.RangeCmd{
		Key:      []byte(key),
		Revision: rev,
	}
	return c.do(ctx, http.MethodGet, transport.UriKvUri+"/get", cmd)
}

func (c *client) delete(ctx context.Context, key string) (*response, error) {
	cmd := command.DeleteCmd{
		Key: []byte(key),
	}
	return c.do(ctx, http.MethodDelete, transport.UriKvUri+"/delete", cmd)
}

func (c *client) readyPeer(ctx context.Context) (bool, error) {
	n := rand.Intn(len(c.peers))
	addr := c.peers[n].GetHttpAddress()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:"+strings.Split(addr, ":")[1]+"/readyz", nil)
	if err != nil {
		return false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func (c *client) do(ctx context.Context, method, path string, payload any) (*response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}

	for attempt := range 2 {
		_ = attempt
		leader := c.currentLeader()
		url := peerAddr(leader) + path

		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusTemporaryRedirect {
			// parse leader_id from redirect body and update our tracking
			if err := c.handleRedirect(respBody); err != nil {
				return nil, fmt.Errorf("%w: %v", errLeaderUnknown, err)
			}
			continue // retry with new leader
		}

		result := &response{StatusCode: resp.StatusCode, Body: respBody}
		if resp.StatusCode == http.StatusOK {
			var r command.Result
			if err := json.Unmarshal(respBody, &r); err != nil {
				return nil, fmt.Errorf("unmarshal result: %w", err)
			}
			result.Result = &r
		}
		return result, nil
	}

	return nil, fmt.Errorf("failed after redirect retry")
}

func (c *client) currentLeader() config.Peer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.peers[c.leaderIdx]
}

func (c *client) handleRedirect(body []byte) error {
	msg := make(map[string]string)
	if err := json.Unmarshal(body, &msg); err != nil || msg["leader_id"] == "" {
		return fmt.Errorf("could not parse leader_id from redirect body")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for i, p := range c.peers {
		if p.NodeID == msg["leader_id"] {
			c.leaderIdx = i
			return nil
		}
	}
	return fmt.Errorf("leader_id %q not found in peer list", msg["leader_id"])
}

func peerAddr(peer config.Peer) string {
	var port string
	switch peer.NodeID {
	case "node1":
		port = "8001"
	case "node2":
		port = "8002"
	case "node3":
		port = "8003"
	default:
		log.Fatal("nodeID not in node{1,2,3}")
	}

	return "http://localhost:" + port
}

func generateWorkload() {
	f := getRunFlags()
	cfg := config.LoadConfig(config.Flags{
		ConfigPath: f.configPath,
		Bootstrap:  false,
		NodeID:     "empty",
		RaftPort:   "empty",
		HttpPort:   "empty",
	})
	wlog, err := NewWorkloadLog(f.outPath)
	if err != nil {
		log.Fatalf("create workload log: %v\n", err)
	}
	defer wlog.Close()

	gen := newOpGen(f.seed, f.keyspace, f.rwRatio, f.delRatio, f.txnRatio)
	client := newClient(cfg.Peers)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	log.Println("workload started")
	for i := range f.workers {
		quota := f.ops / f.workers
		if i == 0 {
			quota += f.ops % f.workers
		}
		g.Go(func() error {
			worker(ctx, i, quota, gen.fork(i), client, wlog)
			return nil
		})
	}

	g.Wait()
	log.Println("workload complete")
}
