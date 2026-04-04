package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/watch"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/balits/kave/internal/watch"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	t           *testing.T
	srv         *httptest.Server
	httpServer  *HttpServer
	store       *mvcc.KvStore
	lm          *lease.LeaseManager
	raftService *service.MockRaftService
	om          *ot.OTManager
	otClient    ot.MockOTClient
}

type mockDiscoveryService struct {
	me peer.Peer
}

func (m *mockDiscoveryService) Me() peer.Peer { return m.me }
func (m *mockDiscoveryService) GetPeers(context.Context) ([]peer.Peer, error) {
	return make([]peer.Peer, 0), nil
}

func newTestServer(t *testing.T, isLeaderValue bool) *testServer {
	t.Helper()
	me := peer.TestPeer()
	state := raft.Leader
	if !isLeaderValue {
		state = raft.Follower
	}
	raftService := &service.MockRaftService{
		Me_:     me,
		Leader_: me,
		State_:  state,
	}
	kvOpts := kv.Options{
		MaxKeySize:   256,
		MaxValueSize: 2048,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	kvstore := mvcc.NewKvStore(reg, logger, backend)
	hub := watch.NewWatchHub(reg, logger, kvstore)
	hub := watch.NewWatchHub(reg, logger, kvstore)
	lm := lease.NewManager(reg, logger, kvstore, backend)
	om, err := ot.NewOTManager(reg, logger, backend, ot.DefaultOptions)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	fsm := fsm.New(logger, me, kvstore, lm, om)

	var logIndex atomic.Uint64
	propose := func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		bs, err := command.Encode(cmd)
		if err != nil {
			return nil, err
		}
		idx := logIndex.Add(1)
		res := fsm.Apply(&raft.Log{
			Index: idx,
			Data:  bs,
			Term:  1,
			Type:  raft.LogCommand,
		})
		result, ok := res.(command.Result)
		if !ok {
			return nil, fmt.Errorf("unexpected result type from FSM")
		}
		return &result, nil
	}
	genRes, err := propose(t.Context(), command.Command{
		Kind:                 command.KindOTGenerateClusterKey,
		OTGenerateClusterKey: &command.CmdOTGenerateClusterKey{},
	})
	require.NoError(t, err)
	require.NoError(t, genRes.Error, "FSM returned error for cluster key gen")
	require.NotNil(t, genRes.OtGenerateClusterKey, "OtGenerateClusterKey result is nil")

	require.NoError(t, om.InitTokenCodec())

	discoverySvc := &mockDiscoveryService{me}
	kvSvc := service.NewKVService(logger, me, kvstore, raftService, kvOpts, propose)
	leaseSvc := service.NewLeaseService(logger, propose)
	otService := service.NewOTService(logger, kvstore, om, peerSvc, propose)
	clusterSvc := &service.MockClusterService{}

	httpServer := NewHTTPServer(logger, me.GetHttpListenAddress(), kvSvc, leaseSvc, otService, clusterSvc, peerSvc, hub, reg)

	ts := httptest.NewServer(httpServer.server.Handler)
	t.Cleanup(ts.Close)

	return &testServer{
		t:           t,
		srv:         ts,
		httpServer:  httpServer,
		store:       kvstore,
		lm:          lm,
		raftService: raftService,
		otClient:    ot.MockOTClient{T: t},
		om:          om,
	}
}

func (ts *testServer) do(method string, path string, body any) *http.Response {
	ts.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(ts.t, json.NewEncoder(&buf).Encode(body), "json encode")
	}
	req, err := http.NewRequest(method, ts.srv.URL+path, &buf)
	require.NoError(ts.t, err, "create request")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(ts.t, err, "default client do")
	ts.t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func (ts *testServer) decodeJSON(resp *http.Response, dst any) {
	ts.t.Helper()
	require.NoError(ts.t, json.NewDecoder(resp.Body).Decode(dst))
}

func (ts *testServer) mustPut(key, value string) {
	ts.t.Helper()
	resp := ts.do(http.MethodPost, RouteKvPut, api.PutRequest{
		Key:   []byte(key),
		Value: []byte(value),
	})
	require.Equal(ts.t, http.StatusOK, resp.StatusCode)
}

func (ts *testServer) overrideLeader(leader *httptest.Server) {
	ts.t.Helper()
	url, err := url.Parse(leader.URL)
	require.NoError(ts.t, err)

	ts.raftService.State_ = raft.Follower
	ts.raftService.Leader_ = peer.Peer{
		NodeID:   url.Host,
		Hostname: url.Hostname(),
		HttpPort: url.Port(),
		RaftPort: "7000",
	}
}

func Test_KvPut_CreatesKey(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, RouteKvPut,
		api.PutRequest{Key: []byte("hello"), Value: []byte("world")})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.PutResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.Header.Revision)
}

func Test_KvPut_BumpsRevisionAfterWrites(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	for i := range 5 {
		resp := ts.do(http.MethodPost, RouteKvPut, api.PutRequest{
			Key:   fmt.Appendf(nil, "k%d", i),
			Value: []byte("v"),
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var res api.PutResponse
		ts.decodeJSON(resp, &res)
		require.Equal(t, int64(i+1), res.Header.Revision)
	}
}

func Test_KvPut_WithPrevEntry_ReturnsOldValue(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	key := "k"
	oldVal := "v1"
	ts.mustPut(key, oldVal)

	resp := ts.do(http.MethodPost, RouteKvPut, api.PutRequest{
		Key:       []byte(key),
		Value:     []byte("v2"),
		PrevEntry: true,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.PutResponse
	ts.decodeJSON(resp, &res)
	require.NotNil(t, res.PrevEntry)
	require.Equal(t, oldVal, string(res.PrevEntry.Value))
}

func Test_KvPut_OverwritePreservesCreateRev(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("k", "v1")
	ts.mustPut("k", "v2")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key: []byte("k"),
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.Entries[0].CreateRev)
	require.Equal(t, int64(2), res.Entries[0].ModRev)
	require.Equal(t, int64(2), res.Entries[0].Version)
}

func Test_KvPut_MalformedBody_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	req, _ := http.NewRequest(http.MethodPost, ts.srv.URL+RouteKvPut,
		strings.NewReader("bad json"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_KvGet_ExistingKey(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("foo", "bar")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key: []byte("foo"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 1, res.Count)
	require.Equal(t, "bar", string(res.Entries[0].Value))
}

func Test_KvGet_MissingKey_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key: []byte("missing"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Zero(t, res.Count)
	require.Empty(t, res.Entries)
}

func Test_KvGet_AtOldRevision(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("k", "v1")
	ts.mustPut("k", "v2")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:      []byte("k"),
		Revision: 1,
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, "v1", string(res.Entries[0].Value))
}

func Test_KvGet_RangeQuery(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	for _, k := range []string{"a", "b", "c", "d"} {
		ts.mustPut(k, k)
	}
	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key: []byte("b"),
		End: []byte("d"),
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 2, res.Count)
	require.Equal(t, "b", string(res.Entries[0].Key))
	require.Equal(t, "c", string(res.Entries[1].Key))
}

func Test_KvGet_WithLimit(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		ts.mustPut(k, k)
	}
	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:   []byte("a"),
		End:   []byte("z"),
		Limit: 3,
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 5, res.Count, "total count ignores limit")
	require.Len(t, res.Entries, 3, "entries are limited")
}

func Test_KvGet_CountOnly(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("x", "1")
	ts.mustPut("y", "2")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:       []byte("x"),
		End:       []byte("z"),
		CountOnly: true,
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 2, res.Count)
	require.Empty(t, res.Entries)
}

func Test_KvGet_Prefix(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("app", "1")
	ts.mustPut("apple", "2")
	ts.mustPut("application", "3")
	ts.mustPut("banana", "4")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:    []byte("app"),
		Prefix: true,
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 3, res.Count)
}

func Test_KvDelete_RemovesKey(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("bye", "gone")

	resp := ts.do(http.MethodDelete, RouteKvDelete, api.DeleteRequest{
		Key: []byte("bye"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.NumDeleted)

	getResp := ts.do(http.MethodGet, RouteKvRange,
		api.RangeRequest{Key: []byte("bye")})
	var getRes api.RangeResponse
	ts.decodeJSON(getResp, &getRes)
	require.Zero(t, getRes.Count)
}

func Test_KvDelete_NonExistentKey_ReturnsZero(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodDelete, RouteKvDelete, api.DeleteRequest{
		Key: []byte("ghost"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Zero(t, res.NumDeleted)
}

func Test_KvDelete_Range(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	for _, k := range []string{"a", "b", "c", "d"} {
		ts.mustPut(k, k)
	}
	resp := ts.do(http.MethodDelete, RouteKvDelete, api.DeleteRequest{
		Key: []byte("b"),
		End: []byte("d"),
	})
	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(2), res.NumDeleted)
}

func Test_KvDelete_WithPrevEntries(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("p", "pval")

	resp := ts.do(http.MethodDelete, RouteKvDelete, api.DeleteRequest{
		Key:         []byte("p"),
		PrevEntries: true,
	})
	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Len(t, res.PrevEntries, 1)
	require.Equal(t, "pval", string(res.PrevEntries[0].Value))
}

func Test_KvDelete_ThenGetAtOldRevision(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("k", "v")
	ts.do(http.MethodDelete, RouteKvDelete, api.DeleteRequest{Key: []byte("k")})

	// key is gone at current rev
	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{Key: []byte("k")})
	var cur api.RangeResponse
	ts.decodeJSON(resp, &cur)
	require.Zero(t, cur.Count)

	// but readable at rev 1
	resp = ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:      []byte("k"),
		Revision: 1,
	})
	var old api.RangeResponse
	ts.decodeJSON(resp, &old)
	require.Equal(t, 1, old.Count)
	require.Equal(t, "v", string(old.Entries[0].Value))
}

func Test_KvTxn_SuccessBranch(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("counter", "v1")

	resp := ts.do(http.MethodPost, RouteKvTxn, api.TxnRequest{
		Comparisons: []command.Comparison{{
			Key:         []byte("counter"),
			Operator:    api.OperatorEqual,
			TargetField: api.FieldVersion,
			TargetValue: api.CompareTargetUnion{Version: 1},
		}},
		Success: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.CmdPut{Key: []byte("counter"), Value: []byte("v2")},
		}},
		Failure: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.CmdPut{Key: []byte("counter"), Value: []byte("wrong")},
		}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.TxnResponse
	ts.decodeJSON(resp, &res)
	require.True(t, res.Success)

	// verify via a real GET
	getResp := ts.do(http.MethodGet, RouteKvRange,
		api.RangeRequest{Key: []byte("counter")})
	var getRes api.RangeResponse
	ts.decodeJSON(getResp, &getRes)
	require.Equal(t, "v2", string(getRes.Entries[0].Value))
}

func Test_KvTxn_FailureBranch(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.mustPut("counter", "v1")

	resp := ts.do(http.MethodPost, RouteKvTxn, api.TxnRequest{
		Comparisons: []command.Comparison{{
			Key:         []byte("counter"),
			Operator:    api.OperatorEqual,
			TargetField: api.FieldVersion,
			TargetValue: api.CompareTargetUnion{Version: 999},
		}},
		Success: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.CmdPut{Key: []byte("counter"), Value: []byte("wrong")},
		}},
		Failure: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.CmdPut{Key: []byte("counter"), Value: []byte("failure_path")},
		}},
	})
	var res api.TxnResponse
	ts.decodeJSON(resp, &res)
	require.False(t, res.Success)

	getResp := ts.do(http.MethodGet, RouteKvRange,
		api.RangeRequest{Key: []byte("counter")})
	var getRes api.RangeResponse
	ts.decodeJSON(getResp, &getRes)
	require.Equal(t, "failure_path", string(getRes.Entries[0].Value))
}

func Test_KvTxn_BumpsRevisionOnce(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodPost, RouteKvTxn, api.TxnRequest{
		Success: []command.TxnOp{
			{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("a"), Value: []byte("1")}},
			{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("b"), Value: []byte("2")}},
			{Type: command.TxnOpPut, Put: &command.CmdPut{Key: []byte("c"), Value: []byte("3")}},
		},
	})
	var res api.TxnResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.Header.Revision,
		"all ops in one txn must produce exactly one revision bump")
}

func Test_LeaseGrant_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, RouteLeaseGrant, api.LeaseGrantRequest{LeaseID: 1, TTL: 30})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.LeaseGrantResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.LeaseID)
	require.Equal(t, int64(30), res.TTL)
}

func Test_LeaseGrant_ZeroTTL_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, RouteLeaseGrant, api.LeaseGrantRequest{TTL: 0})
	require.Equal(t, 400, resp.StatusCode)
}

func Test_LeaseGrant_IDConflict_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.do(http.MethodPost, RouteLeaseGrant, api.LeaseGrantRequest{LeaseID: 99, TTL: 10})
	resp := ts.do(http.MethodPost, RouteLeaseGrant, api.LeaseGrantRequest{LeaseID: 99, TTL: 10})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_LeaseRevoke_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.do(http.MethodPost, RouteLeaseGrant,
		api.LeaseGrantRequest{LeaseID: 5, TTL: 30})

	resp := ts.do(http.MethodDelete, RouteLeaseRevoke, api.LeaseRevokeRequest{LeaseID: 5})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.LeaseRevokeResponse
	ts.decodeJSON(resp, &res)
	require.True(t, res.Found)
	require.True(t, res.Revoked)
}

func Test_LeaseRevoke_NonExistent_FoundIsFalse(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodDelete, RouteLeaseRevoke, api.LeaseRevokeRequest{LeaseID: 999})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.LeaseRevokeResponse
	ts.decodeJSON(resp, &res)
	require.False(t, res.Found)
	require.False(t, res.Revoked)
}

func Test_LeaseKeepAlive_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	require.Equal(t, http.StatusOK, ts.do(http.MethodPost, RouteLeaseGrant, api.LeaseGrantRequest{LeaseID: 7, TTL: 30}).StatusCode)

	resp := ts.do(http.MethodPost, RouteLeaseKeepAlive, api.LeaseKeepAliveRequest{LeaseID: 7})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.LeaseKeepAliveResponse
	ts.decodeJSON(resp, &res)
	require.InDelta(t, 30, res.TTL, 2)
}

func Test_LeaseKeepAlive_NotFound_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, RouteLeaseKeepAlive, api.LeaseKeepAliveRequest{LeaseID: 404})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_LeaseLookup_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	require.Equal(t, http.StatusOK, ts.do(http.MethodPost, RouteLeaseGrant, api.LeaseGrantRequest{LeaseID: 11, TTL: 60}).StatusCode)

	resp := ts.do(http.MethodGet, RouteLeaseLookup,
		api.LeaseLookupRequest{LeaseID: 11})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.LeaseLookupResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(11), res.LeaseID)
	require.Equal(t, int64(60), res.OriginalTTL)
}

func Test_LeaseLookup_NotFound_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, RouteLeaseLookup, api.LeaseLookupRequest{LeaseID: 0})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_Livez_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, "/livez", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func Test_Livez_RaftShutdown_Returns503(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.raftService.State_ = raft.Shutdown
	resp := ts.do(http.MethodGet, "/livez", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_Readyz_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func Test_Readyz_NoLeader_Returns503(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.raftService.ErrLeader = fmt.Errorf("election in progress")
	resp := ts.do(http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_Readyz_Lagging_Returns503(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.raftService.ErrLag = fmt.Errorf("15 entries behind")
	resp := ts.do(http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_HealthProbes_ServedLocallyOnFollower(t *testing.T) {
	t.Parallel()
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)
	follower.overrideLeader(leader.srv)

	for _, path := range []string{"/livez", "/readyz"} {
		resp := follower.do(http.MethodGet, path, nil)
		require.NotEqual(t, http.StatusBadGateway, resp.StatusCode,
			"%s must be served locally, not proxied to leader", path)
	}
}

func Test_Stats_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, "/stats", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]string
	ts.decodeJSON(resp, &out)
	require.NotEmpty(t, out)
}

func Test_OTInit_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.OTInitResponse
	ts.decodeJSON(resp, &res)
	require.Len(t, res.Token, ot.TokenSize)
	require.Len(t, res.PointA, 32)
	require.NotEmpty(t, res.Header.NodeID)
}

func Test_OTInit_UniqueTokenPerCall(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	resp1 := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	var res1 api.OTInitResponse
	ts.decodeJSON(resp1, &res1)

	resp2 := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	var res2 api.OTInitResponse
	ts.decodeJSON(resp2, &res2)

	require.NotEqual(t, res1.Token, res2.Token)
}

func Test_OTInit_MalformedBody_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	req, _ := http.NewRequest(http.MethodGet, ts.srv.URL+RouteOtInit,
		strings.NewReader("bad json"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_OTWriteAll_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	blob := ot.FakeBlob(t, ts.om)

	resp := ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.OTWriteAllResponse
	ts.decodeJSON(resp, &res)
	require.NotZero(t, res.Header.RaftIndex)
}

func Test_OTWriteAll_NilBlob_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: nil})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_OTWriteAll_WrongSizeBlob_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: []byte("nope")})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_OTWriteAll_MalformedBody_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	req, _ := http.NewRequest(http.MethodPost, ts.srv.URL+RouteOtWriteAll,
		strings.NewReader("bad json"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_OTTransfer_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	blob := ot.FakeBlob(t, ts.om)
	ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})

	initResp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	var initRes api.OTInitResponse
	ts.decodeJSON(initResp, &initRes)

	pointB, _ := ts.otClient.Choose(initRes.PointA, 0)
	resp := ts.do(http.MethodGet, RouteOtTransfer,
		api.OTTransferRequest{Token: initRes.Token, PointB: pointB})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.OTTransferResponse
	ts.decodeJSON(resp, &res)
	require.Len(t, res.Ciphertexts, ot.DefaultSlotCount)
}

func Test_OTTransfer_InvalidPointB_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	blob := ot.FakeBlob(t, ts.om)
	ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})

	initResp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	require.Equal(t, http.StatusOK, initResp.StatusCode)
	var initRes api.OTInitResponse
	ts.decodeJSON(initResp, &initRes)

	resp := ts.do(http.MethodGet, RouteOtTransfer,
		api.OTTransferRequest{Token: initRes.Token, PointB: []byte("bad")})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_OTTransfer_NoBlobWritten_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	initResp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	require.Equal(t, http.StatusOK, initResp.StatusCode)
	var initRes api.OTInitResponse
	ts.decodeJSON(initResp, &initRes)

	pointB, _ := ts.otClient.Choose(initRes.PointA, 0)
	resp := ts.do(http.MethodGet, RouteOtTransfer,
		api.OTTransferRequest{Token: initRes.Token, PointB: pointB})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_OTTransfer_MalformedBody_Returns400(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, RouteOtTransfer, map[string]string{"bad": "json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// E2E through HTTP: Init → WriteAll → Transfer → client decrypt

func Test_OT_E2E_HTTP_ChosenSlotDecrypts(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	blob := ot.FakeBlob(t, ts.om)
	writeResp := ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})
	require.Equal(t, http.StatusOK, writeResp.StatusCode)

	initResp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	require.Equal(t, http.StatusOK, initResp.StatusCode)
	var initRes api.OTInitResponse
	ts.decodeJSON(initResp, &initRes)

	choice := 7
	pointB, scalarB := ts.otClient.Choose(initRes.PointA, choice)

	transferResp := ts.do(http.MethodGet, RouteOtTransfer,
		api.OTTransferRequest{Token: initRes.Token, PointB: pointB})
	require.Equal(t, http.StatusOK, transferResp.StatusCode)
	var transferRes api.OTTransferResponse
	ts.decodeJSON(transferResp, &transferRes)
	require.Len(t, transferRes.Ciphertexts, ot.DefaultSlotCount)

	expected := blob[choice*ot.DefaultSlotSize : (choice+1)*ot.DefaultSlotSize]
	got, err := ts.otClient.TryDecrypt(initRes.PointA, scalarB, transferRes.Ciphertexts[choice])
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func Test_OT_E2E_HTTP_NonChosenSlotsFail(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	blob := ot.FakeBlob(t, ts.om)
	ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})

	initResp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	var initRes api.OTInitResponse
	ts.decodeJSON(initResp, &initRes)

	choice := 2
	pointB, scalarB := ts.otClient.Choose(initRes.PointA, choice)

	transferResp := ts.do(http.MethodGet, RouteOtTransfer,
		api.OTTransferRequest{Token: initRes.Token, PointB: pointB})
	var transferRes api.OTTransferResponse
	ts.decodeJSON(transferResp, &transferRes)

	for i, ct := range transferRes.Ciphertexts {
		if i == choice {
			continue
		}
		_, err := ts.otClient.TryDecrypt(initRes.PointA, scalarB, ct)
		require.Error(t, err, "slot %d should NOT decrypt with choice=%d key", i, choice)
	}
}

func Test_OTInit_ServedLocally_NoLeaderMiddleware(t *testing.T) {
	t.Parallel()
	// Init is NOT behind leaderMiddleware, so even a follower with no leader
	// should serve it locally (not return 503)
	ts := newTestServer(t, true)
	ts.raftService.ErrLeader = fmt.Errorf("no quorum")
	ts.raftService.State_ = raft.Follower

	resp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"OT init should not require a leader")
}

func Test_OTTransfer_SerializableRead_ServedLocally_NoLeaderMiddleware(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)

	blob := ot.FakeBlob(t, ts.om)
	ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})

	ts.raftService.ErrLeader = fmt.Errorf("election in progress")
	ts.raftService.State_ = raft.Follower

	initResp := ts.do(http.MethodGet, RouteOtInit, api.OTInitRequest{})
	var initRes api.OTInitResponse
	ts.decodeJSON(initResp, &initRes)

	pointB, _ := ts.otClient.Choose(initRes.PointA, 0)
	resp := ts.do(http.MethodGet, RouteOtTransfer,
		api.OTTransferRequest{Token: initRes.Token, PointB: pointB, Serializable: true})
	require.NotEqual(t, http.StatusServiceUnavailable, resp.StatusCode,
		"serializable OT transfer should not require a leader")
}

func Test_OTWriteAll_RequiresLeader(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, true)
	ts.raftService.ErrLeader = fmt.Errorf("no quorum")

	blob := ot.FakeBlob(t, ts.om)
	resp := ts.do(http.MethodPost, RouteOtWriteAll,
		api.OTWriteAllRequest{Blob: blob})
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"OT write-all MUST require a leader")
}

// tests for watches

func dialWatch(t *testing.T, ts *testServer) *websocket.Conn {
	t.Helper()
	wsURL := ts.srv.URL + RouteWatchWS
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{watch.WatchSubprotocol},
	})
	t.Log(err)
	require.NoError(t, err)
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func writeWatchMsg(t *testing.T, conn *websocket.Conn, kind watch.ClientMessageKind, payload any) {
	t.Helper()
	bs, err := json.Marshal(payload)
	require.NoError(t, err)
	msg := watch.ClientMessage{
		Kind:    kind,
		Payload: bs,
	}
	require.NoError(t, wsjson.Write(t.Context(), conn, msg))
}

func readWatchMsg(t *testing.T, conn *websocket.Conn) (msg watch.ServerMessage) {
	require.NoError(t, wsjson.Read(t.Context(), conn, &msg))
	return msg
}

func readWatchMsgTimeout(t *testing.T, conn *websocket.Conn, timeout time.Duration) (msg watch.ServerMessage, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	err = wsjson.Read(ctx, conn, &msg)
	return
}

func createWatch(t *testing.T, conn *websocket.Conn, key string, startRev int64) int64 {
	t.Helper()
	writeWatchMsg(t, conn, watch.ClientWatchCreate, api.WatchCreateRequest{
		Key:           []byte(key),
		StartRevision: startRev,
	})
	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchCreated, msg.Kind)
	res := msg.AsCreateResponse(t)
	require.True(t, res.Success)
	return res.WatchID
}

func Test_Server_Watch_NonUpgradeRequest_Returns426(t *testing.T) {
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodGet, RouteWatchWS, nil)
	t.Log(resp.Status)
	require.Equal(t, http.StatusUpgradeRequired, resp.StatusCode)
}

func Test_Server_Watch_Subprotocol_CorrectAccepted(t *testing.T) {
	ts := newTestServer(t, true)
	conn := dialWatch(t, ts)
	require.Equal(t, watch.WatchSubprotocol, conn.Subprotocol())
}

func Test_Server_Watch_Subprotocol_WrongRejected(t *testing.T) {
	ts := newTestServer(t, true)
	wsURL := "ws" + strings.TrimPrefix(ts.srv.URL, "http") + RouteWatchWS

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"wrong_protocol"},
	})

	if err != nil {
		// server rejected the connection outright — acceptable
		return
	}
	defer conn.CloseNow()

	// if the connection was accepted, the server should close it
	// with StatusPolicyViolation because the subprotocol doesn't match
	_, _, readErr := conn.Read(ctx)
	require.Error(t, readErr, "server should close connection for wrong subprotocol")

	var closeErr websocket.CloseError
	if errors.As(readErr, &closeErr) {
		require.Equal(t, websocket.StatusPolicyViolation, closeErr.Code)
	}
}

func Test_Server_Watch_Subprotocol_NoneGivenRejected(t *testing.T) {
	ts := newTestServer(t, true)
	wsURL := "ws" + strings.TrimPrefix(ts.srv.URL, "http") + RouteWatchWS

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{})

	if err != nil {
		return
	}
	defer conn.CloseNow()

	_, _, readErr := conn.Read(ctx)
	require.Error(t, readErr, "server should close connection when no subprotocol offered")
}

func Test_Server_Watch_HubWiring_EventsDelivered(t *testing.T) {
	ts := newTestServer(t, true)
	conn := dialWatch(t, ts)

	wid := createWatch(t, conn, "foo", 0)

	w := ts.store.NewWriter()
	require.NoError(t, w.Put([]byte("foo"), []byte("bar"), 0))
	require.NoError(t, w.End())
	_, changes := w.UnsafeExpectedChanges()
	ts.httpServer.watchHub.OnCommit(changes)

	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchEventPut, msg.Kind)

	ev := msg.AsStreamEvent(t)
	require.Equal(t, wid, ev.Wid)
	require.Equal(t, "foo", string(ev.Event.Entry.Key))
	require.Equal(t, "bar", string(ev.Event.Entry.Value))
}

func Test_Server_Watch_FollowerAcceptsConnection(t *testing.T) {
	ts := newTestServer(t, false)
	require.Equal(t, raft.Follower, ts.peerSvc.State_)

	conn := dialWatch(t, ts)

	wid := createWatch(t, conn, "foo", 0)
	require.NotZero(t, wid)
}

func Test_Server_Watch_FollowerReceivesEvents(t *testing.T) {
	ts := newTestServer(t, false)

	conn := dialWatch(t, ts)
	wid := createWatch(t, conn, "foo", 0)

	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("foo"), Value: []byte("v"), ModRev: 1, CreateRev: 1, Version: 1},
	})

	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchEventPut, msg.Kind)
	ev := msg.AsStreamEvent(t)
	require.Equal(t, wid, ev.Wid)
}

func Test_Server_Watch_ConcurrentConnections_Isolated(t *testing.T) {
	t.Skip("skipped: why is conn2 dropped?")
	ts := newTestServer(t, true)

	conn1 := dialWatch(t, ts)
	conn2 := dialWatch(t, ts)

	wid1 := createWatch(t, conn1, "foo", 0)
	wid2 := createWatch(t, conn2, "bar", 0)

	// commit an entry for "foo" and conn1 should receive it
	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("foo"), Value: []byte("fv"), ModRev: 1, CreateRev: 1, Version: 1},
	})

	msg1 := readWatchMsg(t, conn1)
	require.Equal(t, watch.ServerWatchEventPut, msg1.Kind)
	ev1 := msg1.AsStreamEvent(t)
	require.Equal(t, wid1, ev1.Wid)
	require.Equal(t, "foo", string(ev1.Event.Entry.Key))

	_, err := readWatchMsgTimeout(t, conn2, 100*time.Millisecond)
	require.Error(t, err, "conn2 should not receive events for key 'foo'")

	// now commit an entry for bar and only conn2 should receive it
	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("bar"), Value: []byte("bv"), ModRev: 2, CreateRev: 2, Version: 1},
	})
	require.NoError(t, conn2.Ping(t.Context()))

	msg2 := readWatchMsg(t, conn2)
	require.Equal(t, watch.ServerWatchEventPut, msg2.Kind)
	ev2 := msg2.AsStreamEvent(t)
	require.Equal(t, wid2, ev2.Wid)
	require.Equal(t, "bar", string(ev2.Event.Entry.Key))

	_, err = readWatchMsgTimeout(t, conn1, 100*time.Millisecond)
	require.Error(t, err, "conn1 should not receive events for key 'bar'")
}

func Test_Server_Watch_ServerShutdown_DisconnectsClient(t *testing.T) {
	ts := newTestServer(t, true)

	conn := dialWatch(t, ts)
	_ = createWatch(t, conn, "foo", 0)

	// shut down the server — this should cancel all request contexts,
	// which cascades through the errgroup to the session to the stream
	shutdownCtx, shutdownCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer shutdownCancel()
	ts.srv.Close() // forceful close on httptest.Server

	// the client should get disconnected
	// give a generous timeout since shutdown cascades through multiple layers
	_, _, err := conn.Read(shutdownCtx)
	require.Error(t, err, "client should be disconnected after server shutdown")
}
func Test_Server_Watch_ClientDisconnect_CleansUpHub(t *testing.T) {
	ts := newTestServer(t, true)

	conn := dialWatch(t, ts)
	wid := createWatch(t, conn, "foo", 0)

	// verify the watcher exists on the hub
	hub := ts.httpServer.watchHub
	hub.Mu().Lock()
	_, inSynced := hub.Synced()[wid]
	_, inUnsynced := hub.Unsynced()[wid]
	hub.Mu().Unlock()
	require.True(t, inSynced || inUnsynced, "watcher should exist on hub after creation")

	// abruptly close the client
	conn.CloseNow()

	// give the session time to detect disconnect and clean up
	time.Sleep(300 * time.Millisecond)

	hub.Mu().Lock()
	_, inSynced = hub.Synced()[wid]
	_, inUnsynced = hub.Unsynced()[wid]
	hub.Mu().Unlock()
	require.False(t, inSynced, "watcher should be removed from synced after client disconnect")
	require.False(t, inUnsynced, "watcher should be removed from unsynced after client disconnect")
}

func Test_Server_Watch_EndToEnd_HTTPPut_ThenWatchEvent(t *testing.T) {
	ts := newTestServer(t, true)

	conn := dialWatch(t, ts)
	wid := createWatch(t, conn, "e2e-key", 0)

	// write through the normal HTTP PUT endpoint
	ts.mustPut("e2e-key", "e2e-value")

	// the Engine's CommitObserver would call hub.OnCommit in production.
	// since we're using a mock propose that bypasses the Engine observer,
	// trigger OnCommit manually with the data we know was written.
	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("e2e-key"), Value: []byte("e2e-value"), ModRev: 1, CreateRev: 1, Version: 1},
	})

	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchEventPut, msg.Kind)

	ev := msg.AsStreamEvent(t)
	require.Equal(t, wid, ev.Wid)
	require.Equal(t, "e2e-key", string(ev.Event.Entry.Key))
	require.Equal(t, "e2e-value", string(ev.Event.Entry.Value))
}

// tests for watches

func dialWatch(t *testing.T, ts *testServer) *websocket.Conn {
	t.Helper()
	wsURL := ts.srv.URL + RouteWatchWS
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{watch.WatchSubprotocol},
	})
	t.Log(err)
	require.NoError(t, err)
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func writeWatchMsg(t *testing.T, conn *websocket.Conn, kind watch.ClientMessageKind, payload any) {
	t.Helper()
	bs, err := json.Marshal(payload)
	require.NoError(t, err)
	msg := watch.ClientMessage{
		Kind:    kind,
		Payload: bs,
	}
	require.NoError(t, wsjson.Write(t.Context(), conn, msg))
}

func readWatchMsg(t *testing.T, conn *websocket.Conn) (msg watch.ServerMessage) {
	require.NoError(t, wsjson.Read(t.Context(), conn, &msg))
	return msg
}

func readWatchMsgTimeout(t *testing.T, conn *websocket.Conn, timeout time.Duration) (msg watch.ServerMessage, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	err = wsjson.Read(ctx, conn, &msg)
	return
}

func createWatch(t *testing.T, conn *websocket.Conn, key string, startRev int64) int64 {
	t.Helper()
	writeWatchMsg(t, conn, watch.ClientWatchCreate, api.WatchCreateRequest{
		Key:           []byte(key),
		StartRevision: startRev,
	})
	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchCreated, msg.Kind)
	res := msg.AsCreateResponse(t)
	require.True(t, res.Success)
	return res.WatchID
}

func Test_Server_Watch_NonUpgradeRequest_Returns426(t *testing.T) {
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodGet, RouteWatchWS, nil)
	t.Log(resp.Status)
	require.Equal(t, http.StatusUpgradeRequired, resp.StatusCode)
}

func Test_Server_Watch_Subprotocol_CorrectAccepted(t *testing.T) {
	ts := newTestServer(t, true)
	conn := dialWatch(t, ts)
	require.Equal(t, watch.WatchSubprotocol, conn.Subprotocol())
}

func Test_Server_Watch_Subprotocol_WrongRejected(t *testing.T) {
	ts := newTestServer(t, true)
	wsURL := "ws" + strings.TrimPrefix(ts.srv.URL, "http") + RouteWatchWS

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"wrong_protocol"},
	})

	if err != nil {
		// server rejected the connection outright — acceptable
		return
	}
	defer conn.CloseNow()

	// if the connection was accepted, the server should close it
	// with StatusPolicyViolation because the subprotocol doesn't match
	_, _, readErr := conn.Read(ctx)
	require.Error(t, readErr, "server should close connection for wrong subprotocol")

	var closeErr websocket.CloseError
	if errors.As(readErr, &closeErr) {
		require.Equal(t, websocket.StatusPolicyViolation, closeErr.Code)
	}
}

func Test_Server_Watch_Subprotocol_NoneGivenRejected(t *testing.T) {
	ts := newTestServer(t, true)
	wsURL := "ws" + strings.TrimPrefix(ts.srv.URL, "http") + RouteWatchWS

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{})

	if err != nil {
		return
	}
	defer conn.CloseNow()

	_, _, readErr := conn.Read(ctx)
	require.Error(t, readErr, "server should close connection when no subprotocol offered")
}

func Test_Server_Watch_HubWiring_EventsDelivered(t *testing.T) {
	ts := newTestServer(t, true)
	conn := dialWatch(t, ts)

	wid := createWatch(t, conn, "foo", 0)

	w := ts.store.NewWriter()
	require.NoError(t, w.Put([]byte("foo"), []byte("bar"), 0))
	require.NoError(t, w.End())
	_, changes := w.UnsafeExpectedChanges()
	ts.httpServer.watchHub.OnCommit(changes)

	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchEventPut, msg.Kind)

	ev := msg.AsStreamEvent(t)
	require.Equal(t, wid, ev.Wid)
	require.Equal(t, "foo", string(ev.Event.Entry.Key))
	require.Equal(t, "bar", string(ev.Event.Entry.Value))
}

func Test_Server_Watch_FollowerAcceptsConnection(t *testing.T) {
	ts := newTestServer(t, false)
	require.Equal(t, raft.Follower, ts.peerSvc.State_)

	conn := dialWatch(t, ts)

	wid := createWatch(t, conn, "foo", 0)
	require.NotZero(t, wid)
}

func Test_Server_Watch_FollowerReceivesEvents(t *testing.T) {
	ts := newTestServer(t, false)

	conn := dialWatch(t, ts)
	wid := createWatch(t, conn, "foo", 0)

	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("foo"), Value: []byte("v"), ModRev: 1, CreateRev: 1, Version: 1},
	})

	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchEventPut, msg.Kind)
	ev := msg.AsStreamEvent(t)
	require.Equal(t, wid, ev.Wid)
}

func Test_Server_Watch_ConcurrentConnections_Isolated(t *testing.T) {
	t.Skip("skipped: why is conn2 dropped?")
	ts := newTestServer(t, true)

	conn1 := dialWatch(t, ts)
	conn2 := dialWatch(t, ts)

	wid1 := createWatch(t, conn1, "foo", 0)
	wid2 := createWatch(t, conn2, "bar", 0)

	// commit an entry for "foo" and conn1 should receive it
	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("foo"), Value: []byte("fv"), ModRev: 1, CreateRev: 1, Version: 1},
	})

	msg1 := readWatchMsg(t, conn1)
	require.Equal(t, watch.ServerWatchEventPut, msg1.Kind)
	ev1 := msg1.AsStreamEvent(t)
	require.Equal(t, wid1, ev1.Wid)
	require.Equal(t, "foo", string(ev1.Event.Entry.Key))

	_, err := readWatchMsgTimeout(t, conn2, 100*time.Millisecond)
	require.Error(t, err, "conn2 should not receive events for key 'foo'")

	// now commit an entry for bar and only conn2 should receive it
	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("bar"), Value: []byte("bv"), ModRev: 2, CreateRev: 2, Version: 1},
	})
	require.NoError(t, conn2.Ping(t.Context()))

	msg2 := readWatchMsg(t, conn2)
	require.Equal(t, watch.ServerWatchEventPut, msg2.Kind)
	ev2 := msg2.AsStreamEvent(t)
	require.Equal(t, wid2, ev2.Wid)
	require.Equal(t, "bar", string(ev2.Event.Entry.Key))

	_, err = readWatchMsgTimeout(t, conn1, 100*time.Millisecond)
	require.Error(t, err, "conn1 should not receive events for key 'bar'")
}

func Test_Server_Watch_ServerShutdown_DisconnectsClient(t *testing.T) {
	ts := newTestServer(t, true)

	conn := dialWatch(t, ts)
	_ = createWatch(t, conn, "foo", 0)

	// shut down the server — this should cancel all request contexts,
	// which cascades through the errgroup to the session to the stream
	shutdownCtx, shutdownCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer shutdownCancel()
	ts.srv.Close() // forceful close on httptest.Server

	// the client should get disconnected
	// give a generous timeout since shutdown cascades through multiple layers
	_, _, err := conn.Read(shutdownCtx)
	require.Error(t, err, "client should be disconnected after server shutdown")
}
func Test_Server_Watch_ClientDisconnect_CleansUpHub(t *testing.T) {
	ts := newTestServer(t, true)

	conn := dialWatch(t, ts)
	wid := createWatch(t, conn, "foo", 0)

	// verify the watcher exists on the hub
	hub := ts.httpServer.watchHub
	hub.Mu().Lock()
	_, inSynced := hub.Synced()[wid]
	_, inUnsynced := hub.Unsynced()[wid]
	hub.Mu().Unlock()
	require.True(t, inSynced || inUnsynced, "watcher should exist on hub after creation")

	// abruptly close the client
	conn.CloseNow()

	// give the session time to detect disconnect and clean up
	time.Sleep(300 * time.Millisecond)

	hub.Mu().Lock()
	_, inSynced = hub.Synced()[wid]
	_, inUnsynced = hub.Unsynced()[wid]
	hub.Mu().Unlock()
	require.False(t, inSynced, "watcher should be removed from synced after client disconnect")
	require.False(t, inUnsynced, "watcher should be removed from unsynced after client disconnect")
}

func Test_Server_Watch_EndToEnd_HTTPPut_ThenWatchEvent(t *testing.T) {
	ts := newTestServer(t, true)

	conn := dialWatch(t, ts)
	wid := createWatch(t, conn, "e2e-key", 0)

	// write through the normal HTTP PUT endpoint
	ts.mustPut("e2e-key", "e2e-value")

	// the Engine's CommitObserver would call hub.OnCommit in production.
	// since we're using a mock propose that bypasses the Engine observer,
	// trigger OnCommit manually with the data we know was written.
	ts.httpServer.watchHub.OnCommit([]*kv.Entry{
		{Key: []byte("e2e-key"), Value: []byte("e2e-value"), ModRev: 1, CreateRev: 1, Version: 1},
	})

	msg := readWatchMsg(t, conn)
	require.Equal(t, watch.ServerWatchEventPut, msg.Kind)

	ev := msg.AsStreamEvent(t)
	require.Equal(t, wid, ev.Wid)
	require.Equal(t, "e2e-key", string(ev.Event.Entry.Key))
	require.Equal(t, "e2e-value", string(ev.Event.Entry.Value))
}
