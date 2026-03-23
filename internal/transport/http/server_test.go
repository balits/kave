package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/config"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/transport"
	"github.com/balits/kave/internal/types/api"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	t          *testing.T
	srv        *httptest.Server
	httpServer *HttpServer

	store   *mvcc.KVStore
	lm      *lease.LeaseManager
	peerSvc *mockPeerSvc
}

type mockPeerSvc struct {
	me        config.Peer
	leader    config.Peer
	leaderErr error
	state     raft.RaftState
	lagErr    error
}

func (p *mockPeerSvc) Me() config.Peer                      { return p.me }
func (p *mockPeerSvc) State() raft.RaftState                { return p.state }
func (p *mockPeerSvc) GetPeers() map[string]config.Peer     { return nil }
func (p *mockPeerSvc) GetLeader() (config.Peer, error)      { return p.leader, p.leaderErr }
func (p *mockPeerSvc) VerifyLeader(_ context.Context) error { return nil }
func (p *mockPeerSvc) LaggingBehind() error                 { return p.lagErr }

type mockClusterSvc struct{}

func (n *mockClusterSvc) Bootstrap(_ context.Context) error                             { return nil }
func (n *mockClusterSvc) JoinCluster(_ context.Context, _ map[string]config.Peer) error { return nil }
func (n *mockClusterSvc) AddToCluster(_ context.Context, _ transport.JoinRequest) error { return nil }
func (n *mockClusterSvc) Stats() (map[string]string, error) {
	return map[string]string{"state": "Leader"}, nil
}

func newTestServer(t *testing.T, isLeaderValue bool) *testServer {
	t.Helper()
	me := config.Peer{
		NodeID:   "test",
		Hostname: "0.0.0.0",
		HttpPort: "8000",
		RaftPort: "7000",
	}
	state := raft.Leader
	if !isLeaderValue {
		state = raft.Follower
	}
	peerSvc := &mockPeerSvc{me: me, leader: me, state: state}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, storage.StorageOptions{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	kvstore := mvcc.NewKVStore(reg, logger, backend)
	lm := lease.NewManager(reg, logger, kvstore, backend)
	t.Cleanup(func() { backend.Close() })
	fsm := fsm.New(logger, kvstore, lm, me.NodeID)

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

	kvSvc := service.NewKVService(logger, kvstore, peerSvc, propose)
	leaseSvc := service.NewLeaseService(logger, propose)
	clusterSvc := &mockClusterSvc{}

	httpServer := NewHTTPServer(logger, me.GetHttpAdvertisedAddress(), kvSvc, leaseSvc, clusterSvc, peerSvc, reg)

	ts := httptest.NewServer(httpServer.server.Handler)
	t.Cleanup(ts.Close)

	return &testServer{
		t:          t,
		srv:        ts,
		httpServer: httpServer,
		store:      kvstore,
		lm:         lm,
		peerSvc:    peerSvc,
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

func (ts *testServer) encodeJSON(resp *http.Response, dst any) {
	ts.t.Helper()
	require.NoError(ts.t, json.NewDecoder(resp.Body).Decode(dst))
}

func (ts *testServer) mustPut(key, value string) {
	ts.t.Helper()
	resp := ts.do(http.MethodPost, transport.UriKv+"/put", api.PutRequest{
		Key:   []byte(key),
		Value: []byte(value),
	})
	require.Equal(ts.t, http.StatusOK, resp.StatusCode)
}

func (ts *testServer) overrideLeader(leader *httptest.Server) {
	ts.t.Helper()
	url, err := url.Parse(leader.URL)
	require.NoError(ts.t, err)

	ts.peerSvc.state = raft.Follower
	ts.peerSvc.leader = config.Peer{
		NodeID:   url.Host,
		Hostname: url.Hostname(),
		HttpPort: url.Port(),
		RaftPort: "7000",
	}

}

func Test_LeaderMiddleware_UnknownLeader_Returns503(t *testing.T) {
	fix := newTestServer(t, true)
	fix.peerSvc.leaderErr = fmt.Errorf("no quorum")

	resp := fix.do(http.MethodPost, transport.UriKv+"/put",
		api.PutRequest{Key: []byte("k"), Value: []byte("v")})
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_LeaderMiddleware_ProxiesToLeader_AsFollower(t *testing.T) {
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)
	follower.overrideLeader(leader.srv)

	key := []byte("proxy")
	val := []byte("bar")
	resp := follower.do(http.MethodPost, transport.UriKv+"/put", api.PutRequest{
		Key:   key,
		Value: val,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	kv := leader.store.NewReader().Get(key, 0)
	require.NotNil(t, kv, "expected followers put request to be reflected in leaders store")
	require.Equal(t, key, kv.Key)
	require.Equal(t, val, kv.Value)
}

func Test_LeaderMiddleware_LeaderUnreachable_AsFollower_Returns502(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	dead.Close()

	follower := newTestServer(t, true)
	follower.overrideLeader(dead)

	resp := follower.do(http.MethodPost, transport.UriKv+"/put", api.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v")},
	)
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
}

func Test_LeaderMiddleware_OriginalBodyReachesLeader_AsFollower(t *testing.T) {
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)
	follower.overrideLeader(leader.srv)

	key := "foo"
	val := "bar"
	payload := api.PutRequest{
		Key:   []byte(key),
		Value: []byte(val),
	}

	resp := follower.do(http.MethodPost, transport.UriKv+"/put", payload)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	entry := leader.store.NewReader().Get([]byte(key), 0)
	require.NotNil(t, entry)
	require.Equal(t, val, string(entry.Value))
}

func Test_KvPut_CreatesKey(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, transport.UriKv+"/put",
		api.PutRequest{Key: []byte("hello"), Value: []byte("world")})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.PutResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.Header.Revision)
}

func Test_KvPut_BumpsRevisionAfterWrites(t *testing.T) {
	ts := newTestServer(t, true)
	for i := range 5 {
		resp := ts.do(http.MethodPost, transport.UriKv+"/put", api.PutRequest{
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
	ts := newTestServer(t, true)
	key := "k"
	oldVal := "v1"
	ts.mustPut(key, oldVal)

	resp := ts.do(http.MethodPost, transport.UriKv+"/put", api.PutRequest{
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
	ts := newTestServer(t, true)
	ts.mustPut("k", "v1")
	ts.mustPut("k", "v2")

	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
		Key: []byte("k"),
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.Entries[0].CreateRev)
	require.Equal(t, int64(2), res.Entries[0].ModRev)
	require.Equal(t, int64(2), res.Entries[0].Version)
}

func Test_KvPut_MalformedBody_Returns400(t *testing.T) {
	ts := newTestServer(t, true)
	req, _ := http.NewRequest(http.MethodPost, ts.srv.URL+transport.UriKv+"/put",
		strings.NewReader("bad json"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_KvGet_ExistingKey(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("foo", "bar")

	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
		Key: []byte("foo"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 1, res.Count)
	require.Equal(t, "bar", string(res.Entries[0].Value))
}

func Test_KvGet_MissingKey_ReturnsEmpty(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
		Key: []byte("missing"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Zero(t, res.Count)
	require.Empty(t, res.Entries)
}

func Test_KvGet_AtOldRevision(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("k", "v1")
	ts.mustPut("k", "v2")

	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
		Key:      []byte("k"),
		Revision: 1,
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, "v1", string(res.Entries[0].Value))
}

func Test_KvGet_RangeQuery(t *testing.T) {
	ts := newTestServer(t, true)
	for _, k := range []string{"a", "b", "c", "d"} {
		ts.mustPut(k, k)
	}
	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
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
	ts := newTestServer(t, true)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		ts.mustPut(k, k)
	}
	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
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
	ts := newTestServer(t, true)
	ts.mustPut("x", "1")
	ts.mustPut("y", "2")

	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
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
	ts := newTestServer(t, true)
	ts.mustPut("app", "1")
	ts.mustPut("apple", "2")
	ts.mustPut("application", "3")
	ts.mustPut("banana", "4")

	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
		Key:    []byte("app"),
		Prefix: true,
	})
	var res api.RangeResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, 3, res.Count)
}

func Test_KvDelete_RemovesKey(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("bye", "gone")

	resp := ts.do(http.MethodDelete, transport.UriKv+"/delete", api.DeleteRequest{
		Key: []byte("bye"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.NumDeleted)

	getResp := ts.do(http.MethodGet, transport.UriKv+"/get",
		api.RangeRequest{Key: []byte("bye")})
	var getRes api.RangeResponse
	ts.decodeJSON(getResp, &getRes)
	require.Zero(t, getRes.Count)
}

func Test_KvDelete_NonExistentKey_ReturnsZero(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodDelete, transport.UriKv+"/delete", api.DeleteRequest{
		Key: []byte("ghost"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Zero(t, res.NumDeleted)
}

func Test_KvDelete_Range(t *testing.T) {
	ts := newTestServer(t, true)
	for _, k := range []string{"a", "b", "c", "d"} {
		ts.mustPut(k, k)
	}
	resp := ts.do(http.MethodDelete, transport.UriKv+"/delete", api.DeleteRequest{
		Key: []byte("b"),
		End: []byte("d"),
	})
	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(2), res.NumDeleted)
}

func Test_KvDelete_WithPrevEntries(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("p", "pval")

	resp := ts.do(http.MethodDelete, transport.UriKv+"/delete", api.DeleteRequest{
		Key:         []byte("p"),
		PrevEntries: true,
	})
	var res api.DeleteResponse
	ts.decodeJSON(resp, &res)
	require.Len(t, res.PrevEntries, 1)
	require.Equal(t, "pval", string(res.PrevEntries[0].Value))
}

func Test_KvDelete_ThenGetAtOldRevision(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("k", "v")
	ts.do(http.MethodDelete, transport.UriKv+"/delete", api.DeleteRequest{Key: []byte("k")})

	// key is gone at current rev
	resp := ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{Key: []byte("k")})
	var cur api.RangeResponse
	ts.decodeJSON(resp, &cur)
	require.Zero(t, cur.Count)

	// but readable at rev 1
	resp = ts.do(http.MethodGet, transport.UriKv+"/get", api.RangeRequest{
		Key:      []byte("k"),
		Revision: 1,
	})
	var old api.RangeResponse
	ts.decodeJSON(resp, &old)
	require.Equal(t, 1, old.Count)
	require.Equal(t, "v", string(old.Entries[0].Value))
}

func Test_KvTxn_SuccessBranch(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("counter", "v1")

	resp := ts.do(http.MethodPost, transport.UriKv+"/txn", api.TxnRequest{
		Comparisons: []command.Comparison{{
			Key:         []byte("counter"),
			Operator:    api.OperatorEqual,
			TargetField: api.FieldVersion,
			TargetValue: api.CompareTargetUnion{Version: 1},
		}},
		Success: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.PutCmd{Key: []byte("counter"), Value: []byte("v2")},
		}},
		Failure: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.PutCmd{Key: []byte("counter"), Value: []byte("wrong")},
		}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.TxnResponse
	ts.decodeJSON(resp, &res)
	require.True(t, res.Success)

	// verify via a real GET
	getResp := ts.do(http.MethodGet, transport.UriKv+"/get",
		api.RangeRequest{Key: []byte("counter")})
	var getRes api.RangeResponse
	ts.decodeJSON(getResp, &getRes)
	require.Equal(t, "v2", string(getRes.Entries[0].Value))
}

func Test_KvTxn_FailureBranch(t *testing.T) {
	ts := newTestServer(t, true)
	ts.mustPut("counter", "v1")

	resp := ts.do(http.MethodPost, transport.UriKv+"/txn", api.TxnRequest{
		Comparisons: []command.Comparison{{
			Key:         []byte("counter"),
			Operator:    api.OperatorEqual,
			TargetField: api.FieldVersion,
			TargetValue: api.CompareTargetUnion{Version: 999},
		}},
		Success: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.PutCmd{Key: []byte("counter"), Value: []byte("wrong")},
		}},
		Failure: []command.TxnOp{{
			Type: command.TxnOpPut,
			Put:  &command.PutCmd{Key: []byte("counter"), Value: []byte("failure_path")},
		}},
	})
	var res api.TxnResponse
	ts.decodeJSON(resp, &res)
	require.False(t, res.Success)

	getResp := ts.do(http.MethodGet, transport.UriKv+"/get",
		api.RangeRequest{Key: []byte("counter")})
	var getRes api.RangeResponse
	ts.decodeJSON(getResp, &getRes)
	require.Equal(t, "failure_path", string(getRes.Entries[0].Value))
}

func Test_KvTxn_BumpsRevisionOnce(t *testing.T) {
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodPost, transport.UriKv+"/txn", api.TxnRequest{
		Success: []command.TxnOp{
			{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("a"), Value: []byte("1")}},
			{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("b"), Value: []byte("2")}},
			{Type: command.TxnOpPut, Put: &command.PutCmd{Key: []byte("c"), Value: []byte("3")}},
		},
	})
	var res api.TxnResponse
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(1), res.Header.Revision,
		"all ops in one txn must produce exactly one revision bump")
}

func Test_LeaseGrant_OK(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{LeaseID: 1, TTL: 30})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res command.Result
	ts.decodeJSON(resp, &res)
	require.NotNil(t, res.LeaseGrant)
	require.Equal(t, int64(1), res.LeaseGrant.LeaseID)
	require.Equal(t, int64(30), res.LeaseGrant.TTL)
}

func Test_LeaseGrant_ZeroTTL_Returns500(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{TTL: 0})
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func Test_LeaseGrant_IDConflict_Returns500(t *testing.T) {
	ts := newTestServer(t, true)
	ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{LeaseID: 99, TTL: 10})
	resp := ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{LeaseID: 99, TTL: 10})
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func Test_LeaseRevoke_OK(t *testing.T) {
	ts := newTestServer(t, true)
	ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{LeaseID: 5, TTL: 30})

	resp := ts.do(http.MethodDelete, transport.UriLease+"/revoke",
		api.LeaseRevokeRequest{LeaseID: 5})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res command.Result
	ts.decodeJSON(resp, &res)
	require.True(t, res.LeaseRevoke.Found)
	require.True(t, res.LeaseRevoke.Revoked)
}

func Test_LeaseRevoke_NonExistent_FoundIsFalse(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodDelete, transport.UriLease+"/revoke",
		api.LeaseRevokeRequest{LeaseID: 999})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res command.Result
	ts.decodeJSON(resp, &res)
	require.False(t, res.LeaseRevoke.Found)
	require.False(t, res.LeaseRevoke.Revoked)
}

func Test_LeaseKeepAlive_OK(t *testing.T) {
	ts := newTestServer(t, true)
	ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{LeaseID: 7, TTL: 30})

	resp := ts.do(http.MethodPost, transport.UriLease+"/keep-alive",
		api.LeaseKeepAliveRequest{LeaseID: 7})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res command.Result
	ts.decodeJSON(resp, &res)
	require.InDelta(t, 30, res.LeaseKeepAlive.TTL, 2)
}

func Test_LeaseKeepAlive_NotFound_Returns500(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, transport.UriLease+"/keep-alive",
		api.LeaseKeepAliveRequest{LeaseID: 404})
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func Test_LeaseLookup_OK(t *testing.T) {
	ts := newTestServer(t, true)
	ts.do(http.MethodPost, transport.UriLease+"/grant",
		api.LeaseGrantRequest{LeaseID: 11, TTL: 60})

	resp := ts.do(http.MethodPost, transport.UriLease+"/lookup",
		api.LeaseLookupRequest{LeaseID: 11})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res command.Result
	ts.decodeJSON(resp, &res)
	require.Equal(t, int64(11), res.LeaseLookup.LeaseID)
	require.Equal(t, int64(60), res.LeaseLookup.OriginalTTL)
}

func Test_LeaseLookup_NotFound_Returns500(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodPost, transport.UriLease+"/lookup",
		api.LeaseLookupRequest{LeaseID: 0})
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func Test_Livez_OK(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, "/livez", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func Test_Livez_RaftShutdown_Returns503(t *testing.T) {
	ts := newTestServer(t, true)
	ts.peerSvc.state = raft.Shutdown
	resp := ts.do(http.MethodGet, "/livez", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_Readyz_OK(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func Test_Readyz_NoLeader_Returns503(t *testing.T) {
	ts := newTestServer(t, true)
	ts.peerSvc.leaderErr = fmt.Errorf("election in progress")
	resp := ts.do(http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_Readyz_Lagging_Returns503(t *testing.T) {
	ts := newTestServer(t, true)
	ts.peerSvc.lagErr = fmt.Errorf("15 entries behind")
	resp := ts.do(http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_HealthProbes_ServedLocallyOnFollower(t *testing.T) {
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)
	follower.overrideLeader(leader.srv)

	for _, path := range []string{"/livez", "/readyz"} {
		resp := follower.do(http.MethodGet, path, nil)
		require.NotEqual(t, http.StatusBadGateway, resp.StatusCode,
			"%s must be served locally, not proxied to leader", path)
	}
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func Test_Stats_OK(t *testing.T) {
	ts := newTestServer(t, true)
	resp := ts.do(http.MethodGet, "/stats", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]string
	ts.decodeJSON(resp, &out)
	require.NotEmpty(t, out)
}
