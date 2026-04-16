package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/balits/kave/internal/types/api"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func Test_WriteMiddleware_UnknownLeader_Returns503(t *testing.T) {
	fix := newTestServer(t, true)
	fix.raftService.ErrLeader = fmt.Errorf("no quorum")

	resp := fix.do(http.MethodPost, RouteKvPut,
		api.PutRequest{Key: []byte("k"), Value: []byte("v")})
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_WriteMiddleware_ProxiesToLeader_AsFollower(t *testing.T) {
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)
	follower.overrideLeader(leader.srv)

	key := []byte("proxy")
	val := []byte("bar")
	resp := follower.do(http.MethodPost, RouteKvPut, api.PutRequest{
		Key:   key,
		Value: val,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	kv := leader.store.NewReader().Get(key, 0)
	require.NotNil(t, kv, "expected followers put request to be reflected in leaders store")
	require.Equal(t, key, kv.Key)
	require.Equal(t, val, kv.Value)
}

func Test_WriteMiddleware_LeaderUnreachable_AsFollower_Returns503(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	dead.Close()

	follower := newTestServer(t, true)
	follower.overrideLeader(dead)

	resp := follower.do(http.MethodPost, RouteKvPut, api.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v")},
	)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_WriteMiddleware_OriginalBodyReachesLeader_AsFollower(t *testing.T) {
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)
	follower.overrideLeader(leader.srv)

	key := "foo"
	val := "bar"
	payload := api.PutRequest{
		Key:   []byte(key),
		Value: []byte(val),
	}

	resp := follower.do(http.MethodPost, RouteKvPut, payload)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	entry := leader.store.NewReader().Get([]byte(key), 0)
	require.NotNil(t, entry)
	require.Equal(t, val, string(entry.Value))
}

func Test_ReadMiddleware_Serializable_StaysLocal(t *testing.T) {
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:          []byte("k"),
		Serializable: true,
	})

	require.NotEqual(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_ReadMiddleware_NonSerializable_RequiresLeader(t *testing.T) {
	ts := newTestServer(t, true)

	ts.raftService.State_ = raft.Follower
	ts.raftService.ErrLeader = errors.New("no leader")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:          []byte("k"),
		Serializable: false,
	})

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_ReadMiddleware_NonSerializable_ProxiesToLeader_AsFollower(t *testing.T) {
	leader := newTestServer(t, true)
	follower := newTestServer(t, true)

	follower.overrideLeader(leader.srv)

	leader.mustPut("k", "v")

	resp := follower.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:          []byte("k"),
		Serializable: false,
	})

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res api.RangeResponse
	follower.decodeJSON(resp, &res)
	require.Equal(t, "v", string(res.Entries[0].Value))
}

func Test_ReadMiddleware_NonSerializable_VerifyLeaderFails_Returns503(t *testing.T) {
	ts := newTestServer(t, true)
	// if we believe we are the leader but VerifyLeader() fails (partitioned) we refuse to serve the read
	ts.raftService.ErrVerifyLeader = errors.New("lost quorum")

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:          []byte("k"),
		Serializable: false,
	})

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func Test_ReadMiddleware_BodyReplay_AllowsDecode(t *testing.T) {
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodGet, RouteKvRange, api.RangeRequest{
		Key:          []byte("k"),
		Serializable: true,
	})

	// if body wasnt replayed correctly this would EOF
	var res api.RangeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
}

func Test_ReadMiddleware_BadJSON_Returns400(t *testing.T) {
	ts := newTestServer(t, true)

	resp := ts.do(http.MethodGet, RouteKvRange, map[string]string{"bad": "json"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
