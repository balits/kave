package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/balits/kave/internal/compaction"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/node"
	"github.com/balits/kave/internal/ot"
	_http "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/watch"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func Test_Integration_ClusterFormation_3NodeCluster(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
}

func Test_Integration_KVReplication_WriteOnLeader_ReadOnAll(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	putReq := api.PutRequest{Key: []byte("foo"), Value: []byte("bar")}
	var putResp api.PutResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, putReq, &putResp))

	for i := range 3 {
		req := api.RangeRequest{Key: []byte("foo"), Serializable: true}
		var resp api.RangeResponse

		require.Eventually(t, func() bool {
			status := do(t, c.nodes[i], http.MethodGet, _http.RouteKvRange, req, &resp)
			return status == http.StatusOK && len(resp.Entries) == 1
		}, 2*time.Second, 10*time.Millisecond, "node %d failed to replicate data in time", c.nodes[i].Me.NodeID)

		require.Len(t, resp.Entries, 1)
		require.Equal(t, []byte("bar"), resp.Entries[0].Value)
		require.Equal(t, putResp.Header.Revision, resp.Entries[0].ModRev)
	}
}

func Test_Integration_KVReplication_WriteOnFollower_ProxiesToLeader(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, leaderIdx := c.waitLeader(5 * time.Second)
	fw := c.nodes[(leaderIdx+1)%3]

	putReq := api.PutRequest{Key: []byte("foo"), Value: []byte("bar")}
	require.Equal(t, http.StatusOK, do(t, fw, http.MethodPost, _http.RouteKvPut, putReq, nil))

	req := api.RangeRequest{Key: []byte("foo")}
	var resp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, req, &resp))
	require.Equal(t, []byte("bar"), resp.Entries[0].Value)
}

func Test_Integration_KVReplication_RevisionMonotonicity(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	lastRev := int64(0)
	for i := range 20 {
		req := api.PutRequest{Key: []byte(fmt.Sprintf("foo-%d", i)), Value: []byte("bar")}
		var resp api.PutResponse
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, req, &resp))
		require.Greater(t, resp.Header.Revision, lastRev)
		lastRev = resp.Header.Revision
	}
}

func Test_Integration_KVReplication_SerializableRead_MayBeStaleButNeverCorrupted(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, leaderIdx := c.waitLeader(5 * time.Second)
	fw := c.nodes[(leaderIdx+1)%3]

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("stale_key"), Value: []byte("old")}, nil)
	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("stale_key"), Value: []byte("new")}, nil)

	require.Eventually(t, func() bool {
		req := api.RangeRequest{Key: []byte("stale_key"), Serializable: true}
		var resp api.RangeResponse
		if http.StatusOK != do(t, fw, http.MethodGet, _http.RouteKvRange, req, &resp) {
			return false
		}

		if len(resp.Entries) != 1 {
			return false
		}
		val := string(resp.Entries[0].Value)

		if val == "old" || val == "new" {
			// were good here

			return true
		}
		t.Logf("eventually tick: expected 'old' or 'new', got %s", val)
		return false
	}, 3*time.Second, 200*time.Millisecond)
}

func Test_Integration_KVReplication_DeleteRemovesKey(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	putReq := api.PutRequest{Key: []byte("foo"), Value: []byte("bar")}
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, putReq, nil))

	delReq := api.DeleteRequest{Key: []byte("foo")}
	var delResp api.DeleteResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodDelete, _http.RouteKvDelete, delReq, &delResp))
	require.Equal(t, int64(1), delResp.NumDeleted)

	rangeReq := api.RangeRequest{Key: []byte("foo")}
	var getResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, rangeReq, &getResp))
	require.Len(t, getResp.Entries, 0)
}

func Test_Integration_KVReplication_DeleteOnFollower_Proxied(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, leaderIdx := c.waitLeader(5 * time.Second)
	fw := c.nodes[(leaderIdx+1)%3]

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("proxied_del"), Value: []byte("y")}, nil)

	var delResp api.DeleteResponse
	require.Equal(t, http.StatusOK, do(t, fw, http.MethodDelete, _http.RouteKvDelete, api.DeleteRequest{Key: []byte("proxied_del")}, &delResp))
	require.Equal(t, int64(1), delResp.NumDeleted)

	for i := range 3 {
		var getResp api.RangeResponse
		require.Equal(t, http.StatusOK, do(t, c.nodes[i], http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("proxied_del")}, &getResp))
		require.Len(t, getResp.Entries, 0)
	}
}

func Test_Integration_KVReplication_Overwrite_PreservesCreateRevision(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("version_key"), Value: []byte("v1")}, nil)

	var resp1 api.RangeResponse
	do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("version_key")}, &resp1)
	createRev := resp1.Entries[0].CreateRev
	modRev1 := resp1.Entries[0].ModRev
	version1 := resp1.Entries[0].Version

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("version_key"), Value: []byte("v2")}, nil)

	var resp2 api.RangeResponse
	do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("version_key")}, &resp2)

	require.Equal(t, createRev, resp2.Entries[0].CreateRev, "CreateRevision should not change on overwrite")
	require.Greater(t, resp2.Entries[0].ModRev, modRev1, "ModRevision must increment")
	require.Equal(t, version1+1, resp2.Entries[0].Version, "Version must increment")
}

func Test_Integration_KVReplication_RangeScan_ReturnsCorrectSubset(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(k), Value: []byte("val")}, nil)
	}

	var resp api.RangeResponse
	req := api.RangeRequest{Key: []byte("b"), End: []byte("d")}
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, req, &resp))

	require.Len(t, resp.Entries, 2)
	require.Equal(t, []byte("b"), resp.Entries[0].Key)
	require.Equal(t, []byte("c"), resp.Entries[1].Key)
}

func Test_Integration_KVReplication_PrefixScan(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	keys := []string{"app", "app/1", "app/2", "foo"}
	for _, k := range keys {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(k), Value: []byte("val")}, nil)
	}

	var resp api.RangeResponse
	req := api.RangeRequest{Key: []byte("app"), Prefix: true}
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, req, &resp))

	require.Len(t, resp.Entries, 3)
	require.Equal(t, []byte("app"), resp.Entries[0].Key)
	require.Equal(t, []byte("app/1"), resp.Entries[1].Key)
	require.Equal(t, []byte("app/2"), resp.Entries[2].Key)
}

func Test_Integration_Transactions_SuccessBranch(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: []byte("old")}, nil)

	txnReq := api.TxnRequest{
		Comparisons: []api.Comparison{
			{
				Key:         []byte("foo"),
				Operator:    api.OperatorEqual,
				TargetField: api.FieldValue,
				TargetValue: api.CompareTargetUnion{Value: []byte("old")},
			},
		},
		Success: []api.TxnOp{
			{
				Type: api.TxnOpPut,
				Put: &api.PutRequest{
					Key:   []byte("foo"),
					Value: []byte("new"),
				},
			},
		},
	}

	var txnResp api.TxnResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvTxn, txnReq, &txnResp))
	require.True(t, txnResp.Success, "transaction should execute the success branch")

	var rangeResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("foo")}, &rangeResp))
	require.Len(t, rangeResp.Entries, 1)
	require.Equal(t, []byte("new"), rangeResp.Entries[0].Value, "expected updated key")
}

func Test_Integration_Transactions_FailureBranch(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: []byte("old")}, nil)

	txnReq := api.TxnRequest{
		Comparisons: []api.Comparison{
			{
				Key:         []byte("foo"),
				Operator:    api.OperatorEqual,
				TargetField: api.FieldValue,
				TargetValue: api.CompareTargetUnion{Value: []byte("wrong")},
			},
		},
		Failure: []api.TxnOp{
			{
				Type: api.TxnOpPut,
				Put: &api.PutRequest{
					Key:   []byte("other"),
					Value: []byte("fallback"),
				},
			},
		},
	}

	var txnResp api.TxnResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvTxn, txnReq, &txnResp))
	require.False(t, txnResp.Success, "transaction should execute the failure branch")

	var rangeResp1 api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("foo")}, &rangeResp1))
	require.Len(t, rangeResp1.Entries, 1)
	require.Equal(t, []byte("old"), rangeResp1.Entries[0].Value, "expected original key to be unchanged")

	var rangeResp2 api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("other")}, &rangeResp2))
	require.Len(t, rangeResp2.Entries, 1)
	require.Equal(t, []byte("fallback"), rangeResp2.Entries[0].Value, "expected fallback key to be created")
}

func Test_Integration_Transactions_Atomic_NoPartialApplication(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	txnReq := api.TxnRequest{
		Comparisons: []api.Comparison{
			{
				Key:         []byte("atomic_test"),
				Operator:    api.OperatorEqual,
				TargetField: api.FieldCreateRev,
				TargetValue: api.CompareTargetUnion{CreateRevision: 0},
			},
		},
		Success: []api.TxnOp{
			{
				Type: api.TxnOpPut,
				Put: &api.PutRequest{
					Key:   []byte("valid_key"),
					Value: []byte("val"),
				},
			},
			{
				Type: api.TxnOpPut,
				Put: &api.PutRequest{
					Key:   []byte(""), // invalid: empty key
					Value: []byte("val"),
				},
			},
		},
	}

	status := do(t, l, http.MethodPost, _http.RouteKvTxn, txnReq, nil)
	require.NotEqual(t, http.StatusOK, status, "expected transaction to be rejected due to invalid operation")

	var rangeResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("valid_key")}, &rangeResp))
	require.Len(t, rangeResp.Entries, 0, "expected no operation to be applied")
}

func Test_Integration_Transactions_OnFollower_Proxied(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, leaderIdx := c.waitLeader(5 * time.Second)
	fw := c.nodes[(leaderIdx+1)%3]

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: []byte("old")}, nil)

	txnReq := api.TxnRequest{
		Comparisons: []api.Comparison{
			{
				Key:         []byte("foo"),
				Operator:    api.OperatorEqual,
				TargetField: api.FieldValue,
				TargetValue: api.CompareTargetUnion{Value: []byte("old")},
			},
		},
		Success: []api.TxnOp{
			{
				Type: api.TxnOpPut,
				Put: &api.PutRequest{
					Key:   []byte("foo"),
					Value: []byte("new"),
				},
			},
		},
	}

	var txnResp api.TxnResponse
	require.Equal(t, http.StatusOK, do(t, fw, http.MethodPost, _http.RouteKvTxn, txnReq, &txnResp))
	require.True(t, txnResp.Success, "expected proxied transaction to execute the success branch")

	var rangeResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("foo")}, &rangeResp))
	require.Len(t, rangeResp.Entries, 1)
	require.Equal(t, []byte("new"), rangeResp.Entries[0].Value)
}

func Test_Integration_Leases_GrantAndLookup(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	var grantResp api.LeaseGrantResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: 60}, &grantResp))
	require.NotZero(t, grantResp.LeaseID)
	require.Equal(t, int64(60), grantResp.TTL)

	var lookupResp api.LeaseLookupResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteLeaseLookup, api.LeaseLookupRequest{LeaseID: grantResp.LeaseID}, &lookupResp))
	require.Equal(t, grantResp.LeaseID, lookupResp.LeaseID)
	require.Equal(t, int64(60), lookupResp.OriginalTTL)
	require.GreaterOrEqual(t, lookupResp.RemainingTTL, int64(0))
}

func Test_Integration_Leases_AttachKeyToLease_RevokeDeletesKey(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	var grantResp api.LeaseGrantResponse
	do(t, l, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: 60}, &grantResp)

	putReq := api.PutRequest{Key: []byte("lease_key"), Value: []byte("val"), LeaseID: grantResp.LeaseID}
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, putReq, nil))

	var revokeResp api.LeaseRevokeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodDelete, _http.RouteLeaseRevoke, api.LeaseRevokeRequest{LeaseID: grantResp.LeaseID}, &revokeResp))
	require.True(t, revokeResp.Found)
	require.True(t, revokeResp.Revoked)

	var rangeResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("lease_key")}, &rangeResp))
	require.Len(t, rangeResp.Entries, 0, "expected key to be deleted after lease is revoked")
}

func Test_Integration_Leases_ExpiresAfterTTL(t *testing.T) {
	var ttlSec int64 = 2
	ttlDur := time.Duration(ttlSec) * time.Second
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	var grantResp api.LeaseGrantResponse
	do(t, l, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: ttlSec}, &grantResp)

	putReq := api.PutRequest{Key: []byte("foo"), Value: []byte("val"), LeaseID: grantResp.LeaseID}
	do(t, l, http.MethodPost, _http.RouteKvPut, putReq, nil)

	time.Sleep(2 * ttlDur)

	var rangeResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("foo")}, &rangeResp))
	require.Len(t, rangeResp.Entries, 0, "Key should be deleted when lease naturally expires")
}

func Test_Integration_Leases_KeepAliveExtendsTTL(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	ttl := int64(3)
	var grantResp api.LeaseGrantResponse
	do(t, l, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: ttl}, &grantResp)

	time.Sleep(time.Duration(ttl-1) * time.Second)

	var kaResp api.LeaseKeepAliveResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteLeaseKeepAlive, api.LeaseKeepAliveRequest{LeaseID: grantResp.LeaseID}, &kaResp))

	time.Sleep(time.Duration(ttl-1) * time.Second)

	var lookupResp api.LeaseLookupResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteLeaseLookup, api.LeaseLookupRequest{LeaseID: grantResp.LeaseID}, &lookupResp))
	require.Equal(t, grantResp.LeaseID, lookupResp.LeaseID, "expected lease to be alive due to KeepAlive")
}

func Test_Integration_Leases_SurvivesLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, leaderIdx := c.waitLeader(5 * time.Second)

	var grantResp api.LeaseGrantResponse
	do(t, l, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: 30}, &grantResp)

	c.WaitKillLeader(leaderIdx, 3*time.Second)

	l2, _ := c.waitLeader(10 * time.Second)
	require.NotNil(t, l2)
	require.NotEqual(t, l.Me.NodeID, l2.Me.NodeID, "a new node should have been elected leader")

	var lookupResp api.LeaseLookupResponse
	statusCode := do(t, l2, http.MethodGet, _http.RouteLeaseLookup, api.LeaseLookupRequest{LeaseID: grantResp.LeaseID}, &lookupResp)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, grantResp.LeaseID, lookupResp.LeaseID, "expected lease to survive the failover via Raft replication")
}

func Test_Integration_Leases_GrantOnFollowerProxied(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, leaderIdx := c.waitLeader(5 * time.Second)
	fw := c.nodes[(leaderIdx+1)%3]

	var grantResp api.LeaseGrantResponse
	require.Equal(t, http.StatusOK, do(t, fw, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: 60}, &grantResp))
	require.NotZero(t, grantResp.LeaseID)

	var lookupResp api.LeaseLookupResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteLeaseLookup, api.LeaseLookupRequest{LeaseID: grantResp.LeaseID}, &lookupResp))
	require.Equal(t, grantResp.LeaseID, lookupResp.LeaseID)
}

func Test_Integration_Leases_RevokeNonExistent(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	l, _ := c.waitLeader(5 * time.Second)

	var revokeResp api.LeaseRevokeResponse
	status := do(t, l, http.MethodDelete, _http.RouteLeaseRevoke, api.LeaseRevokeRequest{LeaseID: 999999}, &revokeResp)

	require.Equal(t, http.StatusNotFound, status)
	require.False(t, revokeResp.Found)
	require.False(t, revokeResp.Revoked)
}

func dialWatchWS(t *testing.T, n *node.Node) (*websocket.Conn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	url := "ws://" + n.Me.GetHttpAdvertisedAddress() + _http.RouteWatchWS
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{watch.WatchSubprotocol},
	})
	require.NoError(t, err, "failed to dial websocket")
	return conn, cancel
}

func Test_Integration_Watch_ReceivesPutEvent(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	ws, cancel := dialWatchWS(t, l)
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")

	payload, err := json.Marshal(api.WatchCreateRequest{Key: []byte("foo")})
	require.NoError(t, err, "failed to marshal client message payload")
	createWatchMsg := watch.ClientMessage{
		Kind:    watch.ClientWatchCreate,
		Payload: payload,
	}
	require.NoError(t, wsjson.Write(context.Background(), ws, createWatchMsg))

	time.Sleep(200 * time.Millisecond)

	key := []byte("foo")
	value := []byte("bar")
	go func() {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: key, Value: value}, nil)
	}()

	var serverMsg watch.ServerMessage

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchCreated, serverMsg.Kind)

	var createResp api.WatchCreateResponse
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &createResp), "failed to parse create watcher responses")
	require.True(t, createResp.Success)
	require.Nil(t, createResp.Error)
	require.NotZero(t, createResp.WatchID)

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchEventPut, serverMsg.Kind)
	var streamEvent watch.StreamEvent
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &streamEvent), "failed to parse stream event %s", serverMsg.Kind)
	require.Equal(t, kv.EventPut, streamEvent.Event.Kind)
	require.Equal(t, key, streamEvent.Event.Entry.Key)
	require.Equal(t, value, streamEvent.Event.Entry.Value)
}

func Test_Integration_Watch_ReceivesDeleteEvent(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	ws, cancel := dialWatchWS(t, l)
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")

	payload, err := json.Marshal(api.WatchCreateRequest{Key: []byte("foo")})
	require.NoError(t, err, "failed to marshal client message payload")
	createWatchMsg := watch.ClientMessage{
		Kind:    watch.ClientWatchCreate,
		Payload: payload,
	}
	wsjson.Write(context.Background(), ws, createWatchMsg)
	time.Sleep(200 * time.Millisecond)

	go func() {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: []byte("bar")}, nil)
		do(t, l, http.MethodDelete, _http.RouteKvDelete, api.DeleteRequest{Key: []byte("foo")}, nil)
	}()

	var serverMsg watch.ServerMessage

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchCreated, serverMsg.Kind)

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchEventPut, serverMsg.Kind)

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchEventDelete, serverMsg.Kind)
}

func Test_Integration_Watch_RangeReceivesMatchingKeysOnly(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	ws, cancel := dialWatchWS(t, l)
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")

	payload, err := json.Marshal(api.WatchCreateRequest{Key: []byte("a"), End: []byte("d")})
	require.NoError(t, err, "failed to marshal client message payload")
	createWatchMsg := watch.ClientMessage{
		Kind:    watch.ClientWatchCreate,
		Payload: payload,
	}
	wsjson.Write(context.Background(), ws, createWatchMsg)
	time.Sleep(200 * time.Millisecond)

	keys := []string{"a", "c", "d", "z"}
	for _, k := range keys {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(k), Value: []byte("val")}, nil)
	}

	var serverMsg watch.ServerMessage

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchCreated, serverMsg.Kind)

	// should only receive 'a' and 'c'
	for range 2 {
		wsjson.Read(context.Background(), ws, &serverMsg)
		var streamEvent watch.StreamEvent
		require.NoError(t, json.Unmarshal(serverMsg.Payload, &streamEvent), "failed to parse stream event %s", serverMsg.Kind)
		require.Equal(t, kv.EventPut, streamEvent.Event.Kind)
		key := string(streamEvent.Event.Entry.Key)
		require.True(t, key == "a" || key == "c", "unexpected key in range [a, d): %s", key)
	}
}

func Test_Integration_Watch_StartRevCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)
	key := []byte("foo")

	var resp1 api.PutResponse
	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: key, Value: []byte("v1")}, &resp1)
	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: key, Value: []byte("v2")}, nil)

	ws, cancel := dialWatchWS(t, l)
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")

	payload, err := json.Marshal(api.WatchCreateRequest{Key: key, StartRevision: 0})
	require.NoError(t, err, "failed to marshal client message payload")
	createWatchMsg := watch.ClientMessage{
		Kind:    watch.ClientWatchCreate,
		Payload: payload,
	}
	wsjson.Write(context.Background(), ws, createWatchMsg)
	time.Sleep(200 * time.Millisecond)

	var serverMsg watch.ServerMessage
	var streamEvent watch.StreamEvent

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchCreated, serverMsg.Kind)

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg), "expected put v1 event")
	require.Equal(t, watch.ServerWatchEventPut, serverMsg.Kind)
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &streamEvent), "failed to parse stream event %s", serverMsg.Kind)
	require.Equal(t, kv.EventPut, streamEvent.Event.Kind)
	require.Equal(t, key, streamEvent.Event.Entry.Key)
	require.Equal(t, []byte("v1"), streamEvent.Event.Entry.Value)

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg), "expected put v2 event")
	require.Equal(t, watch.ServerWatchEventPut, serverMsg.Kind)
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &streamEvent), "failed to parse stream event %s", serverMsg.Kind)
	require.Equal(t, kv.EventPut, streamEvent.Event.Kind)
	require.Equal(t, key, streamEvent.Event.Entry.Key)
	require.Equal(t, []byte("v2"), streamEvent.Event.Entry.Value)
}

func Test_Integration_Watch_CancelStopsEvents(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	ws, cancel := dialWatchWS(t, l)
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")

	payload, err := json.Marshal(api.WatchCreateRequest{Key: []byte("cancel-key")})
	require.NoError(t, err, "failed to marshal client message payload")
	createWatchMsg := watch.ClientMessage{
		Kind:    watch.ClientWatchCreate,
		Payload: payload,
	}
	wsjson.Write(context.Background(), ws, createWatchMsg)

	var serverMsg watch.ServerMessage
	var createResp api.WatchCreateResponse

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchCreated, serverMsg.Kind)
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &createResp), "failed to parse create watcher responses")
	require.True(t, createResp.Success)
	require.Nil(t, createResp.Error)
	require.NotZero(t, createResp.WatchID)
	watchID := createResp.WatchID

	payload, err = json.Marshal(&api.WatchCancelRequest{WatchID: watchID})
	require.NoError(t, err, "failed to marshal client message payload")
	cancelWatchReq := watch.ClientMessage{
		Kind:    watch.ClientWatchCancel,
		Payload: payload,
	}
	wsjson.Write(context.Background(), ws, cancelWatchReq)

	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg), "expected watch cancel server message")
	require.Equal(t, watch.ServerWatchCanceled, serverMsg.Kind)

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("cancel-key"), Value: []byte("val")}, nil)

	ctx, cancelRead := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelRead()
	err = wsjson.Read(ctx, ws, &serverMsg)
	fmt.Printf("\n\n%+v\n\n", serverMsg)
	require.Error(t, err, "expected timeout reading from cancelled watch, but got event")
}

func Test_Integration_Watch_SurvivesLeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	_, leaderIdx := c.waitLeader(5 * time.Second)
	fw := c.nodes[(leaderIdx+1)%3]

	ws, cancel := dialWatchWS(t, fw)
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")

	payload, err := json.Marshal(api.WatchCreateRequest{Key: []byte("foo")})
	require.NoError(t, err, "failed to marshal client message payload")
	createWatchMsg := watch.ClientMessage{
		Kind:    watch.ClientWatchCreate,
		Payload: payload,
	}
	wsjson.Write(context.Background(), ws, createWatchMsg)

	var serverMsg watch.ServerMessage
	var createResp api.WatchCreateResponse
	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg))
	require.Equal(t, watch.ServerWatchCreated, serverMsg.Kind)
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &createResp), "failed to parse create watcher responses")

	c.WaitKillLeader(leaderIdx, 3*time.Second)
	newLeader, _ := c.waitLeader(10 * time.Second)

	do(t, newLeader, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: []byte("survived")}, nil)

	var streamEvent watch.StreamEvent
	require.NoError(t, wsjson.Read(context.Background(), ws, &serverMsg), "expected follower to recieve failover event")
	require.Equal(t, watch.ServerWatchEventPut, serverMsg.Kind)
	require.NoError(t, json.Unmarshal(serverMsg.Payload, &streamEvent), "failed to parse stream event %s", serverMsg.Kind)
	require.Equal(t, kv.EventPut, streamEvent.Event.Kind)
	require.Equal(t, []byte("foo"), streamEvent.Event.Entry.Key)
	require.Equal(t, []byte("survived"), streamEvent.Event.Entry.Value)
}

func Test_Integration_Failover_LeaderKillTriggersReElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	c.waitClusterReady(10 * time.Second)

	l1, idx1 := c.waitLeader(5 * time.Second)
	c.WaitKillLeader(idx1, 5*time.Second)

	l2, idx2 := c.waitLeader(10 * time.Second)
	require.NotNil(t, l2)
	require.NotEqual(t, l1.Me.NodeID, l2.Me.NodeID, "A new leader should be elected")

	status := c.requireReady(idx2)
	require.Equal(t, http.StatusOK, status)
}

func Test_Integration_Failover_WritesDuringElectionReturn503(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	_, leaderIdx := c.waitLeader(5 * time.Second)

	fwIdx := (leaderIdx + 1) % 3
	fw := c.nodes[fwIdx]

	c.cancels[leaderIdx]()

	putReq := api.PutRequest{Key: []byte("boo"), Value: []byte("far")}
	status := do(t, fw, http.MethodPost, _http.RouteKvPut, putReq, nil)
	require.Equal(t, http.StatusServiceUnavailable, status, "expected StatusServiceUnavailable while cluster has no leader")
}

func Test_Integration_Failover_WritesResumeAfterReElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l1, leaderIdx := c.waitLeader(5 * time.Second)

	var resp1 api.PutResponse
	require.Equal(t, http.StatusOK, do(t, l1, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("pre"), Value: []byte("1")}, &resp1))

	c.WaitKillLeader(leaderIdx, 5*time.Second)
	l2, _ := c.waitLeader(10 * time.Second)

	var resp2 api.PutResponse
	status := do(t, l2, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("post"), Value: []byte("2")}, &resp2)

	require.Equal(t, http.StatusOK, status)
	require.Greater(t, resp2.Header.Revision, resp1.Header.Revision, "expected revision to be stable after failover")
}

func Test_Integration_Failover_DataRetained(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l1, leaderIdx := c.waitLeader(5 * time.Second)

	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		require.Equal(t, http.StatusOK, do(t, l1, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(key), Value: []byte("val")}, nil))
	}

	c.WaitKillLeader(leaderIdx, 5*time.Second)
	l2, _ := c.waitLeader(10 * time.Second)

	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		var resp api.RangeResponse
		require.Equal(t, http.StatusOK, do(t, l2, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte(key)}, &resp))
		require.Len(t, resp.Entries, 1, "key %s missing after failover", key)
	}
}

func Test_Integration_Failover_RestartedNodeCatchesUp(t *testing.T) {
	nodeConfig := defaultNodeConfig()
	nodeConfig.CompactionOpts = compaction.Options{
		Threshold:   100,
		IntervalMin: 60,
		MaxRevGap:   100,
	}
	c := newClusterWithConfig(t, 3, nodeConfig, nil)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)

	followerIdx := (leaderIdx + 1) % 3

	c.cancels[followerIdx]()
	time.Sleep(500 * time.Millisecond)

	for i := range 50 {
		key := fmt.Sprintf("foo-%d", i)
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(key), Value: []byte("val")}, nil))
	}

	c.restartNode(followerIdx)
	c.waitReady(followerIdx)

	fw := c.nodes[followerIdx]
	for i := range 50 {
		key := fmt.Sprintf("foo-%d", i)
		var resp api.RangeResponse
		req := api.RangeRequest{Key: []byte(key), Serializable: true}
		require.Eventually(t, func() bool {
			status := do(t, fw, http.MethodGet, _http.RouteKvRange, req, &resp)
			return status == http.StatusOK && len(resp.Entries) == 1
		}, 2*time.Second, 100*time.Millisecond, "Restarted node is missing data for %s", key)
	}
}

func Test_Integration_Failover_RestartedLeaderRejoinsAsFollower(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	_, oldLeaderIdx := c.waitLeader(5 * time.Second)

	c.WaitKillLeader(oldLeaderIdx, 5*time.Second)

	newLeader, newLeaderIdx := c.waitLeader(10 * time.Second)
	require.NotEqual(t, oldLeaderIdx, newLeaderIdx)

	c.restartNode(oldLeaderIdx)

	pokeReq := api.PutRequest{Key: []byte("poke"), Value: []byte("raft_heartbeat")}
	require.Equal(t, http.StatusOK, do(t, newLeader, http.MethodPost, _http.RouteKvPut, pokeReq, nil))

	require.Eventually(t, func() bool {
		return c.requireReady(oldLeaderIdx) == http.StatusOK
	}, 10*time.Second, 500*time.Millisecond, "restarted old leader node failed to become ready")

	require.Equal(t, raft.Follower, c.nodes[oldLeaderIdx].Raft.State(), "old leader should rejoin as a Follower")
}

func Test_Integration_Quorum_TwoNodesDown_NoWrites(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	_, leaderIdx := c.waitLeader(5 * time.Second)

	followerIdx := (leaderIdx + 1) % 3
	survivorIdx := (leaderIdx + 2) % 3

	c.WaitKillLeader(leaderIdx, 5*time.Second)
	c.cancels[followerIdx]()

	survivor := c.nodes[survivorIdx]

	putReq := api.PutRequest{Key: []byte("quorum_test"), Value: []byte("fail")}

	require.Eventually(t, func() bool {
		status := do(t, survivor, http.MethodPost, _http.RouteKvPut, putReq, nil)
		return status == http.StatusServiceUnavailable
	}, 5*time.Second, 500*time.Millisecond, "expected survivor to reject writes with 503")
}

func Test_Integration_Quorum_TwoNodesDown_SerializableReadsWork(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)
	survivorIdx := (leaderIdx + 1) % 3
	otherIdx := (leaderIdx + 2) % 3

	putReq := api.PutRequest{Key: []byte("persistent"), Value: []byte("data")}
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, putReq, nil))

	require.Eventually(t, func() bool {
		var resp api.RangeResponse
		do(t, c.nodes[survivorIdx], http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("persistent"), Serializable: true}, &resp)
		return len(resp.Entries) == 1
	}, 5*time.Second, 100*time.Millisecond)

	c.WaitKillLeader(leaderIdx, 5*time.Second)
	c.cancels[otherIdx]()

	survivor := c.nodes[survivorIdx]

	status := do(t, survivor, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("persistent"), Serializable: false}, nil)
	require.Equal(t, http.StatusServiceUnavailable, status, "expected linearizable read to fail (no leader)")

	var getResp api.RangeResponse
	status = do(t, survivor, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("persistent"), Serializable: true}, &getResp)
	require.Equal(t, http.StatusOK, status, "expected serializable read to still work (it reads nodes local state)")
	require.Len(t, getResp.Entries, 1)
	require.Equal(t, []byte("data"), getResp.Entries[0].Value)
}

func Test_Integration_Quorum_Restored_ClusterRecovers(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()

	_, leaderIdx := c.waitLeader(5 * time.Second)
	node1Idx := (leaderIdx + 1) % 3
	node2Idx := (leaderIdx + 2) % 3

	c.WaitKillLeader(leaderIdx, 5*time.Second)
	c.cancels[node1Idx]()
	time.Sleep(200 * time.Millisecond)

	status := do(t, c.nodes[node2Idx], http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("k"), Value: []byte("v")}, nil)
	require.Equal(t, http.StatusServiceUnavailable, status, "expected 503 since quorom is lsot")

	c.restartNode(node1Idx)

	newLeader, _ := c.waitLeader(15 * time.Second)
	require.NotNil(t, newLeader)

	putReq := api.PutRequest{Key: []byte("recovery"), Value: []byte("success")}
	require.Equal(t, http.StatusOK, do(t, newLeader, http.MethodPost, _http.RouteKvPut, putReq, nil), "expected writes to work after quorom is reestablished")

	var getResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, newLeader, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("recovery")}, &getResp), "expected writes to work after quorom is reestablished")
	require.Equal(t, []byte("success"), getResp.Entries[0].Value)
}

func Test_Integration_Snapshot_NodeCatchesUpViaSnapshot(t *testing.T) {
	raftCfg := raft.DefaultConfig()
	raftCfg.SnapshotInterval = 2 * time.Second
	raftCfg.SnapshotThreshold = 5
	c := newClusterWithConfig(t, 3, nil, raftCfg)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)
	followerIdx := (leaderIdx + 1) % 3

	c.cancels[followerIdx]()

	for i := range 50 {
		key := fmt.Sprintf("snap-key-%d", i)
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(key), Value: []byte("val")}, nil))
	}

	// little over snapshot interval
	time.Sleep(2500 * time.Millisecond)

	c.cfgs[followerIdx].Bootstrap = false
	c.restartNode(followerIdx)

	fw := c.nodes[followerIdx]
	c.waitReady(followerIdx)

	for i := range 50 {
		key := fmt.Sprintf("snap-key-%d", i)
		var resp api.RangeResponse

		require.Eventually(t, func() bool {
			status := do(t, fw, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte(key), Serializable: true}, &resp)
			return status == http.StatusOK && len(resp.Entries) == 1
		}, 20*time.Second, 100*time.Millisecond, "Follower failed to recover key %s via snapshot", key)
	}
}

func Test_Integration_Snapshot_StateMatchesPreSnapshot(t *testing.T) {
	raftCfg := raft.DefaultConfig()
	raftCfg.SnapshotInterval = 2 * time.Second
	raftCfg.SnapshotThreshold = 5
	c := newClusterWithConfig(t, 3, nil, raftCfg)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)
	followerIdx := (leaderIdx + 1) % 3

	for i := range 10 {
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "pre-%d", i), Value: []byte("val")}, nil))
	}

	c.cancels[followerIdx]()
	for i := range 10 {
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "post-%d", i), Value: []byte("val")}, nil))
	}

	c.restartNode(followerIdx)
	fw := c.nodes[followerIdx]

	do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("poke"), Value: []byte("poke")}, nil)
	c.waitReady(followerIdx)

	require.Eventually(t, func() bool {
		var resp1, resp2 api.RangeResponse
		do(t, fw, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("pre-9"), Serializable: true}, &resp1)
		do(t, fw, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("post-9"), Serializable: true}, &resp2)
		return len(resp1.Entries) == 1 && len(resp2.Entries) == 1
	}, 10*time.Second, 100*time.Millisecond, "Follower failed to restore mixed snapshot/log state")
}

func Test_Integration_Snapshot_CompactedRevisionsNotReadable(t *testing.T) {
	nodeCfg := configWithoutCompactionScheduler()
	raftCfg := raftConfigWithFastCompaction()
	c := newClusterWithConfig(t, 3, nodeCfg, raftCfg)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)
	followerIdx := (leaderIdx + 1) % 3

	var lastResp api.PutResponse
	for i := range 10 {
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "k-%d", i), Value: []byte("v")}, &lastResp))
	}

	// doAdmin: compaction trigger is an admin route
	compactRev := lastResp.Header.Revision - 5
	require.Equal(t, http.StatusOK, doAdmin(t, l, http.MethodPost, _http.RouteTriggerCompaction, api.CompactionRequest{TargetRev: compactRev}, nil))

	for i := range 10 {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "k-%d", i), Value: []byte("v")}, nil)
	}

	c.cancels[followerIdx]()
	c.restartNode(followerIdx)
	fw := c.nodes[followerIdx]
	c.waitReady(followerIdx)

	var getResp api.RangeResponse
	status := do(t, fw, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("k-1"), Revision: compactRev - 1, Serializable: true}, &getResp)
	require.NotEqual(t, http.StatusOK, status, "expected error when reading compacted revision")

	status = do(t, fw, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("k-5"), Revision: compactRev + 1, Serializable: true}, &getResp)
	require.Equal(t, http.StatusOK, status, "expected read to succees after compacted revision")
	require.Len(t, getResp.Entries, 1)
}

func Test_Integration_Snapshot_LeasesSurvive(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)
	followerIdx := (leaderIdx + 1) % 3

	var grantResp api.LeaseGrantResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteLeaseGrant, api.LeaseGrantRequest{TTL: 300}, &grantResp))

	for i := range 50 {
		do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "foo-%d", i), Value: []byte("bar")}, nil)
	}

	c.cancels[followerIdx]()
	c.restartNode(followerIdx)
	fw := c.nodes[followerIdx]
	c.waitReady(followerIdx)

	require.Eventually(t, func() bool {
		var lookupResp api.LeaseLookupResponse
		status := do(t, fw, http.MethodGet, _http.RouteLeaseLookup, api.LeaseLookupRequest{LeaseID: grantResp.LeaseID}, &lookupResp)
		return status == http.StatusOK && lookupResp.RemainingTTL > 0
	}, 10*time.Second, 100*time.Millisecond, "Lease did not survive the snapshot transfer")
}

func Test_Integration_Compaction_RemovesOldRevisions(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithoutCompactionScheduler(), raftConfigWithFastCompaction())
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var resp1 api.PutResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("k"), Value: []byte("v1")}, &resp1))
	rev1 := resp1.Header.Revision

	var resp2 api.PutResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("k"), Value: []byte("v2")}, &resp2))
	rev2 := resp2.Header.Revision

	// doAdmin: compaction trigger is an admin route
	require.Equal(t, http.StatusOK, doAdmin(t, l, http.MethodPost, _http.RouteTriggerCompaction, api.CompactionRequest{TargetRev: rev1 + 1}, nil))

	var getResp api.RangeResponse
	status := do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("k"), Revision: rev1}, &getResp)
	require.NotEqual(t, http.StatusOK, status, "expected error when reading compacted revision")

	status = do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("k"), Revision: rev2}, &getResp)
	require.Equal(t, http.StatusOK, status, "expected read after compacted rev to succeed  ")
	require.Len(t, getResp.Entries, 1)
	require.Equal(t, []byte("v2"), getResp.Entries[0].Value)
}

func Test_Integration_Compaction_DoesNotRemoveLatest(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithoutCompactionScheduler(), raftConfigWithFastCompaction())
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var resp1 api.PutResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: []byte("v1")}, &resp1))
	rev1 := resp1.Header.Revision

	// doAdmin: compaction trigger is an admin route
	require.Equal(t, http.StatusOK, doAdmin(t, l, http.MethodPost, _http.RouteTriggerCompaction, api.CompactionRequest{TargetRev: rev1}, nil))

	var getResp api.RangeResponse
	status := do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("foo")}, &getResp)
	require.Equal(t, http.StatusOK, status)
	require.Len(t, getResp.Entries, 1)
	require.Equal(t, []byte("v1"), getResp.Entries[0].Value)
}

func Test_Integration_Compaction_ContinuousLoad(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithoutCompactionScheduler(), raftConfigWithFastCompaction())
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var wg sync.WaitGroup
	putNum := 50
	wg.Go(func() {
		for i := range putNum {
			do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "foo-%d", i), Value: []byte("val")}, nil)
			time.Sleep(2 * time.Millisecond)
		}
	})

	wg.Go(func() {
		for range 3 {
			time.Sleep(15 * time.Millisecond)

			var rangeResp api.RangeResponse
			do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("foo-1")}, &rangeResp)
			currRev := rangeResp.Header.Revision
			if currRev < 3 {
				continue
			}
			targetRev := currRev - 2
			// doAdmin: compaction trigger is an admin route
			doAdmin(t, l, http.MethodPost, _http.RouteTriggerCompaction, api.CompactionRequest{TargetRev: targetRev}, nil)
		}
	})

	wg.Wait()

	for i := range 3 {
		var getResp api.RangeResponse
		status := do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: fmt.Appendf(nil, "foo-%d", i)}, &getResp)
		require.Equal(t, http.StatusOK, status, "failed to get key foo-%d", i)
		require.Len(t, getResp.Entries, 1, "key foo-%d went missing during continuous compaction", i)
	}
}

func Test_Integration_Compaction_ReplicatedAcrossCluster(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithoutCompactionScheduler(), raftConfigWithFastCompaction())
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var lastResp api.PutResponse
	for i := range 5 {
		require.Equal(t, http.StatusOK, do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("foo"), Value: fmt.Appendf(nil, "v%d", i)}, &lastResp))
	}

	targetRev := lastResp.Header.Revision - 2
	// doAdmin: compaction trigger is an admin route
	require.Equal(t, http.StatusOK, doAdmin(t, l, http.MethodPost, _http.RouteTriggerCompaction, api.CompactionRequest{TargetRev: targetRev}, nil))

	for _, n := range c.nodes {
		require.Eventually(t, func() bool {
			var getResp api.RangeResponse
			status := do(t, n, http.MethodGet, _http.RouteKvRange, api.RangeRequest{
				Key:          []byte("foo"),
				Revision:     targetRev - 1,
				Serializable: true,
			}, &getResp)

			return status != http.StatusOK // ErrCompacted
		}, 5*time.Second, 200*time.Millisecond, "node %s failed to replicate compaction boundary", n.Me.NodeID)
	}
}

func Test_Integration_OT_FullRoundtrip(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	otOpts := c.cfgs[0].OtOpts
	blob := ot.FakeBlob(t, otOpts)
	cl := ot.MockOTClient{T: t}
	choice := rand.IntN(otOpts.SlotCount)

	// doAdmin: OT WriteAll is an admin route
	require.Equal(t, http.StatusOK, doAdmin(t, l, http.MethodPost, _http.RouteOtWriteAll, api.OTWriteAllRequest{Blob: blob}, nil))

	var initResp api.OTInitResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteOtInit, api.OTInitRequest{}, &initResp))

	pointB, scalarB := cl.BlindedChoice(initResp.PointA, choice)

	var transferResp api.OTTransferResponse
	transferReq := api.OTTransferRequest{Token: initResp.Token, PointB: pointB}
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteOtTransfer, transferReq, &transferResp))

	expectedSlot := blob[choice*ot.DefaultSlotSize : (choice+1)*ot.DefaultSlotSize]
	plaintext := cl.Decrypt(initResp.PointA, scalarB, transferResp.Ciphertexts, choice)
	require.Equal(t, expectedSlot, plaintext, "decrypted slot should exactly match the written blob segment")
}

func Test_Integration_OT_NonChosenSlotsFail(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	otOpts := c.cfgs[0].OtOpts
	blob := ot.FakeBlob(t, otOpts)
	cl := ot.MockOTClient{T: t}
	choice := rand.IntN(otOpts.SlotCount)

	// doAdmin: OT WriteAll is an admin route
	require.Equal(t, http.StatusOK, doAdmin(t, l, http.MethodPost, _http.RouteOtWriteAll, api.OTWriteAllRequest{Blob: blob}, nil))

	var initResp api.OTInitResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteOtInit, api.OTInitRequest{}, &initResp))

	pointB, scalarB := cl.BlindedChoice(initResp.PointA, choice)

	var transferResp api.OTTransferResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteOtTransfer, api.OTTransferRequest{Token: initResp.Token, PointB: pointB}, &transferResp))

	for i, ct := range transferResp.Ciphertexts {
		if i == choice {
			continue
		}
		_, err := cl.TryDecrypt(initResp.PointA, scalarB, ct)
		require.Error(t, err, "slot %d should not decrypt with choice=%d's key", i, choice)
	}
}

func Test_Integration_OT_InitOnFollower(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	_, leaderIdx := c.waitLeader(5 * time.Second)

	followerIdx := (leaderIdx + 1) % 3
	fw := c.nodes[followerIdx]

	var initResp api.OTInitResponse
	status := do(t, fw, http.MethodGet, _http.RouteOtInit, api.OTInitRequest{}, &initResp)

	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, initResp.Token, "follower failed to generate a valid token")
	require.NotEmpty(t, initResp.PointA, "follower failed to generate PointA")
}

func Test_Integration_OT_WriteAllProxiedFromFollower(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, leaderIdx := c.waitLeader(5 * time.Second)

	followerIdx := (leaderIdx + 1) % 3
	fw := c.nodes[followerIdx]

	otOpts := c.cfgs[0].OtOpts
	blob := ot.FakeBlob(t, otOpts)
	cl := ot.MockOTClient{T: t}
	choice := rand.IntN(otOpts.SlotCount)

	// doAdmin: OT WriteAll is an admin route (proxied through follower)
	require.Equal(t, http.StatusOK, doAdmin(t, fw, http.MethodPost, _http.RouteOtWriteAll, api.OTWriteAllRequest{Blob: blob}, nil))
	var initResp api.OTInitResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteOtInit, api.OTInitRequest{}, &initResp))

	pointB, scalarB := cl.BlindedChoice(initResp.PointA, choice)
	var transferResp api.OTTransferResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteOtTransfer, api.OTTransferRequest{Token: initResp.Token, PointB: pointB}, &transferResp))

	expectedSlot := blob[choice*otOpts.SlotSize : (choice+1)*otOpts.SlotSize]
	plaintext := cl.Decrypt(initResp.PointA, scalarB, transferResp.Ciphertexts, choice)
	require.Equal(t, expectedSlot, plaintext, "blob written via follower proxy was not correctly applied to the state machine")
}

func Test_Integration_Edge_EmptyBodyReturns400(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	url := fmt.Sprintf("http://%s:%s%s", l.Me.Hostname, l.Me.HttpPort, _http.RouteKvPut)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(nil))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_Integration_Edge_OversizedKeyRejected(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)
	maxKeySize := c.cfgs[0].KvOptions.MaxKeySize
	maxValueSize := c.cfgs[0].KvOptions.MaxValueSize

	status := do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{
		Key:   []byte(strings.Repeat("A", maxKeySize*2)),
		Value: []byte("val"),
	}, nil)
	require.Equal(t, http.StatusBadRequest, status, "expected oversized key to be rejected with 400")

	status = do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{
		Key:   []byte("key"),
		Value: []byte(strings.Repeat("A", maxValueSize*2)),
	}, nil)
	require.Equal(t, http.StatusBadRequest, status, "expected oversized value to be rejected with 400")
}

func Test_Integration_Edge_HealthProbesDuringStartup(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	_, leaderIdx := c.waitLeader(5 * time.Second)
	fw1Idx := (leaderIdx + 1) % 3

	c.cancels[leaderIdx]()
	c.cancels[fw1Idx]()

	fw2Idx := (leaderIdx + 1 + 1) % 3
	fw2 := c.nodes[fw2Idx]

	liveStatus := do(t, fw2, http.MethodGet, _http.RouteLivez, nil, nil)
	require.Equal(t, http.StatusOK, liveStatus, "/livez should be 200 when running w/o leaderless")

	require.Eventually(t, func() bool {
		readyStatus := do(t, fw2, http.MethodGet, _http.RouteReadyz, nil, nil)
		return readyStatus == http.StatusServiceUnavailable
	}, 3*time.Second, 50*time.Millisecond, "/readyz should become 503 once the heartbeat timeout expires")
}

func Test_Integration_Edge_DoubleBootstrapRejected(t *testing.T) {
	c := newCluster(t, 3)
	defer c.teardown()
	_, leaderIdx := c.waitLeader(5 * time.Second)

	c.cancels[leaderIdx]()

	c.cfgs[leaderIdx].Bootstrap = true
	c.restartNode(leaderIdx)

	newL, _ := c.waitLeader(5 * time.Second)
	require.NotNil(t, newL, "node should have successfully ignored the second bootstrap and recovered")

	status := do(t, newL, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte("k"), Value: []byte("v")}, nil)
	require.Equal(t, http.StatusOK, status, "expectec new leader to still have old data")
}

func Test_Integration_Edge_RateLimiter(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithRateLimitTest(), nil)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var got429 bool

	for i := range 15 {
		status := do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: fmt.Appendf(nil, "k-%d", i), Value: []byte("v")}, nil)

		if status == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}

	require.True(t, got429, "expected to hit the write burst limit and receive a 429 Too Many Requests")
}
