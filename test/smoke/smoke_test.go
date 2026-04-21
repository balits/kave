//go:build smoke

package smoke

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	parseEnv()
	os.Exit(m.Run())
}

func Test_Smoke_ClusterFormed(t *testing.T) {
	k := k8sClient(t)
	k.requireClusterReady()
	k.printClusterInfo(false)

	c := httpClient(t)
	statuscode, err := c.readyz()
	require.NoError(t, err, "/readyz unreachable")
	require.Equal(t, statuscode, 200, "expected /readyz to return 200, got %d", statuscode)
}

func Test_Smoke_LeaderExists(t *testing.T) {
	k := k8sClient(t)
	k.requireClusterReady()

	c := httpClient(t)
	statuscode, err := c.readyz()
	require.NoError(t, err)
	require.Equal(t, statuscode, 200, "expected /readyz to return 200, got %d", statuscode)

	stats, statuscode, err := c.stats()
	require.NoError(t, err)
	require.Equal(t, statuscode, 200, "expected /stats to return 200, got %d", statuscode)

	bs, err := json.MarshalIndent(stats, "", "  ")
	require.NoError(t, err, "failed to print stats")
	t.Logf("raft state: %s", string(bs))
	require.NotEmpty(t, stats["leader_id"], "/stats returned empty \"leader_id\"")
	require.NotEmpty(t, stats["leader_addr"], "/stats returned empty \"leader_addr\"")
}

func Test_Smoke_WriteRead(t *testing.T) {
	k := k8sClient(t)
	k.requireClusterReady()

	c := httpClient(t)
	key, val := "foo", "bar"
	c.mustPut(key, val)
	c.mustGetVal(key, val)
}

func Test_Smoke_PodDisruptionRecovery(t *testing.T) {
	k := k8sClient(t)
	k.requireClusterReady()

	c := httpClient(t)

	time.Sleep(5 * time.Second)
	c.mustPut("pre-disrupt", "alive")

	pods := k.getPodNames()
	require.NotEmpty(t, pods)
	victim := pods[0]
	oldID := k.getPodID(victim)
	t.Logf("deleting pod %s", victim)
	k.deletePod(victim, false)
	k.waitPodReplaced(victim, oldID, 30*time.Second)
	// extra half a min if victim is the leader -> reelection lag

	// Wait for the cluster to heal from the disruption we just caused
	// (with the victim gone for sure asserted by waitPodDelete)
	k.requireClusterReady()

	c.waitGetVal("pre-disrupt", "alive", 20*time.Second)

	c.mustPut("post-disrupt", "recovered")
	c.mustGetVal("post-disrupt", "recovered")
	t.Logf("pod %s recovered successfully", victim)
}

func Test_Smoke_PDBEnforcement(t *testing.T) {
	t.Skip("new tests skipped for now")
	k := k8sClient(t)
	k.requireClusterReady()

	pods := k.getPodNames()
	require.GreaterOrEqual(t, len(pods), 2, "need at least 2 nodes to test pdb")
	v1, v2 := pods[0], pods[1]

	oldV1ID := k.getPodID(v1)
	t.Logf("evicting pod %s", v1)
	require.NoError(t, k.tryEvictPod(v1), "first eviction should be allowed by pdb")

	t.Logf("attempting to evic pod %s (should be blocked by pdb)", v2)
	err := k.tryEvictPod(v2)
	require.Error(t, err, "second eviction should be blocked by pdb")
	require.Contains(t, err.Error(), "disruption budget", "expected a pdb distruption error, got %v", err)
	t.Logf("pdb correctly blocked second eviction: %v", err)

	k.waitPodReplaced(v1, oldV1ID, 60*time.Second)
	k.requireClusterReady()

	t.Logf("re-evicting pod %s now that cluster has recovered", v2)
	oldV2ID := k.getPodID(v2)
	require.NoError(t, k.tryEvictPod(v2), "eviction should succeed after cluster recovery")
	k.waitPodReplaced(v2, oldV2ID, 60*time.Second)
	k.requireClusterReady()
	t.Logf("pdb enforcement verified: blocks during disruption, allows after recovery")
}

func Test_Smoke_LeaderFailover(t *testing.T) {
	t.Skip("new tests skipped for now")
	k := k8sClient(t)
	k.requireClusterReady()
	c := httpClient(t)

	stats, statusCode, err := c.stats()
	require.NoError(t, err, "/stats")
	require.Equal(t, 200, statusCode)
	leaderID := stats["leader_id"]
	require.NotEmpty(t, leaderID, "no leader found before test")
	t.Logf("current leader: %s", leaderID)

	c.mustPut("pre-failover", "leader-was-here")

	oldID := k.getPodID(leaderID)
	t.Logf("deleting leader pod %s", leaderID)
	k.deletePod(leaderID, false)

	newLeaderID := c.waitLeaderChanged(leaderID, 30*time.Second)
	t.Logf("new leader elected: %s (replaced: %s)", newLeaderID, leaderID)

	k.waitPodReplaced(leaderID, oldID, 60*time.Second)
	k.requireClusterReady()
	c.waitReady(30 * time.Second)

	c.waitGetVal("pre-failover", "leader-was-here", 20*time.Second)

	c.mustPut("post-failover", "new-leader-works")
	c.mustGetVal("post-failover", "new-leader-works")
	t.Logf("leader failover complete: %s is serving writes", newLeaderID)
}

func Test_Smoke_QuorumLossRecovery(t *testing.T) {
	t.Skip("new tests skipped for now")
	k := k8sClient(t)
	k.requireClusterReady()
	c := httpClient(t)

	// keep leader and kill both followers
	stats, statusCode, err := c.stats()
	require.NoError(t, err, "/stats")
	require.Equal(t, 200, statusCode)
	leaderID := stats["leader_id"]
	require.NotEmpty(t, leaderID)
	t.Logf("current leader: %s (will be kept alive)", leaderID)

	c.mustPut("quorum-pre", "written-before-loss")

	pods := k.getPodNames()
	victims := make([]string, 0, 2)
	for _, p := range pods {
		if p != leaderID {
			victims = append(victims, p)
		}
	}
	require.Len(t, victims, 2, "expected exactly 2 non-leader pods")
	t.Logf("force-deleting followers: %v", victims)

	v1ID := k.getPodID(victims[0])
	v2ID := k.getPodID(victims[1])

	k.deletePod(victims[0], true)
	k.deletePod(victims[1], true)

	t.Logf("verifying writes are refused during quorum loss...")
	writeRefused := false
	for i := range 5 {
		_, resp, err := c.tryPut("quorum-during-loss", "should-fail")
		if err != nil || resp.StatusCode != 200 {
			writeRefused = true
			t.Logf("attempt %d: write correctly refused (status=%d err=%v)", i+1, resp.StatusCode, err)
			break
		}
		t.Logf("attempt %d: write succeeded with status=%d (quorum not yet lost, retrying)", i+1, resp.StatusCode)
		time.Sleep(2 * time.Second)
	}
	require.True(t, writeRefused,
		"expected writes to be refused during quorum loss, but all 5 attempts succeeded")

	k.waitPodReplaced(victims[0], v1ID, 90*time.Second)
	k.waitPodReplaced(victims[1], v2ID, 90*time.Second)
	k.requireClusterReady()
	c.waitReady(30 * time.Second)

	c.waitGetVal("quorum-pre", "written-before-loss", 20*time.Second)

	c.mustPut("quorum-post", "written-after-recovery")
	c.mustGetVal("quorum-post", "written-after-recovery")
	t.Logf("quorum loss recovery verified successfully")
}

func Test_Smoke_RollingRestart(t *testing.T) {
	k := k8sClient(t)
	k.requireClusterReady()
	c := httpClient(t)

	c.mustPut("pre-rollout", "stable")

	// trigger the restart and wait for it to finish gracefully
	k.rolloutRestart()
	k.waitRollout(2 * time.Minute)
	c.waitReady(30 * time.Second)
	time.Sleep(5 * time.Second)

	c.mustGetVal("pre-rollout", "stable")
	c.mustPut("post-rollout", "ok")
	c.mustGetVal("post-rollout", "ok")
}

func Test_Smoke_SequentialWrites(t *testing.T) {
	k := k8sClient(t)
	k.requireClusterReady()
	c := httpClient(t)
	time.Sleep(5 * time.Second)

	var lastRev int64
	for i := range 10 {
		key := fmt.Sprintf("seq-%d", i)
		val := fmt.Sprintf("val-%d", i)
		putResp := c.mustPut(key, val)
		require.Greater(t, putResp.Header.Revision, lastRev,
			"revision should increase monotonically")
		lastRev = putResp.Header.Revision
	}

	for i := range 10 {
		key := fmt.Sprintf("seq-%d", i)
		val := fmt.Sprintf("val-%d", i)
		c.mustGetVal(key, val)
	}
}
