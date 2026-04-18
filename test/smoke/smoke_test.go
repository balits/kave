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
	k.deletePod(victim)
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
