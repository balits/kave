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

func requireClusterReady(t *testing.T, k *kubectl) {
	pods := k.getPodNames()
	require.Len(t, pods, env.clusterSize, "expected %d pods, got %d", env.clusterSize, len(pods))
	require.True(t, k.arePodsReady(), "not all poods are ready")
	t.Logf("pods: %v", pods)
}

func Test_Smoke_ClusterFormed(t *testing.T) {
	k := k8sClient(t)
	requireClusterReady(t, k)

	c := httpClient(t)
	statuscode, err := c.readyz()
	require.NoError(t, err, "/readyz unreachable")
	require.Equal(t, statuscode, 200, "expected /readyz to return 200, got %d", statuscode)
}
func Test_Smoke_LeaderExists(t *testing.T) {
	k := k8sClient(t)
	requireClusterReady(t, k)

	c := httpClient(t)
	statuscode, err := c.readyz()
	require.NoError(t, err)
	require.Equal(t, statuscode, 200, "expected /readyz to return 200, got %d", statuscode)

	stats, statuscode := c.stats()
	require.Equal(t, statuscode, 200, "expected /stats to return 200, got %d", statuscode)

	bs, err := json.MarshalIndent(stats, "", "  ")
	require.NoError(t, err, "failed to print stats")
	t.Logf("raft state: %s", string(bs))
	require.NotEmpty(t, stats["leader_id"], "/stats returned empty \"leader_id\"")
	require.NotEmpty(t, stats["leader_addr"], "/stats returned empty \"leader_addr\"")
}

func Test_Smoke_WriteRead(t *testing.T) {
	k := k8sClient(t)
	requireClusterReady(t, k)

	c := httpClient(t)
	key, val := "foo", "bar"
	c.mustPut(key, val)
	c.mustGetVal(key, val)
}

func Test_Smoke_PodDisruptionRecovery(t *testing.T) {
	k := k8sClient(t)
	requireClusterReady(t, k)

	c := httpClient(t)

	// Write a key before disruption
	c.mustPut("pre-disrupt", "alive")

	pods := k.getPodNames()
	require.NotEmpty(t, pods)
	victim := pods[0]
	t.Logf("deleting pod %s", victim)
	k.deletePod(victim)

	k.waitPodsReady(60 * time.Second)
	c.waitReady(30 * time.Second)

	c.mustGetVal("pre-disrupt", "alive")

	c.mustPut("post-disrupt", "recovered")
	c.mustGetVal("post-disrupt", "recovered")

	t.Logf("pod %s recovered successfully", victim)
}

func Test_Smoke_RollingRestart(t *testing.T) {
	k := k8sClient(t)
	requireClusterReady(t, k)
	c := httpClient(t)

	c.mustPut("pre-rollout", "stable")

	k.rolloutRestart()
	k.waitRollout(2 * time.Minute)
	c.waitReady(30 * time.Second)

	c.mustGetVal("pre-rollout", "stable")

	c.mustPut("post-rollout", "ok")
	c.mustGetVal("post-rollout", "ok")
}

func Test_Smoke_SequentialWrites(t *testing.T) {
	k := k8sClient(t)
	requireClusterReady(t, k)
	c := httpClient(t)

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
