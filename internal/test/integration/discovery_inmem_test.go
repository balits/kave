package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/balits/thesis/internal/test"
)

func TestClusterDiscoveryInMemory(t *testing.T) {
	t.Run("3 node cluster", func(t *testing.T) { discover_inmem(3, t) })
	t.Run("5 node cluster", func(t *testing.T) { discover_inmem(5, t) })
}
func discover_inmem(n int, t *testing.T) {
	baseCfg := test.NewMockConfig(n)
	services, err := test.NewInmemCluster(baseCfg)
	if err != nil {
		t.Error(fmt.Errorf("Failed to create mock cluster: %v", err))
		return
	}
	t.Log("Cluster created")
	condition := func() bool {
		for _, svc := range services {
			addr, ID := svc.Raft.LeaderWithID()
			t.Logf("node: %s checking for current leader: %s, %s", svc.Config.ThisService.RaftID, addr, ID)
			if string(addr) == "" || string(ID) == "" {
				t.Logf("%s cant see the leader", svc.Config.ThisService.RaftID)
			}
			if string(addr) == "" || string(ID) == "" {
				// t.Errorf("Service %s did not join cluster: leader not found", svc.Config.ThisService.RaftID)
				return false
			}
		}
		return true
	}

	test.AssertEventually(t, condition, 5*time.Second, 200*time.Millisecond)
}
