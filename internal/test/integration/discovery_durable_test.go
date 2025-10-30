package integration_test

import (
	"testing"
	"time"

	"github.com/balits/thesis/internal/test"
)

func TestClusterDiscoveryOnNetwork(t *testing.T) {
	t.Run("3 node cluster", func(t *testing.T) { discover_on_network(3, t) })
	t.Run("5 node cluster", func(t *testing.T) { discover_on_network(5, t) })
}

func discover_on_network(n int, t *testing.T) {
	baseCfg := test.NewMockConfig(n)
	baseCfg.InMemory = false
	services, err := test.NewDurableCluster(t, baseCfg, test.NewMockLogger())
	if err != nil {
		t.Errorf("Failed to create mock cluster: %v", err)
		return
	}
	defer func() {
		for _, s := range services {
			s.Shutdown(2 * time.Second)
		}
	}()
	t.Log("Cluster created")

	condition := func() bool {
		return test.DiscoverCondition(t, services)
	}

	test.AssertEventually(t, condition, 10*time.Second, 500*time.Millisecond)
}
