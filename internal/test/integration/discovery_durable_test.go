package integration_test

import (
	"testing"
	"time"

	"github.com/balits/thesis/internal/test/testutil"
)

func TestClusterDiscoveryOnNetwork(t *testing.T) {
	t.Run("3 node cluster", func(t *testing.T) { discover_on_network(3, t) })
	t.Run("5 node cluster", func(t *testing.T) { discover_on_network(5, t) })
}

func discover_on_network(n int, t *testing.T) {
	baseCfg := testutil.NewMockConfig(n)
	baseCfg.InMemory = false
	tempdir, cleanupTempdirs, err := testutil.Tempdir("discover_on_network_test")
	if err != nil {
		t.Errorf("Failed to create tempdirs: %v", err)
		return
	}
	defer cleanupTempdirs()

	services, err := testutil.NewDurableMockCluster(tempdir, baseCfg, testutil.NewMockLogger())
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
		return testutil.DiscoverCondition(t, services)
	}

	testutil.AssertEventually(t, condition, 10*time.Second, 500*time.Millisecond)
}
