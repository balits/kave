package integration_test

import (
	"testing"
)

func TestClusterDiscoveryOnNetwork(t *testing.T) {
	t.Run("3 node cluster", func(t *testing.T) { discover_on_network(3, t) })
}

func discover_on_network(n int, t *testing.T) {
	// baseCfg := testutil.NewMockConfig(n)
	// baseCfg.InMemory = false
	// tempdir, cleanupTempdirs, err := testutil.Tempdir("thesis_test_discovery")
	// if err != nil {
	// 	t.Errorf("Failed to create tempdirs: %v", err)
	// 	return
	// }
	// defer cleanupTempdirs()

	// t.Log("Cluster created")

	// condition := func() bool {
	// 	return testutil.DiscoverCondition(t, services)
	// }

	// testutil.AssertEventually(t, condition, 10*time.Second, 500*time.Millisecond)
}
