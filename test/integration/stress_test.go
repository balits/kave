package integration

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/balits/thesis/internal/store"
	"github.com/balits/thesis/internal/testx"
	"github.com/balits/thesis/internal/testx/mock"
	"github.com/hashicorp/raft"
)

func applyAndWait(tb testing.TB, node *testx.TestNode, n, sz int) int {
	var futures []raft.ApplyFuture
	for range n {
		log := setRaftLog(rand.IntN(10_000), rand.IntN(1000))
		futures = append(futures, node.Raft.Apply(log, 0))
	}
	for _, f := range futures {
		testx.NoErr(tb, testx.WaitForFuture(tb, f))
		node.Logger.Debug("applied", "index", f.Index(), "size", sz)
	}
	return n
}

func setRaftLog(k, v int) []byte {
	c := store.Cmd{
		Kind:  store.CmdKindSet,
		Key:   strconv.Itoa(k),
		Value: []byte(strconv.Itoa(v)),
	}
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(c)
	return buf.Bytes()
}

func debugFsms(nodes []*testx.TestNode) {
	for i, n := range nodes {
		s := n.LoggingFsm.Store.(*mock.LoggingStore)
		s.Lock()
		len := len(s.Logs)
		fmt.Printf("fsm %d, size %d, first %v, last %v\n", i+1, len, s.Logs[0], s.Logs[len-1])
		s.Unlock()
	}
}

// TestStress by creating a cluster, growing it to 5 nodes while
// causing various stressful conditions
func TestStress(t *testing.T) {
	fmt.Println(os.Getenv("FOO"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	node1 := testx.NewTestNode(t, ctx, "node1")
	testx.NoErr(t, node1.Bootstrap())
	testx.NoErr(t, testx.WaitForState(node1, raft.Leader))

	totalApplied := 0
	totalApplied += applyAndWait(t, node1, 100, 10)
	testx.NoErr(t, testx.WaitForFuture(t, node1.Raft.Snapshot()))

	var nodes []*testx.TestNode
	for i := range 4 {
		id := fmt.Sprintf("node%d", i+2)
		n := testx.NewTestNode(t, ctx, id)
		addr := n.Transport.LocalAddr()
		testx.NoErr(t, testx.WaitForFuture(t, node1.Raft.AddVoter(raft.ServerID(id), addr, 0, 0)))
		nodes = append(nodes, n)
	}

	leader, err := testx.WaitForNode(raft.Leader, append([]*testx.TestNode{node1}, nodes...))
	testx.NoErr(t, err)

	totalApplied += applyAndWait(t, leader, 100, 10)
	testx.NoErr(t, testx.WaitForFuture(t, leader.Raft.Snapshot()))

	debugFsms(append([]*testx.TestNode{node1}, nodes...))
	testx.CheckConsistent(t, append([]*testx.TestNode{node1}, nodes...))

	disconnected := nodes[len(nodes)-1]
	disconnected.Shutdown(200 * time.Millisecond) // keeps Dir -> keeps state

	// Do some more commits [make sure the resulting snapshot will be a reasonable size]
	totalApplied += applyAndWait(t, leader, 100, 10000)

	// snapshot the leader [leaders log should be compacted past the disconnected follower log now]
	testx.NoErr(t, testx.WaitForFuture(t, leader.Raft.Snapshot()))

	// Unfortunately we need to wait for the leader to start backing off RPCs to the down follower
	// such that when the follower comes back up it'll run an election before it gets an rpc from
	// the leader
	time.Sleep(time.Second * 5)

	disconnected.Restart(t)

	timeout := time.Now().Add(time.Second * 10)
	for disconnected.Raft.LastIndex() < leader.Raft.LastIndex() {
		time.Sleep(time.Millisecond)
		if time.Now().After(timeout) {
			t.Fatalf("Gave up waiting for follower to get caught up to leader")
		}
	}

	testx.CheckConsistent(t, append([]*testx.TestNode{node1}, nodes...))

	// kill two nodes
	rm1, rm2 := nodes[0], nodes[1]
	rm1.Release()
	rm2.Release()
	nodes = nodes[2:]
	time.Sleep(10 * time.Millisecond)

	// Wait for a leader
	leader, err = testx.WaitForNode(raft.Leader, append([]*testx.TestNode{node1}, nodes...))
	testx.NoErr(t, err)

	totalApplied += applyAndWait(t, leader, 100, 10)

	// join more
	for i := range 2 {
		id := fmt.Sprintf("final-node%d", i+1)
		n := testx.NewTestNode(t, ctx, id)
		addr := n.Transport.LocalAddr()
		testx.NoErr(t, testx.WaitForFuture(t, node1.Raft.AddVoter(raft.ServerID(id), addr, 0, 0)))
		nodes = append(nodes, n)
	}

	leader, err = testx.WaitForNode(raft.Leader, append([]*testx.TestNode{node1}, nodes...))
	testx.NoErr(t, err)

	testx.NoErr(t, testx.WaitForFuture(t, leader.Raft.RemoveServer(rm1.RaftConfig.LocalID, 0, 0)))
	testx.NoErr(t, testx.WaitForFuture(t, leader.Raft.RemoveServer(rm2.RaftConfig.LocalID, 0, 0)))

	// kill leader
	leader.Release()
	time.Sleep(3 * leader.RaftConfig.HeartbeatTimeout)

	// new leader
	_, err = testx.WaitForNode(raft.Leader, append([]*testx.TestNode{node1}, nodes...))
	testx.NoErr(t, err)

	allNodes := append(nodes, node1)
	for _, n := range allNodes {
		if len(n.LoggingFsm.Store.(*mock.LoggingStore).Logs) != totalApplied {
			t.Fatalf("node %s shouldve applied %d logs", n.Config.NodeID, totalApplied)
		}
	}

	for _, n := range allNodes {
		n.Release()
	}
}
