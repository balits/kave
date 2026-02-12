package metrics

import "sync/atomic"

type NodeMetricsProvider interface {
	NodeMetrics() *NodeMetrics
}

type NodeMetrics struct {
	NodeState          NodeState
	StorageState       StorageState
	LastNodeStateError error
}

type NodeMetricsAtomic struct {
	NodeState          atomic.Uint32
	LastNodeStateError atomic.Value
	StorageState       atomic.Uint32
}

func (n *NodeMetricsAtomic) NodeMetrics() *NodeMetrics {
	m := new(NodeMetrics)
	if err := n.LastNodeStateError.Load(); err != nil {
		m.LastNodeStateError = err.(error)
	}
	m.NodeState = NodeState(n.NodeState.Load())
	m.StorageState = StorageState(n.StorageState.Load())
	return m
}

type NodeState uint32

const (
	NodeStateUninit NodeState = iota
	NodeStateInit
	NodeStateJoined
	NodeStateLeft
	NodeStateSetupFailed
	NodeStateStartFailed
	NodeStateShutdownFailed
)

func (r NodeState) String() string {
	switch r {
	case NodeStateUninit:
		return "uninitialized"
	case NodeStateInit:
		return "initialized"
	case NodeStateJoined:
		return "joined_cluster"
	case NodeStateLeft:
		return "left_cluster"
	default:
		return "unknown"
	}
}

type StorageState uint32

const (
	StorageStateUninit StorageState = iota
	StorageStateOperational
	StorageStateCorrupted
)

func (r StorageState) String() string {
	switch r {
	case StorageStateUninit:
		return "uninitialized"
	case StorageStateOperational:
		return "operational"
	case StorageStateCorrupted:
		return "corrupted"
	default:
		return "unknown"
	}
}
