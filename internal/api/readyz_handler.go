package api

import (
	"fmt"

	"github.com/balits/thesis/internal/metrics"
	"github.com/balits/thesis/internal/web"
	"github.com/hashicorp/raft"
)

func (s *Server) readyHandler(ctx *web.Context) {
	var status metrics.Status
	status.FsmMetrics = s.node.FsmMetrics()
	status.NodeMetrics = s.node.NodeMetrics()
	status.StorageMetrics = s.node.StorageMetrics()

	// might be a problem if we dont set it to nil later?
	if status.NodeMetrics.LastNodeStateError != nil {
		ctx.ErrorWithData(status, status.NodeMetrics.LastNodeStateError.Error(), 500)
		return
	}

	if s.node.Raft.State() == raft.Shutdown {
		ctx.ErrorWithData(status, "raft is shutdown", 500)
		return
	}

	if status.NodeMetrics.StorageState != metrics.StorageStateOperational {
		ctx.ErrorWithData(status, "storage is not operational", 500)
		return
	}

	if status.NodeMetrics.NodeState != metrics.NodeStateJoined {
		ctx.ErrorWithData(status, fmt.Sprintf("node state: %s", status.NodeMetrics.NodeState), 500)
		return
	}

	ctx.Ok(status)
}
