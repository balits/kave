package transport

const (
	ApiVersion   = "/v1"
	RouteKv      = ApiVersion + "/kv"
	RouteLease   = ApiVersion + "/lease"
	RouteCluster = ApiVersion + "/cluster"
	RouteOt      = ApiVersion + "/ot"
)

type JoinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
}
