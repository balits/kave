package transport

type URI string

const (
	UriApiVersion = "/v1"
	UriKvUri      = UriApiVersion + "/kv"
	UriCluster    = UriApiVersion + "/cluster"
)

type JoinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
}
