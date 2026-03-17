package transport

const (
	UriApiVersion = "/v1"
	UriKv         = UriApiVersion + "/kv"
	UriLease      = UriApiVersion + "/lease"
	UriCluster    = UriApiVersion + "/cluster"
)

type JoinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
}
