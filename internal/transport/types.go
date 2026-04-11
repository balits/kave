package transport

import "github.com/balits/kave/internal/peer"

const (
	ApiVersion   = "/v1"
	RouteKv      = ApiVersion + "/kv"
	RouteLease   = ApiVersion + "/lease"
	RouteCluster = ApiVersion + "/cluster"
	RouteOt      = ApiVersion + "/ot"
	RouteWatch   = ApiVersion + "/watch"
)

type JoinRequest struct {
	Peer peer.Peer `json:"peer"`
}
