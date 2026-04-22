package transport

import "github.com/balits/kave/internal/peer"

const (
	ApiVersion = "/v1"
	RouteKv    = ApiVersion + "/kv"
	RouteLease = ApiVersion + "/lease"
	RouteOt    = ApiVersion + "/ot"
	RouteWatch = ApiVersion + "/watch"

	RouteAdmin   = ApiVersion + "/admin"
	RouteCluster = RouteAdmin + "/cluster"
)

type JoinRequest struct {
	Peer peer.Peer `json:"peer"`
}
