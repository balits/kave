package transport

import "github.com/balits/kave/internal/peer"

// here so no import cycle forms, should be refactored though
// but a new internal/auth package with one constant seems silly
const AdminAuthTokenHeaderName = "X-Kave-Admin-Token"

const (
	ApiVersion = "/v1"
	RouteKv    = ApiVersion + "/kv"
	RouteLease = ApiVersion + "/lease"
	RouteOt    = ApiVersion + "/ot"
	RouteWatch = ApiVersion + "/watch"

	RouteAdmin = ApiVersion + "/admin"
)

type JoinRequest struct {
	Peer peer.Peer `json:"peer"`
}
