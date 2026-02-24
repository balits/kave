package common

type URI string

const (
	UriApiVersion = "/v1"
	UriKvUri      = UriApiVersion + "/kv"
	UriCluster    = UriApiVersion + "/cluster"
)
