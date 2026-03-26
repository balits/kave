package command

import "github.com/balits/kave/internal/types/api"

type CmdOTWriteAll = api.OTWriteAllRequest
type ResultOTWriteAll = api.OTWriteAllResponseNoHeader

type CmdOTGenerateClusterKey struct{}
type ResultOTGenerateClusterKey struct {
	Key []byte
}
