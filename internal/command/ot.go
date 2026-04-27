package command

import "github.com/balits/kave/internal/types/api"

type CmdOTWriteAll = api.OTWriteAllRequest
type ResultOTWriteAll = api.OTWriteAllResponseNoHeader

type CmdOTGenerateClusterKey struct {
	// key needs to be generated at propose time before it hits the FSM
	// otherwise each nodes fsm creates a different CSPRNG-ed key
	Key []byte
}
