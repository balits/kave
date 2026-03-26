package command

import (
	"github.com/balits/kave/internal/types/api"
)

// A CmdRange alparancs a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva,
// és ha End nil akkor csak a Key-t olvassuk
type CmdRange = api.RangeRequest

// A ResultRange egy Range művelet eredménye
type ResultRange = api.RangeResponseNoHeader

// A CmdPut alparancs a PUT művelethez szükséges adatokat tartalmazza
type CmdPut = api.PutRequest

type ResultPut = api.PutResponseNoHeader

// A CmdDelete alparancs a DELETE művelethez szükséges adatokat tartalmazza
type CmdDelete = api.DeleteRequest

type ResultDelete = api.DeleteResponseNoHeader

type CmdTxn = api.TxnRequest

type ResultTxn = api.TxnResultNoHeader

type TxnOpType = api.TxnOpType

type TxnOp = api.TxnOp

type TxnOpResult = api.TxnOpResult

// TODO: no checks for union field
type Comparison = api.Comparison

const (
	TxnOpPut    = api.TxnOpPut
	TxnOpDelete = api.TxnOpDelete
	TxnOpRange  = api.TxnOpRange
)
