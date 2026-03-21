package command

import (
	"github.com/balits/kave/internal/types/api"
)

// A RangeCmd alparancs a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva,
// és ha End nil akkor csak a Key-t olvassuk
type RangeCmd = api.RangeRequest

// A RangeResult egy Range művelet eredménye
type RangeResult = api.RangeResponseNoHeader

// A PutCmd alparancs a PUT művelethez szükséges adatokat tartalmazza
type PutCmd = api.PutRequest

type PutResult = api.PutResponseNoHeader

// A DeleteCmd alparancs a DELETE művelethez szükséges adatokat tartalmazza
type DeleteCmd = api.DeleteRequest

type DeleteResult = api.DeleteResponseNoHeader

type TxnCmd = api.TxnRequest

type TxnResult = api.TxnResultNoHeader

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
