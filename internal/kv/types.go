package kv

// ========= common keys in the store =========

type MetaKey []byte

var (
	MetaKeyCurrentRevision   MetaKey = []byte("current_revision")
	MetaKeyCompactedRevision MetaKey = []byte("compacted_revision")
	MetaKeyConsistentIndex   MetaKey = []byte("consistent_index")
)

// used in "key_version/" bucket, as append only history
// (key, latestRev) -> value
type CompositeKey struct {
	Key []byte
	Rev Revision
}

// ========= common values in the store =========

var EmptyEntry Entry // zero values, for non existent keys

// Metadata about a key, containing revisions, version count and tombstone flag
type Meta struct {
	CreateRev int64 `json:"create_revision"`
	ModRev    int64 `json:"mod_revision"`
	Version   int64 `json:"version"`
	Tombstone bool  `json:"tombstone"`
}

type Entry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
	Meta
}

// ========= command =========

type Command struct {
	Type  CmdType
	Key   []byte // for Get and Delete
	Value []byte // for Set
	Txn   *TxnCommand
}

type CmdType string

const (
	CmdPut    CmdType = "PUT"
	CmdDelete CmdType = "DEL"
	CmdTxn    CmdType = "TXN"
)

// ========= results =========

type Result struct {
	Err          error `json:"error,omitempty"`
	GetResult    GetResult
	SetResult    SetResult
	DeleteResult *DeleteResult
	TxnResult    *TxnResult
}

type SetResult *Entry

type GetResult *Entry

type DeleteResult struct {
	Deleted   bool   `json:"deleted"`
	PrevEntry *Entry `json:"prev_kv,omitempty"`
}

var TombstoneMarker []byte = []byte{0x00}

// ========= transaction types =========

type TxnCommand struct {
	Comparisons []Comparison `json:"comparisons"`
	Success     []TxnOp      `json:"success"`
	Failure     []TxnOp      `json:"failure"`
}

type TxnResult struct {
	Error   error    `json:"error,omitempty"`
	Success bool     `json:"success"`
	Results []Result `json:"results,omitempty"`
}

type TxnOp struct {
	Type  TxnOpType `json:"type"`
	Key   []byte    `json:"key"`
	Value []byte    `json:"value"`
}

type TxnOpType string

const (
	TxnOpPut    TxnOpType = "PUT"
	TxnOpDelete TxnOpType = "DEL"
)
