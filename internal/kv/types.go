package kv

import (
	"bytes"
	"encoding/gob"
	"errors"
)

var (
	ErrCompacted   = errors.New("requested revision has been compacted away")
	ErrKeyNotFound = errors.New("key not found")
)

// MetaKey constants for the _meta bucket
type MetaKey []byte

var (
	// Set to the index last applied log entry
	MetaKeyRaftApplyIndex MetaKey = []byte("consistent_index")

	// Set to the term last applied log entry
	MetaKeyRaftTerm MetaKey = []byte("term")

	MetaKeyCurrentRevision MetaKey = []byte("current_revision")

	// Stores the last requested compaction revision.
	// Stored at request, before compaction is actually run
	MetaKeyCompactScheduled MetaKey = []byte("scheduled_compaction")

	// Stores the last finished compaction revision.
	// Stored after compaction is finished
	MetaKeyCompactFinished MetaKey = []byte("finished_compaction")
)

// A Command az alparancsok úniója, amit raft log entryként küldönk az állapotgépnek
type Command struct {
	Type   CmdType     `json:"type"`
	Put    *PutCmd     `json:"put,omitempty"`
	Delete *DeleteCmd  `json:"delete,omitempty"`
	Txn    *TxnCommand `json:"txn,omitempty"`
}

type CmdType string

const (
	CmdPut    CmdType = "PUT"
	CmdDelete CmdType = "DEL"
	CmdTxn    CmdType = "TXN"
)

func EncodeCommand(cmd Command) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cmd)
	return buf.Bytes(), err
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&cmd)
	return cmd, err
}

// A PutCmd alparancs a PUT művelethez szükséges adatokat tartalmazza
type PutCmd struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`

	// Az LeaseID egy lease hozzáadásához szükséges, amennyiben 0, akkor nem lesz lease-e
	LeaseID int64 `json:"lease_id,omitempty"`

	// A PrevEntry kapcsolóval visszaküldjök a kulcs előző értékét, ami egy extra olvasást igényel
	PrevEntry bool `json:"prev_kv,omitempty"`

	// Az IgnoreValue kapcsolóval frissíthetjük a lease-et egy kulcson anélkül, hogy megváltoztatnánk az értékét
	IgnoreValue bool `json:"ignore_value,omitempty"`

	// Az RenewLease kapcsolóval újrahasznosíthatjuk a meglévő lease-et
	RenewLease bool `json:"renew_lease,omitempty"`
}

func (c *PutCmd) Check() error {
	if len(c.Key) == 0 {
		return errors.New("key is required")
	}
	if len(c.Value) == 0 {
		return errors.New("value is required")
	}
	if c.LeaseID < 0 {
		return errors.New("lease id cannot be negative")
	}
	if c.IgnoreValue && c.RenewLease {
		return errors.New("ignore_value and renew_lease cannot be both true")
	}
	if c.RenewLease && c.LeaseID != 0 {
		return errors.New("lease_id must not be set when renew_lease is true")
	}

	return nil
}

// A DeleteCmd alparancs a DELETE művelethez szükséges adatokat tartalmazza
type DeleteCmd struct {
	// A kulcs
	Key []byte `json:"key"`

	// Az opcionális End a [Key, End) tartományt definiálja, és ha nem nil akkor a [Key, End) tartományt töröljük
	End []byte `json:"end,omitempty"`

	// A PrevEntries kapcsolóval visszaküldjök a törölt kulcs előző értékét, ami egy extra olvasást igényel
	PrevEntries bool `json:"prev_entries,omitempty"`
}

func (c *DeleteCmd) Check() error {
	if len(c.Key) == 0 {
		return errors.New("key is required")
	}
	return nil
}

// A Result a Command végrehajtásának eredményét tartalmazza, a Header pedig a művelethez tartozó reviziót
type Result struct {
	Header ResultHeader `json:"header"`

	Error  error         `json:"error,omitempty"`
	Put    *PutResult    `json:"put,omitempty"`
	Delete *DeleteResult `json:"delete,omitempty"`
	Txn    *TxnResult    `json:"txn,omitempty"`
	Range  *RangeResult  `json:"range,omitempty"`
}

// A ResultHeader minden eredmény közös metaadatait tartalmazza
type ResultHeader struct {
	// Revision az írás végrehajtása utáni revíziót tartalmazza. Olvasás esetén ez a revízió, amin a lekérdezést kiszolgáltuk
	Revision int64 `json:"revision"`
	// NodeID a végrahtó node azonosítója
	NodeID string `json:"node_id"`
	// RaftTerm a jelenlegi raft term
	RaftTerm uint64 `json:"raft_term"`
	// RaftIndex a jelenlegi raft index
	RaftIndex uint64 `json:"raft_index"`
}

type PutResult struct {
	// PrevEntry az előző értéket tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntry *Entry `json:"prev_entry,omitempty"`
}

type DeleteResult struct {
	// NumDeleted az eltávolított kulcsok számát tartalmazza
	NumDeleted int64 `json:"num_deleted"`

	// PrevEntries az eltávolított kulcsok előző értékeit tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntries []Entry `json:"prev_entries,omitempty"`
}

type TxnCommand struct {
	Comparisons []Comparison `json:"comparisons"`
	Success     []TxnOp      `json:"success"`
	Failure     []TxnOp      `json:"failure"`
}

type TxnOpType string

const (
	TxnOpPut    TxnOpType = "PUT"
	TxnOpDelete TxnOpType = "DEL"
	TxnOpRange  TxnOpType = "RANGE"
)

// TxnOp egy művelet egy TxnCommand-ben, lehet PUT, DELETE vagy RANGE
type TxnOp struct {
	Type   TxnOpType  `json:"type"`
	Put    *PutCmd    `json:"put,omitempty"`
	Delete *DeleteCmd `json:"delete,omitempty"`
	Range  *RangeCmd  `json:"range,omitempty"`
}

// A RangeCmd egy olvasási művelet, ami egy tranzakcióban szereplhet
// NEM jelenhet meg felső szintű Command-ként, mert az olvasások nem mennek a raft-on keresztül
// De egy tranzakcióban az olvasásnak ugyanabban a revízióban kell történnie, mint az írások, az atomicitás miatt
// Így a raft-on megy keresztül a tranzakció többi részével
//
// A RangeCmd a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva, és ha End nil akkor csak a Key-t olvassuk
type RangeCmd struct {
	Key   []byte `json:"key"`
	End   []byte `json:"end,omitempty"`
	Limit int64  `json:"limit,omitempty"`

	// A Linearizable kapcsolóval biztosíthatjuk, hogy a lekérdezés a legfrissebb adatokat adja vissza,
	// de ez extra cluster roundtripet igényel, hogy megerősítsük mi vagyunk a vezető
	Linearizable bool `json:"linearizable,omitempty"`

	// A Revision rögzít az olvasást egy adott revízióra (0 = aktuális)
	// Egy Txn-ben ez szinte mindig 0 (olvasás a txn revíziójánál)
	Revision int64 `json:"revision,omitempty"`

	// A CountOnly kapcsoló csak a számlálót adja vissza, nem az aktuális bejegyzéseket
	CountOnly bool `json:"count_only,omitempty"`

	// ha true akkor a kulcsokat prefixként kezeljük, és az End mezőt figyelmen kívül hagyjuk
	// illetve felülírjuk a End mezőt a Key + 0xFF értékére, így a [Key, Key+0xFF) tartományt olvassuk,
	// ami minden olyan kulcsot tartalmaz, ami a Keyyel kezdődik
	Prefix bool `json:"prefix,omitempty"`
}

func (c *RangeCmd) Check() error {
	if len(c.Key) == 0 {
		return errors.New("key is required")
	}
	if c.Limit < 0 {
		return errors.New("limit cannot be negative")
	}
	// ez nem hiva, ha revision negativ akkor targetRev = 0 lesz
	// if c.Revision < 0 {
	// 	return errors.New("revision cannot be negative")
	// }
	return nil
}

// A RangeResult egy Range művelet eredménye
type RangeResult struct {
	Entries []Entry `json:"entries"`
	Count   int     `json:"count"` // teljes egyezésű kulcsok száma (nagyobb lehet, mint len(Entries) ha limit volt alkalmazva)
}

type TxnResult struct {
	// A Success jelzi, hogy melyik ág lett választva (true = összehasonlítások sikeresek voltak)
	Success bool `json:"success"`

	// A Results egy bejegyzést tartalmaz az összes operációhoz a választott ágban, sorrendben
	Results []TxnOpResult `json:"results"`
}

// A TxnOpResult unió egy tranzakcióban szereplő műveleti eredményt tartalmazza
// Pontosan egy mező nem nil, amely az operáció típusához illeszkedik
type TxnOpResult struct {
	Put    *PutResult    `json:"put,omitempty"`
	Delete *DeleteResult `json:"delete,omitempty"`
	Range  *RangeResult  `json:"range,omitempty"`
}

// Entry a valódi tipus amit az adatbázisban tárolunk
type Entry struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	CreateRev int64  `json:"create_revision"`
	ModRev    int64  `json:"mod_revision"`
	Version   int64  `json:"version"`
	LeaseID   int64  `json:"lease_id,omitempty"`
}

var EmptyEntry Entry // nulla érték, nem létező kulcsokhoz
