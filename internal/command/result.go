package command

import "github.com/balits/kave/internal/types"

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
	PrevEntry *types.Entry `json:"prev_entry,omitempty"`
}

type DeleteResult struct {
	// NumDeleted az eltávolított kulcsok számát tartalmazza
	NumDeleted int64 `json:"num_deleted"`

	// PrevEntries az eltávolított kulcsok előző értékeit tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntries []types.Entry `json:"prev_entries,omitempty"`
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

// A RangeResult egy Range művelet eredménye
type RangeResult struct {
	Entries []types.Entry `json:"entries"`
	Count   int           `json:"count"` // teljes egyezésű kulcsok száma (nagyobb lehet, mint len(Entries) ha limit volt alkalmazva)
}
