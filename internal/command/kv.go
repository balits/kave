package command

import (
	"errors"
	"fmt"

	"github.com/balits/kave/internal/types"
)

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

type PutResult struct {
	// PrevEntry az előző értéket tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntry *types.KvEntry `json:"prev_entry,omitempty"`
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

type DeleteResult struct {
	// NumDeleted az eltávolított kulcsok számát tartalmazza
	NumDeleted int64 `json:"num_deleted"`

	// PrevEntries az eltávolított kulcsok előző értékeit tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntries []types.KvEntry `json:"prev_entries,omitempty"`
}

type TxnCmd struct {
	Comparisons []Comparison `json:"comparisons"`
	Success     []TxnOp      `json:"success"`
	Failure     []TxnOp      `json:"failure"`
}

func (c *TxnCmd) Check() error {
	for _, cmp := range c.Comparisons {
		if err := cmp.Check(); err != nil {
			return fmt.Errorf("invalid comparison: %w", err)
		}
	}
	for _, op := range c.Success {
		if err := op.Check(); err != nil {
			return fmt.Errorf("invalid success operation: %w", err)
		}
	}
	for _, op := range c.Failure {
		if err := op.Check(); err != nil {
			return fmt.Errorf("invalid failure operation: %w", err)
		}
	}
	return nil
}

type TxnResult struct {
	// A Success jelzi, hogy melyik ág lett választva (true = összehasonlítások sikeresek voltak)
	Success bool `json:"success"`

	// A Results egy bejegyzést tartalmaz az összes operációhoz a választott ágban, sorrendben
	Results []TxnOpResult `json:"results"`
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

func (op *TxnOp) Check() error {
	switch op.Type {
	case TxnOpPut:
		if op.Put == nil {
			return errors.New("put command is required for put operation")
		}
		return op.Put.Check()
	case TxnOpDelete:
		if op.Delete == nil {
			return errors.New("delete command is required for delete operation")
		}
		return op.Delete.Check()
	case TxnOpRange:
		if op.Range == nil {
			return errors.New("range command is required for range operation")
		}
		return op.Range.Check()
	default:
		return fmt.Errorf("invalid operation type: %s", op.Type)
	}
}

// A TxnOpResult unió egy tranzakcióban szereplő műveleti eredményt tartalmazza
// Pontosan egy mező nem nil, amely az operáció típusához illeszkedik
type TxnOpResult struct {
	Put    *PutResult    `json:"put,omitempty"`
	Delete *DeleteResult `json:"delete,omitempty"`
	Range  *RangeResult  `json:"range,omitempty"`
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
	Entries []types.KvEntry `json:"entries"`
	Count   int             `json:"count"` // teljes egyezésű kulcsok száma (nagyobb lehet, mint len(Entries) ha limit volt alkalmazva)
}
