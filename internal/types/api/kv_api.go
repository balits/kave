package api

import (
	"errors"
	"fmt"

	"github.com/balits/kave/internal/kv"
)

// A RangeRequest a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva,
// és ha End nil akkor csak a Key-t olvassuk.
// Megadhatjuk a reviziót, amit olvasni akarunk, illetve a CountOnly kapcsolóval
// csak a az kulcsok számát kapjuk vissza.
// Emellett a Serializable kapcsoló szabályozza a konzisztencia modelt
// (Serializable = TRUE == eventual consistency | Serializable = FALSE == Linearizable == string consistency)
type RangeRequest struct {
	Key   []byte `json:"key"`
	End   []byte `json:"end,omitempty"`
	Limit int64  `json:"limit,omitempty"`

	// A Serializable kapcsolóval biztosíthatjuk a lekérdezés konzisztencia modeljét:
	//
	// Ha Serializable = true (aka.: eventual consistency), akkor a lekérdezések a leaderhez érkeznek be (a followerek redirectelnek),
	// de fentartjuk a lehetőséget, hogy az request és a response között új választás történhet és már nem vagyunk vezető
	//
	// Ha Serializable = false (aka.: strong consistency), akkor a lekérdezések a leaderhez érkeznek be (a followerek redirectelnek)
	// a leader megerősíti hogy még mindig leader, ami  extra cluster roundtripet igényel, de így a legfirsebb adatok kapjuk meg
	Serializable bool `json:"serializable,omitempty"`

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

func (r *RangeRequest) Check() error {
	if len(r.Key) == 0 && !r.Prefix {
		return errors.New("key is required")
	}
	if r.Limit < 0 {
		return errors.New("limit cannot be negative")
	}
	if r.Revision < 0 {
		return errors.New("revision cannot be negative")
	}
	// ez nem hiva, ha revision negativ akkor targetRev = 0 lesz
	// if c.Revision < 0 {
	// 	return errors.New("revision cannot be negative")
	// }
	return nil
}

// A RangeResponse egy Range művelet eredménye
type RangeResponse struct {
	Header ResponseHeader `json:"header"`
	RangeResponseNoHeader
}

type RangeResponseNoHeader struct {
	Entries []*kv.Entry `json:"entries"`
	Count   int         `json:"count"` // teljes egyezésű kulcsok száma (nagyobb lehet, mint len(Entries) ha limit volt alkalmazva)
}

// A PutRequest a PUT művelethez szükséges adatokat tartalmazza
type PutRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`

	// Az LeaseID egy lease hozzáadásához szükséges, amennyiben 0, akkor nem lesz lease-e
	LeaseID int64 `json:"lease_id,omitempty"`

	// A PrevEntry kapcsolóval visszaküldjök a kulcs előző értékét, ami egy extra olvasást igényel
	PrevEntry bool `json:"prev_kv,omitempty"`

	// Az IgnoreValue kapcsolóval frissíthetjük a kulcsot az érték módosítása nélkül. Hibát dob a kulcs nem létezett ezelőtt.
	IgnoreValue bool `json:"ignore_value,omitempty"`

	// Az IgnoreLease kapcsolóval frissíthetjük a kulcsot az lease módosítása nélkül. Hibát dob a kulcs nem létezett ezelőtt.
	IgnoreLease bool `json:"ignore_lease,omitempty"`
}

func (r *PutRequest) Check() error {
	if len(r.Key) == 0 {
		return errors.New("key is required")
	}
	if len(r.Value) == 0 && !r.IgnoreValue {
		return errors.New("value is required")
	}
	if r.LeaseID < 0 && !r.IgnoreLease {
		return errors.New("lease id cannot be negative")
	}

	return nil
}

type PutResponse struct {
	Header ResponseHeader `json:"header"`
	PutResponseNoHeader
}

type PutResponseNoHeader struct {
	// PrevEntry az előző értéket tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntry *kv.Entry `json:"prev_entry,omitempty"`
}

// A DeleteRequest a DELETE művelethez szükséges adatokat tartalmazza
type DeleteRequest struct {
	// A kulcs
	Key []byte `json:"key"`

	// Az opcionális End a [Key, End) tartományt definiálja, és ha nem nil akkor a [Key, End) tartományt töröljük
	End []byte `json:"end,omitempty"`

	// A PrevEntries kapcsolóval visszaküldjök a törölt kulcs előző értékét, ami egy extra olvasást igényel
	PrevEntries bool `json:"prev_entries,omitempty"`
}

func (r *DeleteRequest) Check() error {
	if len(r.Key) == 0 {
		return errors.New("key is required")
	}
	return nil
}

type DeleteResponse struct {
	Header ResponseHeader `json:"header"`
	DeleteResponseNoHeader
}

type DeleteResponseNoHeader struct {
	// NumDeleted az eltávolított kulcsok számát tartalmazza
	NumDeleted int64 `json:"num_deleted"`

	// PrevEntries az eltávolított kulcsok előző értékeit tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntries []*kv.Entry `json:"prev_entries,omitempty"`
}

type TxnRequest struct {
	Comparisons []Comparison `json:"comparisons"`
	Success     []TxnOp      `json:"success"`
	Failure     []TxnOp      `json:"failure"`
}

func (r *TxnRequest) Check() error {
	for _, cmp := range r.Comparisons {
		if err := cmp.Check(); err != nil {
			return fmt.Errorf("invalid comparison: %w (value = %+v)", err, cmp)
		}
	}
	for _, op := range r.Success {
		if err := op.Check(); err != nil {
			return fmt.Errorf("invalid success operation: %w (value = %+v)", err, op)
		}
	}
	for _, op := range r.Failure {
		if err := op.Check(); err != nil {
			return fmt.Errorf("invalid failure operation: %w (value = %+v)", err, op)
		}
	}
	return nil
}

type TxnResponse struct {
	Header ResponseHeader `json:"header"`
	TxnResultNoHeader
}

type TxnResultNoHeader struct {
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
	Type   TxnOpType      `json:"type"`
	Put    *PutRequest    `json:"put,omitempty"`
	Delete *DeleteRequest `json:"delete,omitempty"`
	Range  *RangeRequest  `jsgn:"range,omitempty"`
}

func (op *TxnOp) Check() error {
	switch op.Type {
	case TxnOpPut:
		if op.Put == nil {
			return errors.New("txn_put: put request is required")
		}
		if err := op.Put.Check(); err != nil {
			return fmt.Errorf("txn_put: %w", err)
		}
	case TxnOpDelete:
		if op.Delete == nil {
			return errors.New("txn_delete: delete request is required")
		}
		if err := op.Delete.Check(); err != nil {
			return fmt.Errorf("txn_delete: %w", err)
		}
	case TxnOpRange:
		if op.Range == nil {
			return errors.New("txn_range: range request is required")
		}
		if err := op.Range.Check(); err != nil {
			return fmt.Errorf("txn_range: %w", err)
		}
	default:
		return fmt.Errorf("invalid txn_operation type: %s", op.Type)
	}

	return nil
}

// A TxnOpResult unió egy tranzakcióban szereplő műveleti eredményt tartalmazza
// Pontosan egy mező nem nil, amely az operáció típusához illeszkedik
type TxnOpResult struct {
	Put    *PutResponseNoHeader    `json:"put,omitempty"`
	Delete *DeleteResponseNoHeader `json:"delete,omitempty"`
	Range  *RangeResponseNoHeader  `json:"range,omitempty"`
}
