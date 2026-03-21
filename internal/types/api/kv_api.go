package api

import (
	"errors"

	"github.com/balits/kave/internal/types"
)

// A KvRangeRequest a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva,
// és ha End nil akkor csak a Key-t olvassuk.
// Megadhatjuk a reviziót, amit olvasni akarunk, illetve a CountOnly kapcsolóval
// csak a az kulcsok számát kapjuk vissza.
// Emellett a Serializable kapcsoló szabályozza a konzisztencia modelt
// (Serializable = TRUE == eventual consistency | Serializable = FALSE == Linearizable == string consistency)
type KvRangeRequest struct {
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

func (c *KvRangeRequest) Check() error {
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

// A KvRangeResponse egy Range művelet eredménye
type KvRangeResponse struct {
	Header ResponseHeader `json:"header"`
	KvRangeResponseNoHeader
}

type KvRangeResponseNoHeader struct {
	Entries []types.KvEntry `json:"entries"`
	Count   int             `json:"count"` // teljes egyezésű kulcsok száma (nagyobb lehet, mint len(Entries) ha limit volt alkalmazva)
}

// A KvPutRequest a PUT művelethez szükséges adatokat tartalmazza
type KvPutRequest struct {
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

func (c *KvPutRequest) Check() error {
	if len(c.Key) == 0 {
		return errors.New("key is required")
	}
	if len(c.Value) == 0 {
		return errors.New("value is required")
	}
	if c.LeaseID < 0 {
		return errors.New("lease id cannot be negative")
	}
	if c.IgnoreValue && c.IgnoreLease {
		return errors.New("ignore_value and renew_lease cannot be both true")
	}
	if c.IgnoreLease && c.LeaseID != 0 {
		return errors.New("lease_id must not be set when renew_lease is true")
	}

	return nil
}

type KvPutResponse struct {
	Header ResponseHeader `json:"header"`
	KvPutResponseNoHeader
}

type KvPutResponseNoHeader struct {
	// PrevEntry az előző értéket tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntry *types.KvEntry `json:"prev_entry,omitempty"`
}

// A KvDeleteRequest a DELETE művelethez szükséges adatokat tartalmazza
type KvDeleteRequest struct {
	// A kulcs
	Key []byte `json:"key"`

	// Az opcionális End a [Key, End) tartományt definiálja, és ha nem nil akkor a [Key, End) tartományt töröljük
	End []byte `json:"end,omitempty"`

	// A PrevEntries kapcsolóval visszaküldjök a törölt kulcs előző értékét, ami egy extra olvasást igényel
	PrevEntries bool `json:"prev_entries,omitempty"`
}

func (c *KvDeleteRequest) Check() error {
	if len(c.Key) == 0 {
		return errors.New("key is required")
	}
	return nil
}

type KvDeleteResponse struct {
	Header ResponseHeader `json:"header"`
	KvDeleteResponseNoHeader
}

type KvDeleteResponseNoHeader struct {
	// NumDeleted az eltávolított kulcsok számát tartalmazza
	NumDeleted int64 `json:"num_deleted"`

	// PrevEntries az eltávolított kulcsok előző értékeit tartalmazza, ha PrevEntry kapcsolóval kértük, egyébként nil
	PrevEntries []types.KvEntry `json:"prev_entries,omitempty"`
}
