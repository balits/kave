package api

import (
	"errors"

	"github.com/balits/kave/internal/types"
)

// A RangeRequest egy olvasási művelet, ami egy tranzakcióban szereplhet
// NEM jelenhet meg felső szintű Command-ként, mert az olvasások nem mennek a raft-on keresztül
// De egy tranzakcióban az olvasásnak ugyanabban a revízióban kell történnie, mint az írások, az atomicitás miatt
// Így a raft-on megy keresztül a tranzakció többi részével
//
// A RangeRequest a kulcsok egy tartományát olvassa, ahol a tartomány [Key, End) formában van megadva, és ha End nil akkor csak a Key-t olvassuk
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

func (c *RangeRequest) Check() error {
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

// A RangeResponse egy Range művelet eredménye
type RangeResponse struct {
	Header ResponseHeader `json:"header"`
	RangeResponseNoHeader
}

type RangeResponseNoHeader struct {
	Entries []types.KvEntry `json:"entries"`
	Count   int             `json:"count"` // teljes egyezésű kulcsok száma (nagyobb lehet, mint len(Entries) ha limit volt alkalmazva)
}
