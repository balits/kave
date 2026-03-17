package types

import "fmt"

// KvEntry a valódi tipus amit az adatbázisban tárolunk
type KvEntry struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	CreateRev int64  `json:"create_revision"`
	ModRev    int64  `json:"mod_revision"`
	Version   int64  `json:"version"`
	LeaseID   int64  `json:"lease_id,omitempty"`
}

// for debugging
func (e KvEntry) String() string {
	return fmt.Sprintf(
		"Entry{Key: %s, Value: %s, CreateRev: %d, ModRev: %d, Version: %d, LeaseID: %d}",
		string(e.Key),
		string(e.Value),
		e.CreateRev,
		e.ModRev,
		e.Version,
		e.LeaseID,
	)
}
