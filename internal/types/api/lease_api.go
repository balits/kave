package api

type LeaseGrantRequest struct {
	TTL     int64 `json:"ttl"`       // time-to-live másodpercben
	LeaseID int64 `json:"id,string"` // kért lease ID, ha 0 a LeaseManager generál egy új ID-t
}

type LeaseGrantResponse struct {
	TTL     int64 `json:"ttl"`       // szerver által generát time-to-live másodpercben
	LeaseID int64 `json:"id,string"` // elkészült lease ID
}

type LeaseRevokeRequest struct {
	LeaseID int64 `json:"id,string"` // törölni való lease ID-ja
}

type LeaseRevokeResponse struct {
	Found   bool `json:"found"`   // jelzi, hogy volt-e Lease ezzel az ID-val
	Revoked bool `json:"revoked"` // jelzi, hogy a talált lease ki lett törölve vagy sem
}

type LeaseKeepAliveRequest struct {
	LeaseID int64 `json:"id,string"` // frissítendő lease ID
}

type LeaseKeepAliveResponse struct {
	TTL     int64 `json:"ttl"`       // új frissített time-to-live másodpercben
	LeaseID int64 `json:"id,string"` // frissítendő lease ID
}

type LeaseLookupRequest struct {
	LeaseID int64 `json:"id,string"`
}

type LeaseLookupResponse struct {
	// LeaseID can only be 0 if lease was not found
	LeaseID      int64 `json:"id,string"`
	OriginalTTL  int64 `json:"original_ttl"`
	RemainingTTL int64 `json:"remaining_ttl"`
}
