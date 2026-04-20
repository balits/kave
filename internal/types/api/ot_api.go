package api

type OTWriteAllRequest struct {
	Blob []byte `json:"blob"`
}

// Only here for future proofing and to adhere to the packages conversions
// Theres nothing to return yet,
// but maybe later we wanna return something interesing (or not)
type OTWriteAllResponse struct {
	Header ResponseHeader `json:"header"`
	OTWriteAllResponseNoHeader
}
type OTWriteAllResponseNoHeader struct{}

// Only here for future proofing and to adhere to the packages conversions
// Theres nothing to return yet,
// but maybe later we wanna return something interesing (or not)
type OTInitRequest struct {
}

type OTInitResponse struct {
	Header ResponseHeader `json:"header"`
	OTInitResponseNoHeader
}

type OTInitResponseNoHeader struct {
	PointA []byte `json:"point_a"`
	Token  []byte `json:"token"`
}

type OTTransferRequest struct {
	Token        []byte `json:"token"`
	PointB       []byte `json:"point_b"`
	Serializable bool   `json:"serializable"`
}

type OTTransferResponse struct {
	Header ResponseHeader `json:"header"`
	OTTransferResponseNoHeader
}

type OTTransferResponseNoHeader struct {
	Ciphertexts [][]byte `json:"ciphertexts"`
}
