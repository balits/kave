package command

type CmdOTWriteAll struct {
	Blob []byte
}

// Only here for future proofing and to adhere to the command packages conversions
// Theres nothing to return yet,
// but maybe later we wanna return something interesing (or not)
type ResultOTWriteAll struct{}

// Only here for future proofing and to adhere to the command packages conversions
// Theres nothing to send as command payload,
// but maybe later we wanna send something interesing (or not)
type CmdOTGenerateClusterKey struct{}

type ResultOTGenerateClusterKey struct {
	Key []byte
}
