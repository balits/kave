package command

import (
	"bytes"
	"encoding/gob"
)

// A Command az alparancsok úniója, amit raft log entryként küldönk az állapotgépnek
// Minden command tipushoz tartozik egy CmdType, illetve kell hozzá egy Check() error függvény is,
// amivel ellenőrizhetjük a parancs helyességét a feldolgozás előtt
// hogy ne a raft log apply előtt derüljön ki, hogy hibás a parancs (illetve ne vesztegessünk több időt az apply-ban)
type Command struct {
	Kind CmdKind `json:"kind"`

	// =====  public, client facing commands =====

	Put            *CmdPut            `json:"put,omitempty"`
	Delete         *CmdDelete         `json:"delete,omitempty"`
	Txn            *CmdTxn            `json:"txn,omitempty"`
	LeaseGrant     *CmdLeaseGrant     `json:"lease_grant,omitempty"`
	LeaseRevoke    *CmdLeaseRevoke    `json:"lease_revoke,omitempty"`
	LeaseKeepAlive *CmdLeaseKeepAlive `json:"lease_keep_alive,omitempty"`
	LeaseLookup    *CmdLeaseLookup    `json:"lease_lookup,omitempty"`

	// =====  private, internal commands =====

	LeaseCheckpoint *CmdLeaseCheckpoint
	LeaseExpired    *CmdLeaseExpire
	Compaction      *CompactionCmd
	OTWriteAll      *CmdOTWriteAll
}

type CmdKind string

const (
	KindPut    CmdKind = "KV_PUT"
	KindDelete CmdKind = "KV_DEL"
	KindTxn    CmdKind = "KV_TXN"

	KindLeaseGrant      CmdKind = "LEASE_GRANT"
	KindLeaseRevoke     CmdKind = "LEASE_REVOKE"
	KindLeaseKeepAlive  CmdKind = "LEASE_KEEP_ALIVE"
	KindLeaseLookup     CmdKind = "LEASE_LOOKUP"
	KindLeaseCheckpoint CmdKind = "LEASE_CHECKPOINT"
	KindLeaseExpire     CmdKind = "LEASE_EXPIRE"

	KindCompaction CmdKind = "COMPACTION"

	KindOTWriteAll           CmdKind = "OT_WRITE_ALL"
	KindOTGenerateClusterKey CmdKind = "OT_GENERATE_CLUSTER_KEY"
)

func Encode(cmd Command) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	err := gob.NewEncoder(buf).Encode(&cmd)
	return buf.Bytes(), err
}

func Decode(data []byte) (cmd Command, err error) {
	err = gob.NewDecoder(bytes.NewBuffer(data)).Decode(&cmd)
	return
}
