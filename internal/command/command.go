package command

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

	LeaseCheckpoint      *CmdLeaseCheckpoint
	LeaseExpired         *CmdLeaseExpire
	Compaction           *CompactionCmd
	OTWriteAll           *CmdOTWriteAll
	OTGenerateClusterKey *CmdOTGenerateClusterKey
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

type InvalidCommandPayloadError struct {
	kind CmdKind
}

func (e *InvalidCommandPayloadError) Error() string {
	return fmt.Sprintf("command error: invalid or nil payload or command kind %s", e.kind)
}

func (cmd *Command) Check() error {
	switch cmd.Kind {
	case KindPut:
		if cmd.Put == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindDelete:
		if cmd.Delete == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindTxn:
		if cmd.Txn == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindLeaseGrant:
		if cmd.LeaseGrant == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindLeaseRevoke:
		if cmd.LeaseRevoke == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindLeaseKeepAlive:
		if cmd.LeaseKeepAlive == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindLeaseLookup:
		if cmd.LeaseLookup == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindLeaseCheckpoint:
		if cmd.LeaseCheckpoint == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindLeaseExpire:
		if cmd.LeaseExpired == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindCompaction:
		if cmd.Compaction == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindOTWriteAll:
		if cmd.OTWriteAll == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	case KindOTGenerateClusterKey:
		if cmd.OTGenerateClusterKey == nil {
			return &InvalidCommandPayloadError{cmd.Kind}
		}
	}

	return nil
}
