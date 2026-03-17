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
	Type            CmdType             `json:"type"`
	Put             *PutCmd             `json:"put,omitempty"`
	Delete          *DeleteCmd          `json:"delete,omitempty"`
	Txn             *TxnCmd             `json:"txn,omitempty"`
	LeaseGrant      *LeaseGrantCmd      `json:"lease_grant,omitempty"`
	LeaseRevoke     *LeaseRevokeCmd     `json:"lease_revoke,omitempty"`
	LeaseKeepAlive  *LeaseKeepAliveCmd  `json:"lease_keep_alive,omitempty"`
	LeaseCheckpoint *LeaseCheckpointCmd // its not a client facing command, so no need for json tag
	LeaseExpired    *LeaseExpiredCmd    // its not a client facing command, so no need for json tag
}

type CmdType string

const (
	CmdPut             CmdType = "KV_PUT"
	CmdDelete          CmdType = "KV_DEL"
	CmdTxn             CmdType = "KV_TXN"
	CmdLeaseGrant      CmdType = "LEASE_GRANT"
	CmdLeaseRevoke     CmdType = "LEASE_REVOKE"
	CmdLeaseKeepAlive  CmdType = "LEASE_KEEP_ALIVE"
	CmdLeaseCheckpoint CmdType = "LEASE_CHECKPOINT"
	CmdLeaseExpired    CmdType = "LEASE_EXPIRED"
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
