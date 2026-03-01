package service

import (
	"context"
	"fmt"

	"github.com/balits/kave/internal/common"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/storage"
	"github.com/hashicorp/raft"
)

// KVService az interfész, amely a kulcs-érték műveleteket definiálja.
// Ez a szolgáltatás felelős a kv műveletek végrehajtásáért, és a raft konszenzus mechanizmus használatával biztosítja a konzisztenciát
// Minden művelethez tartozik egy kontextus, amely lehetővé teszi a műveletek időkorlátjának beállítását és a műveletek megszakítását, ha szükséges.
// Illetve egy request+response struktúra, amely a művelet paramétereit és eredményét tartalmazza.
//
// Minden művelet ellenőrzi, hogy a szolgáltatás jelenleg a raft klaszter vezetője-e. Ha nem, akkor egy ErrNotLeader hibát ad vissza.
type KVService interface {
	// Visszaadja a megadott kulcshoz tartozó értéket.
	// Először ellenőrzi, hogy vezetők vagyunk-e, viszont tökéletes lineárisítást csak
	// akkor tudunk garantálni, ha a művelet előtt megvárjuk, hogy apply-oljuk az eddigi logokat ( raft.ApplyIndex == raft.CommitIndex )
	Get(ctx context.Context, req common.GetRequest) (common.GetResponse, error)

	Set(ctx context.Context, req common.SetRequest) (common.SetResponse, error)

	Delete(ctx context.Context, req common.DeleteRequest) (common.DeleteResponse, error)

	//Txn(ctx context.Context, req common.TxnRequest) (common.TxnResponse, error)
}

func NewKVService(raft *raft.Raft, store storage.Storage) KVService {
	return &raftKvService{
		raft:  raft,
		store: store,
	}
}

type raftKvService struct {
	raft  *raft.Raft
	store storage.Storage
}

func (s *raftKvService) Get(ctx context.Context, req common.GetRequest) (common.GetResponse, error) {
	if s.raft.VerifyLeader().Error() != nil {
		return common.GetResponse{}, common.ErrNotLeader
	}
	// TODO: maybe check manually for raft.ApplyIndex() >= raft.CommitIndex()

	raw, err := s.store.Get(kv.BucketKeyMeta, req.Key)
	if err != nil {
		return common.GetResponse{}, err
	}

	entry, err := common.DecodeEntry(raw)
	if err != nil {
		return common.GetResponse{}, err
	}

	if err != nil {
		return common.GetResponse{}, err
	}

	return common.GetResponse{Entry: entry}, nil
}

func (s *raftKvService) Set(ctx context.Context, req common.SetRequest) (common.SetResponse, error) {
	cmd, err := fsm.EncodeCommand(fsm.Command{
		Type:   fsm.CmdSet,
		Bucket: kv.BucketKeyMeta,
		Key:    req.Key,
		Value:  req.Value,
	})
	if err != nil {
		return common.SetResponse{}, err
	}

	fut := s.raft.Apply(cmd, 0)
	if err = waitFuture(ctx, fut); err != nil {
		return common.SetResponse{}, err
	}

	result := fut.Response().(fsm.AppyResult)
	if result.Error() != nil {
		return common.SetResponse{}, fmt.Errorf("%w: %v", common.ErrStateMachineError, result.Error())
	}

	entry := result.SetResult
	if entry == nil {
		return common.SetResponse{}, fmt.Errorf("%w: nil result from fsm", common.ErrStateMachineError)
	}
	return common.SetResponse{Entry: entry}, nil
}

func (s *raftKvService) Delete(ctx context.Context, req common.DeleteRequest) (common.DeleteResponse, error) {
	cmd, err := fsm.EncodeCommand(fsm.Command{
		Type:   fsm.CmdDelete,
		Bucket: kv.BucketKeyMeta,
		Key:    req.Key,
	})
	if err != nil {
		return common.DeleteResponse{}, err
	}

	fut := s.raft.Apply(cmd, 0)
	if err = waitFuture(ctx, fut); err != nil {
		return common.DeleteResponse{}, err
	}

	result := fut.Response().(fsm.AppyResult)
	if result.Error() != nil {
		return common.DeleteResponse{}, fmt.Errorf("%w: %v", common.ErrStateMachineError, result.Error())
	}

	response := common.DeleteResponse{Deleted: result.DeleteResult.Deleted, PrevEntry: result.DeleteResult.PrevEntry}
	return response, nil
}
