package fsm

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/lease"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/ot"
	"github.com/hashicorp/raft"
)

var (
	ErrStateMachineError = errors.New("FSM error")
	// Hiba ha az fsm üres értékkel tér vissza, vagy ha a kívánt subresult üres
	ErrNilApplyResult = fmt.Errorf("%w: nil result from FSM", ErrStateMachineError)
)

type Fsm struct {
	myID           string // hack to set Result.Header.NodeID
	store          *mvcc.KvStore
	engine         *mvcc.Engine
	lm             *lease.LeaseManager
	om             *ot.OTManager
	metrics        *metrics.RaftMetrics
	writeObservers []WriteObserver
	logger         *slog.Logger
}

func NewWithEngine(logger *slog.Logger, store *mvcc.KvStore, lm *lease.LeaseManager, om *ot.OTManager, engine *mvcc.Engine, nodeID string) *Fsm {
	f := &Fsm{
		engine: engine,
		lm:     lm,
		store:  store,
		om:     om,
		logger: logger.With("component", "fsm"),
		myID:   nodeID,
	}
	return f
}

func New(logger *slog.Logger, store *mvcc.KvStore, lm *lease.LeaseManager, om *ot.OTManager, nodeID string) *Fsm {
	return NewWithEngine(logger, store, lm, om, mvcc.NewEngine(store, lm), nodeID)
}

// SetMetrics is needed for a two phase init of the fsm
// because of the unavoidable circular dependency:
//
// 1) raft needs fsm
//
// 2) fsm.metrics need raft
func (f *Fsm) SetMetrics(m *metrics.RaftMetrics) {
	f.metrics = m
}

func (f *Fsm) RegisterObservers(obs ...WriteObserver) {
	f.writeObservers = append(f.writeObservers, obs...)
}

// Apply should be as fast as possible, therefore:
// 1) validate command structure and arguments before callig Apply
func (f *Fsm) Apply(log *raft.Log) interface{} {
	// this makes test way easier
	if f.metrics != nil {
		start := time.Now()
		defer func() { f.metrics.ApplyDurationSec.Observe(time.Since(start).Seconds()) }()
		f.metrics.ApplyTotal.Inc()
	}

	cmd, err := command.Decode(log.Data)
	if err != nil {
		return command.Result{Error: err}
	}

	f.store.UpdateRaftMeta(log.Index, log.Term)

	var res command.Result

	switch cmd.Kind {
	case command.KindPut, command.KindDelete, command.KindTxn:
		res = f.applyKv(cmd)
	case command.KindLeaseGrant, command.KindLeaseRevoke, command.KindLeaseKeepAlive, command.KindLeaseLookup, command.KindLeaseCheckpoint:
		res = f.applyLease(cmd)
	case command.KindCompact:
		res = f.applyCompaction(cmd)
	case command.KindOTWriteAll, command.KindOTGenerateClusterKey:
		res = f.applyOT(cmd)
	case "":
		panic("No command kind specified")
	default:
		panic(fmt.Sprintf("Unsupported command kind: %v", cmd.Kind))
	}

	// set fields that apply could touch
	res.Header.RaftTerm = log.Term
	res.Header.RaftIndex = log.Index
	res.Header.NodeID = f.myID

	if res.Error == nil {
		for _, o := range f.writeObservers {
			o.OnWrite(res.Header.Revision)
		}
	}

	return res
}

func (f *Fsm) applyKv(cmd command.Command) command.Result {
	switch cmd.Kind {
	case command.KindPut, command.KindDelete, command.KindTxn:
	default:
		panic(fmt.Sprintf("applyKv called with non-kv command: %s", cmd.Kind))
	}

	res, err := f.engine.ApplyWrite(cmd)
	if err != nil {
		return command.Result{Error: err}
	}
	return *res
}

// TODO: should we create more meaningful return type for lease commands?
func (f *Fsm) applyLease(cmd command.Command) (res command.Result) {
	switch cmd.Kind {
	case command.KindLeaseGrant, command.KindLeaseRevoke, command.KindLeaseKeepAlive, command.KindLeaseLookup, command.KindLeaseCheckpoint:
	default:
		panic(fmt.Sprintf("applyLease called with non-lease command: %s", cmd.Kind))
	}

	var err error
	switch cmd.Kind {
	case command.KindLeaseGrant:
		var lease *lease.Lease
		lease, err = f.lm.Grant(cmd.LeaseGrant.LeaseID, cmd.LeaseGrant.TTL)
		if err == nil {
			res.LeaseGrant = &command.ResultLeaseGrant{
				TTL:     lease.TTL,
				LeaseID: lease.ID,
			}
		}

	case command.KindLeaseRevoke:
		var found, revoked bool
		found, revoked = f.lm.Revoke(cmd.LeaseRevoke.LeaseID)
		res.LeaseRevoke = &command.ResultLeaseRevoke{
			Found:   found,
			Revoked: revoked,
		}

	case command.KindLeaseKeepAlive:
		var ttl int64
		ttl, err = f.lm.KeepAlive(cmd.LeaseKeepAlive.LeaseID)
		if err == nil {
			res.LeaseKeepAlive = &command.ResultLeaseKeepAlive{
				TTL:     ttl,
				LeaseID: cmd.LeaseKeepAlive.LeaseID,
			}
		}

	case command.KindLeaseLookup:
		l := f.lm.Lookup(cmd.LeaseLookup.LeaseID)
		if l != nil {
			res.LeaseLookup = &command.ResultLeaseLookup{
				LeaseID:      l.ID,
				OriginalTTL:  l.TTL,
				RemainingTTL: l.RemainingTTL(),
			}
		} else {
			err = lease.ErrLeaseNotFound
		}

	case command.KindLeaseCheckpoint:
		f.lm.ApplyCheckpoint(*cmd.LeaseCheckpoint)

	case command.KindLeaseExpire:
		var subres *command.ResultLeaseExpire
		subres, err = f.lm.ApplyExpired(*cmd.LeaseExpired)
		if err != nil {
			res.LeaseExpire = subres
		}

	case "":
		panic("Command Kind not specified")

	default:
		panic(fmt.Sprintf("Unsupported lease command type: %v", cmd.Kind))
	}

	// Lease resultoknál legyen mind az error, mind a result kitölrve
	// igy jóbban látható mi történt
	// TODO: gondoljuk át, talán ez lenne a jobb megoldást a kv parancsoknál is?
	if err != nil {
		res.Error = err
	}
	return res
}

func (f *Fsm) applyCompaction(cmd command.Command) command.Result {
	if cmd.Kind != command.KindCompact {
		panic(fmt.Sprintf("applyCompaction called with non-compaction command: %s", cmd.Kind))
	}

	doneC, err := f.store.Compact(cmd.Compact.TargetRev)
	return command.Result{
		Compact: &command.ResultCompact{
			DoneC: doneC,
			Error: err,
		},
	}
}

func (f *Fsm) applyOT(cmd command.Command) (res command.Result) {
	switch cmd.Kind {
	case command.KindOTGenerateClusterKey, command.KindOTWriteAll:
	default:
		panic(fmt.Sprintf("applyOT called with non-OT command: %s", cmd.Kind))
	}

	var err error
	switch cmd.Kind {
	case command.KindOTGenerateClusterKey:
		var sub *command.ResultOTGenerateClusterKey
		sub, err = f.om.ApplyGenerateClusterKey()
		if err == nil {
			res.OtGenerateClusterKey = sub
		}
	case command.KindOTWriteAll:
		var sub *command.ResultOTWriteAll
		sub, err = f.om.ApplyWriteAll(*cmd.OTWriteAll)
		if err == nil {
			res.OtWriteAll = sub
		}
	}

	if err != nil {
		res.Error = err
	}
	return res
}

// Snapshot also should be fast, just take a pointer to the data
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.store.Snapshot(), nil
}

// Restore can be slower, it will never run concurrently with Apply.
// Also, no metrics should be replayed during restoration!
func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	err := f.store.Restore(snapshot)
	if err != nil {
		return err
	}
	return f.lm.Restore()
}
