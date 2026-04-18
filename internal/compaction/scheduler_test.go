package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/balits/kave/internal/command"
	"github.com/balits/kave/internal/fsm"
	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/metrics"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/peer"
	"github.com/balits/kave/internal/schema"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/backend"
	"github.com/balits/kave/internal/util"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func newTestScheduler(t *testing.T, threshold int64, ticker util.Ticker, isLeaderValue bool, opts *Options) *CompactionScheduler {
	t.Helper()
	isLeader := func() bool { return isLeaderValue }

	logger := slog.Default()
	reg := metrics.InitTestPrometheus()
	backend := backend.New(reg, slog.Default(), storage.Options{
		Kind:           storage.StorageKindInMemory,
		InitialBuckets: schema.AllBuckets,
	})
	store := mvcc.NewKvStore(reg, logger, backend)
	t.Cleanup(func() { backend.Close() })

	fsm := fsm.New(logger, peer.TestPeer(), store, nil, nil)
	var logIndex atomic.Uint64
	propose := func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		bs, err := command.Encode(cmd)
		if err != nil {
			return nil, err
		}
		index := logIndex.Add(1)
		log := &raft.Log{
			Term:  1,
			Index: index,
			Type:  raft.LogCommand,
			Data:  bs,
		}

		applyResult := fsm.Apply(log)
		result, ok := applyResult.(command.Result)
		if !ok {
			return nil, fmt.Errorf("propose error: unexpected result type")
		}
		return &result, nil
	}

	var o Options
	if opts != nil {
		o = *opts
	} else {
		o = Options{
			Threshold:   threshold,
			IntervalMin: DefaultIntervalMin,
			MaxRevGap:   math.MaxInt64,
		}
	}

	cs := NewScheduler(logger, store, propose, isLeader, &o)
	cs.ticker = ticker
	fsm.RegisterObservers(cs) // for write pressure tests
	return cs
}

func mustPut(t *testing.T, propose util.ProposeFunc, key, value string) {
	cmd := command.Command{
		Kind: command.KindPut,
		Put: &command.CmdPut{
			Key:   []byte(key),
			Value: []byte(value),
		},
	}
	result, err := propose(t.Context(), cmd)
	require.NoError(t, err, "proposing Put(%s,%s) failed", key, value)
	require.NotNil(t, result)
	require.NotNil(t, result.Put)
}

func loop(t *testing.T, cs *CompactionScheduler) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go cs.Run(ctx)
}

func Test_CompactionScheduler_ThresholdNotMet(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker()
	cs := newTestScheduler(t, threshold, ft, true, nil)
	cs.ctx = t.Context()

	for i := range threshold - 1 {
		mustPut(t, cs.propose, fmt.Sprintf("foo%d", i), "bar")
	}
	cs.tick(false)
	_, compactedRev := cs.store.Revisions()
	require.Equal(t, int64(0), compactedRev, "threshold not met, but compacted rev stil bumped up")
	cs.tick(false)
	_, compactedRev = cs.store.Revisions()
	require.Equal(t, int64(0), compactedRev, "threshold not met, but compacted rev stil bumped up")
}

func Test_CompactionScheduler_ThresholdMet(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker()
	cs := newTestScheduler(t, threshold, ft, true, nil)
	cs.ctx = t.Context()

	var lastCompactedRev int64
	for i := range threshold {
		mustPut(t, cs.propose, fmt.Sprintf("1foo%d", i), "bar")
	}
	cs.tick(false)

	_, lastCompactedRev = cs.store.Revisions()
	require.Equal(t, int64(0), lastCompactedRev, "threshold not met, but compacted rev stil bumped up")

	for i := range threshold {
		mustPut(t, cs.propose, fmt.Sprintf("2foo%d", i), "bar")
	}
	cs.tick(false)

	_, lastCompactedRev = cs.store.Revisions()
	require.Equal(t, int64(10), lastCompactedRev, "threshold met, shouldve have bumped compactedRev")

	N := 10
	for n := range N {
		for i := range threshold {
			mustPut(t, cs.propose, fmt.Sprintf("foo%d-%d", i, i), "bar")
		}
		cs.tick(false)

		_, lastCompactedRev = cs.store.Revisions()
		expectedRev := int64(10 + (n+1)*10)
		require.Equal(t, expectedRev, lastCompactedRev, "threshold met, shouldve have bumped compactedRev")
	}
}

func Test_CompactionScheduler_DoesntDeleteMoreThanThreshold(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker()
	cs := newTestScheduler(t, threshold, ft, true, nil)
	cs.ctx = t.Context()

	N := 10
	x := 10
	for n := range N {
		for i := range int(threshold) * x {
			mustPut(t, cs.propose, fmt.Sprintf("foo%d-%d", i, i), "bar")
		}

		cs.tick(false)
		_, lastCompactedRev := cs.store.Revisions()
		t.Logf("\nafter %d: putTotal=%d, lastCompacted=%d\n", n, (n+1)*int(threshold)*x, lastCompactedRev)
		require.Equal(t, n*int(threshold)*x, int(lastCompactedRev))
	}

}

func Test_CompactionScheduler_DoesntRegress(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, threshold, ft, true, nil)
	cs.ctx = t.Context()

	for i := range threshold {
		mustPut(t, cs.propose, fmt.Sprintf("key-%d", i), "bar")
	}
	cs.tick(false) // first empty tick

	cs.tick(false)
	_, lastCompactedRev := cs.store.Revisions()
	t.Log("lastCompactedRev =", lastCompactedRev)

	mustPut(t, cs.propose, "last", "bar")
	cs.tick(false)
	_, latest := cs.store.Revisions()

	require.Equal(t, lastCompactedRev, latest)
}

func Test_CompactionScheduler_LeadershipLost_NoCompaction(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, threshold, ft, true, nil)

	for i := range threshold {
		mustPut(t, cs.propose, fmt.Sprintf("key-%d", i), "bar")
	}

	oldPropose := cs.propose
	var calls atomic.Int64
	cs.propose = func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		if cmd.Kind == command.KindCompaction && cmd.Compaction.TargetRev != 0 {
			calls.Add(1)
		}
		return oldPropose(ctx, cmd)
	}

	loop(t, cs)
	time.Sleep(20 * time.Millisecond)

	cs.OnLeadershipLost()
	time.Sleep(20 * time.Millisecond)

	go ft.TickOrDone(t.Context())

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(0), calls.Load())
}

func Test_CompactionScheduler_LeadershipRegained_ResumeCompaction(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, threshold, ft, true, nil)

	for i := range threshold * 2 {
		mustPut(t, cs.propose, fmt.Sprintf("key-%d", i), "bar")
	}

	var calls atomic.Int64
	oldPropose := cs.propose
	cs.propose = func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		if cmd.Kind == command.KindCompaction && cmd.Compaction.TargetRev != 0 {
			calls.Add(1)
		}
		return oldPropose(ctx, cmd)
	}

	loop(t, cs)
	time.Sleep(10 * time.Millisecond)

	cs.OnLeadershipLost()
	time.Sleep(10 * time.Millisecond)
	cs.OnLeadershipGranted()
	time.Sleep(10 * time.Millisecond)

	ft.TickOrDone(t.Context())
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, int64(0), calls.Load(), "first tick is always a no-op")

	// put more keys so threshold is met on tick 2
	for i := range threshold {
		mustPut(t, cs.propose, fmt.Sprintf("key2-%d", i), "bar")
	}

	// tick 2: compacts to the revision recorded at tick 1
	ft.TickOrDone(t.Context())
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, int64(1), calls.Load())
	_, lastCompactedRev := cs.store.Revisions()
	require.Greater(t, lastCompactedRev, int64(0))
}

func Test_CompactionScheduler_CancelledContext_DoesntHang(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker()
	cs := newTestScheduler(t, threshold, ft, true, nil)
	go cs.Run(t.Context())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		cs.Run(ctx)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// we might wait for compaction to finish, we dont hang forever
	case <-time.After(2 * time.Second):
		t.Fatal("compactor hung during stop while compacting")
	}
}

func Test_CompactionScheduler_StopDuringActiveCompaction(t *testing.T) {

	threshold := int64(10)
	ft := util.NewFakeTicker()
	cs := newTestScheduler(t, threshold, ft, true, nil)
	go cs.Run(t.Context())

	done := make(chan struct{})
	go func() {
		cs.Run(t.Context())
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cs.Stop()

	select {
	case <-done:
		// we might wait for compaction to finish, we dont hang forever
	case <-time.After(2 * time.Second):
		t.Fatal("compactor hung during stop while compacting")
	}
}

// real compaction tests

func Test_CompactionScheduler_CompactsToCorrectRevision(t *testing.T) {

	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 3, ft, true, nil)
	cs.ctx = t.Context()

	mustPut(t, cs.propose, "a", "v1") // rev 1
	mustPut(t, cs.propose, "a", "v2") // rev 2 — supersedes rev 1
	mustPut(t, cs.propose, "b", "v1") // rev 3
	mustPut(t, cs.propose, "c", "v1") // rev 4

	// tick 1: candidateRev = 4, nothing compacted yet
	cs.tick(false)
	_, compacted := cs.store.Revisions()
	require.Equal(t, int64(0), compacted, "no compaction on first tick")

	mustPut(t, cs.propose, "d", "v1") // rev 5 — enough to exceed threshold from rev 4

	// tick 2: compacts to rev 4 (the previous candidate)
	cs.tick(false)
	_, compacted = cs.store.Revisions()
	require.Equal(t, int64(4), compacted)
}

func Test_CompactionScheduler_OldRevisionBecomesUnreadable(t *testing.T) {

	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 2, ft, true, nil)
	cs.ctx = t.Context()

	mustPut(t, cs.propose, "key", "v1") // rev 1
	mustPut(t, cs.propose, "key", "v2") // rev 2

	cs.tick(false)                      // candidateRev = 2
	mustPut(t, cs.propose, "key", "v3") // rev 3
	cs.tick(false)                      // compacts to rev 2

	// rev 1 must be rejected
	r := cs.store.(*mvcc.KvStore).NewReader()
	_, _, _, err := r.Range([]byte("key"), nil, 1, 0)
	require.ErrorIs(t, err, kv.ErrCompacted)

	// current state is still readable
	entries, _, _, err := r.Range([]byte("key"), nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "v3", string(entries[0].Value))
}

func Test_CompactionScheduler_SupersededRevisionGone_LatestRetained(t *testing.T) {

	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 2, ft, true, nil)
	cs.ctx = t.Context()

	mustPut(t, cs.propose, "a", "v1") // rev 1
	mustPut(t, cs.propose, "a", "v2") // rev 2 supersedes v1
	mustPut(t, cs.propose, "b", "v1") // rev 3

	cs.tick(false) // candidateRev = 0

	cs.tick(false)                    // candidateRev = 3
	mustPut(t, cs.propose, "c", "v1") // rev 4
	cs.tick(false)                    // compacts to rev 3

	_, lastCompactedRev := cs.store.Revisions()
	t.Log("lastCompactedRev =", lastCompactedRev)

	var err error

	r := cs.store.(*mvcc.KvStore).NewReader()

	_, _, _, err = r.Range([]byte("a"), nil, 1, 0)
	require.ErrorIs(t, err, kv.ErrCompacted, "1 is below compaction rev")

	_, _, _, err = r.Range([]byte("a"), nil, 2, 0)
	require.ErrorIs(t, err, kv.ErrCompacted, "expected 2 is also below compaction rev")

	entries, _, _, err := r.Range([]byte("a"), nil, 0, 0)
	require.NoError(t, err, "expected current read still returns latest value")
	require.Equal(t, "v2", string(entries[0].Value))

	entries, _, _, err = r.Range([]byte("b"), nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "v1", string(entries[0].Value))
}

func Test_CompactionScheduler_ThresholdNotMet_NoCompaction(t *testing.T) {

	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 100, ft, true, nil)
	cs.ctx = t.Context()

	mustPut(t, cs.propose, "a", "1")
	mustPut(t, cs.propose, "b", "2")

	cs.tick(false) // candidateRev = 2
	mustPut(t, cs.propose, "c", "3")
	cs.tick(false)

	_, compacted := cs.store.Revisions()
	require.Equal(t, int64(0), compacted)
}

// test cases to check write pressure works correclty

func Test_CompactionScheduler_WritePressure_TriggerFires(t *testing.T) {

	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 0, ft, true, &Options{
		IntervalMin: 30,
		Threshold:   100,
		MaxRevGap:   10,
	})

	var compactions atomic.Int64
	oldPropose := cs.propose
	cs.propose = func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		if cmd.Kind == command.KindCompaction {
			compactions.Add(1)
		}
		return oldPropose(ctx, cmd)
	}

	loop(t, cs)
	time.Sleep(10 * time.Millisecond)

	for i := range cs.opts.MaxRevGap * 2 {
		mustPut(t, oldPropose, fmt.Sprintf("key%d", i), "bar")
	}

	// process writes, writePressureC and compaction
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		_, compacted := cs.store.Revisions()
		t.Log("compacted =", compacted)
		return compacted > 0
	}, time.Second, 10*time.Millisecond)

}

func Test_CompactionScheduler_WritePressure_IgnoredAsNonLeader(t *testing.T) {

	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 0, ft, true, &Options{
		IntervalMin: 30,
		Threshold:   100,
		MaxRevGap:   10,
	})
	cs.isLeader = func() bool { return false }

	var compactions atomic.Int64
	oldPropose := cs.propose
	cs.propose = func(ctx context.Context, cmd command.Command) (*command.Result, error) {
		if cmd.Kind == command.KindCompaction {
			compactions.Add(1)
		}
		return oldPropose(ctx, cmd)
	}

	loop(t, cs)
	time.Sleep(10 * time.Millisecond)

	for i := range cs.opts.MaxRevGap * 4 {
		mustPut(t, oldPropose, fmt.Sprintf("key%d", i), "bar")
	}

	// process writes, writePressureC and compaction
	time.Sleep(100 * time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	_, compacted := cs.store.Revisions()
	require.Equal(t, int64(0), compacted, "non-leader should not compact even under write pressure")
}

func Test_CompactionScheduler_WritePressure_NoDuplicateSignals(t *testing.T) {

	opts := Options{
		Threshold:   2,
		IntervalMin: 30,
		MaxRevGap:   5,
	}
	cs := newTestScheduler(t, 10, util.NewFakeTicker(), true, &opts)

	for i := range 50 {
		mustPut(t, cs.propose, fmt.Sprintf("key-%d", i), "bar")
	}

	require.LessOrEqual(t, len(cs.writePressureC), 1,
		"writePressureC should never have more than one pending signal")
}

func Test_CompactionScheduler_NormalInterval_WhenGapIsSmall(t *testing.T) {

	opts := Options{
		Threshold:   2,
		IntervalMin: 30,
		MaxRevGap:   100, // high enough
	}
	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 0, ft, true, &opts)
	cs.ctx = t.Context() // needed if we dont have loop

	mustPut(t, cs.propose, "a", "v1") // rev 1
	mustPut(t, cs.propose, "b", "v1") // rev 2
	mustPut(t, cs.propose, "c", "v1") // rev 3

	require.Equal(t, 0, len(cs.writePressureC), "no write pressure triggered when gap is small")

	cs.tick(false) // first empty tick
	_, compacted := cs.store.Revisions()
	require.Equal(t, int64(0), compacted)

	mustPut(t, cs.propose, "d", "v1") // rev 4

	cs.tick(false)
	_, compacted = cs.store.Revisions()
	require.Equal(t, int64(3), compacted)

	require.Equal(t, 0, len(cs.writePressureC))
}

func Test_CompactionScheduler_BothIntervalAndBackpressure_WorkTogether(t *testing.T) {
	opts := Options{
		Threshold:   2,
		IntervalMin: 30,
		MaxRevGap:   5,
	}
	ft := util.NewFakeTicker().(*util.FakeTicker)
	cs := newTestScheduler(t, 0, ft, true, &opts)
	cs.ctx = t.Context() // needed if we dont have loop

	// phase 1: normal periodic tick while gap is small
	mustPut(t, cs.propose, "a", "v1") // rev 1
	mustPut(t, cs.propose, "b", "v1") // rev 2
	mustPut(t, cs.propose, "c", "v1") // rev 3

	require.Equal(t, 0, len(cs.writePressureC), "gap=3 < MaxRevGap=5, no urgent signal yet")

	cs.tick(false) // tick 1: records candidateRev=3, no compaction
	cs.tick(false) // tick 2: compacts to 3

	_, compacted := cs.store.Revisions()
	require.Equal(t, int64(3), compacted, "periodic path should have compacted")

	// phase 2: burst of writes exceeds MaxRevGap, urgent path takes over
	// compacted=3, so gap reaches MaxRevGap=5 at rev 3+5=8
	for i := range 5 {
		mustPut(t, cs.propose, fmt.Sprintf("burst-%d", i), "v1")
	}

	// record new candidateRev without compacting
	cs.tick(false)
	require.Equal(t, 1, len(cs.writePressureC), "gap exceeded MaxRevGap, urgent signal expected")

	// drain the signal and run a forced tick, as the run loop would
	<-cs.writePressureC
	cs.tick(true)

	_, compacted = cs.store.Revisions()
	require.Greater(t, compacted, int64(3), "urgent path should have advanced compactedRev beyond periodic result")
}
