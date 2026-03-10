package compactor

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var defaultPollInterval = 10 * time.Millisecond

func runMock(
	t *testing.T,
	pollInterval time.Duration,
	rev,
	compacted,
	window,
	threshold int64,
) *mockCompactable {
	mock := newMockCompactable(rev, compacted, pollInterval)
	opts := CompactorOptions{
		WindowSize:   window,
		Threshold:    threshold,
		PollInterval: pollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)
	ctx, cancel := context.WithCancel(t.Context())
	go c.Run(ctx)
	time.Sleep(pollInterval * 4) // wait at least one tick
	cancel()
	return mock
}

func Test_WindowRetention_CompactsWhenThresholdMet(t *testing.T) {
	mock := runMock(t, defaultPollInterval, 50, 0, 10, 20)
	// 50 - 10 = 40 > 20 - 0 -> compact; compactedRev := 40
	calls := mock.getCalls()
	require.NotEqual(t, 0, len(calls), "expected at least one call to Compact()")
	want := int64(40)
	require.Equal(t, want, calls[0], "first compact rev = %d, want %d", calls[0], want)
}

func Test_WindowRetention_CompactionUpdatesCompactedRev(t *testing.T) {
	// rev=50, window=10, threshold=20, compacted=0
	// First tick: targetRev=40, gap=40 >= 20 → compact at 40
	// After: compactedRev=40
	// Next ticks: targetRev=40, gap=40-40=0 < 20 → skip
	// So we should get exactly one Compact call (assuming rev doesn't change)
	mock := runMock(t, defaultPollInterval, 50, 0, 10, 20)
	// 50 - 10 = 40 > 20 - 0 -> compact; compactedRev := 40
	calls := mock.getCalls()
	if len(calls) != 1 {
		t.Errorf("expected exactly 1 c.Compact() call (threshold prevents re-compact), got %d: %v", len(calls), calls)
	}
	if len(calls) > 0 && calls[0] != 40 {
		t.Errorf("compact rev = %d, want 40", calls[0])
	}
}

func Test_WindowRetention_SkipsBelowThreshold(t *testing.T) {
	mock := runMock(t, defaultPollInterval, 30, 15, 10, 20)
	// 30  - 10 = 20 < 5 = 20 - 15 -> no compaction
	// ^^rev ||            ||   ||
	//       ^^window      ||   ||
	//            threshold^^   ||
	//                          ^^ compactedRev
	calls := mock.getCalls()
	require.Equal(t, 0, len(calls), "no compaction should happen")
}

func Test_WindowRetention_SkipsTargetRevLessOrEqualCompactedRev(t *testing.T) {
	mock := runMock(t, defaultPollInterval, 20, 15, 10, 5)
	// target rev = 10 = 20 - 10
	// compacted  = 15
	calls := mock.getCalls()
	require.Equal(t, 0, len(calls), "no compaction should happen")
}

func Test_WindowRetention_SkipsNegativeTargetRev(t *testing.T) {
	mock := runMock(t, defaultPollInterval, 5, 0, 10, 1)
	// target rev = 10 = 20 - 10
	// compacted  = 15
	calls := mock.getCalls()
	require.Equal(t, 0, len(calls), "no compaction should happen")
}

func Test_WindowRetention_DoubleCompaction(t *testing.T) {
	mock := newMockCompactable(50, 0, defaultPollInterval*2)
	opts := CompactorOptions{
		WindowSize:   10,
		Threshold:    20,
		PollInterval: defaultPollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)
	ctx, cancel := context.WithCancel(t.Context())
	go c.Run(ctx)
	time.Sleep(defaultPollInterval * 4) // wait at least one tick
	calls := mock.getCalls()
	require.Equal(t, int64(40), calls[0], "fist compacted rev = %d, want 40", calls[0])

	// bump version and wait for second compaction
	mock.setRev(100)
	time.Sleep(defaultPollInterval * 4) // wait at least one tick
	calls = mock.getCalls()
	require.Equal(t, int64(90), calls[1], "second compact rev = %d, want 90", calls[1])
	cancel()
}

func Test_WindowRetention_ResumeCompaction(t *testing.T) {
	mock := newMockCompactable(50, 0, defaultPollInterval*2)
	opts := CompactorOptions{
		WindowSize:   10,
		Threshold:    20,
		PollInterval: defaultPollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)
	c.Pause() // no calls before Resume
	ctx, cancel := context.WithCancel(t.Context())
	go c.Run(ctx)
	time.Sleep(defaultPollInterval * 10)
	calls := mock.getCalls()
	require.Equal(t, 0, len(calls), "expected no calls while paused, got %d", len(calls))

	c.Resume()

	time.Sleep(defaultPollInterval * 4) // wait at least one tick
	calls = mock.getCalls()
	require.Equal(t, int64(40), calls[0], "second compact rev = %d, want 40", calls[0])
	cancel()
}

func Test_WindowRetention_StopsExistsWithoutError(t *testing.T) {
	mock := newMockCompactable(50, 0, defaultPollInterval*2)
	opts := CompactorOptions{
		WindowSize:   10,
		Threshold:    20,
		PollInterval: defaultPollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)
	done := make(chan struct{})
	go func() {
		c.Run(t.Context())
		close(done)
	}()

	time.Sleep(defaultPollInterval * 4) // wait at least one tick

	c.Stop()

	select {
	case <-done:
		//ok
	case <-time.After(2 * time.Second):
		t.Fatal("compactor did not exist after c.Stop() was called")
	}
}

func Test_WindowRetention_StopDuringActiveCompaction(t *testing.T) {
	mock := newMockCompactable(50, 0, 200*time.Millisecond)
	opts := CompactorOptions{
		WindowSize:   10,
		Threshold:    20,
		PollInterval: defaultPollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)
	done := make(chan struct{})
	go func() {
		c.Run(t.Context())
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)
	c.Stop()

	select {
	case <-done:
		// we might wait for compaction to finish, we dont hang forever
	case <-time.After(2 * time.Second):
		t.Fatal("compactor hung during stop while compacting")
	}
}

func Test_WindowRetention_CompactErrorDoesNotPanic(t *testing.T) {
	mock := &mockCompactable{
		rev:   50,
		err:   errors.New("compaction failure"),
		delay: defaultPollInterval * 2,
	}
	opts := CompactorOptions{
		WindowSize:   10,
		Threshold:    20,
		PollInterval: defaultPollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)

	calls := mock.getCalls()
	require.Empty(t, calls, "expected no successful calls, got %d", len(calls))

	mock.err = nil
	mock.setRev(100)

	time.Sleep(80 * time.Millisecond)
	calls = mock.getCalls()
	require.Equal(t, int64(90), calls[0], "compact rev after error cleared = %d, want 90", calls[0])

	cancel()
	<-done
}

func Test_WindowRetention_DoubleRunErrors(t *testing.T) {
	mock := newMockCompactable(50, 0, defaultPollInterval*2)
	opts := CompactorOptions{
		WindowSize:   10,
		Threshold:    20,
		PollInterval: defaultPollInterval,
	}
	c := newWindowRetentionCompactor(slog.Default(), mock, opts)
	go c.Run(t.Context())
	time.Sleep(defaultPollInterval * 10)
	secondRun := make(chan struct{})
	go func() {
		c.Run(t.Context())
		close(secondRun)
	}()

	select {
	case <-secondRun:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("second Run() call did not return immedietly")
	}
}
