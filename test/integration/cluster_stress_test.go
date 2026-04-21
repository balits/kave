//go:build stress

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	_http "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/watch"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func Test_Integration_Stress_ConcurrentWriters(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithoutCompactionScheduler(), raftConfigWithFastCompaction())
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var wg sync.WaitGroup
	numRoutines := 10
	writesPerRoutine := 400

	// collect errors here, cuz we cant call require.Error in a go routine
	errCh := make(chan error, numRoutines*writesPerRoutine)

	for i := range numRoutines {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := range writesPerRoutine {
				key := fmt.Sprintf("foo-%d-%d", routineID, j)
				status := do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(key), Value: []byte("val")}, nil)
				if status != http.StatusOK {
					errCh <- fmt.Errorf("routine %d failed to put key %s: status %d", routineID, key, status)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	for i := range numRoutines {
		for j := range writesPerRoutine {
			key := fmt.Sprintf("foo-%d-%d", i, j)
			var resp api.RangeResponse
			require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte(key)}, &resp))
			require.Len(t, resp.Entries, 1, "Key %s was lost!", key)
		}
	}
}

func Test_Integration_Stress_ReadWriteMix(t *testing.T) {
	cfg := configWithoutCompactionScheduler()
	raftCfg := raft.DefaultConfig()
	raftCfg.HeartbeatTimeout = 1000 * time.Millisecond
	raftCfg.ElectionTimeout = 1000 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 500 * time.Millisecond
	c := newClusterWithConfig(t, 3, cfg, raftCfg)
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	var wgWriters sync.WaitGroup
	var wgReaders sync.WaitGroup
	numWriters := 10
	writesPerWriter := 200 // higher passes, but sometiems can be flaky and fail
	// maybe create a diff http client with Keep-Alive to reuse tcp sockets

	ctx, cancelReaders := context.WithCancel(context.Background())
	defer cancelReaders()

	readerErrCh := make(chan error, 100)

	// readers
	for i := range 5 {
		wgReaders.Add(1)
		go func(readerID int) {
			defer wgReaders.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					var resp api.RangeResponse
					// broad prefix scan to stress the tree index
					status := do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("mix-"), Prefix: true}, &resp)
					if status != http.StatusOK {
						readerErrCh <- fmt.Errorf("reader %d got status %d", readerID, status)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	// writers
	writerErrCh := make(chan error, numWriters*writesPerWriter)
	for i := range numWriters {
		wgWriters.Add(1)
		go func(writerID int) {
			defer wgWriters.Done()
			for j := 0; j < writesPerWriter; j++ {
				key := fmt.Sprintf("mix-%d-%d", writerID, j)
				status := do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(key), Value: []byte("val")}, nil)
				if status != http.StatusOK {
					writerErrCh <- fmt.Errorf("writer %d failed on key %s with status %d", writerID, key, status)
				}
			}
		}(i)
	}

	// wait for writes to finish + then cancel the readers
	wgWriters.Wait()
	cancelReaders()
	wgReaders.Wait()

	close(writerErrCh)
	close(readerErrCh)

	var lastErr error
	for err := range writerErrCh {
		lastErr = err
		t.Error(err)
	}
	for err := range readerErrCh {
		t.Error(err)
	}
	if t.Failed() {
		t.Log(lastErr)
		t.FailNow()
	}

	var finalResp api.RangeResponse
	require.Equal(t, http.StatusOK, do(t, l, http.MethodGet, _http.RouteKvRange, api.RangeRequest{Key: []byte("mix-"), Prefix: true}, &finalResp))

	require.Equal(t, numWriters*writesPerWriter, finalResp.Count, "Not all writes survived the concurrent read/write mix")
}

func Test_Integration_Stress_WatchAndWrite(t *testing.T) {
	c := newClusterWithConfig(t, 3, configWithoutCompactionScheduler(), raftConfigWithFastCompaction())
	defer c.teardown()
	l, _ := c.waitLeader(5 * time.Second)

	numWatches := 5
	writesPerWatch := 300

	type watchState struct {
		conn   *websocket.Conn
		prefix string
		events []watch.StreamEvent
	}

	states := make([]*watchState, numWatches)

	// watches
	for i := range numWatches {
		ws, cancel := dialWatchWS(t, l)
		defer cancel()
		defer ws.Close(websocket.StatusNormalClosure, "")

		key := fmt.Sprintf("wkey-%d-", i)
		payload, _ := json.Marshal(api.WatchCreateRequest{Key: []byte(key), Prefix: true})
		require.NoError(t, wsjson.Write(context.Background(), ws, watch.ClientMessage{
			Kind:    watch.ClientWatchCreate,
			Payload: payload,
		}))

		var srvMsg watch.ServerMessage
		require.NoError(t, wsjson.Read(context.Background(), ws, &srvMsg))
		require.Equal(t, watch.ServerWatchCreated, srvMsg.Kind)

		states[i] = &watchState{
			conn:   ws,
			prefix: key,
			events: make([]watch.StreamEvent, 0, writesPerWatch),
		}
	}

	// register all channels in memory
	time.Sleep(200 * time.Millisecond)

	var wg sync.WaitGroup

	for i := range numWatches {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < writesPerWatch; j++ {
				key := fmt.Sprintf("%s%d", states[idx].prefix, j)
				do(t, l, http.MethodPost, _http.RouteKvPut, api.PutRequest{Key: []byte(key), Value: []byte("val")}, nil)
			}
		}(i)
	}

	// watch readers
	var readerWg sync.WaitGroup
	errCh := make(chan error, numWatches*writesPerWatch)

	for i := range numWatches {
		readerWg.Add(1)
		go func(idx int) {
			defer readerWg.Done()
			var lastRev int64 = 0

			// block + read writesPerWatch events
			for len(states[idx].events) < writesPerWatch {
				var msg watch.ServerMessage
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := wsjson.Read(ctx, states[idx].conn, &msg)
				cancel()

				if err != nil {
					errCh <- fmt.Errorf("watch %d read error: %v", idx, err)
					return
				}

				if msg.Kind == watch.ServerWatchEventPut {
					var ev watch.StreamEvent
					json.Unmarshal(msg.Payload, &ev)
					states[idx].events = append(states[idx].events, ev)

					// monotonic revisions
					if ev.Event.Entry.ModRev <= lastRev {
						errCh <- fmt.Errorf("watch %d received out-of-order revision: %d came after %d", idx, ev.Event.Entry.ModRev, lastRev)
						return
					}
					lastRev = ev.Event.Entry.ModRev
				}
			}
		}(i)
	}

	wg.Wait()
	readerWg.Wait()
	close(errCh)

	var lastErr error
	for err := range errCh {
		lastErr = err
		t.Error(err)
	}
	if t.Failed() {
		require.Error(t, lastErr)
	}

	for i := range numWatches {
		require.Len(t, states[i].events, writesPerWatch, "watcher %d missed %s events", i, (writesPerWatch - len(states[i].events)))
	}
}
