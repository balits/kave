package watch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/types/api"
)

const streamEventBufferSize = 4

var ErrStreamClosed = errors.New("watch stream is closed")

// Stream is an ephemeral handle to the [WatchHub],
// providing methods to create and cancel watchers,
// and a collector channel, through which each watcher
// sends its events.
//
// Each stream only knows about its own watchers
// and each of those watchers gets dropped once the stream
// is closed. Each watcher gets its own go routine to collect events.
type Stream interface {
	// Watch creates a watcher with the provided context and request,
	// returning the result wrapped into the response.
	Watch(ctx context.Context, req api.WatchCreateRequest) api.WatchCreateResponse
	// Cancel cancels a watcher, returning the result wrapped into the response.
	Cancel(req api.WatchCancelRequest) api.WatchCancelResponse
	// Events returns the read only channel of type [StreamEvent].
	Events() <-chan StreamEvent
	// Close releases all resources held by the stream, like the watchers and channels
	Close()
}

type stream struct {
	hub      *WatchHub
	closed   atomic.Bool
	watchers map[int64]*watcher
	outputC  chan StreamEvent
	intputC  chan ClientMessage
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *slog.Logger
}

func NewStream(ctx context.Context, logger *slog.Logger, hub *WatchHub) Stream {
	derived, cancel := context.WithCancel(ctx)
	return &stream{
		hub:      hub,
		watchers: make(map[int64]*watcher),
		outputC:  make(chan StreamEvent, streamEventBufferSize),
		intputC:  make(chan ClientMessage, streamEventBufferSize/2),
		ctx:      derived,
		cancel:   cancel,
		logger:   logger,
	}
}

// Watch registers a watch described by the request, and spins up a go routine that
// collects events emitted by the watcher into the output channel.
func (s *stream) Watch(ctx context.Context, req api.WatchCreateRequest) (res api.WatchCreateResponse) {
	if s.closed.Load() {
		res.Error = ErrStreamClosed
		return
	}

	s.logger.WithGroup("request").
		Info("Recieved WATCH request",
			"watch_id", req.WatchID,
			"key", req.Key,
			"end", req.End,
			"start_rev", req.StartRevision,
		)

	if err := req.Check(); err != nil {
		res.Error = fmt.Errorf("invalid request: %w", err)
		s.logger.Info("failed to create watcher", "error", res.Error)
		return
	}

	w, err := s.hub.NewWatcher(ctx, req.WatchID, req.StartRevision, req.Key, req.End, req.Filter, req.PrevEntry)
	if err != nil {
		res.Error = err
		s.logger.Info("failed to create watcher", "error", res.Error)
		return
	}

	s.watchers[w.id] = w
	s.logger.Info("starting watcher", "id", w.id)
	go s.runWatcher(w)

	res.WatchID = w.id
	res.Success = true
	return
}

func (s *stream) Cancel(req api.WatchCancelRequest) (res api.WatchCreateResponse) {
	s.logger.Info("Recieved WATCH_CANCEL request")
	if s.closed.Load() {
		res.Error = ErrStreamClosed
		return
	}
	res.Success = s.hub.DropWatcher(req.WatchID)
	if res.Success {
		res.WatchID = req.WatchID
	}
	return
}

func (s *stream) Events() <-chan StreamEvent {
	return s.outputC
}

func (s *stream) Messages() chan<- ClientMessage {
	return s.intputC
}

func (s *stream) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		s.logger.Info("Attempted to double close watch stream")
		return
	}
	s.logger.Info("Closing stream")
	s.logger.Info("stream context canceled")
	s.cancel()

	// batch unsafeDrops under lock
	// instead of N Lock() + N Drop attempts to accuire
	count := len(s.watchers)
	s.hub.mu.Lock()
	for _, w := range s.watchers {
		s.hub.unsafeDropWatcher(w.id)
	}
	s.hub.mu.Unlock()
	s.logger.Info("dropped watchers", "watcher_count", count)

	close(s.intputC)
	s.logger.Info("input channel closed")
	close(s.outputC)
	s.logger.Info("output channel closed")
}

// runWatch collects events emitted by the watcher into the output channel.
func (s *stream) runWatcher(watcher *watcher) {
	defer func() {
		// if the stream is canceled/closed,
		// the watchers are gonne get dropped
		if s.ctx.Err() == nil {
			// otherwise, drop the watcher in place
			s.logger.Info("watcher canceled while stream is intact, dropping it manually")
			s.hub.DropWatcher(watcher.id)
		}
	}()

	s.logger.Info("starting watcher go routine", "watcher_id", watcher.id)

	for {
		select {
		case <-watcher.ctx.Done():
			// if watcher is canceled, return
			return
		case <-s.ctx.Done():
			// if the stream is canceled/closed, return
			return
		case ev, ok := <-watcher.c:
			if !ok {
				return
			}

			// if the watcher is dropped, send last event
			select {
			case <-s.ctx.Done():
				// except if the stream is closed
				return
			case s.outputC <- toStreamEvent(watcher.id, ev):
			}
		}
	}
}

func toStreamEvent(wid int64, ev kv.Event) StreamEvent {
	switch ev.Kind {
	case kv.EventPut:
		return putEvent(wid, ev)
	case kv.EventDelete:
		return deleteEvent(wid, ev)
	}
	panic("unexpected kv.EventKind")
}
