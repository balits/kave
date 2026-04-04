package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/types/api"
	"github.com/balits/kave/internal/util"
	"github.com/coder/websocket"
	"golang.org/x/sync/errgroup"
)

const (
	// subprotocol for the websocket
	WatchSubprotocol      = "ws_watch_protocol"
	defaultWriteTimeoutMs = 200
)

var (
	errInvalidClientMessage       = errors.New("client message error:")
	errInvalidWatchRequestPayload = fmt.Errorf("%w: invalid request payload", errInvalidClientMessage)
	closeSessionSignal            = errors.New("client requested closing the session")
)

// Session is a wrapper around a watch websocket connection.
// Each session is a one-shot, it handles its own lifecycle, and should not be used
// after Run is completed, as the sessions resources are gonna get freed.
// See [Run] for more information.
//
// Internaly it spins up two go routines, one blocks on reading client
// messages, the other sends control messages or watcher events to the client.
// If any of these go routines produce an error, the session is terminated
type Session struct {
	id             int64              // ID of the session
	conn           *websocket.Conn    // handle to ws
	g              *errgroup.Group    // err group to handle go routines
	ctx            context.Context    // session level ctx
	stream         Stream             // handle to event stream
	writeTimeoutMs int64              // timeout for writes
	outC           chan ServerMessage // channel for writing messages to the client
	logger         *slog.Logger       // hmm i wonder what this could be...
}

func NewSession(
	conn *websocket.Conn,
	ctx context.Context,
	hub *WatchHub,
	logger *slog.Logger,
	readTimeoutMs,
	writeTimeoutMs int64,
) *Session {
	g, derived := errgroup.WithContext(ctx)

	var wt int64

	if writeTimeoutMs <= 0 {
		wt = defaultWriteTimeoutMs
	} else {
		wt = writeTimeoutMs
	}

	id := util.NextNonNullID()
	return &Session{
		id:             id,
		conn:           conn,
		g:              g,
		ctx:            derived,
		stream:         NewStream(derived, logger, hub),
		writeTimeoutMs: wt,
		outC:           make(chan ServerMessage, streamEventBufferSize/2),
		logger:         logger.With("component", "session", "session_id", id),
	}
}

// Close closes the stream and the channels
func (s *Session) Close() {
	s.logger.Info("Closing ws")
	s.stream.Close()
	close(s.outC)
}

// Run defines the whole lifecylce of the session:
// it spins up the dispatcher (reader) and collector (writer) go routines
// and blocks until they are completed,
// closing the session before returning.
//
// If any of the go routines produced an error,
// they both close and the error is returned.
func (s *Session) Run() error {
	s.logger.Info("Session started")

	s.g.Go(func() error {
		return s.writer()
	})

	s.g.Go(func() error {
		return s.reader()
	})

	if err := s.g.Wait(); err != nil {
		s.logger.Error("error after waiting for dispatcher/collector go routines",
			"error", err,
		)
		if errors.Is(err, context.Canceled) {
			// TODO: handle differently on the caller side
			return fmt.Errorf("session exited as context has been canceled")
		}
		return fmt.Errorf("session failed: %w", err)
	}

	s.logger.Info("Session exiting without error, all goroutines have finished successfuly")
	return nil
}

// writer defines the go routine that selects events from
// the underlying stream, and the session itself for control messages.
//
// If writing fails or the context is canceled, the error is propagated.
func (s *Session) writer() (err error) {
	defer func() {
		if errors.Is(err, context.Canceled) {
			s.logger.Info("collector exited with canceled error")
			err = fmt.Errorf("collector error: %w", err)
			return
		}
		// err is never non nil, otherwise the for loop wouldnt have returned
		s.logger.Warn("collector exited with error", "error", err)
		err = fmt.Errorf("collector error: %w", err)
	}()

	s.logger.Info("collector started")

	for {
		var msg ServerMessage
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case msg = <-s.outC:
			s.logger.Info("writing control message", "kind", msg.Kind)
		case ev := <-s.stream.Events():
			msg = serverStreamEvent(ev)
		}

		if err = s.write(msg); err != nil {
			return err
		}
		if msg.Kind == ServerMessageKind(ClientClose) {
			return closeSessionSignal
		}
	}
}

// reader defines the go routine that reads messages from
// the client in a blocking fashion, forwarding it to the underlying watch stream,
// then sending the result to to output channel as control messages.
//
// If reading fails or the context is canceled, the error is propagated.
func (s *Session) reader() (err error) {
	defer func() {
		if errors.Is(err, context.Canceled) {
			s.logger.Info("dispatcher exited with canceled error")
			err = fmt.Errorf("dispatcher failed: %w", err)
			return
		}
		// err is never non nil, otherwise the for loop wouldnt have returned
		s.logger.Warn("dispatcher exited with error", "error", err)
		err = fmt.Errorf("dispatcher failed: %w", err)
	}()

	s.logger.Info("dispatcher started")
	var req *ClientMessage

	for {
		req, err = s.read()
		if err != nil {
			return err
		}

		var response ServerMessage

		switch req.Kind {
		case ClientWatchCreate:
			var payload api.WatchCreateRequest
			if err = json.Unmarshal(req.Payload, &payload); err != nil {
				msg := fmt.Sprintf("could not decode payload: %v", err)
				s.logger.Info(errInvalidWatchRequestPayload.Error(),
					"error", msg,
				)
				response = serverErrorMessage(errInvalidWatchRequestPayload, msg)
			} else {
				res := s.stream.Watch(s.ctx, payload)
				response = serverWatchCreate(res)
			}
		case ClientWatchCancel:
			var payload api.WatchCancelRequest
			if err = json.Unmarshal(req.Payload, &payload); err != nil {
				msg := fmt.Sprintf("could not decode payload: %v", err)
				response = serverErrorMessage(errInvalidWatchRequestPayload, msg)
			} else {
				res := s.stream.Cancel(payload)
				response = serverWatchCanceled(res)
			}
		case ClientClose:
			response = serverCloseSession()
		default:
			s.logger.Info("Unknown kind while reading client message",
				"error", errInvalidClientMessage,
				"kind", req.Kind,
			)
			response = serverErrorMessage(errInvalidClientMessage, "unknown kind while reading client message")
		}

		s.outC <- response
	}
}

// read reads a client message from the connection with the
// sessions context, making it block until a message is, or
// the session context is canceled.
func (s *Session) read() (msg *ClientMessage, err error) {
	typ, raw, err := s.conn.Read(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("read error: unsupported message type, server only supports MessageText")
	}

	msg = new(ClientMessage)
	if err := json.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("read error: failed to decode json body: %w", err)
	}
	return msg, nil
}

// write writes a server message to the client with the sessions write timeout.
func (s *Session) write(msg ServerMessage) error {
	timeout := time.Duration(s.writeTimeoutMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()

	bs, err := json.Marshal(&msg)
	if err != nil {
		return fmt.Errorf("write error: failed to encode json body: %w", err)
	}
	if err := s.conn.Write(ctx, websocket.MessageText, bs); err != nil {
		return fmt.Errorf("write error: %w", err)
	}
	return nil
}

// ServerMessage mappers

func serverWatchCreate(res api.WatchCreateResponse) ServerMessage {
	bs, err := res.MarshalJSON()
	if err != nil {
		panic(err)
	}

	var r api.WatchCreateResponse
	r.UnmarshalJSON(bs)

	return ServerMessage{
		Kind:    ServerWatchCreated,
		Payload: bs,
	}
}

func serverWatchCanceled(res api.WatchCancelResponse) ServerMessage {
	bs, err := res.MarshalJSON()
	if err != nil {
		panic(err)
	}

	return ServerMessage{
		Kind:    ServerWatchCanceled,
		Payload: bs,
	}
}

func serverStreamEvent(ev StreamEvent) ServerMessage {
	var kind ServerMessageKind

	switch ev.Kind {
	case StreamWatchPut:
		kind = ServerWatchEventPut
	case StreamWatchDelete:
		kind = ServerWatchEventDelete
	default:
		kind = ServerError
	}

	bs, err := json.Marshal(ev)
	if err != nil {
		panic(err)
	}

	return ServerMessage{
		Kind:    kind,
		Payload: bs,
	}
}

func serverErrorMessage(cause error, msg string) ServerMessage {
	bs, err := serverErrorPayload{
		Cause:   cause,
		Message: msg,
	}.MarshalJSON()

	if err != nil {
		panic(err)
	}

	return ServerMessage{
		Kind:    ServerError,
		Payload: bs,
	}
}

func serverCloseSession() ServerMessage {
	return ServerMessage{
		Kind: ServerCloseSession,
	}
}
