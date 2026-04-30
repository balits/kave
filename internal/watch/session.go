package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/mvcc"
	"github.com/balits/kave/internal/peer"
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
	errClientMessage              = errors.New("client message error:")
	errUnkownClientMessageKind    = fmt.Errorf("%w: unknown client message kind", errClientMessage)
	errInvalidWatchRequestPayload = fmt.Errorf("%w: invalid request payload", errClientMessage)

	errStreamEventParse      = errors.New("failed to parse stream event")
	errServerMessageMarshall = errors.New("failed to marshal server message")

	_closeSessionSignal = errors.New("client requested closing the session")
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
	id             int64                // ID of the session
	conn           *websocket.Conn      // handle to ws
	g              *errgroup.Group      // err group to handle go routines
	ctx            context.Context      // session level ctx
	stream         Stream               // handle to event stream
	writeTimeoutMs int64                // timeout for writes
	outC           chan ServerMessage   // channel for writing messages to the client
	store          mvcc.StoreMetaReader // used to stamp a Header on newly created watches
	me             peer.Peer            // used to stamp a Header on newly created watches
	logger         *slog.Logger         // hmm i wonder what this could be...
}

func NewSession(
	conn *websocket.Conn,
	ctx context.Context,
	hub *WatchHub,
	store mvcc.StoreMetaReader,
	me peer.Peer,
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
		store:          store,
		me:             me,
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
		s.logger.Error("error after waiting for reader/writer go routines",
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
			s.logger.Info("writer exited with canceled error")
			err = fmt.Errorf("writer error: %w", err)
			return
		}
		// err is never non nil, otherwise the for loop wouldnt have returned
		s.logger.Warn("writer exited with error", "error", err)
		err = fmt.Errorf("writer error: %w", err)
	}()

	s.logger.Info("writer started")

	for {
		var (
			msg      ServerMessage
			msgError error
		)

		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case msg = <-s.outC:
			s.logger.Info("writing control message", "kind", msg.Kind)
		case ev := <-s.stream.Events():
			msg, msgError = toServerStreamEvent(ev)
		}

		if msgError != nil {
			s.logger.Warn(errStreamEventParse.Error())
			msg.Kind = ServerError
			msg, err = toServerErrorMessage(err, "")

			if err != nil {
				return closeSessionSignal(s.logger, "failed to marshal error message", err)
			}
		}

		if err = s.write(msg); err != nil {
			return err
		}
		if msg.Kind == ServerMessageKind(ClientClose) {
			return closeSessionSignal(s.logger, fmt.Sprintf("recieved %s controll message", msg.Kind), nil)
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
			s.logger.Info("reader exited with canceled error")
			err = fmt.Errorf("reader failed: %w", err)
			return
		}
		// err is never non nil, otherwise the for loop wouldnt have returned
		s.logger.Warn("reader exited with error", "error", err)
		err = fmt.Errorf("reader failed: %w", err)
	}()

	s.logger.Info("reader started")

	for {
		var req *ClientMessage
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
				response, err = toServerErrorMessage(errInvalidWatchRequestPayload, msg)
			} else {
				res := s.stream.Watch(s.ctx, payload)
				currRev, compactedRev, raftIndex, raftTerm := s.store.Meta()
				res.Header = api.ResponseHeader{
					Revision:     currRev.Main,
					CompactedRev: compactedRev,
					RaftTerm:     raftTerm,
					RaftIndex:    raftIndex,
					NodeID:       s.me.NodeID,
				}
				response, err = toServerWatchCreate(res)
			}
		case ClientWatchCancel:
			var payload api.WatchCancelRequest
			if err = json.Unmarshal(req.Payload, &payload); err != nil {
				msg := fmt.Sprintf("could not decode payload: %v", err)
				response, err = toServerErrorMessage(errInvalidWatchRequestPayload, msg)
			} else {
				res := s.stream.Cancel(payload)
				response, err = toServerWatchCanceled(res)
			}
		case ClientClose:
			response = serverCloseSession()
		default:
			s.logger.Error(errUnkownClientMessageKind.Error(),
				"kind", req.Kind,
			)
			response, err = toServerErrorMessage(errUnkownClientMessageKind, "unknown kind")
		}

		if err != nil {
			if response.Kind == ServerError {
				return closeSessionSignal(s.logger, "failed to marshal error message", err)
			}
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

func toServerWatchCreate(res api.WatchCreateResponse) (msg ServerMessage, err error) {
	var bs []byte
	bs, err = res.MarshalJSON()
	if err != nil {
		err = fmt.Errorf("%w: %w", errServerMessageMarshall, err)
		return
	}

	err = res.UnmarshalJSON(bs)
	if err != nil {
		err = fmt.Errorf("%w: %w", errServerMessageMarshall, err)
		return
	}

	msg.Kind = ServerWatchCreated
	msg.Payload = bs
	return
}

func toServerWatchCanceled(res api.WatchCancelResponse) (msg ServerMessage, err error) {
	var bs []byte
	bs, err = res.MarshalJSON()
	if err != nil {
		err = fmt.Errorf("%w: %w", errServerMessageMarshall, err)
		return
	}

	err = res.UnmarshalJSON(bs)
	if err != nil {
		err = fmt.Errorf("%w: %w", errServerMessageMarshall, err)
		return
	}

	msg.Kind = ServerWatchCanceled
	msg.Payload = bs
	return
}

func toServerStreamEvent(ev StreamEvent) (msg ServerMessage, err error) {
	var kind ServerMessageKind
	var bs []byte

	switch ev.Event.Kind {
	case kv.EventPut:
		kind = ServerWatchEventPut
	case kv.EventDelete:
		kind = ServerWatchEventDelete
	}

	bs, err = json.Marshal(ev)
	if err != nil {
		err = fmt.Errorf("%w: %w", errStreamEventParse, err)
		return
	}

	msg.Kind = kind
	msg.Payload = bs
	return
}

func toServerErrorMessage(cause error, msg string) (msgOut ServerMessage, err error) {
	var bs []byte
	p := serverErrorPayload{
		Cause:   cause,
		Message: msg,
	}

	bs, err = p.MarshalJSON()
	if err != nil {
		err = fmt.Errorf("%w: %w", errServerMessageMarshall, err)
		return
	}

	msgOut.Kind = ServerError
	msgOut.Payload = bs
	return
}

func serverCloseSession() ServerMessage {
	return ServerMessage{
		Kind: ServerCloseSession,
	}
}

func closeSessionSignal(l *slog.Logger, msg string, err error) error {
	if err != nil {
		l.Error(msg+"; closing connection",
			"cause", err,
		)
	} else {
		l.Error(msg + "; closing connection")
	}
	return _closeSessionSignal
}
