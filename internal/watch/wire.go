package watch

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/balits/kave/internal/kv"
	"github.com/balits/kave/internal/types/api"
	"github.com/stretchr/testify/require"
)

// StreamEvent is a wrapper around an [kv.Event]
// thats produced by a [watcher]. It can optionally
// attach an error if something went wrong.
type StreamEvent struct {
	Wid   int64    `json:"watcher_id"`
	Event kv.Event `json:"event"`
}

func putEvent(wid int64, ev kv.Event) StreamEvent {
	return StreamEvent{
		Wid:   wid,
		Event: ev,
	}
}

func deleteEvent(wid int64, ev kv.Event) StreamEvent {
	return StreamEvent{
		Wid:   wid,
		Event: ev,
	}
}

type ClientMessage struct {
	Kind    ClientMessageKind `json:"kind"`
	Payload json.RawMessage   `json:"payload"`
}

type ClientMessageKind string

const (
	ClientWatchCreate ClientMessageKind = "WATCH_CREATE"
	ClientWatchCancel ClientMessageKind = "WATCH_CANCEL"
	ClientClose       ClientMessageKind = "SESSION_CLOSE"
)

type ServerMessage struct {
	Kind    ServerMessageKind `json:"kind"`
	Payload json.RawMessage   `json:"payload"`
}

type ServerMessageKind string

const (
	ServerError            ServerMessageKind = "ERROR" // some unrecoverable error
	ServerCloseSession     ServerMessageKind = "SESSION_CLOSE"
	ServerWatchCreated     ServerMessageKind = "WATCH_CREATED"
	ServerWatchCanceled    ServerMessageKind = "WATCH_CANCELED"
	ServerWatchEventPut    ServerMessageKind = ServerMessageKind(kv.EventPut)
	ServerWatchEventDelete ServerMessageKind = ServerMessageKind(kv.EventDelete)
)

type serverErrorPayload struct {
	Cause   error
	Message string
}

func (e serverErrorPayload) MarshalJSON() ([]byte, error) {
	bs := []byte("{")
	if e.Cause == nil {
		bs = fmt.Appendf(bs, `"cause": "%s"`, e.Cause.Error())
	}

	if len(e.Message) != 0 {
		if e.Cause != nil {
			bs = fmt.Append(bs, byte(','))
		}
		bs = fmt.Appendf(bs, `"message": "%s"`, e.Cause.Error())
	}

	bs = fmt.Appendf(bs, "}")
	return bs, nil
}

func (r *serverErrorPayload) UnmarshalJSON(bs []byte) error {
	var alias struct {
		Cause   string `json:"cause"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(bs, &alias); err != nil {
		return err
	}

	r.Message = alias.Message

	if len(alias.Cause) != 0 {
		r.Cause = errors.New(alias.Cause)
	} else {
		r.Cause = nil
	}

	return nil
}

func (s ServerMessage) AsCreateResponse(t *testing.T) (p api.WatchCreateResponse) {
	t.Helper()
	require.Equal(t, s.Kind, ServerWatchCreated)
	err := p.UnmarshalJSON(s.Payload)
	require.NoError(t, err)
	return p
}

func (s ServerMessage) AsCancelResponse(t *testing.T) api.WatchCancelResponse {
	t.Helper()
	require.Equal(t, s.Kind, ServerWatchCanceled)
	var p api.WatchCancelResponse
	require.NoError(t, json.Unmarshal(s.Payload, &p))
	return p
}

func (s ServerMessage) AsStreamEvent(t *testing.T) StreamEvent {
	t.Helper()
	require.True(t, s.Kind == ServerWatchEventDelete || s.Kind == ServerWatchEventPut,
		"expected stream event got %s", s.Kind)
	var p StreamEvent
	require.NoError(t, json.Unmarshal(s.Payload, &p))
	return p
}

func (s ServerMessage) AsError(t *testing.T) serverErrorPayload {
	t.Helper()
	require.Equal(t, s.Kind, ServerError)
	var p serverErrorPayload
	require.NoError(t, json.Unmarshal(s.Payload, &p))
	return p
}
