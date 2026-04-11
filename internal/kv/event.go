package kv

type EventKind string

const (
	EventPut    EventKind = "PUT"
	EventDelete EventKind = "DELETE"
)

type EventFilter string

const (
	FilterNoPut    EventFilter = "NO_PUT"
	FilterNoDelete EventFilter = "NO_DELETE"
)

type Event struct {
	Kind  EventKind `json:"kind"`
	Entry *Entry    `json:"entry"`
	// TODO(1): not used yet, but will add later:
	// ([]changes kv.Entry) -> []{Entry, PrevEntry} but idk if we should add extra reads for each write:(
	PrevEntry *Entry `json:"prev_entry,omitempty"`
}

func ToKvEvents(in []*Entry) (out []Event) {
	out = make([]Event, len(in))
	for i, e := range in {
		out[i] = ToKvEvent(e)
	}
	return
}

func ToKvEvent(in *Entry) Event {
	if in == nil {
		// TODO(1): this needs fixing
		panic("nil entry to kv Event")
	}
	if in.Tombstone() {
		return Event{
			Kind:  EventDelete,
			Entry: in,
		}
	}
	return Event{
		Kind:  EventPut,
		Entry: in,
	}
}
