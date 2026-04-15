package api

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/balits/kave/internal/kv"
)

type WatchCreateRequest struct {
	WatchID       int64           `json:"id"`
	Key           []byte          `json:"key"`
	End           []byte          `json:"end"`
	Prefix        bool            `json:"prefix"` // flag to indicate a watch over the range [Key, Key+1)
	StartRevision int64           `json:"start_revision"`
	PrevEntry     bool            `json:"prev_entry"`
	Filter        *kv.EventFilter `json:"filter,omitempty"`
}

func (r *WatchCreateRequest) Check() error {
	if r.StartRevision < 0 {
		return errors.New("start_revision cannot be negative")
	}
	return nil
}

type WatchCreateResponse = watchResponse

type WatchCancelRequest struct {
	WatchID int64 `json:"watch_id"`
}

func (r *WatchCancelRequest) Check() error {
	if r.WatchID == 0 {
		return errors.New("watch_id cannot be zero")
	}
	return nil
}

type WatchCancelResponse = watchResponse

type watchResponse struct {
	WatchID int64 `json:"watch_id"`
	Success bool  `json:"success"` // created or canceled: can be false while error == nil, to signal "No watch found"
	Error   error `json:"error,omitempty"`
}

// error is an interface with non exported fields -> json will fail to marshal/unmarshal it
func (r watchResponse) MarshalJSON() ([]byte, error) {
	var bs []byte
	if r.Error == nil {
		bs = fmt.Appendf(nil, `{"watch_id": %d, "success": %t}`,
			r.WatchID, r.Success)
	} else {
		bs = fmt.Appendf(nil, `{"watch_id": %d, "success": %t, "error": "%s" }`,
			r.WatchID, r.Success, r.Error.Error())
	}

	return []byte(bs), nil
}

func (r *watchResponse) UnmarshalJSON(bs []byte) error {
	var alias struct {
		WatchID int64  `json:"watch_id"`
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}

	if err := json.Unmarshal(bs, &alias); err != nil {
		return err
	}

	r.WatchID = alias.WatchID
	r.Success = alias.Success

	if alias.Error != "" {
		r.Error = errors.New(alias.Error)
	} else {
		r.Error = nil
	}

	return nil
}
