package api

import "fmt"

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type Response struct {
	Meta   Meta      `json:"metadata"`
	Result any       `json:"result,omitempty"`
	Error  *APIError `json:"error,omitempty"`
}

type Meta struct {
	Revision uint64 `json:"current_revision"`
	Term     uint64 `json:"raft_term"`
	LeaderID string `json:"leader_id"`
}
