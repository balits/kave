package transport

import (
	"github.com/balits/kave/internal/common"
)

type Response struct {
	Header common.Meta `json:"header"`
	Result any     `json:"result,omitempty"`
	Error  error   `json:"error,omitempty"`
}
