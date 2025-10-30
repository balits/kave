package raftnode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/balits/thesis/internal/config"
)

type JoinBody struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func Join(me *config.ServiceInfo, urls []string, attempts int) error {
	body := JoinBody{
		ID:   me.RaftID,
		Addr: me.GetRaftAddress(),
	}
	jsonbody, _ := json.Marshal(body)
	bytes := bytes.NewReader(jsonbody)

	var lastError error
	for a := range attempts {
		for _, url := range urls {
			if err := join(url, bytes); err == nil {
				return nil // ok
			} else {
				lastError = err
			}
		}
		time.Sleep(time.Duration(2<<a) * time.Second)
	}

	return fmt.Errorf("could not join peers after %d attempts, last error: %v", attempts, lastError)
}

func join(url string, body io.Reader) error {
	res, err := http.DefaultClient.Post(url, "application/json", body)
	if err != nil {
		return err
	}
	switch res.StatusCode {
	case 200, 204:
		// succesful join, already joined
		return nil
	case http.StatusConflict:
		// node wasnt the leader
		return errors.New("node was not leader")
	case http.StatusBadRequest:
		// bad json body
		return errors.New("bad request")
	default:
		return fmt.Errorf("unexpected error, status: %s", res.Status)
	}
}
