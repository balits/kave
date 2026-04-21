//go:build smoke

package smoke

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	_http "github.com/balits/kave/internal/transport/http"
	"github.com/balits/kave/internal/types/api"
	"github.com/stretchr/testify/require"
)

// client talks to a kave cluster through its Service URL.
type client struct {
	tb         testing.TB
	url        string
	httpClient *http.Client
}

func httpClient(t testing.TB) *client {
	return &client{
		tb:  t,
		url: env.url,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *client) tryDo(method, path string, body any, dst any) (*http.Response, error) {
	c.tb.Helper()
	var r io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(bs)
	}

	req, err := http.NewRequest(method, c.url+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if dst != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err := json.NewDecoder(resp.Body).Decode(dst)
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func readBody(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	bs, _ := io.ReadAll(resp.Body)
	return string(bs)
}

func (c *client) tryPut(key, value string) (api.PutResponse, *http.Response, error) {
	c.tb.Helper()
	var out api.PutResponse
	resp, err := c.tryDo(http.MethodPost, _http.RouteKvPut,
		api.PutRequest{Key: []byte(key), Value: []byte(value)}, &out)
	return out, resp, err
}

func (c *client) tryGet(key string) (api.RangeResponse, *http.Response, error) {
	c.tb.Helper()
	var out api.RangeResponse
	resp, err := c.tryDo(http.MethodGet, _http.RouteKvRange,
		api.RangeRequest{Key: []byte(key), Serializable: false}, &out)
	return out, resp, err
}

func (c *client) mustPut(key, value string) api.PutResponse {
	c.tb.Helper()
	var out api.PutResponse
	var resp *http.Response
	var err error

	// retry bcs of network failures
	require.Eventually(c.tb, func() bool {
		out, resp, err = c.tryPut(key, value)
		return err == nil && resp.StatusCode == 200
	}, 5*time.Second, 500*time.Millisecond, "PUT %s failed repeatedly", key)

	return out
}

func (c *client) mustGet(key string) api.RangeResponse {
	c.tb.Helper()
	var out api.RangeResponse
	var resp *http.Response
	var err error

	// retry bcs of network failures
	require.Eventually(c.tb, func() bool {
		out, resp, err = c.tryGet(key)
		return err == nil && resp.StatusCode == 200
	}, 5*time.Second, 500*time.Millisecond, "PUT %s failed repeatedly", key)

	return out
}

func (c *client) mustGetVal(key, expectedValue string) {
	c.tb.Helper()
	out := c.mustGet(key)
	require.Equal(c.tb, 1, out.Count, "expected 1 entry for key %q, got %d", key, out.Count)
	require.Equal(c.tb, expectedValue, string(out.Entries[0].Value))
}

func (c *client) waitGetVal(key, expectedValue string, timeout time.Duration) {
	c.tb.Helper()
	require.Eventually(c.tb, func() bool {
		out, resp, err := c.tryGet(key)
		if err != nil {
			return false
		}
		if resp.StatusCode != 200 {
			return false
		}
		if out.Count != 1 {
			return false
		}
		return string(out.Entries[0].Value) == expectedValue
	}, timeout, timeout/10, "waitGetValu: failed to get value %s for key %s in %s", expectedValue, key, timeout)
}

func (c *client) readyz() (int, error) {
	c.tb.Helper()
	resp, err := c.httpClient.Get(c.url + _http.RouteReadyz)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return resp.StatusCode, nil
}

func (c *client) stats() (out map[string]string, status int, err error) {
	c.tb.Helper()
	resp, err := c.tryDo(http.MethodGet, _http.RouteStats, nil, &out)
	if err != nil {
		return nil, 0, err
	}
	return out, resp.StatusCode, nil
}

func (c *client) waitReady(timeout time.Duration) {
	c.tb.Helper()
	require.Eventually(c.tb, func() bool {
		code, err := c.readyz()
		return err == nil && code == 200
	}, timeout, timeout/10, "service not ready after %s", timeout.String())
}

func (c *client) waitLeaderChanged(oldLeaderID string, timeout time.Duration) string {
	c.tb.Helper()
	var newLeaderID string
	require.Eventually(c.tb, func() bool {
		stats, status, err := c.stats()
		if err != nil {
			return false
		}
		if status != http.StatusOK {
			return false
		}
		id := stats["leader_id"]
		if id == "" || id == oldLeaderID {
			return false
		}
		newLeaderID = id
		return true
	}, timeout, 2*time.Second, "leader did not change from %s within %s", oldLeaderID, timeout)
	return newLeaderID
}
