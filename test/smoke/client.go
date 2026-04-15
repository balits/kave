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

func (c *client) do(method, path string, body any, dst any) *http.Response {
	c.tb.Helper()
	var r io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		require.NoError(c.tb, err, "marshal request")
		r = bytes.NewReader(bs)
	}

	req, err := http.NewRequest(method, c.url+path, r)
	require.NoError(c.tb, err, "create request")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	require.NoError(c.tb, err, "%s %s", method, path)

	if dst != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err := json.NewDecoder(resp.Body).Decode(dst)
		require.NoError(c.tb, err, "decode %s %s", method, path)
	}

	return resp
}

func readBody(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	bs, _ := io.ReadAll(resp.Body)
	return string(bs)
}

func (c *client) put(key, value string) (api.PutResponse, *http.Response) {
	c.tb.Helper()
	var out api.PutResponse
	resp := c.do(http.MethodPost, _http.RouteKvPut,
		api.PutRequest{Key: []byte(key), Value: []byte(value)}, &out)
	return out, resp
}

func (c *client) get(key string) (api.RangeResponse, *http.Response) {
	c.tb.Helper()
	var out api.RangeResponse
	resp := c.do(http.MethodGet, _http.RouteKvRange,
		api.RangeRequest{Key: []byte(key), Serializable: true}, &out)
	return out, resp
}

func (c *client) mustPut(key, value string) api.PutResponse {
	c.tb.Helper()
	out, resp := c.put(key, value)
	require.Equal(c.tb, 200, resp.StatusCode, "PUT %s failed: %s", key, readBody(resp))
	return out
}

func (c *client) mustGet(key string) api.RangeResponse {
	c.tb.Helper()
	out, resp := c.get(key)
	require.Equal(c.tb, 200, resp.StatusCode, "GET %s failed: %s", key, readBody(resp))
	return out
}

func (c *client) mustGetVal(key, expectedValue string) {
	c.tb.Helper()
	out := c.mustGet(key)
	require.Equal(c.tb, 1, out.Count, "expected 1 entry for key %q, got %d", key, out.Count)
	require.Equal(c.tb, expectedValue, string(out.Entries[0].Value))
}

func (c *client) readyz() (int, error) {
	c.tb.Helper()
	resp, err := c.httpClient.Get(c.url + _http.RouteReadyz)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func (c *client) stats() (out map[string]string, status int) {
	c.tb.Helper()
	resp := c.do(http.MethodGet, _http.RouteStats, nil, &out)
	return out, resp.StatusCode
}

func (c *client) waitReady(timeout time.Duration) {
	c.tb.Helper()
	require.Eventually(c.tb, func() bool {
		code, err := c.readyz()
		return err == nil && code == 200
	}, timeout, timeout/10, "service not ready after %s", timeout.String())
}
