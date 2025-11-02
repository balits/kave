package integration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/balits/thesis/internal/config"
	"github.com/balits/thesis/internal/test/testutil"
	"github.com/balits/thesis/internal/web"
)

// var rng *rand.Rand

// func init() {
// 	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
// }

func TestGetSetDelete_OnDurableClusterOfSize3(t *testing.T) {
	baseCfg := testutil.NewMockConfig(3)
	baseCfg.InMemory = false
	tempdir, cleanupTempdirs, err := testutil.Tempdir("discover_on_network_test")
	if err != nil {
		t.Errorf("Failed to create tempdirs: %v", err)
		return
	}
	defer cleanupTempdirs()

	services, err := testutil.NewMockDurableCluster(tempdir, baseCfg, testutil.NewMockLogger())
	if err != nil {
		t.Errorf("Failed to create mock cluster: %v", err)
		return
	}

	defer func() {
		for _, s := range services {
			s.Shutdown(2 * time.Second)
		}
	}()

	condition := func() bool {
		return testutil.DiscoverCondition(t, services)
	}

	testutil.AssertEventually(t, condition, 10*time.Second, 500*time.Millisecond)

	client := http.Client{Timeout: 2 * time.Second}

	doRequest := func(me config.ServiceInfo, op string, bodies []body) {
		base := "http://" + me.InternalHost + ":" + me.ExternalHttpPort

		for _, body := range bodies {
			rawBody, err := json.Marshal(body)
			if err != nil {
				t.Logf("Node(%s): error marshaling request body: %v", me.RaftID, err)
			}
			req, err := http.NewRequest("POST", base+"/"+op, bytes.NewBuffer(rawBody))
			if err != nil {
				t.Logf("Node(%s): error creating request: %v", me.RaftID, err)
				return
			}
			res, err := client.Do(req)
			if err != nil {
				t.Logf("Node(%s): error sending request: %v", me.RaftID, err)
				return
			}
			t.Logf("Node(%s): sending %s, key=%s, value=%s", me.RaftID, "SET", body.Key, body.Value)

			var payload web.ResponsePayload
			if err = json.NewDecoder(res.Body).Decode(&payload); err != nil {
				t.Logf("Node(%s): error decoding response: %v", me.RaftID, err)
				return
			}
			t.Logf("Node(%s): response.status=%v, response.body=%+v", me.RaftID, res.Status, payload)

			if res.StatusCode != http.StatusOK {
				t.Logf("Node(%s): error expecting Status OK, got %s", me.RaftID, res.Status)
				return
			}
		}
	}

	randomBodies := newRandomRequestBodies(5)
	for _, b := range randomBodies {
		t.Logf("random body: %+v", b)
	}

	for _, s := range services {
		doRequest(*s.Config.ThisService, "set", randomBodies)
	}
}

type body struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func newRandomRequestBodies(n int) []body {
	bodies := make([]body, n)
	for i := range n {
		bodies[i] = body{
			Key:   randStringRunes(4),
			Value: base64.StdEncoding.EncodeToString([]byte(randStringRunes(12))),
		}
	}
	return bodies
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
