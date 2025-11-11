package testx

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/balits/thesis/internal/store"
)

func NoErr(tb testing.TB, err error) {
	if err != nil {
		tb.Fatalf("Check failed: %v", err)
	}
}
func CheckConsistent(tb testing.TB, nodes []*TestNode) {
	timeout := WaitConsistencyTimeout
	limit := time.Now().Add(timeout)
	sleep := timeout / 20
	f1 := nodes[0].LoggingFsm
	f1.Lock()
	len1 := len(f1.Logs)
	defer f1.Unlock()

	var lastError error
	for time.Now().Before(limit) {
		var currentError error
		for i := 1; i < len(nodes); i++ {
			f2 := nodes[i].LoggingFsm
			f2.Lock()
			len2 := len(f2.Logs)
			if len1 != len2 {
				currentError = fmt.Errorf("logs of different lenghts, expected: %d: got: %d", len1, len2)
				f2.Unlock()
				break
			}

			for j := range len1 {
				log1 := f1.Logs[j]
				log2 := f2.Logs[j]

				if !cmdEq(log1, log2) {
					currentError = fmt.Errorf("log%d mismatch between %s/%s, expected: %s, got: %s", j, nodes[0].Config.NodeID, nodes[i].Config.NodeID, log1, log2)
					f2.Unlock()
					break
				}
			}

			f2.Unlock()
		}

		lastError = currentError

		f1.Unlock()
		time.Sleep(sleep)
		f1.Lock()
	}

	if lastError != nil {
		tb.Fatalf("consistency check failed: %v", lastError)
	}
}

func cmdEq(this, that store.Cmd) bool {
	return this.Kind == that.Kind && this.Key == that.Key && bytes.Equal(this.Value, that.Value)
}
