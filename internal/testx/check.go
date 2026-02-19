package testx

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/balits/thesis/internal/command"
	"github.com/balits/thesis/internal/testx/mock"
)

func NoErr(tb testing.TB, err error) {
	if err != nil {
		tb.Fatalf("Check failed: %v", err)
	}
}
func CheckConsistent(tb testing.TB, nodes []*TestNode) {
	limit := time.Now().Add(WaitConsistencyTimeout)
	sleep := WaitConsistencyTimeout / 10
	first := nodes[0]
	firstStore := first.LoggingFsm.Store.(*mock.LoggingStore)
	firstStore.Lock()
	defer firstStore.Unlock()
	var err error
CHECK:
	l1 := len(firstStore.Logs)
	for i := 1; i < len(nodes); i++ {
		currentNode := nodes[i]
		currentStore := currentNode.LoggingFsm.Store.(*mock.LoggingStore)
		currentStore.Lock()
		l2 := len(currentStore.Logs)
		if l1 != l2 {
			err = fmt.Errorf("log length mismatch %s:%d != %s:%d", first.Config.NodeID, l1, currentNode.Config.NodeID, l2)
			currentStore.Unlock()
			goto ERR
		}
		for idx, log := range firstStore.Logs {
			other := currentStore.Logs[idx]
			if !cmdEq(log, other) {
				err = fmt.Errorf("log entry %d mismatch between %s/%s : '%v' / '%v'", idx, first.Config.NodeID, currentNode.Config.NodeID, log, other)
				currentStore.Unlock()
				goto ERR
			}
		}
		currentStore.Unlock()
	}
	return
ERR:
	if time.Now().After(limit) {
		tb.Fatalf("%v", err)
	}
	firstStore.Unlock()
	time.Sleep(sleep)
	firstStore.Lock()
	goto CHECK
	/*
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
	*/
}

func cmdEq(this, that command.Command) bool {
	return this.Type == that.Type && bytes.Equal(this.Key, that.Key) && bytes.Equal(this.Value, that.Value)
}
