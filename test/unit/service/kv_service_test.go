package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/balits/kave/internal/common"
	s "github.com/balits/kave/internal/service"
	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/inmem"
	"github.com/balits/kave/test"
	"github.com/stretchr/testify/require"
)

type KVServiceTester struct {
	kv  s.KVService
	ctx context.Context
	n   int
}

func Test_KVService(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	st := inmem.NewStore(storage.StorageOptions{
		InitialBuckets: []storage.Bucket{
			test.BucketTest,
		},
	})
	r := test.NewTestRaftWithStore(t, st)
	kv := s.NewKVService(r, st)

	tester := &KVServiceTester{kv: kv, ctx: ctx, n: 10}

	t.Run("SET", tester.testSet)
	t.Run("GET", tester.testGet)
	t.Run("GET_STALE", tester.testGet)
	t.Run("DELETE", tester.testDelete)
}

func (kvt *KVServiceTester) testSet(t *testing.T) {
	response, err := kvt.kv.Set(kvt.ctx, common.SetRequest{
		Key:   b("key1"),
		Value: b("value1"),
	})

	require.NoError(t, err)
	require.Equal(t, uint64(1), response.Version)

	response, err = kvt.kv.Set(kvt.ctx, common.SetRequest{
		Key:   b("key1"),
		Value: b("value1"),
	})

	require.NoError(t, err)
	require.Equal(t, uint64(2), response.Version)

	for i := range kvt.n {
		response, err = kvt.kv.Set(kvt.ctx, common.SetRequest{
			Key:   b(fmt.Sprintf("key%d", i)),
			Value: b(fmt.Sprintf("value%d", i)),
		})

		require.NoError(t, err)
		fmt.Println("set key: ", fmt.Sprintf("key%d", i), "version: ", response.Version)
	}
}

func (kvt *KVServiceTester) testGet(t *testing.T) {
	for i := range kvt.n {
		response, err := kvt.kv.Get(kvt.ctx, common.GetRequest{
			Key: b(fmt.Sprintf("key%d", i)),
		})

		require.NoError(t, err)
		require.Equal(t, b(fmt.Sprintf("value%d", i)), response.Value)
	}
}

func (kvt *KVServiceTester) testDelete(t *testing.T) {
	for i := range kvt.n {
		_, err := kvt.kv.Delete(kvt.ctx, common.DeleteRequest{
			Key: b(fmt.Sprintf("key%d", i)),
		})

		require.NoError(t, err)
	}
}

func b(s string) []byte {
	return []byte(s)
}
