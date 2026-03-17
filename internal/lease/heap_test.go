package lease

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_HeapItemLess_EarlierExpiry(t *testing.T) {
	now := time.Now()
	a := &heapItem{expiry: now}
	b := &heapItem{expiry: now.Add(time.Second)}
	require.Equal(t, -1, a.less(b))
}

func Test_HeapItemLess_LaterExpiry(t *testing.T) {
	now := time.Now()
	a := &heapItem{expiry: now.Add(time.Second)}
	b := &heapItem{expiry: now}
	require.Equal(t, 1, a.less(b))
}

func Test_HeapItemLess_EqualExpiry(t *testing.T) {
	now := time.Now()
	a := &heapItem{expiry: now}
	b := &heapItem{expiry: now}
	require.Equal(t, 0, a.less(b))
}

func Test_HeapItemLess_NilB(t *testing.T) {
	a := &heapItem{expiry: time.Now()}
	require.Equal(t, 1, a.less(nil))
}

func Test_LeaseHeap_Len(t *testing.T) {
	h := leaseHeap{}
	require.Empty(t, h)
	now := time.Now()
	h = append(h, &heapItem{expiry: now}, &heapItem{expiry: now.Add(time.Second)})
	require.Len(t, h, 2)
}

func Test_LeaseHeap_Less(t *testing.T) {
	now := time.Now()
	h := leaseHeap{
		&heapItem{expiry: now, index: 0},
		&heapItem{expiry: now.Add(time.Second), index: 1},
	}
	require.True(t, h.Less(0, 1))
	require.False(t, h.Less(1, 0))
}

func Test_LeaseHeap_Swap(t *testing.T) {
	now := time.Now()
	a := &heapItem{expiry: now, index: 0}
	b := &heapItem{expiry: now.Add(time.Second), index: 1}
	h := leaseHeap{a, b}

	h.Swap(0, 1)

	require.Equal(t, b, h[0], "elements not swapped correctly")
	require.Equal(t, a, h[1], "elements not swapped correctly")
	if h[0] != b || h[1] != a {
		t.Error("elements were not swapped correctly")
	}

	require.Equal(t, 0, h[0].index, "indicies not swapped correctly: h[0].index=%d", h[0].index)
	require.Equal(t, 1, h[1].index, "indicies not swapped correctly: h[1].index=%d", h[1].index)
}

func Test_LeaseHeap_PushSetsIndex(t *testing.T) {
	h := &leaseHeap{}
	item := &heapItem{expiry: time.Now(), lease: lease(1)}
	heap.Push(h, item)

	require.Equal(t, 0, item.index, "expected index 0 after first Push")

	popped := heap.Pop(h).(*heapItem)
	require.Equal(t, -1, popped.index, "expected index -1 after Pop")
}

func Test_LeaseHeap_MinHeapOrdering(t *testing.T) {
	now := time.Now()
	items := []*heapItem{
		item(lease(3), now.Add(3*time.Second)),
		item(lease(1), now.Add(1*time.Second)),
		item(lease(5), now.Add(5*time.Second)),
		item(lease(2), now.Add(2*time.Second)),
		item(lease(4), now.Add(4*time.Second)),
	}
	h := buildHeap(items)
	expiries := drainHeap(h)

	for i := 1; i < len(expiries); i++ {
		require.True(t, expiries[i].After(expiries[i-1]))
	}
}

func Test_LeaseHeap_Single(t *testing.T) {
	now := time.Now()
	h := buildHeap([]*heapItem{item(lease(1), now)})
	require.Equal(t, 1, h.Len())

	popped := heap.Pop(h).(*heapItem)
	require.Equal(t, now, popped.expiry)
	require.Equal(t, 0, h.Len())
}

func Test_LeaseHeap_Duplicates(t *testing.T) {
	now := time.Now()
	h := &leaseHeap{}
	for i := range 5 {
		heap.Push(h, item(lease(int64(i)), now))
	}
	require.Equal(t, 5, h.Len(), "expected 5 items in heap after pushing duplicates")
	expiries := drainHeap(h)
	for _, e := range expiries {
		require.Equal(t, now, e, "expected all expiries to be equal to the same time")
	}
}

func Test_LeaseHeap_PeekMin(t *testing.T) {
	h := &leaseHeap{}
	require.Nil(t, h.peekMin())

	now := time.Now()
	earliest := now.Add(1 * time.Second)
	items := []*heapItem{
		item(lease(1), now.Add(10*time.Second)),
		item(lease(2), earliest),
		item(lease(3), now.Add(5*time.Second)),
	}
	h = buildHeap(items)

	got := h.peekMin()
	require.NotNil(t, got)
	require.Equal(t, earliest, got.expiry)
}

func Test_LeaseHeap_PeekMinConsistentWithPop(t *testing.T) {
	now := time.Now()
	items := []*heapItem{
		item(lease(1), now.Add(2*time.Second)),
		item(lease(2), now.Add(1*time.Second)),
		item(lease(3), now.Add(3*time.Second)),
	}
	h := buildHeap(items)

	peeked := h.peekMin()
	popped := heap.Pop(h).(*heapItem)

	require.Equal(t, peeked, popped)
}

func Test_LeaseHeap_IndexConsistencyAfterMixedOps(t *testing.T) {
	now := time.Now()
	h := &leaseHeap{}

	for i := range 10 {
		heap.Push(h, item(lease(int64(i)), now.Add(time.Duration(i)*time.Second)))
	}

	for range 5 {
		heap.Pop(h)
	}

	for i := 10; i < 15; i++ {
		heap.Push(h, item(lease(int64(i)), now.Add(time.Duration(i)*time.Second)))
	}

	for pos, item := range *h {
		require.Equal(t, pos, item.index, "item at position %d has index  %d", pos, item.index)
	}
}

func item(lease *Lease, expiry time.Time) *heapItem {
	return &heapItem{lease: lease, expiry: expiry}
}

func lease(id int64) *Lease {
	return &Lease{ID: id, keySet: make(map[LeasedKey]struct{})}
}

func buildHeap(items []*heapItem) *leaseHeap {
	h := &leaseHeap{}
	for _, item := range items {
		heap.Push(h, item)
	}
	return h
}

func drainHeap(h *leaseHeap) []time.Time {
	var out []time.Time
	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)
		out = append(out, item.expiry)
	}
	return out
}
