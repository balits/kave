package lease

import (
	"time"
)

const initHeapSize = 32

type heapItem struct {
	lease  *Lease
	expiry time.Time
	index  int
}

func (a *heapItem) less(b *heapItem) int {
	if b == nil {
		return 1
	}
	if a.expiry.Before(b.expiry) {
		return -1
	} else if a.expiry.After(b.expiry) {
		return 1
	}
	return 0
}

// leaseHeap egy minimum kupac ahol a hátralévő alapján rendezzük az elemeket
type leaseHeap []*heapItem

func NewLeaseHeap() leaseHeap {
	return make(leaseHeap, 0, initHeapSize)
}

func (h leaseHeap) Len() int           { return len(h) }
func (h leaseHeap) Less(i, j int) bool { return h[i].less(h[j]) < 0 }
func (h leaseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *leaseHeap) Push(x any) {
	n := len(*h)
	item := x.(*heapItem)
	item.index = n
	*h = append(*h, item)
}

func (h *leaseHeap) Pop() any {
	oldLen := len(*h)
	old := *h
	lastItem := old[oldLen-1]
	old[oldLen-1] = nil // gc cleans it up later
	lastItem.index = -1
	*h = old[:oldLen-1]
	return lastItem
}

// peekMin visszaadja a legkisebb elemet a kupacból,
// anélkül hogy kitörölnénk azt
func (h *leaseHeap) peekMin() *heapItem {
	if len(*h) > 0 {
		return (*h)[0]
	}
	return nil
}
