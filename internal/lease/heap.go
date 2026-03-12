package lease

import (
	"time"
)

type heapItem struct {
	lease *Lease
	time  time.Time
	index int
}

func (a *heapItem) less(b *heapItem) int {
	if b == nil {
		return 1
	}
	if a.time.Before(b.time) {
		return -1
	} else if a.time.After(b.time) {
		return 1
	}
	return 0
}

// leaseHeap is a minimum heap of Leases ordered by their expiration time.
type leaseHeap []*heapItem

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
	old[oldLen] = nil // gc cleans it up later
	lastItem.index = -1
	*h = old[:oldLen-1]
	return lastItem
}

func (h *leaseHeap) peekMin() *heapItem {
	if len(*h) > 0 {
		return (*h)[0]
	}
	return nil
}
