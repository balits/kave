package util

import "sync/atomic"

func AtomicSubNoUnderflow(u *atomic.Uint64, n uint64) (new uint64) {
	if n == 0 {
		return u.Load()
	}

	for {
		old := u.Load()

		if old < n {
			new = 0
		} else {
			new = old - n
		}

		if u.CompareAndSwap(old, new) {
			return new
		}
		// CAS failed → retry
	}
}
