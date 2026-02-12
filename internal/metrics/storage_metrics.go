package metrics

import "sync/atomic"

type StorageMetricsProvider interface {
	StorageMetrics() *StorageMetrics
}

type StorageMetrics struct {
	SetCount    uint64
	GetCount    uint64
	DeleteCount uint64
	KeyCount    uint64

	ByteSize uint64 //TODO overflow
}

type StorageMetricsAtomic struct {
	SetCount    atomic.Uint64
	GetCount    atomic.Uint64
	DeleteCount atomic.Uint64
	KeyCount    atomic.Uint64
	ByteSize    atomic.Uint64
}

func (a *StorageMetricsAtomic) StorageMetrics() *StorageMetrics {
	return &StorageMetrics{
		SetCount:    a.SetCount.Load(),
		GetCount:    a.GetCount.Load(),
		DeleteCount: a.DeleteCount.Load(),
		KeyCount:    a.KeyCount.Load(),
		ByteSize:    a.ByteSize.Load(),
	}
}
