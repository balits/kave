package mvcc

import (
	"github.com/balits/kave/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func newKVMetrics(reg prometheus.Registerer, s *KvStore) *metrics.KVMetrics {
	// double locking but ugh....
	currentRevFn := func() float64 {
		rev, _ := s.Revisions()
		return float64(rev.Main)
	}
	compactedRevFn := func() float64 {
		_, compacted := s.Revisions()
		return float64(compacted)
	}
	keyCountFn := func() float64 {
		return float64(s.KeyCount())
	}

	return metrics.NewKVMetrics(reg, currentRevFn, compactedRevFn, keyCountFn)
}
