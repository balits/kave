package mvcc

import (
	"github.com/balits/kave/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func newKVMetrics(reg prometheus.Registerer, s *KVStore) *metrics.KVMetrics {
	currentRevFn := func() float64 {
		rev, _ := s.Revisions()
		return float64(rev.Main)
	}
	compactedRevFn := func() float64 {
		_, compacted := s.Revisions()
		return float64(compacted)
	}

	return metrics.NewKVMetrics(reg, currentRevFn, compactedRevFn)
}
