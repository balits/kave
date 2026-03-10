package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type gaugeFn func() float64

func InitPrometheus() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

func InitTestPrometheus() *prometheus.Registry {
	if !testing.Testing() {
		panic("InitTestPrometheus should only be used in tests")
	}
	return prometheus.NewRegistry()
}
