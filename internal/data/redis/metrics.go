package redis

import (
	"fmt"
	"io"
)

// WriteMetrics emits per-process counters with fixed-cardinality labels. These
// are local observations, not replicated gauges that should be summed twice.
func (c *Coordinator) WriteMetrics(w io.Writer) {
	m := &c.Metrics
	_, _ = fmt.Fprintf(w, "route_mind_coordination_operations_total %d\nroute_mind_coordination_errors_total %d\nroute_mind_coordination_duration_seconds_sum %g\nroute_mind_coordination_lease_lost_total %d\nroute_mind_coordination_half_open_probes_total %d\n", m.Operations.Load(), m.Errors.Load(), float64(m.Nanoseconds.Load())/1e9, m.LeaseLost.Load(), m.Probes.Load())
	_, _ = fmt.Fprintf(w, "route_mind_coordination_rejections_total{reason=\"principal\"} %d\nroute_mind_coordination_rejections_total{reason=\"capacity\"} %d\nroute_mind_coordination_rejections_total{reason=\"circuit\"} %d\n", m.RateDenied.Load(), m.CapacityDenied.Load(), m.CircuitDenied.Load())
}
