package main

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector tracks lookups, repairs, and storage statistics.
type MetricsCollector struct {
	mu            sync.Mutex
	latencies     []float64
	lookupsTotal  uint64
	lookupSuccess uint64
	lookupFailed  uint64
	repairCounts  map[string]uint64
	startTime     time.Time
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		latencies:    make([]float64, 0, 1000),
		repairCounts: make(map[string]uint64),
		startTime:    time.Now(),
	}
}

// LogLookup records latency (in milliseconds) and whether the lookup succeeded.
func (m *MetricsCollector) LogLookup(latencyMs float64, success bool) {
	atomic.AddUint64(&m.lookupsTotal, 1)
	if success {
		atomic.AddUint64(&m.lookupSuccess, 1)
	} else {
		atomic.AddUint64(&m.lookupFailed, 1)
	}

	m.mu.Lock()
	m.latencies = append(m.latencies, latencyMs)
	// Cap slice size to prevent unbounded memory growth in long benchmarks
	if len(m.latencies) > 20000 {
		m.latencies = m.latencies[len(m.latencies)-10000:]
	}
	m.mu.Unlock()
}

// LogRepair records a background or proactive repair event.
func (m *MetricsCollector) LogRepair(eventType string, durationMs float64) {
	m.mu.Lock()
	m.repairCounts[eventType]++
	m.mu.Unlock()
}

// DumpStorageStats dumps current memory and Go runtime stats.
func (m *MetricsCollector) DumpStorageStats() map[string]interface{} {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return map[string]interface{}{
		"alloc_bytes":       mem.Alloc,
		"total_alloc_bytes": mem.TotalAlloc,
		"sys_bytes":         mem.Sys,
		"num_gc":            mem.NumGC,
		"goroutines":        runtime.NumGoroutine(),
	}
}

// GetSummary returns a snapshot of metrics including p50, p95, p99 latencies.
func (m *MetricsCollector) GetSummary() map[string]interface{} {
	m.mu.Lock()
	lats := make([]float64, len(m.latencies))
	copy(lats, m.latencies)
	repairs := make(map[string]uint64)
	for k, v := range m.repairCounts {
		repairs[k] = v
	}
	m.mu.Unlock()

	p50, p95, p99 := calculatePercentiles(lats)

	total := atomic.LoadUint64(&m.lookupsTotal)
	success := atomic.LoadUint64(&m.lookupSuccess)
	failed := atomic.LoadUint64(&m.lookupFailed)

	var successRate float64
	if total > 0 {
		successRate = float64(success) / float64(total) * 100.0
	}

	return map[string]interface{}{
		"uptime_seconds": time.Since(m.startTime).Seconds(),
		"lookups_total":  total,
		"lookup_success": success,
		"lookup_failed":  failed,
		"success_rate":   successRate,
		"latency_p50_ms": p50,
		"latency_p95_ms": p95,
		"latency_p99_ms": p99,
		"repair_counts":  repairs,
		"storage":        m.DumpStorageStats(),
	}
}

func calculatePercentiles(data []float64) (p50, p95, p99 float64) {
	if len(data) == 0 {
		return 0, 0, 0
	}
	sort.Float64s(data)
	n := len(data)
	// Nearest-rank percentiles, shared with the orchestrator.
	p50 = data[(n*50+99)/100-1]
	p95 = data[(n*95+99)/100-1]
	p99 = data[(n*99+99)/100-1]
	return p50, p95, p99
}
