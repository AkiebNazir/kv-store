package metrics

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Latency histogram for proper percentile tracking
type LatencyHistogram struct {
	buckets [20]int64 // Exponential buckets: <1µs, <2µs, <4µs, ..., <1s, >1s
	mu      sync.RWMutex
}

func (lh *LatencyHistogram) Record(latencyNs int64) {
	// Convert to microseconds for bucketing
	latencyUs := latencyNs / 1000

	bucketIdx := 0
	threshold := int64(1)

	// Find appropriate bucket (exponential: 1, 2, 4, 8, 16, ...)
	for bucketIdx < 19 && latencyUs >= threshold {
		bucketIdx++
		threshold *= 2
	}

	atomic.AddInt64(&lh.buckets[bucketIdx], 1)
}

func (lh *LatencyHistogram) Get99thPercentile() int64 {
	// Calculate 99th percentile from histogram
	total := int64(0)
	for i := 0; i < 20; i++ {
		total += atomic.LoadInt64(&lh.buckets[i])
	}

	if total == 0 {
		return 0
	}

	target := total * 99 / 100
	cumulative := int64(0)

	for i := 0; i < 20; i++ {
		cumulative += atomic.LoadInt64(&lh.buckets[i])
		if cumulative >= target {
			// Return upper bound of this bucket in nanoseconds
			return (1 << uint(i)) * 1000
		}
	}

	return 0
}

type Metrics struct {
	// --- High Frequency Metrics ---
	TotalWrites     int64
	TotalWriteBytes int64

	// Use histogram instead of single value
	WriteLatencyHist LatencyHistogram

	TotalReads      int64
	TotalReadBytes  int64
	ReadLatencyHist LatencyHistogram

	TotalScans      int64
	ScanLatencyHist LatencyHistogram

	TotalDeletes      int64
	DeleteLatencyHist LatencyHistogram

	TotalBatches     int64
	BatchLatencyHist LatencyHistogram

	// --- Error Tracking ---
	TotalErrors int64

	// --- Low Frequency / Engine Metrics ---
	WALSyncs           int64
	WALSyncLatencyHist LatencyHistogram

	Compactions     int64
	MemTableFlushes int64

	// --- Internal State for Rate Calculation ---
	lastWrites  int64
	lastReads   int64
	lastScans   int64
	lastDeletes int64
	lastBatches int64
	lastErrors  int64
	lastTime    time.Time
	mu          sync.Mutex
}

var GlobalMetrics = &Metrics{lastTime: time.Now()}

func (m *Metrics) RecordWrite(bytes int, latencyNs int64) {
	atomic.AddInt64(&m.TotalWrites, 1)
	atomic.AddInt64(&m.TotalWriteBytes, int64(bytes))
	m.WriteLatencyHist.Record(latencyNs)
}

func (m *Metrics) RecordRead(bytes int, latencyNs int64) {
	atomic.AddInt64(&m.TotalReads, 1)
	atomic.AddInt64(&m.TotalReadBytes, int64(bytes))
	m.ReadLatencyHist.Record(latencyNs)
}

func (m *Metrics) RecordScan(latencyNs int64) {
	atomic.AddInt64(&m.TotalScans, 1)
	m.ScanLatencyHist.Record(latencyNs)
}

func (m *Metrics) RecordDelete(latencyNs int64) {
	atomic.AddInt64(&m.TotalDeletes, 1)
	m.DeleteLatencyHist.Record(latencyNs)
}

func (m *Metrics) RecordBatch(items int, latencyNs int64) {
	atomic.AddInt64(&m.TotalBatches, 1)
	atomic.AddInt64(&m.TotalWrites, int64(items))
	m.BatchLatencyHist.Record(latencyNs)
}

func (m *Metrics) RecordError() {
	atomic.AddInt64(&m.TotalErrors, 1)
}

func (m *Metrics) RecordWALSync(latencyNs int64) {
	atomic.AddInt64(&m.WALSyncs, 1)
	m.WALSyncLatencyHist.Record(latencyNs)
}

func (m *Metrics) RecordCompaction() {
	atomic.AddInt64(&m.Compactions, 1)
}

func (m *Metrics) RecordMemTableFlush() {
	atomic.AddInt64(&m.MemTableFlushes, 1)
}

func (m *Metrics) PrintStats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(m.lastTime).Seconds()
	if elapsed == 0 {
		return
	}

	// 1. Snapshot
	currWrites := atomic.LoadInt64(&m.TotalWrites)
	currReads := atomic.LoadInt64(&m.TotalReads)
	currScans := atomic.LoadInt64(&m.TotalScans)
	currDeletes := atomic.LoadInt64(&m.TotalDeletes)
	currBatches := atomic.LoadInt64(&m.TotalBatches)
	currErrors := atomic.LoadInt64(&m.TotalErrors)

	currCompactions := atomic.LoadInt64(&m.Compactions)
	currFlushes := atomic.LoadInt64(&m.MemTableFlushes)
	currWAL := atomic.LoadInt64(&m.WALSyncs)

	// 2. Deltas
	dWrites := currWrites - m.lastWrites
	dReads := currReads - m.lastReads
	dScans := currScans - m.lastScans
	dDeletes := currDeletes - m.lastDeletes
	dBatches := currBatches - m.lastBatches
	dErrors := currErrors - m.lastErrors

	// 3. Update State
	m.lastWrites = currWrites
	m.lastReads = currReads
	m.lastScans = currScans
	m.lastDeletes = currDeletes
	m.lastBatches = currBatches
	m.lastErrors = currErrors
	m.lastTime = now

	// 4. Rates
	wRps := float64(dWrites) / elapsed
	rRps := float64(dReads) / elapsed
	sRps := float64(dScans) / elapsed
	dRps := float64(dDeletes) / elapsed
	bRps := float64(dBatches) / elapsed
	errRps := float64(dErrors) / elapsed

	// 5. P99 Latencies (µs)
	wLat := float64(m.WriteLatencyHist.Get99thPercentile()) / 1000.0
	rLat := float64(m.ReadLatencyHist.Get99thPercentile()) / 1000.0
	sLat := float64(m.ScanLatencyHist.Get99thPercentile()) / 1000.0
	dLat := float64(m.DeleteLatencyHist.Get99thPercentile()) / 1000.0
	bLat := float64(m.BatchLatencyHist.Get99thPercentile()) / 1000.0

	// 6. Runtime Metrics
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	allocMB := mem.Alloc / 1024 / 1024
	numGC := mem.NumGC

	fmt.Printf("\n========== API METRICS (Last %.1fs) ==========\n", elapsed)
	fmt.Printf("Reads:   %6.0f/s | P99: %7.1f µs\n", rRps, rLat)
	fmt.Printf("Writes:  %6.0f/s | P99: %7.1f µs\n", wRps, wLat)
	fmt.Printf("Scans:   %6.0f/s | P99: %7.1f µs\n", sRps, sLat)
	fmt.Printf("Deletes: %6.0f/s | P99: %7.1f µs\n", dRps, dLat)
	fmt.Printf("Batches: %6.0f/s | P99: %7.1f µs\n", bRps, bLat)
	fmt.Printf("Errors:  %6.0f/s | Total: %d\n", errRps, currErrors)
	fmt.Printf("----------------------------------------------\n")
	fmt.Printf("System -> RAM: %d MB | GC Cycles: %d | Goroutines: %d\n", allocMB, numGC, runtime.NumGoroutine())
	fmt.Printf("Engine -> WAL: %d | Flush: %d | Compact: %d\n", currWAL, currFlushes, currCompactions)
	fmt.Printf("==============================================\n")
}
