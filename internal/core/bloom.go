package core

import (
	"hash/fnv"
	"math"
	"sync/atomic"
)

type BloomFilter struct {
	bitset        []uint64
	size          uint
	hashes        int
	expectedItems int   // Track expected items
	actualItems   int64 // Track actual items added
}

// Calculate optimal parameters based on desired false positive rate
func NewBloomFilter(expectedItems int) *BloomFilter {
	return NewBloomFilterWithFPRate(expectedItems, 0.01) // 1% FP rate default
}

// NEW: Create bloom filter with custom false positive rate
func NewBloomFilterWithFPRate(expectedItems int, fpRate float64) *BloomFilter {
	// Guard against invalid inputs
	if expectedItems <= 0 {
		expectedItems = 1000 // Reasonable default
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01 // 1% default
	}

	// Optimal bloom filter sizing:
	// m = -n * ln(p) / (ln(2)^2)  where m=bits, n=items, p=FP rate
	// k = (m/n) * ln(2)           where k=hash functions

	n := float64(expectedItems)
	p := fpRate

	// Calculate optimal bit array size
	m := -n * math.Log(p) / (math.Ln2 * math.Ln2)
	size := uint(math.Ceil(m))

	// Ensure minimum size
	if size < 64 {
		size = 64
	}

	// Calculate optimal number of hash functions
	k := int(math.Ceil((m / n) * math.Ln2))

	// Clamp k to reasonable range [1, 20]
	if k < 1 {
		k = 1
	}
	if k > 20 {
		k = 20 // Diminishing returns beyond this
	}

	return &BloomFilter{
		bitset:        make([]uint64, (size+63)/64),
		size:          size,
		hashes:        k,
		expectedItems: expectedItems,
		actualItems:   0,
	}
}

func (bf *BloomFilter) Add(key string) {
	// Track actual items (already atomic)
	atomic.AddInt64(&bf.actualItems, 1)

	// Atomic bitset updates
	for i := 0; i < bf.hashes; i++ {
		idx := bf.hash(key, i) % bf.size
		bucketIdx := idx / 64
		bitPos := idx % 64
		bitMask := uint64(1) << bitPos

		// Lock-free atomic OR using CAS loop
		// This is the standard pattern for atomic bit manipulation
		for {
			oldVal := atomic.LoadUint64(&bf.bitset[bucketIdx])
			newVal := oldVal | bitMask

			// If bit already set, no need to CAS
			if oldVal == newVal {
				break
			}

			// Try to update atomically
			if atomic.CompareAndSwapUint64(&bf.bitset[bucketIdx], oldVal, newVal) {
				break
			}
			// CAS failed, retry with new oldVal (no sleep needed - will succeed quickly)
		}
	}
}

func (bf *BloomFilter) MaybeContains(key string) bool {
	for i := 0; i < bf.hashes; i++ {
		idx := bf.hash(key, i) % bf.size
		bucketIdx := idx / 64
		bitPos := idx % 64

		// Atomic read for consistency
		val := atomic.LoadUint64(&bf.bitset[bucketIdx])
		if val&(1<<bitPos) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(key string, seed int) uint {
	h := fnv.New64a()
	h.Write([]byte{byte(seed)})
	h.Write([]byte(key))
	return uint(h.Sum64())
}

// Get actual false positive rate based on fill ratio
func (bf *BloomFilter) EstimatedFPRate() float64 {
	actual := atomic.LoadInt64(&bf.actualItems)
	if actual == 0 {
		return 0
	}

	// FP rate ≈ (1 - e^(-kn/m))^k
	// where k=hashes, n=items, m=size
	k := float64(bf.hashes)
	n := float64(actual)
	m := float64(bf.size)

	exponent := -k * n / m
	base := 1 - math.Exp(exponent)
	fpRate := math.Pow(base, k)

	return fpRate
}

// Check if bloom filter is oversaturated
func (bf *BloomFilter) IsOverSaturated() bool {
	actual := atomic.LoadInt64(&bf.actualItems)
	// Warn if we've exceeded expected items by 50%
	return actual > int64(float64(bf.expectedItems)*1.5)
}

// Statistics for monitoring
func (bf *BloomFilter) Stats() map[string]interface{} {
	actual := atomic.LoadInt64(&bf.actualItems)
	return map[string]interface{}{
		"expected_items": bf.expectedItems,
		"actual_items":   actual,
		"size_bits":      bf.size,
		"hash_functions": bf.hashes,
		"fp_rate":        bf.EstimatedFPRate(),
		"oversaturated":  bf.IsOverSaturated(),
		"fill_ratio":     float64(actual) / float64(bf.expectedItems),
	}
}
