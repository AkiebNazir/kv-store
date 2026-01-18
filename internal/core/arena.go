package core

import (
	"sync/atomic"
)

// Arena manages a large continuous block of memory
// Lock-free allocation using atomic operations
type Arena struct {
	data   []byte
	offset int64 // CHANGED: atomic operations instead of mutex
}

func NewArena(size int) *Arena {
	return &Arena{
		data:   make([]byte, size),
		offset: 0,
	}
}

// Lock-free allocation using CAS
// Returns (offset, success)
func (a *Arena) Allocate(size int) (int, bool) {
	for {
		// Read current offset atomically
		currentOffset := atomic.LoadInt64(&a.offset)

		// Check if allocation would exceed capacity
		newOffset := currentOffset + int64(size)
		if newOffset > int64(len(a.data)) {
			return 0, false // OOM
		}

		// Try to atomically update offset
		if atomic.CompareAndSwapInt64(&a.offset, currentOffset, newOffset) {
			// Success! We own the space from currentOffset to newOffset
			return int(currentOffset), true
		}
		// CAS failed, retry (another thread won the race)
	}
}

// GetBytes returns the byte slice for a given offset and size
// SAFE: Read-only operation, no synchronization needed
func (a *Arena) GetBytes(offset, size int) []byte {
	// Bounds check to prevent panic
	if offset < 0 || offset+size > len(a.data) {
		return nil
	}
	return a.data[offset : offset+size]
}

// Size returns current allocated size
// Atomic read
func (a *Arena) Size() int64 {
	return atomic.LoadInt64(&a.offset)
}

// Reset arena (for testing or reuse)
func (a *Arena) Reset() {
	atomic.StoreInt64(&a.offset, 0)
}

// Available returns remaining space
func (a *Arena) Available() int64 {
	used := atomic.LoadInt64(&a.offset)
	return int64(len(a.data)) - used
}
