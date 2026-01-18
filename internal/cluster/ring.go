package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Ring struct {
	Nodes    map[uint32]string // Hash -> Node Address
	Keys     []uint32          // Sorted Hashes
	Replicas int               // Virtual Nodes per physical node
	mu       sync.RWMutex
}

func NewRing(replicas int) *Ring {
	return &Ring{
		Nodes:    make(map[uint32]string),
		Replicas: replicas,
	}
}

func (r *Ring) AddNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.Replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(addr + strconv.Itoa(i)))
		r.Nodes[hash] = addr
		r.Keys = append(r.Keys, hash)
	}
	sort.Slice(r.Keys, func(i, j int) bool { return r.Keys[i] < r.Keys[j] })
}

// GetNodes returns the N replica nodes responsible for a key
// Prevent infinite loop when all virtual nodes map to same physical node
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.Keys) == 0 {
		return nil
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary Search
	idx := sort.Search(len(r.Keys), func(i int) bool {
		return r.Keys[i] >= hash
	})

	if idx == len(r.Keys) {
		idx = 0
	}

	// Collect N unique physical nodes
	res := make([]string, 0, n)
	seen := make(map[string]bool)

	// Track iterations to prevent infinite loop
	maxIterations := len(r.Keys)
	iterations := 0

	for len(res) < n && iterations < maxIterations {
		node := r.Nodes[r.Keys[idx]]
		if !seen[node] {
			res = append(res, node)
			seen[node] = true
		}
		idx = (idx + 1) % len(r.Keys)
		iterations++
	}

	return res
}
