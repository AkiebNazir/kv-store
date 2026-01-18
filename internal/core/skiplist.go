package core

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel = 16
	p        = 0.5
)

type Node struct {
	keyOff, keyLen int
	valOff, valLen int
	next           []*Node
}

type SkipList struct {
	head  *Node
	level int
	mu    sync.RWMutex
	Size  int64
	arena *Arena
}

func NewSkipList(maxSize int64) *SkipList {
	rand.Seed(time.Now().UnixNano())
	return &SkipList{
		head:  &Node{next: make([]*Node, maxLevel)},
		level: 1,
		Size:  0,
		arena: NewArena(int(maxSize)),
	}
}

func (s *SkipList) Put(key, value []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*Node, maxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(s.getKey(current.next[i]), key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]

	// Update existing key
	if current != nil && bytes.Equal(s.getKey(current), key) {
		oldValLen := current.valLen

		valOff, ok := s.arena.Allocate(len(value))
		if !ok {
			return false
		}

		s.Size = s.Size - int64(oldValLen) + int64(len(value))

		copy(s.arena.data[valOff:], value)
		current.valOff = valOff
		current.valLen = len(value)

		return true
	}

	// Insert new node
	lvl := randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	// Check space before allocating
	spaceNeeded := int64(len(key) + len(value))
	if s.arena.Available() < spaceNeeded {
		return false
	}

	keyOff, ok1 := s.arena.Allocate(len(key))
	valOff, ok2 := s.arena.Allocate(len(value))
	if !ok1 || !ok2 {
		return false
	}

	copy(s.arena.data[keyOff:], key)
	copy(s.arena.data[valOff:], value)

	newNode := &Node{
		keyOff: keyOff,
		keyLen: len(key),
		valOff: valOff,
		valLen: len(value),
		next:   make([]*Node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	s.Size += int64(len(key) + len(value))
	return true
}

func (s *SkipList) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(s.getKey(current.next[i]), key) < 0 {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && bytes.Equal(s.getKey(current), key) {
		return s.getVal(current), true
	}
	return nil, false
}

// Proper iterator implementation
type Iterator struct {
	current *Node
	list    *SkipList
}

func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{
		current: s.head,
		list:    s,
	}
}

// Next() advances to next element
// Call Next() before accessing Key()/Value() for first time
func (it *Iterator) Next() bool {
	if it.current == nil {
		return false
	}

	// Advance to next node
	it.current = it.current.next[0]

	// Return true if we have a valid node (not head, not nil)
	return it.current != nil
}

// Key() never returns nil after successful Next()
func (it *Iterator) Key() []byte {
	if it.current == nil || it.current == it.list.head {
		return nil
	}
	return it.list.getKey(it.current)
}

// Value() never returns nil after successful Next()
func (it *Iterator) Value() []byte {
	if it.current == nil || it.current == it.list.head {
		return nil
	}
	return it.list.getVal(it.current)
}

func (s *SkipList) getKey(n *Node) []byte {
	if n == s.head {
		return nil
	}
	return s.arena.GetBytes(n.keyOff, n.keyLen)
}

func (s *SkipList) getVal(n *Node) []byte {
	if n == s.head {
		return nil
	}
	return s.arena.GetBytes(n.valOff, n.valLen)
}

func randomLevel() int {
	lvl := 1
	for rand.Float64() < p && lvl < maxLevel {
		lvl++
	}
	return lvl
}
