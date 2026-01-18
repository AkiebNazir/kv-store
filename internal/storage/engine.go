package storage

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"kvstore/internal/config"
	"kvstore/internal/core"
	"kvstore/internal/metrics"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- DATA STRUCTURES ---

type IndexEntry struct {
	Key    string
	Offset int64
}

// SSTableMetadata holds index data.
// CHANGED: No longer holds an open *os.File to prevent FD exhaustion.
type SSTableMetadata struct {
	Filename    string
	Level       int
	Size        int64
	Bloom       *core.BloomFilter
	SparseIndex []IndexEntry
	MinKey      []byte
	MaxKey      []byte

	// Reference counting for safe deletion (ensures we don't delete a file being read)
	refCount int32
	deleted  atomic.Bool
}

func (sst *SSTableMetadata) Acquire() bool {
	if sst.deleted.Load() {
		return false
	}
	atomic.AddInt32(&sst.refCount, 1)
	if sst.deleted.Load() {
		atomic.AddInt32(&sst.refCount, -1)
		return false
	}
	return true
}

func (sst *SSTableMetadata) Release() {
	newCount := atomic.AddInt32(&sst.refCount, -1)
	if newCount == 0 && sst.deleted.Load() {
		os.Remove(sst.Filename)
	}
}

func (sst *SSTableMetadata) MarkDeleted() {
	sst.deleted.Store(true)
	if atomic.LoadInt32(&sst.refCount) == 0 {
		os.Remove(sst.Filename)
	}
}

// LRUCache (O(1) access/eviction)
type LRUCache struct {
	capacity int
	mu       sync.RWMutex
	cache    map[string]*list.Element
	lruList  *list.List
}

type cacheEntry struct {
	key   string
	value []byte
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lruList:  list.New(),
	}
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value, true
	}
	return nil, false
}

func (c *LRUCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value.(*cacheEntry).value = value
		return
	}

	if c.lruList.Len() >= c.capacity {
		oldest := c.lruList.Back()
		if oldest != nil {
			c.lruList.Remove(oldest)
			delete(c.cache, oldest.Value.(*cacheEntry).key)
		}
	}

	entry := &cacheEntry{key: key, value: value}
	elem := c.lruList.PushFront(entry)
	c.cache[key] = elem
}

type Engine struct {
	cfg       *config.Config
	activeMem atomic.Pointer[core.SkipList]
	immutMem  atomic.Pointer[core.SkipList]
	wal       *WAL
	dir       string

	sstMu  sync.RWMutex
	levels [5][]*SSTableMetadata

	// Coordination flags
	compactionInProgress [5]atomic.Bool
	flushScheduled       atomic.Bool

	flushQueue chan *core.SkipList
	flushDone  chan struct{}
	compChan   chan int

	cache          *LRUCache
	shutdown       chan struct{}
	shutdownOnce   sync.Once
	wg             sync.WaitGroup
	writesInFlight atomic.Int64
}

// --- CONSTRUCTOR & RECOVERY ---

func NewEngine(cfg *config.Config, dir string) *Engine {
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(err)
	}

	wal, err := OpenWAL(cfg, dir)
	if err != nil {
		panic(err)
	}

	e := &Engine{
		cfg:        cfg,
		wal:        wal,
		dir:        dir,
		flushQueue: make(chan *core.SkipList, 5),
		flushDone:  make(chan struct{}, 1),
		compChan:   make(chan int, 10),
		cache:      NewLRUCache(65536),
		shutdown:   make(chan struct{}),
	}

	newMem := core.NewSkipList(cfg.MemTableSize)
	e.activeMem.Store(newMem)

	e.loadExistingSSTables()

	e.wg.Add(1)
	go e.flushDaemon()

	e.wg.Add(1)
	go e.compactionCoordinator()

	// Recovery
	replayCount := 0
	replayStart := time.Now()

	err = wal.Replay(func(k, v []byte) {
		// Skip oversize keys to prevent infinite crash loops
		if int64(len(k)+len(v)) > cfg.MemTableSize {
			fmt.Printf("CRITICAL: Skipping oversized WAL entry (Key: %d, Val: %d). Increase MEMTABLE_SIZE.\n", len(k), len(v))
			return
		}

		if !e.writeToMemDirect(k, v) {
			e.forceFlushBlocking()

			// Retry with backoff
			retries := 0
			for retries < 10 {
				if e.writeToMemDirect(k, v) {
					break
				}
				time.Sleep(10 * time.Millisecond)
				retries++
			}
			if retries >= 10 {
				panic(fmt.Sprintf("FATAL: WAL replay failed after flush for key: %s", string(k)))
			}
		}
		replayCount++
		if replayCount%10000 == 0 {
			fmt.Printf("  Replayed %d entries...\n", replayCount)
		}
	})

	if err != nil {
		panic(fmt.Sprintf("FATAL: WAL replay error: %v", err))
	}

	if replayCount > 0 {
		fmt.Printf("✓ Replayed %d WAL entries in %v\n", replayCount, time.Since(replayStart))
		e.forceFlushBlocking()
	}

	return e
}

func (e *Engine) loadExistingSSTables() {
	files, err := os.ReadDir(e.dir)
	if err != nil {
		return
	}

	loadStart := time.Now()
	var counts [5]int

	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}

		name := fileInfo.Name()
		if !strings.HasPrefix(name, "sst_") || !strings.HasSuffix(name, ".db") {
			continue
		}

		var level int
		if strings.Contains(name, "_l0_") {
			level = 0
		} else if strings.Contains(name, "_l1_") {
			level = 1
		} else if strings.Contains(name, "_l2_") {
			level = 2
		} else if strings.Contains(name, "_l3_") {
			level = 3
		} else if strings.Contains(name, "_l4_") {
			level = 4
		} else {
			continue
		}

		fullPath := filepath.Join(e.dir, name)
		meta := e.buildSSTableMetadata(fullPath, level)
		if meta != nil {
			e.levels[level] = append(e.levels[level], meta)
			counts[level]++
		}
	}

	totalFiles := counts[0] + counts[1] + counts[2] + counts[3] + counts[4]
	if totalFiles > 0 {
		fmt.Printf("✓ Loaded %d SSTables in %v (L0:%d L1:%d L2:%d L3:%d L4:%d)\n",
			totalFiles, time.Since(loadStart),
			counts[0], counts[1], counts[2], counts[3], counts[4])
	}
}

// buildSSTableMetadata opens file briefly, reads index, and closes it.
// FIX: No longer keeps file open.
func (e *Engine) buildSSTableMetadata(filename string, level int) *SSTableMetadata {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Printf("WARNING: Failed to open SSTable %s: %v\n", filename, err)
		return nil
	}
	defer f.Close()

	stat, _ := f.Stat()
	bloomSize := e.cfg.BloomFilterSizeL0
	if level > 0 {
		bloomSize = e.cfg.BloomFilterSizeL1
	}
	bloom := core.NewBloomFilter(bloomSize)

	var sparseIndex []IndexEntry
	var offset int64
	var count int
	var minKey, maxKey []byte

	for {
		// FIX: Read 12 bytes header (KeyLen + ValLen + CRC)
		header := make([]byte, 12)
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}

		kLen := binary.LittleEndian.Uint32(header[0:4])
		vLen := binary.LittleEndian.Uint32(header[4:8])
		// crc := binary.LittleEndian.Uint32(header[8:12]) // Skip verification on load for speed

		if kLen > 10*1024*1024 || vLen > 100*1024*1024 {
			fmt.Printf("WARNING: Corrupted SSTable %s at offset %d\n", filename, offset)
			break
		}

		data := make([]byte, kLen+vLen)
		if _, err := io.ReadFull(f, data); err != nil {
			break
		}

		k := data[:kLen]

		if count == 0 {
			minKey = append([]byte(nil), k...)
		}
		maxKey = append([]byte(nil), k...)

		if count%e.cfg.SparseIndexInterval == 0 {
			sparseIndex = append(sparseIndex, IndexEntry{Key: string(k), Offset: offset})
		}

		bloom.Add(string(k))
		offset += int64(12 + kLen + vLen)
		count++
	}

	if count == 0 {
		return nil
	}

	return &SSTableMetadata{
		Filename:    filename,
		Level:       level,
		Size:        stat.Size(),
		Bloom:       bloom,
		SparseIndex: sparseIndex,
		MinKey:      minKey,
		MaxKey:      maxKey,
		refCount:    0,
	}
}

// --- PUBLIC API ---

func (e *Engine) Put(key, value []byte) error {
	start := time.Now()
	e.writesInFlight.Add(1)
	defer e.writesInFlight.Add(-1)

	versionedVal := make([]byte, 8+len(value))
	binary.LittleEndian.PutUint64(versionedVal[:8], uint64(time.Now().UnixNano()))
	copy(versionedVal[8:], value)

	if err := e.wal.Append(key, versionedVal); err != nil {
		return err
	}

	if !e.writeToMem(key, versionedVal) {
		return fmt.Errorf("failed to write to memtable after retries")
	}

	metrics.GlobalMetrics.RecordWrite(len(key)+len(value), time.Since(start).Nanoseconds())
	return nil
}

func (e *Engine) PutRaw(key, rawValue []byte) error {
	e.writesInFlight.Add(1)
	defer e.writesInFlight.Add(-1)

	if err := e.wal.Append(key, rawValue); err != nil {
		return err
	}

	if !e.writeToMem(key, rawValue) {
		return fmt.Errorf("failed to write to memtable after retries")
	}
	return nil
}

func (e *Engine) BatchPut(keys, values [][]byte) error {
	start := time.Now()
	e.writesInFlight.Add(1)
	defer e.writesInFlight.Add(-1)

	versionedValues := make([][]byte, len(values))
	now := uint64(time.Now().UnixNano())
	for i, v := range values {
		vv := make([]byte, 8+len(v))
		binary.LittleEndian.PutUint64(vv[:8], now)
		copy(vv[8:], v)
		versionedValues[i] = vv
	}

	// 1. Write to WAL (Durability barrier)
	if err := e.wal.BatchAppend(keys, versionedValues); err != nil {
		return err
	}

	// 2. Write to MemTable
	// If WAL succeeds, we MUST succeed in memory eventually.
	// We cannot return an error here, or we get phantom data on restart.
	var totalBytes int
	for i := range keys {
		for !e.writeToMem(keys[i], versionedValues[i]) {
			// Backpressure / Retry loop
			time.Sleep(10 * time.Millisecond)
		}
		totalBytes += len(keys[i]) + len(versionedValues[i])
	}

	metrics.GlobalMetrics.RecordWrite(totalBytes, time.Since(start).Nanoseconds())
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, bool) {
	raw, found := e.GetRaw(key)
	if !found {
		return nil, false
	}
	return e.checkTombstone(raw[8:])
}

func (e *Engine) GetRaw(key []byte) ([]byte, bool) {
	// 1. MemTables
	if val, found := e.activeMem.Load().Get(key); found {
		return val, true
	}

	if immut := e.immutMem.Load(); immut != nil {
		if val, found := immut.Get(key); found {
			return val, true
		}
	}

	// 2. Cache
	cacheKey := string(key)
	if val, ok := e.cache.Get(cacheKey); ok {
		return val, true
	}

	// 3. Disk
	e.sstMu.RLock()
	candidateFiles := make([]*SSTableMetadata, 0, 10)

	for i := len(e.levels[0]) - 1; i >= 0; i-- {
		if e.levels[0][i].Acquire() {
			candidateFiles = append(candidateFiles, e.levels[0][i])
		}
	}
	for l := 1; l < len(e.levels); l++ {
		for _, sst := range e.levels[l] {
			if sst.Acquire() {
				candidateFiles = append(candidateFiles, sst)
			}
		}
	}
	e.sstMu.RUnlock()

	defer func() {
		for _, sst := range candidateFiles {
			sst.Release()
		}
	}()

	for _, sst := range candidateFiles {
		if val, found := e.searchSSTable(sst, key); found {
			e.cache.Put(cacheKey, val)
			return val, true
		}
	}

	return nil, false
}

func (e *Engine) Scan(start, end []byte) map[string]string {
	res := make(map[string]string)

	memIt := e.activeMem.Load().NewIterator()
	for memIt.Next() {
		k, v := memIt.Key(), memIt.Value()
		if k == nil {
			continue
		}
		if bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) <= 0 {
			res[string(k)] = string(v[8:])
		}
	}

	if immut := e.immutMem.Load(); immut != nil {
		immutIt := immut.NewIterator()
		for immutIt.Next() {
			k, v := immutIt.Key(), immutIt.Value()
			if k == nil {
				continue
			}
			if bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) <= 0 {
				if _, exists := res[string(k)]; !exists {
					res[string(k)] = string(v[8:])
				}
			}
		}
	}

	e.sstMu.RLock()
	candidateFiles := make([]*SSTableMetadata, 0, 50)
	for l := 0; l < len(e.levels); l++ {
		for _, sst := range e.levels[l] {
			if len(sst.MaxKey) > 0 && bytes.Compare(sst.MaxKey, start) < 0 {
				continue
			}
			if len(sst.MinKey) > 0 && bytes.Compare(sst.MinKey, end) > 0 {
				continue
			}
			if sst.Acquire() {
				candidateFiles = append(candidateFiles, sst)
			}
		}
	}
	e.sstMu.RUnlock()

	defer func() {
		for _, sst := range candidateFiles {
			sst.Release()
		}
	}()

	for _, sst := range candidateFiles {
		it := e.newSSTableIterator(sst)
		for it.Next() {
			k, v := it.Key(), it.Value()
			if bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) <= 0 {
				if _, ok := res[string(k)]; !ok {
					res[string(k)] = string(v[8:])
				}
			}
			if bytes.Compare(k, end) > 0 {
				break
			}
		}
		it.Close()
	}

	for k, v := range res {
		if v == "__DELETED__" {
			delete(res, k)
		}
	}

	return res
}

func (e *Engine) searchSSTable(sst *SSTableMetadata, key []byte) ([]byte, bool) {
	if len(sst.MinKey) > 0 && bytes.Compare(key, sst.MinKey) < 0 {
		return nil, false
	}
	if len(sst.MaxKey) > 0 && bytes.Compare(key, sst.MaxKey) > 0 {
		return nil, false
	}

	if !sst.Bloom.MaybeContains(string(key)) {
		return nil, false
	}

	idx := sort.Search(len(sst.SparseIndex), func(i int) bool {
		return sst.SparseIndex[i].Key >= string(key)
	})

	startPos := int64(0)
	if idx > 0 {
		startPos = sst.SparseIndex[idx-1].Offset
	}

	endPos := int64(-1)
	if idx < len(sst.SparseIndex) {
		endPos = sst.SparseIndex[idx].Offset
	}

	return e.scanDiskSegment(sst, startPos, endPos, key)
}

func (e *Engine) scanDiskSegment(sst *SSTableMetadata, startPos, endPos int64, targetKey []byte) ([]byte, bool) {
	// FIX: Open file on demand
	f, err := os.Open(sst.Filename)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	currentPos := startPos
	for {
		if endPos > 0 && currentPos >= endPos {
			break
		}

		// FIX: Read 12 byte header
		header := make([]byte, 12)
		if _, err := f.ReadAt(header, currentPos); err != nil {
			break
		}

		kLen := binary.LittleEndian.Uint32(header[0:4])
		vLen := binary.LittleEndian.Uint32(header[4:8])
		expectedCRC := binary.LittleEndian.Uint32(header[8:12])

		data := make([]byte, kLen+vLen)
		if _, err := f.ReadAt(data, currentPos+12); err != nil {
			break
		}

		// FIX: Verify CRC
		if crc32.ChecksumIEEE(data) != expectedCRC {
			fmt.Printf("CRC mismatch in %s at %d\n", sst.Filename, currentPos)
			return nil, false
		}

		currentPos += int64(12 + kLen + vLen)

		if bytes.Equal(data[:kLen], targetKey) {
			return data[kLen:], true
		}

		if bytes.Compare(data[:kLen], targetKey) > 0 {
			break
		}
	}
	return nil, false
}

func (e *Engine) checkTombstone(val []byte) ([]byte, bool) {
	if bytes.Equal(val, []byte("__DELETED__")) {
		return nil, false
	}
	return val, true
}

func (e *Engine) writeToMemDirect(key, val []byte) bool {
	mem := e.activeMem.Load()
	return mem.Put(key, val)
}

// --- FLUSH LOGIC ---

func (e *Engine) forceFlushBlocking() {
	select {
	case <-e.shutdown:
		return
	default:
	}

	newMem := core.NewSkipList(e.cfg.MemTableSize)
	oldMem := e.activeMem.Swap(newMem)
	e.immutMem.Store(oldMem)

	select {
	case e.flushQueue <- oldMem:
		<-e.flushDone
	case <-e.shutdown:
		return
	}
}

func (e *Engine) triggerFlush() {
	select {
	case <-e.shutdown:
		return
	default:
	}

	newMem := core.NewSkipList(e.cfg.MemTableSize)
	oldMem := e.activeMem.Swap(newMem)
	e.immutMem.Store(oldMem)

	select {
	case e.flushQueue <- oldMem:
	case <-e.shutdown:
		return
	default:
		e.flushQueue <- oldMem
	}
}

func (e *Engine) flushDaemon() {
	defer e.wg.Done()

	for {
		select {
		case memtable := <-e.flushQueue:
			e.performFlush(memtable)
			e.flushScheduled.Store(false)

			select {
			case e.flushDone <- struct{}{}:
			default:
			}
			e.triggerCompaction(0)

		case <-e.shutdown:
			for {
				select {
				case memtable := <-e.flushQueue:
					e.performFlush(memtable)
				default:
					if active := e.activeMem.Load(); active != nil && active.Size > 0 {
						e.performFlush(active)
					}
					return
				}
			}
		}
	}
}

func (e *Engine) performFlush(memtable *core.SkipList) {
	if memtable == nil || memtable.Size == 0 {
		return
	}

	flushStart := time.Now()
	finalName := filepath.Join(e.dir, fmt.Sprintf("sst_l0_%d.db", time.Now().UnixNano()))

	f, err := os.Create(finalName)
	if err != nil {
		fmt.Printf("ERROR: Failed to create SSTable file %s: %v\n", finalName, err)
		return
	}

	bloom := core.NewBloomFilter(e.cfg.BloomFilterSizeL0)
	var sparseIndex []IndexEntry
	var offset int64
	var count int
	var minKey, maxKey []byte

	it := memtable.NewIterator()
	for it.Next() {
		k, v := it.Key(), it.Value()
		if k == nil || v == nil {
			continue
		}

		if count == 0 {
			minKey = append([]byte(nil), k...)
		}
		maxKey = append([]byte(nil), k...)

		if count%e.cfg.SparseIndexInterval == 0 {
			sparseIndex = append(sparseIndex, IndexEntry{Key: string(k), Offset: offset})
		}

		// FIX: Calculate and write CRC
		data := append(k, v...)
		checksum := crc32.ChecksumIEEE(data)

		binary.Write(f, binary.LittleEndian, uint32(len(k)))
		binary.Write(f, binary.LittleEndian, uint32(len(v)))
		binary.Write(f, binary.LittleEndian, checksum)
		f.Write(k)
		f.Write(v)

		bloom.Add(string(k))
		offset += int64(12 + len(k) + len(v)) // 4+4+4+k+v
		count++
	}

	f.Close() // FIX: Close file after flush

	if count == 0 {
		os.Remove(finalName)
		return
	}

	e.sstMu.Lock()
	e.levels[0] = append(e.levels[0], &SSTableMetadata{
		Filename:    finalName,
		Level:       0,
		Size:        offset,
		Bloom:       bloom,
		SparseIndex: sparseIndex,
		MinKey:      minKey,
		MaxKey:      maxKey,
		refCount:    0,
	})
	e.sstMu.Unlock()

	if e.immutMem.Load() == memtable {
		e.immutMem.Store(nil)
	}

	e.rotateWAL()
	metrics.GlobalMetrics.RecordMemTableFlush()
	fmt.Printf("✓ Flushed memtable to L0 (%d keys) in %v\n", count, time.Since(flushStart))
}

func (e *Engine) rotateWAL() {
	e.wal.Close()
	walPath := filepath.Join(e.dir, "wal.log")
	archivePath := filepath.Join(e.dir, fmt.Sprintf("wal.%d.log", time.Now().Unix()))
	os.Rename(walPath, archivePath)

	newWal, err := OpenWAL(e.cfg, e.dir)
	if err != nil {
		panic(fmt.Sprintf("FATAL: Failed to rotate WAL: %v", err))
	}
	e.wal = newWal

	go e.cleanupOldWALs()
}

func (e *Engine) cleanupOldWALs() {
	files, err := os.ReadDir(e.dir)
	if err != nil {
		return
	}
	var walArchives []string
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, "wal.") && strings.HasSuffix(name, ".log") && name != "wal.log" {
			walArchives = append(walArchives, name)
		}
	}
	if len(walArchives) > 3 {
		sort.Strings(walArchives)
		for i := 0; i < len(walArchives)-3; i++ {
			os.Remove(filepath.Join(e.dir, walArchives[i]))
		}
	}
}

// --- COMPACTION ---

func (e *Engine) triggerCompaction(level int) {
	select {
	case e.compChan <- level:
	case <-e.shutdown:
		return
	default:
	}
}

func (e *Engine) compactionCoordinator() {
	defer e.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.checkCompactionNeeds()
		case <-e.compChan:
			e.checkCompactionNeeds()
		case <-e.shutdown:
			return
		}
	}
}

func (e *Engine) checkCompactionNeeds() {
	e.sstMu.RLock()
	l0Count := len(e.levels[0])
	l1Count := len(e.levels[1])
	l2Count := len(e.levels[2])
	l3Count := len(e.levels[3])
	e.sstMu.RUnlock()

	if l0Count >= 2 {
		if e.compactionInProgress[0].CompareAndSwap(false, true) {
			go func() {
				defer e.compactionInProgress[0].Store(false)
				e.compactL0ToL1()
			}()
		}
	}
	if l1Count >= 10 {
		if e.compactionInProgress[1].CompareAndSwap(false, true) {
			go func() {
				defer e.compactionInProgress[1].Store(false)
				e.compactLevel(1, 2)
			}()
		}
	}
	if l2Count >= 20 {
		if e.compactionInProgress[2].CompareAndSwap(false, true) {
			go func() {
				defer e.compactionInProgress[2].Store(false)
				e.compactLevel(2, 3)
			}()
		}
	}
	if l3Count >= 40 {
		if e.compactionInProgress[3].CompareAndSwap(false, true) {
			go func() {
				defer e.compactionInProgress[3].Store(false)
				e.compactLevel(3, 4)
			}()
		}
	}
}

func (e *Engine) writeToMem(key, val []byte) bool {
	maxRetries := 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		mem := e.activeMem.Load()
		if mem.Put(key, val) {
			if mem.Size >= e.cfg.MemTableSize*90/100 {
				if e.flushScheduled.CompareAndSwap(false, true) {
					e.triggerFlush()
				}
			}
			return true
		}
		if attempt == 0 {
			e.triggerFlush()
		}
		backoff := time.Duration(20+attempt*20) * time.Millisecond
		if backoff > 200*time.Millisecond {
			backoff = 200 * time.Millisecond
		}
		time.Sleep(backoff)
	}
	return false
}

func (e *Engine) compactL0ToL1() {
	compactStart := time.Now()
	metrics.GlobalMetrics.RecordCompaction()

	e.sstMu.Lock()
	if len(e.levels[0]) == 0 {
		e.sstMu.Unlock()
		return
	}
	l0Files := e.levels[0]
	e.levels[0] = nil
	e.sstMu.Unlock()

	iterators := make([]*SSTableIterator, len(l0Files))
	for i, sst := range l0Files {
		iterators[i] = e.newSSTableIterator(sst)
	}
	mergeIt := NewMergeIterator(iterators)

	finalName := filepath.Join(e.dir, fmt.Sprintf("sst_l1_%d.db", time.Now().UnixNano()))
	f, err := os.Create(finalName)
	if err != nil {
		e.sstMu.Lock()
		e.levels[0] = l0Files
		e.sstMu.Unlock()
		for _, it := range iterators {
			it.Close()
		}
		return
	}

	// Defer cleanup on error
	var compactErr error
	defer func() {
		if compactErr != nil {
			f.Close()
			os.Remove(finalName)
			e.sstMu.Lock()
			e.levels[0] = l0Files
			e.sstMu.Unlock()
		}
	}()

	bloom := core.NewBloomFilter(e.cfg.BloomFilterSizeL1)
	var sparseIndex []IndexEntry
	var offset int64
	var count int
	var lastKey []byte
	var minKey, maxKey []byte

	for mergeIt.Next() {
		k, v := mergeIt.Key(), mergeIt.Value()

		if bytes.Equal(k, lastKey) {
			continue
		}
		lastKey = append(lastKey[:0], k...)

		if len(v) >= 8 && bytes.Equal(v[8:], []byte("__DELETED__")) {
			continue
		}

		if count == 0 {
			minKey = append([]byte(nil), k...)
		}
		maxKey = append([]byte(nil), k...)

		if count%e.cfg.SparseIndexInterval == 0 {
			sparseIndex = append(sparseIndex, IndexEntry{Key: string(k), Offset: offset})
		}

		data := append(k, v...)
		checksum := crc32.ChecksumIEEE(data)

		binary.Write(f, binary.LittleEndian, uint32(len(k)))
		binary.Write(f, binary.LittleEndian, uint32(len(v)))
		binary.Write(f, binary.LittleEndian, checksum)
		f.Write(k)
		f.Write(v)

		bloom.Add(string(k))
		offset += int64(12 + len(k) + len(v))
		count++
	}

	if err := f.Sync(); err != nil {
		compactErr = err
		for _, it := range iterators {
			it.Close()
		}
		return
	}

	f.Close() // FIX: Close file after compaction

	e.sstMu.Lock()
	e.levels[1] = append(e.levels[1], &SSTableMetadata{
		Filename:    finalName,
		Level:       1,
		Size:        offset,
		Bloom:       bloom,
		SparseIndex: sparseIndex,
		MinKey:      minKey,
		MaxKey:      maxKey,
		refCount:    0,
	})
	e.sstMu.Unlock()

	for _, sst := range l0Files {
		sst.MarkDeleted()
	}

	for _, it := range iterators {
		it.Close()
	}

	fmt.Printf("Compacted L0→L1 (%d keys) in %v\n", count, time.Since(compactStart))
}

func (e *Engine) compactLevel(fromLevel, toLevel int) {
	compactStart := time.Now()
	metrics.GlobalMetrics.RecordCompaction()

	e.sstMu.Lock()
	if len(e.levels[fromLevel]) == 0 {
		e.sstMu.Unlock()
		return
	}

	sourceFiles := e.levels[fromLevel]
	destFiles := e.levels[toLevel]
	e.levels[fromLevel] = nil
	e.levels[toLevel] = nil
	e.sstMu.Unlock()

	allFiles := append(sourceFiles, destFiles...)
	iterators := make([]*SSTableIterator, len(allFiles))
	for i, sst := range allFiles {
		iterators[i] = e.newSSTableIterator(sst)
	}
	mergeIt := NewMergeIterator(iterators)

	finalName := filepath.Join(e.dir, fmt.Sprintf("sst_l%d_%d.db", toLevel, time.Now().UnixNano()))
	f, err := os.Create(finalName)
	if err != nil {
		e.sstMu.Lock()
		e.levels[fromLevel] = sourceFiles
		e.levels[toLevel] = destFiles
		e.sstMu.Unlock()
		for _, it := range iterators {
			it.Close()
		}
		return
	}

	bloom := core.NewBloomFilter(e.cfg.BloomFilterSizeL1)
	var sparseIndex []IndexEntry
	var offset int64
	var count int
	var lastKey []byte
	var minKey, maxKey []byte

	for mergeIt.Next() {
		k, v := mergeIt.Key(), mergeIt.Value()

		if bytes.Equal(k, lastKey) {
			continue
		}
		lastKey = append(lastKey[:0], k...)

		if len(v) >= 8 && bytes.Equal(v[8:], []byte("__DELETED__")) {
			continue
		}

		if count == 0 {
			minKey = append([]byte(nil), k...)
		}
		maxKey = append([]byte(nil), k...)

		if count%e.cfg.SparseIndexInterval == 0 {
			sparseIndex = append(sparseIndex, IndexEntry{Key: string(k), Offset: offset})
		}

		data := append(k, v...)
		checksum := crc32.ChecksumIEEE(data)

		binary.Write(f, binary.LittleEndian, uint32(len(k)))
		binary.Write(f, binary.LittleEndian, uint32(len(v)))
		binary.Write(f, binary.LittleEndian, checksum)
		f.Write(k)
		f.Write(v)

		bloom.Add(string(k))
		offset += int64(12 + len(k) + len(v))
		count++
	}

	f.Close()

	e.sstMu.Lock()
	e.levels[toLevel] = append(e.levels[toLevel], &SSTableMetadata{
		Filename:    finalName,
		Level:       toLevel,
		Size:        offset,
		Bloom:       bloom,
		SparseIndex: sparseIndex,
		MinKey:      minKey,
		MaxKey:      maxKey,
		refCount:    0,
	})
	e.sstMu.Unlock()

	for _, sst := range allFiles {
		sst.MarkDeleted()
	}

	for _, it := range iterators {
		it.Close()
	}

	fmt.Printf("✓ Compacted L%d→L%d (%d keys) in %v\n", fromLevel, toLevel, count, time.Since(compactStart))
}

func (e *Engine) Close() error {
	e.shutdownOnce.Do(func() {
		fmt.Printf("Engine shutting down...\n")
		close(e.shutdown)
	})

	for e.writesInFlight.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	e.wg.Wait()
	e.wal.Close()
	fmt.Printf("Engine shutdown complete\n")
	return nil
}

// --- ITERATORS ---

type SSTableIterator struct {
	sst  *SSTableMetadata
	file *os.File // FIX: Owns file handle
	pos  int64
	key  []byte
	val  []byte
	done bool
}

// FIX: Open file on demand
func (e *Engine) newSSTableIterator(sst *SSTableMetadata) *SSTableIterator {
	f, err := os.Open(sst.Filename)
	if err != nil {
		return &SSTableIterator{done: true}
	}
	return &SSTableIterator{
		sst:  sst,
		file: f,
		pos:  0,
		done: false,
	}
}

func (it *SSTableIterator) Next() bool {
	if it.done || it.file == nil {
		return false
	}

	// FIX: Check file bounds
	if it.pos >= it.sst.Size {
		it.done = true
		return false
	}

	header := make([]byte, 12)
	if _, err := it.file.ReadAt(header, it.pos); err != nil {
		it.done = true
		return false
	}

	kLen := binary.LittleEndian.Uint32(header[0:4])
	vLen := binary.LittleEndian.Uint32(header[4:8])
	expectedCRC := binary.LittleEndian.Uint32(header[8:12])

	// Sanity checks
	if kLen > 10*1024*1024 || vLen > 100*1024*1024 {
		it.done = true
		return false
	}

	if it.pos+int64(12+kLen+vLen) > it.sst.Size {
		it.done = true
		return false
	}

	data := make([]byte, kLen+vLen)
	if _, err := it.file.ReadAt(data, it.pos+12); err != nil {
		it.done = true
		return false
	}

	if crc32.ChecksumIEEE(data) != expectedCRC {
		it.done = true
		return false
	}

	it.key = data[:kLen]
	it.val = data[kLen:]
	it.pos += int64(12 + kLen + vLen)
	return true
}

func (it *SSTableIterator) Key() []byte   { return it.key }
func (it *SSTableIterator) Value() []byte { return it.val }
func (it *SSTableIterator) Close() {
	if it.file != nil {
		it.file.Close()
		it.file = nil
	}
}

type MergeIterator struct {
	iters []*SSTableIterator
}

func NewMergeIterator(iters []*SSTableIterator) *MergeIterator {
	mi := &MergeIterator{iters: iters}
	for _, it := range iters {
		it.Next()
	}
	return mi
}

func (mi *MergeIterator) Next() bool {
	minIdx := mi.findMinIterator()
	if minIdx == -1 {
		return false
	}
	minKey := mi.iters[minIdx].Key()
	for _, it := range mi.iters {
		if it.Key() != nil && bytes.Equal(it.Key(), minKey) {
			it.Next()
		}
	}
	return true
}

func (mi *MergeIterator) Key() []byte {
	minIdx := mi.findMinIterator()
	if minIdx == -1 {
		return nil
	}
	return mi.iters[minIdx].Key()
}

func (mi *MergeIterator) Value() []byte {
	minIdx := mi.findMinIterator()
	if minIdx == -1 {
		return nil
	}
	return mi.iters[minIdx].Value()
}

func (mi *MergeIterator) findMinIterator() int {
	minIdx := -1
	var minKey []byte
	for i, it := range mi.iters {
		if it.Key() != nil {
			if minIdx == -1 || bytes.Compare(it.Key(), minKey) < 0 {
				minIdx = i
				minKey = it.Key()
			}
		}
	}
	return minIdx
}
