package api

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"kvstore/internal/cluster"
	"kvstore/internal/config"
	"kvstore/internal/metrics"
	"kvstore/internal/storage"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	Engine     *storage.Engine
	Ring       *cluster.Ring
	Self       string
	Config     *config.Config
	HttpClient *http.Client
	HintStore  *HintStore
	connPools  map[string]*http.Client
	poolMu     sync.RWMutex
}

type KVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type BatchRequest struct {
	Items []KVRequest `json:"items"`
}

type KVRecord struct {
	Key   string
	Value string
}

// Buffered hint store with background writer
type HintStore struct {
	dir    string
	mu     sync.Mutex
	counts map[string]int

	// Buffered writes
	files       map[string]*bufio.Writer
	fileHandles map[string]*os.File
	hintQueue   chan hintQueueItem
	shutdown    chan struct{}
	wg          sync.WaitGroup
}

type hintQueueItem struct {
	targetNode string
	key        string
	value      string
}

func NewHintStore(dir string) *HintStore {
	os.MkdirAll(filepath.Join(dir, "hints"), 0755)

	hs := &HintStore{
		dir:         filepath.Join(dir, "hints"),
		counts:      make(map[string]int),
		files:       make(map[string]*bufio.Writer),
		fileHandles: make(map[string]*os.File),
		hintQueue:   make(chan hintQueueItem, 10000), // Large buffer
		shutdown:    make(chan struct{}),
	}

	// Background writer goroutine
	hs.wg.Add(1)
	go hs.writerLoop()

	return hs
}

// Non-blocking enqueue
func (hs *HintStore) Store(targetNode, key, val string) error {
	select {
	case hs.hintQueue <- hintQueueItem{targetNode, key, val}:
		return nil
	default:
		// Queue full - this is very rare, write synchronously
		return hs.writeHintDirect(targetNode, key, val)
	}
}

// Background writer goroutine
func (hs *HintStore) writerLoop() {
	defer hs.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case item := <-hs.hintQueue:
			hs.writeHintDirect(item.targetNode, item.key, item.value)

		case <-ticker.C:
			// Periodic flush
			hs.flushAll()

		case <-hs.shutdown:
			// Aggressive flush during shutdown drain
			for {
				select {
				case item := <-hs.hintQueue:
					hs.writeHintDirect(item.targetNode, item.key, item.value)

					// Flush every 100 items during drain
					// Balances between flush frequency and performance
					if len(hs.hintQueue)%100 == 0 || len(hs.hintQueue) == 0 {
						hs.flushAll()
					}

				default:
					// Queue empty - final flush and close
					hs.flushAll()

					// Sync all file descriptors to disk
					hs.mu.Lock()
					for _, f := range hs.fileHandles {
						f.Sync() // Force OS to flush to disk
					}
					hs.mu.Unlock()

					hs.closeAll()
					return
				}
			}
		}
	}
}

// Reuses open file handles
func (hs *HintStore) writeHintDirect(targetNode, key, val string) error {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	filename := sanitizeNodeName(targetNode)

	// Get or create buffered writer
	writer, ok := hs.files[filename]
	if !ok {
		fullPath := filepath.Join(hs.dir, filename+".hints")
		f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		hs.fileHandles[filename] = f
		writer = bufio.NewWriterSize(f, 64*1024) // 64KB buffer
		hs.files[filename] = writer
	}

	// Write entry
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(val)))
	writer.Write(buf)
	writer.Write([]byte(key))
	writer.Write([]byte(val))

	hs.counts[targetNode]++
	return nil
}

func (hs *HintStore) flushAll() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	for _, writer := range hs.files {
		writer.Flush()
	}
}

func (hs *HintStore) closeAll() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	for _, writer := range hs.files {
		writer.Flush()
	}

	for _, f := range hs.fileHandles {
		f.Close()
	}

	hs.files = make(map[string]*bufio.Writer)
	hs.fileHandles = make(map[string]*os.File)
}

// CycleHintsForProcessing atomically closes the current active hint file for a node
// and renames it to a .processing file. It returns the path to the .processing file.
// This ensures that new writes immediately go to a fresh file, fixing the race condition.
func (hs *HintStore) CycleHintsForProcessing(targetNode string) (string, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	filename := sanitizeNodeName(targetNode)
	originalPath := filepath.Join(hs.dir, filename+".hints")

	// If no file exists, nothing to do
	if _, err := os.Stat(originalPath); os.IsNotExist(err) {
		return "", nil
	}

	// 1. Flush and close active writer/handle if open
	if writer, ok := hs.files[filename]; ok {
		writer.Flush()
		delete(hs.files, filename)
	}
	if handle, ok := hs.fileHandles[filename]; ok {
		handle.Close()
		delete(hs.fileHandles, filename)
	}
	delete(hs.counts, targetNode)

	// 2. Rename to .processing with timestamp to avoid collisions
	processingPath := filepath.Join(hs.dir, fmt.Sprintf("%s.hints.%d.processing", filename, time.Now().UnixNano()))
	if err := os.Rename(originalPath, processingPath); err != nil {
		return "", err
	}

	return processingPath, nil
}

func (hs *HintStore) Shutdown() {
	close(hs.shutdown)
	hs.wg.Wait()
}

func sanitizeNodeName(node string) string {
	sanitized := ""
	for _, c := range node {
		if c == ':' || c == '/' {
			sanitized += "_"
		} else {
			sanitized += string(c)
		}
	}
	return sanitized
}

// PUBLIC HANDLERS

func (s *Server) HandlePut(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		metrics.GlobalMetrics.RecordError()
		http.Error(w, "Bad Request", 400)
		return
	}

	if s.quorumWrite(req.Key, req.Value) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		metrics.GlobalMetrics.RecordWrite(len(req.Value), time.Since(start).Nanoseconds())
	} else {
		metrics.GlobalMetrics.RecordError()
		http.Error(w, "Quorum Failed", http.StatusServiceUnavailable)
	}
}

func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key required", 400)
		return
	}

	nodes := s.Ring.GetNodes(key, s.Config.ReplicationFactor)
	results, contactedCount := s.gatherReads(nodes, key)

	// With R=2, W=2, N=3: We need at least 2 nodes to respond
	if contactedCount < s.Config.ReadQuorum {
		metrics.GlobalMetrics.RecordError()
		http.Error(w, fmt.Sprintf("Read Quorum Not Met: contacted %d/%d nodes", contactedCount, s.Config.ReadQuorum), http.StatusServiceUnavailable)
		return
	}

	// Find the most recent value
	var finalValue []byte
	var maxTs uint64
	found := false

	for _, raw := range results {
		if len(raw) < 8 {
			continue
		}
		ts := binary.LittleEndian.Uint64(raw[:8])
		if ts > maxTs {
			maxTs = ts
			finalValue = raw[8:]
			found = true
		}
	}

	// Key doesn't exist - this is normal, not an error
	if !found || bytes.Equal(finalValue, []byte("__DELETED__")) {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// Async read repair
	repairKey := make([]byte, len(key))
	copy(repairKey, []byte(key))
	repairValue := make([]byte, len(finalValue))
	copy(repairValue, finalValue)
	repairTs := maxTs
	repairResults := make(map[string][]byte, len(results))
	for k, v := range results {
		vcopy := make([]byte, len(v))
		copy(vcopy, v)
		repairResults[k] = vcopy
	}

	go s.performReadRepair(string(repairKey), repairValue, repairTs, repairResults, nodes)

	w.Write(finalValue)
	metrics.GlobalMetrics.RecordRead(len(finalValue), time.Since(start).Nanoseconds())
}

func (s *Server) HandleBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		metrics.GlobalMetrics.RecordError()
		http.Error(w, "Bad Request", 400)
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, s.Config.BatchConcurrency)
	var failureCount int32

	for _, item := range req.Items {
		wg.Add(1)
		sem <- struct{}{}
		go func(k, v string) {
			defer wg.Done()
			defer func() { <-sem }()

			if !s.quorumWrite(k, v) {
				atomic.AddInt32(&failureCount, 1)
				metrics.GlobalMetrics.RecordError()
			}
		}(item.Key, item.Value)
	}

	wg.Wait()

	failures := atomic.LoadInt32(&failureCount)
	if failures > 0 {
		http.Error(w, fmt.Sprintf("Batch finished with %d failures", failures), 500)
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Batch Processed"))
		metrics.GlobalMetrics.RecordBatch(len(req.Items), time.Since(start).Nanoseconds())
	}
}

func (s *Server) HandleScan(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	startKey := r.URL.Query().Get("start")
	endKey := r.URL.Query().Get("end")

	uniqueNodes := s.getUniqueNodes()
	results := make(chan map[string]string, len(uniqueNodes))
	var wg sync.WaitGroup

	for _, node := range uniqueNodes {
		wg.Add(1)
		go func(targetNode string) {
			defer wg.Done()
			var res map[string]string
			if targetNode == s.Self {
				res = s.Engine.Scan([]byte(startKey), []byte(endKey))
			} else {
				res = s.forwardScan(targetNode, startKey, endKey)
			}
			results <- res
		}(node)
	}

	wg.Wait()
	close(results)

	finalMap := make(map[string]string)
	for res := range results {
		for k, v := range res {
			if existing, ok := finalMap[k]; !ok || v > existing {
				finalMap[k] = v
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(finalMap)
	metrics.GlobalMetrics.RecordScan(time.Since(start).Nanoseconds())
}

func (s *Server) HandleDelete(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	key := r.URL.Query().Get("key")
	if key == "" {
		metrics.GlobalMetrics.RecordError()
		http.Error(w, "Key required", 400)
		return
	}
	if s.quorumWrite(key, "__DELETED__") {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Deleted"))
		metrics.GlobalMetrics.RecordDelete(time.Since(start).Nanoseconds())
	} else {
		metrics.GlobalMetrics.RecordError()
		http.Error(w, "Delete Failed", 503)
	}
}

// HINTED HANDOFF LOGIC

func (s *Server) StartHintReplayer() {
	// First pass: look for any stuck .processing files from a previous crash
	// and queue them for processing immediately
	go func() {
		files, _ := os.ReadDir(s.HintStore.dir)
		for _, f := range files {
			if strings.Contains(f.Name(), ".processing") {
				// Parse node name from filename (e.g., "node_8081.hints.1234.processing")
				parts := strings.Split(f.Name(), ".hints")
				if len(parts) > 0 {
					nodeName := parts[0]
					nodeAddr := unsanitizeNodeName(nodeName)

					// Attempt to process immediately if node is up
					if s.isNodeAlive(nodeAddr) {
						s.processHintFile(nodeAddr, filepath.Join(s.HintStore.dir, f.Name()))
					}
				}
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for range ticker.C {
			s.replayHints()
		}
	}()
}

func (s *Server) replayHints() {
	files, err := os.ReadDir(s.HintStore.dir)
	if err != nil {
		return
	}

	// We use a map to ensure we only process one file per node per cycle
	// to avoid overwhelming the network
	processedNodes := make(map[string]bool)

	for _, f := range files {
		// Only look for active .hints files, we ignore .processing files
		// as those are handled by specific logic or next startup
		if !strings.HasSuffix(f.Name(), ".hints") {
			continue
		}

		nodeName := string(bytes.TrimSuffix([]byte(f.Name()), []byte(".hints")))
		nodeAddr := unsanitizeNodeName(nodeName)

		if processedNodes[nodeAddr] {
			continue
		}

		if !s.isNodeAlive(nodeAddr) {
			continue
		}

		// Atomically cycle the file to .processing
		processingPath, err := s.HintStore.CycleHintsForProcessing(nodeAddr)
		if err != nil || processingPath == "" {
			continue
		}

		processedNodes[nodeAddr] = true

		// Process the file in background (semaphores could be added here for concurrency control)
		go s.processHintFile(nodeAddr, processingPath)
	}
}

func (s *Server) processHintFile(nodeAddr, filepath string) {
	// Ensure cleanup happens regardless of success/failure
	// If partial failure, we might want to requeue, but for simple POC
	// deleting avoids infinite loops of bad data.
	defer os.Remove(filepath)

	records, err := s.loadHintsFromFile(filepath)
	if err != nil || len(records) == 0 {
		return
	}

	fmt.Printf("♻️ Replaying %d hints to recovered node: %s\n", len(records), nodeAddr)

	successCount := 0
	for _, rec := range records {
		if s.forwardPut(nodeAddr, rec.Key, rec.Value) {
			successCount++
		}
	}

	if successCount > 0 {
		fmt.Printf("Successfully replayed %d/%d hints to %s\n", successCount, len(records), nodeAddr)
	}
}

// Separate file loading method for isolation
func (s *Server) loadHintsFromFile(filepath string) ([]KVRecord, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []KVRecord
	for {
		header := make([]byte, 8)
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				break
			}
			return records, err
		}

		kLen := binary.LittleEndian.Uint32(header[0:4])
		vLen := binary.LittleEndian.Uint32(header[4:8])

		// Sanity check
		if kLen > 10*1024*1024 || vLen > 100*1024*1024 {
			break // Corrupted, stop reading
		}

		data := make([]byte, kLen+vLen)
		if _, err := io.ReadFull(f, data); err != nil {
			return records, err
		}

		records = append(records, KVRecord{
			Key:   string(data[:kLen]),
			Value: string(data[kLen:]),
		})
	}

	return records, nil
}

func unsanitizeNodeName(sanitized string) string {
	result := ""
	for _, c := range sanitized {
		if c == '_' {
			result += ":"
		} else {
			result += string(c)
		}
	}
	return result
}

func (s *Server) isNodeAlive(addr string) bool {
	client := s.getConnectionPool(addr)
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (s *Server) getConnectionPool(addr string) *http.Client {
	s.poolMu.RLock()
	if client, ok := s.connPools[addr]; ok {
		s.poolMu.RUnlock()
		return client
	}
	s.poolMu.RUnlock()

	s.poolMu.Lock()
	defer s.poolMu.Unlock()

	if client, ok := s.connPools[addr]; ok {
		return client
	}

	client := &http.Client{
		Timeout: s.Config.NetworkTimeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}

	if s.connPools == nil {
		s.connPools = make(map[string]*http.Client)
	}
	s.connPools[addr] = client
	return client
}

// INTERNAL HANDLERS

func (s *Server) HandleInternalPut(w http.ResponseWriter, r *http.Request) {
	var req KVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	if err := s.Engine.Put([]byte(req.Key), []byte(req.Value)); err != nil {
		http.Error(w, "Internal Error", 500)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandleInternalPutRaw(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Internal Error", 500)
		return
	}
	if err := s.Engine.PutRaw([]byte(key), body); err != nil {
		http.Error(w, "Internal Error", 500)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandleInternalGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val, found := s.Engine.GetRaw([]byte(key))
	if found {
		w.Write(val)
	} else {
		http.Error(w, "Not Found", 404)
	}
}

func (s *Server) HandleInternalBatch(w http.ResponseWriter, r *http.Request) {
	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	var keys, vals [][]byte
	for _, item := range req.Items {
		keys = append(keys, []byte(item.Key))
		vals = append(vals, []byte(item.Value))
	}
	if err := s.Engine.BatchPut(keys, vals); err != nil {
		http.Error(w, "Internal Error", 500)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) HandleInternalScan(w http.ResponseWriter, r *http.Request) {
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	res := s.Engine.Scan([]byte(start), []byte(end))
	json.NewEncoder(w).Encode(res)
}

// CORE LOGIC

func (s *Server) quorumWrite(key, val string) bool {
	nodes := s.Ring.GetNodes(key, s.Config.ReplicationFactor)
	required := s.Config.WriteQuorum

	successChan := make(chan bool, len(nodes))

	for _, node := range nodes {
		go func(targetNode string) {
			var success bool
			if targetNode == s.Self {
				success = s.Engine.Put([]byte(key), []byte(val)) == nil
			} else {
				success = s.forwardPut(targetNode, key, val)
			}

			if !success && targetNode != s.Self {
				// Non-blocking hint storage
				s.HintStore.Store(targetNode, key, val)
				// Hint storage counts as success for availability (Sloppy Quorum)
				success = true
			}
			successChan <- success
		}(node)
	}

	successCount := 0
	timeout := time.After(s.Config.NetworkTimeout)

	for i := 0; i < len(nodes); i++ {
		select {
		case ok := <-successChan:
			if ok {
				successCount++
			}
			if successCount >= required {
				return true
			}
		case <-timeout:
			return successCount >= required
		}
	}
	return successCount >= required
}

func (s *Server) gatherReads(nodes []string, key string) (map[string][]byte, int) {
	results := make(map[string][]byte)
	contactedCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(targetNode string) {
			defer wg.Done()
			var val []byte
			contacted := false

			if targetNode == s.Self {
				v, ok := s.Engine.GetRaw([]byte(key))
				contacted = true
				if ok {
					val = v
				}
			} else {
				v := s.fetchRemoteRaw(targetNode, key)
				if len(v) > 0 {
					val = []byte(v)
					contacted = true
				} else {
					// Check if node is reachable
					contacted = s.isNodeAlive(targetNode)
				}
			}

			mu.Lock()
			if contacted {
				contactedCount++
			}
			if len(val) > 0 {
				results[targetNode] = val
			}
			mu.Unlock()
		}(node) // FIX: Pass node as argument to goroutine
	}
	wg.Wait()
	return results, contactedCount
}

func (s *Server) performReadRepair(key string, correctVal []byte, ts uint64, results map[string][]byte, allNodes []string) {
	payload := make([]byte, 8+len(correctVal))
	binary.LittleEndian.PutUint64(payload[:8], ts)
	copy(payload[8:], correctVal)

	for _, node := range allNodes {
		val, hasData := results[node]

		shouldRepair := !hasData ||
			(len(val) >= 8 && binary.LittleEndian.Uint64(val[:8]) < ts) ||
			(len(val) >= 8 && !bytes.Equal(val[8:], correctVal))

		if shouldRepair {
			// Re-check before repair to avoid overwriting newer writes
			// Since read repair runs asynchronously, a newer write could have occurred
			// causing a "ghost write" race if we blindly repair.

			var currentVal []byte
			var currentFound bool

			if node == s.Self {
				currentVal, currentFound = s.Engine.GetRaw([]byte(key))
			} else {
				// Remote check (expensive but safe)
				fetchedVal := s.fetchRemoteRaw(node, key)
				if len(fetchedVal) > 0 {
					currentVal = []byte(fetchedVal)
					currentFound = true
				}
			}

			// Only repair if STILL stale (or missing)
			shouldStillRepair := !currentFound ||
				(len(currentVal) >= 8 && binary.LittleEndian.Uint64(currentVal[:8]) < ts)

			if shouldStillRepair {
				if node == s.Self {
					s.Engine.PutRaw([]byte(key), payload)
				} else {
					s.forwardPutRaw(node, key, payload)
				}
			}
		}
	}
}

// --- NETWORK HELPERS ---

func (s *Server) fetchRemoteRaw(addr, key string) string {
	client := s.getConnectionPool(addr)
	resp, err := client.Get("http://" + addr + "/internal/get?key=" + key)
	if err != nil || resp.StatusCode != 200 {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func (s *Server) forwardPut(addr, key, val string) bool {
	client := s.getConnectionPool(addr)
	data, _ := json.Marshal(KVRequest{Key: key, Value: val})
	resp, err := client.Post("http://"+addr+"/internal/put", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (s *Server) forwardPutRaw(addr, key string, rawPayload []byte) bool {
	client := s.getConnectionPool(addr)
	resp, err := client.Post("http://"+addr+"/internal/put_raw?key="+key, "application/octet-stream", bytes.NewBuffer(rawPayload))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (s *Server) forwardScan(addr, start, end string) map[string]string {
	client := s.getConnectionPool(addr)
	resp, err := client.Get("http://" + addr + "/internal/scan?start=" + start + "&end=" + end)
	res := make(map[string]string)
	if err == nil && resp.StatusCode == 200 {
		defer resp.Body.Close()
		json.NewDecoder(resp.Body).Decode(&res)
	}
	return res
}

func (s *Server) getUniqueNodes() []string {
	unique := make(map[string]bool)
	for _, node := range s.Ring.Nodes {
		unique[node] = true
	}
	list := make([]string, 0, len(unique))
	for k := range unique {
		list = append(list, k)
	}
	return list
}
