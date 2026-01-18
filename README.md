#  kv-store - Distributed Key-Value Store

A production-grade distributed key-value store built in Go, featuring:
-  **High-performance LSM-tree** storage engine
-  **Crash-proof** with Write-Ahead Logging
-  **Distributed** with consistent hashing and tunable replication
-  **Self-healing** with read repair and hinted handoff

---

## 📋 Table of Contents

1. [Quick Start](#quick-start)
2. [Running a 3-Node Cluster](#running-a-3-node-cluster)
3. [API Reference](#api-reference)
4. [Testing & Validation](#testing--validation)
5. [Data Consistency Verification](#data-consistency-verification)
6. [Performance Benchmarking](#performance-benchmarking)
7. [Troubleshooting](#troubleshooting)
8. [Configuration](#configuration)

---

##  Quick Start

### Prerequisites

```bash
# Install Go 1.19 or higher
go version

# Install development tools (optional but recommended)
go install github.com/rakyll/hey@latest  # HTTP load testing
export PATH=$PATH:$(go env GOPATH)/bin
```

### Build

```bash
# Clone and build
git clone <your-repo>
cd kv-store

# Initialize Go modules
go mod tidy

# Build the binary
go build -o kvstore cmd/server/main.go

# Make test scripts executable
chmod +x *.sh
```

---

##  Running a 3-Node Cluster

### Method 1: Using the Automated Script (Recommended)

```bash
# Start all 3 nodes, seed data, and run tests
./ingress_script.sh
```

This script will:
1.  Build the binary
2.  Start 3 nodes on ports 8081, 8082, 8083
3.  Seed test data to all nodes
4.  Verify replication
5.  Test all endpoints
6.  Validate data consistency

### Method 2: Manual Startup (For Learning)

**Terminal 1 - Node 1 (Port 8081):**
```bash
./kvstore -port 8081 \
          -peers "localhost:8082,localhost:8083" \
          -dir ./data/node1
```

**Terminal 2 - Node 2 (Port 8082):**
```bash
./kvstore -port 8082 \
          -peers "localhost:8081,localhost:8083" \
          -dir ./data/node2
```

**Terminal 3 - Node 3 (Port 8083):**
```bash
./kvstore -port 8083 \
          -peers "localhost:8081,localhost:8082" \
          -dir ./data/node3
```

**Expected Output** (each node):
```
===============================================
 kv-store Starting...
===============================================
Port:             8081
Data Directory:   ./data/node1
MemTable Size:    64 MB
Virtual Nodes:    10
Replication (N):  3
Write Quorum (W): 1
Read Quorum (R):  1
===============================================
✓ Loaded 0 SSTables in 123µs
✓ Replayed 0 WAL entries in 45µs
✓ Hinted Handoff: ACTIVE
===============================================
 kv-store Node running on port 8081
===============================================
```

---

## API Reference

### Base URL
```
http://localhost:{PORT}
```

### 1. PUT - Write a Key-Value Pair

**Endpoint:** `POST /put`

**Request Body:**
```json
{
  "key": "user:1001",
  "value": "Alice Smith"
}
```

**Example:**
```bash
curl -X POST http://localhost:8081/put \
  -H "Content-Type: application/json" \
  -d '{"key":"user:1001","value":"Alice Smith"}'
```

**Response:**
```
OK
```

**What Happens Internally:**
1. Hash ring determines replica nodes (e.g., N1, N2, N3)
2. Coordinator writes to local WAL (fsync)
3. Parallel replication to N2 and N3
4. Returns OK when W=1 quorum is met

---

### 2. GET - Read a Value

**Endpoint:** `GET /get?key={key}`

**Example:**
```bash
curl "http://localhost:8081/get?key=user:1001"
```

**Response:**
```
Alice Smith
```

**What Happens Internally:**
1. Coordinator queries N replicas in parallel
2. Compares timestamps, returns newest version
3. Triggers read repair if versions differ

---

### 3. BATCH - Bulk Insert

**Endpoint:** `POST /batch`

**Request Body:**
```json
{
  "items": [
    {"key": "product:101", "value": "Laptop"},
    {"key": "product:102", "value": "Mouse"},
    {"key": "product:103", "value": "Keyboard"}
  ]
}
```

**Example:**
```bash
curl -X POST http://localhost:8081/batch \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {"key":"product:101","value":"Laptop"},
      {"key":"product:102","value":"Mouse"},
      {"key":"product:103","value":"Keyboard"}
    ]
  }'
```

**Response:**
```
Batch Processed
```

**Performance:** Uses worker pool (default: 50 concurrent workers) to parallelize writes.

---

### 4. SCAN - Range Query

**Endpoint:** `GET /scan?start={start_key}&end={end_key}`

**Example:**
```bash
curl "http://localhost:8081/scan?start=product:101&end=product:199"
```

**Response:**
```json
{
  "product:101": "Laptop",
  "product:102": "Mouse",
  "product:103": "Keyboard"
}
```

**Note:** Scans are inclusive of start and end keys.

---

### 5. DELETE - Remove a Key

**Endpoint:** `POST /delete?key={key}`

**Example:**
```bash
curl -X POST "http://localhost:8081/delete?key=user:1001"
```

**Response:**
```
Deleted
```

**Implementation:** Uses tombstone markers, actual deletion happens during compaction.

---

### 6. METRICS - Health Check

**Endpoint:** `GET /metrics`

**Example:**
```bash
curl http://localhost:8081/metrics
```

**Response:**
```json
{
  "writes": 1523,
  "reads": 847,
  "batches": 12,
  "scans": 5,
  "deletes": 23,
  "errors": 0,
  "compactions": 3,
  "flushes": 8,
  "wal_syncs": 156
}
```

---

##  Testing & Validation

### Test Suite Overview

```bash
# 1. Full integration test (recommended first run)
./test_cluster.sh

# 2. Individual test scripts
./test_read_intensity.sh    # Read performance test
./test_write_intensity.sh   # Write performance test
./test_mixed_workload.sh    # 70/30 read/write mix
./test_recovery.sh          # Crash recovery test
./test_failover.sh          # Node failure handling
```

### Manual Test Scenarios

#### Scenario 1: Basic Write-Read Flow

```bash
# 1. Write to Node 1
curl -X POST http://localhost:8081/put \
  -d '{"key":"test:1","value":"hello"}'

# 2. Read from Node 2 (different node!)
curl "http://localhost:8082/get?key=test:1"

# Expected: "hello"
# This proves replication worked!
```

#### Scenario 2: Batch Insert & Scan

```bash
# 1. Bulk insert 100 products
for i in {1..100}; do
  echo "{\"key\":\"prod:$i\",\"value\":\"item-$i\"}"
done | jq -s '{items:.}' | \
curl -X POST http://localhost:8081/batch -d @-

# 2. Scan range
curl "http://localhost:8081/scan?start=prod:1&end=prod:50"

# Expected: JSON with 50 products
```

#### Scenario 3: Delete & Verify

```bash
# 1. Write data
curl -X POST http://localhost:8081/put \
  -d '{"key":"temp:1","value":"delete-me"}'

# 2. Confirm it exists
curl "http://localhost:8081/get?key=temp:1"
# Output: delete-me

# 3. Delete it
curl -X POST "http://localhost:8081/delete?key=temp:1"

# 4. Verify deletion
curl "http://localhost:8081/get?key=temp:1"
# Expected: "Not Found" or empty response
```

---

## Data Consistency Verification

### Test 1: Cross-Node Read Consistency

**Objective:** Verify all replicas have the same data.

```bash
# Write to Node 1
curl -X POST http://localhost:8081/put \
  -d '{"key":"consistency:test","value":"v1"}'

# Read from all 3 nodes
for port in 8081 8082 8083; do
  echo "Node $port:"
  curl -s "http://localhost:$port/get?key=consistency:test"
  echo ""
done
```

**Expected Output:**
```
Node 8081:
v1
Node 8082:
v1
Node 8083:
v1
```

### Test 2: Write-Read from Different Nodes

```bash
# Write to Node 2
curl -X POST http://localhost:8082/put \
  -d '{"key":"cross:node","value":"data-from-n2"}'

# Immediately read from Node 3
curl "http://localhost:8083/get?key=cross:node"

# Expected: data-from-n2
# This tests replication speed (<50ms typically)
```

### Test 3: Timestamp Consistency (Read Repair)

```bash
# 1. Write initial value to all nodes
curl -X POST http://localhost:8081/put \
  -d '{"key":"versioned:key","value":"v1"}'

sleep 1

# 2. Write updated value (creates newer timestamp)
curl -X POST http://localhost:8081/put \
  -d '{"key":"versioned:key","value":"v2"}'

# 3. Read from any node - should get v2
for port in 8081 8082 8083; do
  VAL=$(curl -s "http://localhost:$port/get?key=versioned:key")
  if [ "$VAL" != "v2" ]; then
    echo " FAIL: Node $port returned $VAL instead of v2"
  else
    echo " PASS: Node $port has correct version"
  fi
done
```

### Test 4: Quorum Behavior

**Scenario:** Write with W=1, verify it's readable even if 1 node is down.

```bash
# 1. Verify cluster is healthy
for port in 8081 8082 8083; do
  curl -s http://localhost:$port/metrics > /dev/null && echo "✓ Node $port up"
done

# 2. Write data
curl -X POST http://localhost:8081/put \
  -d '{"key":"quorum:test","value":"data"}'

# 3. Simulate node failure (kill Node 3)
pkill -f "kvstore.*8083" || echo "Node already stopped"

# 4. Read should still work (W=1, R=1, N=3)
curl "http://localhost:8081/get?key=quorum:test"
# Expected: data (success with 2/3 nodes)

# 5. Restart Node 3
./kvstore -port 8083 -peers "localhost:8081,localhost:8082" -dir ./data/node3 &

# 6. Wait for hinted handoff (10 seconds)
sleep 12

# 7. Verify Node 3 has the data
curl "http://localhost:8083/get?key=quorum:test"
# Expected: data (hinted handoff replayed)
```

---

## 📊 Performance Benchmarking

### Using `hey` Load Tester

**Install hey:**
```bash
go install github.com/rakyll/hey@latest
```

### Benchmark 1: Write Throughput

```bash
# 10,000 writes, 50 concurrent workers
hey -n 10000 -c 50 -m POST \
  -H "Content-Type: application/json" \
  -d '{"key":"bench:{{.RequestNumber}}","value":"data"}' \
  http://localhost:8081/put
```

**Expected Output:**
```
Summary:
  Total:        2.3456 secs
  Requests/sec: 4263.12

Latency distribution:
  P50:  8.23 ms
  P90:  15.67 ms
  P99:  25.89 ms
```

### Benchmark 2: Read Throughput

```bash
# First, seed data
for i in {1..1000}; do
  curl -sS -X POST http://localhost:8081/put \
    -d "{\"key\":\"read:$i\",\"value\":\"val-$i\"}" > /dev/null
done

# Then benchmark reads
hey -n 10000 -c 50 \
  "http://localhost:8081/get?key=read:500"
```

**Expected:**
```
Requests/sec: 6500+ (cache hit)
P99 latency:  < 5ms
```

### Benchmark 3: Mixed Workload

```bash
# 70% reads, 30% writes
./test_mixed_workload.sh
```

---

## 🔧 Troubleshooting

### Problem: Node won't start - "Port already in use"

**Diagnosis:**
```bash
lsof -ti tcp:8081
# Output: 12345 (PID)
```

**Solution:**
```bash
kill -9 $(lsof -ti tcp:8081)
# Or kill all kvstore processes
pkill -9 kvstore
```

### Problem: Data not replicating

**Diagnosis:**
```bash
# Check if nodes can reach each other
curl http://localhost:8082/metrics
curl http://localhost:8083/metrics

# Check logs
tail -f ./data/node1/node.log
```

**Common Causes:**
1. Firewall blocking ports
2. Wrong peer configuration (typo in addresses)
3. Node crashed (check logs)

**Solution:**
```bash
# Verify peer list
ps aux | grep kvstore
# Should show: -peers "localhost:8082,localhost:8083"

# Restart with correct peers
```

### Problem: High latency on reads

**Diagnosis:**
```bash
curl http://localhost:8081/metrics
# Check "compactions" and "l0_files"
```

**Solution:**
```bash
# If L0 files > 8, compaction is behind
# Option 1: Wait (compaction runs automatically)
# Option 2: Reduce write rate temporarily
# Option 3: Increase MAX_L0_FILES in config
```

### Problem: Out of disk space

**Diagnosis:**
```bash
du -sh ./data/node*
# Output: 5.2G, 4.8G, 5.1G
```

**Solution:**
```bash
# SSTables are immutable, safe to delete old levels
# WARNING: Only do this if you understand LSM trees!

# Or just increase disk size
```

---

## ⚙️ Configuration

### Environment Variables

Create a `.env` file:

```env
# Server
NET_PORT=8081
KVSTORE_PEERS=localhost:8082,localhost:8083
KVSTORE_DIR=./data/node1

# Storage Engine
MEMTABLE_SIZE=67108864        # 64MB
MAX_L0_FILES=4
SPARSE_INDEX_INTERVAL=128

# WAL
WAL_COMMIT_INTERVAL=5ms
WAL_MAX_BATCH_SIZE=512

# Cluster
CLUSTER_VIRTUAL_NODES=10
CLUSTER_REPLICATION_FACTOR=3
CLUSTER_WRITE_QUORUM=1
CLUSTER_READ_QUORUM=1

# Network
NETWORK_TIMEOUT=2s
BATCH_CONCURRENCY=50

# Bloom Filters
BLOOM_SIZE_L0=650000
BLOOM_SIZE_L1=3000000
```

### Tuning for Different Workloads

**Write-Heavy (Logs, Events):**
```env
MEMTABLE_SIZE=134217728       # 128MB
WAL_COMMIT_INTERVAL=10ms
MAX_L0_FILES=8
```

**Read-Heavy (User Profiles):**
```env
MEMTABLE_SIZE=33554432        # 32MB
MAX_L0_FILES=2
BLOOM_SIZE_L0=2000000
```

**Balanced (General Purpose):**
```env
# Use defaults
```

---

## 🧑‍💻 Development

### Running Tests

```bash
# Unit tests
go test ./...

# Integration tests
./test_cluster.sh

# Benchmark tests
go test -bench=. ./internal/storage
```

### Project Structure

```
kv-store/
├── cmd/server/
│   └── main.go           # Entry point
├── internal/
│   ├── api/              # HTTP handlers
│   │   └── handler.go
│   ├── cluster/          # Consistent hashing
│   │   └── ring.go
│   ├── config/           # Configuration
│   │   └── config.go
│   ├── core/             # Core data structures
│   │   ├── arena.go      # Memory allocator
│   │   ├── bloom.go      # Bloom filter
│   │   └── skiplist.go   # MemTable
│   ├── metrics/          # Metrics collection
│   │   └── metrics.go
│   └── storage/          # Storage engine
│       ├── engine.go     # LSM-tree engine
│       └── wal.go        # Write-ahead log
├── test_cluster.sh       # Main test script
└── README.md
```

---