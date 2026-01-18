# kv-store Quick Reference Guide

##  Common Operations

### Start Cluster

```bash
# Method 1: Automated (recommended)
./test_cluster.sh

# Method 2: Manual (3 terminals)
# Terminal 1
./kvstore -port 8081 -peers "localhost:8082,localhost:8083" -dir ./data/node1

# Terminal 2  
./kvstore -port 8082 -peers "localhost:8081,localhost:8083" -dir ./data/node2

# Terminal 3
./kvstore -port 8083 -peers "localhost:8081,localhost:8082" -dir ./data/node3
```

### Stop Cluster

```bash
pkill kvstore
# Or gracefully with SIGTERM
pkill -SIGTERM kvstore
```

---

## 📝 API Cheat Sheet

### Write Data
```bash
curl -X POST http://localhost:8081/put \
  -H "Content-Type: application/json" \
  -d '{"key":"user:1","value":"Alice"}'
```

### Read Data
```bash
curl "http://localhost:8081/get?key=user:1"
```

### Batch Write
```bash
curl -X POST http://localhost:8081/batch \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {"key":"k1","value":"v1"},
      {"key":"k2","value":"v2"}
    ]
  }'
```

### Range Scan
```bash
curl "http://localhost:8081/scan?start=user:1&end=user:99"
```

### Delete
```bash
curl -X POST "http://localhost:8081/delete?key=user:1"
```

### Health Check
```bash
curl http://localhost:8081/metrics
```

---

## 🔍 Verification Commands

### Check Node Health
```bash
for port in 8081 8082 8083; do
  echo "Node $port:"
  curl -s http://localhost:$port/metrics | jq '{writes:.writes, reads:.reads, errors:.errors}'
done
```

### Verify Replication
```bash
# Write to Node 1
curl -X POST http://localhost:8081/put -d '{"key":"test","value":"data"}'

# Read from Node 2 and 3
curl "http://localhost:8082/get?key=test"
curl "http://localhost:8083/get?key=test"
```

### Check Data Consistency
```bash
KEY="consistency:check"

# Write
curl -X POST http://localhost:8081/put -d "{\"key\":\"$KEY\",\"value\":\"v1\"}"

# Read from all nodes
for port in 8081 8082 8083; do
  echo "Node $port: $(curl -s http://localhost:$port/get?key=$KEY)"
done
```

---

## 🐛 Debugging

### View Logs
```bash
# If using test script
tail -f ./test_logs/node_0.log

# If manual start
# Logs go to stdout
```

### Check Port Usage
```bash
lsof -i :8081
lsof -i :8082
lsof -i :8083
```

### Monitor System Resources
```bash
# CPU and Memory
ps aux | grep kvstore

# Disk Usage
du -sh ./data/node*
```

### Inspect SSTables
```bash
# List SSTable files
ls -lh ./data/node1/*.db

# Count L0 files (should be < 4 normally)
ls ./data/node1/sst_l0_*.db 2>/dev/null | wc -l
```

---

## 📊 Performance Testing

### Simple Write Test
```bash
# 1000 sequential writes
for i in {1..1000}; do
  curl -sS -X POST http://localhost:8081/put \
    -d "{\"key\":\"perf:$i\",\"value\":\"data-$i\"}" > /dev/null
done
```

### Simple Read Test
```bash
# 1000 sequential reads
for i in {1..1000}; do
  curl -sS "http://localhost:8081/get?key=perf:$i" > /dev/null
done
```

### Using `hey` (Advanced)
```bash
# Install
go install github.com/rakyll/hey@latest
export PATH=$PATH:$(go env GOPATH)/bin
# Write benchmark
hey -n 10000 -c 50 -m POST \
  -H "Content-Type: application/json" \
  -d '{"key":"bench","value":"data"}' \
  http://localhost:8081/put

# Read benchmark  
hey -n 10000 -c 50 \
  "http://localhost:8081/get?key=bench"
```

---

## ⚙️ Configuration Quick Tweaks

### Increase Write Throughput
```bash
export MEMTABLE_SIZE=134217728      # 128MB
export WAL_COMMIT_INTERVAL=10ms
export MAX_L0_FILES=8
```

### Improve Read Latency
```bash
export MEMTABLE_SIZE=33554432       # 32MB
export MAX_L0_FILES=2
export BLOOM_SIZE_L0=2000000
```

### Production Defaults (Balanced)
```bash
export MEMTABLE_SIZE=67108864       # 64MB
export WAL_COMMIT_INTERVAL=5ms
export MAX_L0_FILES=4
export BLOOM_SIZE_L0=650000
```

---

## 🔧 Common Issues & Fixes

### Issue: Port Already in Use
```bash
# Find process
lsof -ti tcp:8081

# Kill it
kill -9 $(lsof -ti tcp:8081)
```

### Issue: Data Not Replicating
```bash
# Check if nodes can reach each other
for port in 8081 8082 8083; do
  curl -s http://localhost:$port/metrics > /dev/null && echo "✓ $port" || echo "✗ $port"
done

# Verify peer configuration
ps aux | grep kvstore | grep peers
```

### Issue: High Memory Usage
```bash
# Check if compaction is running
curl http://localhost:8081/metrics | jq '{compactions:.compactions, flushes:.flushes}'

# Force restart (will trigger compaction)
pkill kvstore
./test_cluster.sh
```

### Issue: Slow Reads
```bash
# Check L0 file count
ls ./data/node1/sst_l0_*.db 2>/dev/null | wc -l

# If > 8, compaction is behind - reduce write rate or wait
```

---

## 📈 Monitoring Metrics

### Key Metrics to Watch

```bash
curl -s http://localhost:8081/metrics | jq '{
  writes: .writes,
  reads: .reads,
  errors: .errors,
  compactions: .compactions,
  flushes: .flushes,
  wal_syncs: .wal_syncs
}'
```

### Healthy Values
- **Errors**: Should be 0
- **Compactions**: Should increment (shows background work)
- **Flushes**: 1 flush per 64MB written
- **WAL Syncs**: Should be high (shows durability)

### Warning Signs
- **Errors > 0**: Check logs
- **Compactions = 0**: May indicate compaction stuck
- **Flushes = 0**: No writes happening

---

## 🧪 Test Scenarios

### Scenario 1: Write → Read Consistency
```bash
# Write
curl -X POST http://localhost:8081/put -d '{"key":"test1","value":"data1"}'

# Read from different node
curl "http://localhost:8082/get?key=test1"
# Expected: data1
```

### Scenario 2: Node Failure
```bash
# Write data
curl -X POST http://localhost:8081/put -d '{"key":"fail","value":"test"}'

# Kill Node 3
pkill -f "kvstore.*8083"

# Write more data (should still work with W=1)
curl -X POST http://localhost:8081/put -d '{"key":"fail2","value":"test2"}'

# Restart Node 3
./kvstore -port 8083 -peers "localhost:8081,localhost:8082" -dir ./data/node3 &

# Wait 12 seconds for hinted handoff
sleep 12

# Verify Node 3 has both keys
curl "http://localhost:8083/get?key=fail"
curl "http://localhost:8083/get?key=fail2"
```

### Scenario 3: Batch Performance
```bash
# Create 100-item batch
ITEMS=""
for i in {1..100}; do
  ITEMS="$ITEMS{\"key\":\"batch:$i\",\"value\":\"data$i\"},"
done
ITEMS="${ITEMS%,}"  # Remove trailing comma

# Send batch
time curl -X POST http://localhost:8081/batch \
  -H "Content-Type: application/json" \
  -d "{\"items\":[$ITEMS]}"

# Verify random samples
curl "http://localhost:8081/get?key=batch:50"
curl "http://localhost:8081/get?key=batch:100"
```

---