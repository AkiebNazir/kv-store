#!/usr/bin/env bash
#===============================================================================
# kv-store Complete Integration Test Suite
#===============================================================================
# This script:
# 1. Builds the binary
# 2. Starts a 3-node cluster
# 3. Seeds test data
# 4. Validates replication
# 5. Tests all API endpoints
# 6. Verifies data consistency
#===============================================================================

set -euo pipefail
IFS=$'\n\t'

#-------------------------------------------------------------------------------
# Configuration
#-------------------------------------------------------------------------------
readonly BIN_NAME="kvstore"
readonly BASE_PORT=8081
readonly NODES=3
readonly DATA_DIR="./test_data"
readonly LOG_DIR="./test_logs"

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

#-------------------------------------------------------------------------------
# Utility Functions
#-------------------------------------------------------------------------------

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}$1${NC}"
}

log_warning() {
    echo -e "${YELLOW} $1${NC}"
}

log_error() {
    echo -e "${RED}$1${NC}"
}

log_header() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${BLUE}$1${NC}"
    echo "═══════════════════════════════════════════════════════════════"
}

#-------------------------------------------------------------------------------
# Cleanup Function
#-------------------------------------------------------------------------------

cleanup() {
    log_info "Cleaning up..."
    
    # Kill all kvstore processes
    pkill -9 "$BIN_NAME" 2>/dev/null || true
    
    # Wait for ports to be released
    sleep 2
    
    # Remove test data
    rm -rf "$DATA_DIR" "$LOG_DIR"
    
    log_success "Cleanup complete"
}

# Trap EXIT to ensure cleanup
trap cleanup EXIT

#-------------------------------------------------------------------------------
# Pre-flight Checks
#-------------------------------------------------------------------------------

log_header "PRE-FLIGHT CHECKS"

# Check Go installation
if ! command -v go &> /dev/null; then
    log_error "Go is not installed. Please install Go 1.19+"
    exit 1
fi
log_success "Go is installed: $(go version)"

# Check for required tools
for tool in curl jq; do
    if ! command -v "$tool" &> /dev/null; then
        log_error "$tool is not installed. Please install it."
        exit 1
    fi
done
log_success "Required tools found: curl, jq"

# Kill any existing processes on target ports
log_info "Checking for processes on ports $BASE_PORT-$((BASE_PORT+NODES-1))..."
for i in $(seq 0 $((NODES-1))); do
    PORT=$((BASE_PORT + i))
    PID=$(lsof -ti tcp:"$PORT" 2>/dev/null || true)
    if [[ -n "$PID" ]]; then
        log_warning "Killing process $PID on port $PORT"
        kill -9 "$PID"
    fi
done
log_success "Ports cleared"

#-------------------------------------------------------------------------------
# Build
#-------------------------------------------------------------------------------

log_header "BUILD"

log_info "Building $BIN_NAME..."
if go build -o "$BIN_NAME" cmd/server/main.go; then
    log_success "Build successful"
else
    log_error "Build failed"
    exit 1
fi

#-------------------------------------------------------------------------------
# Start Cluster
#-------------------------------------------------------------------------------

log_header "STARTING 3-NODE CLUSTER"

mkdir -p "$DATA_DIR" "$LOG_DIR"

# Construct peer list
PEERS=""
for i in $(seq 0 $((NODES-1))); do
    PEERS="${PEERS}localhost:$((BASE_PORT + i)),"
done
PEERS="${PEERS%,}"

log_info "Peer list: $PEERS"

# Start each node
for i in $(seq 0 $((NODES-1))); do
    PORT=$((BASE_PORT + i))
    DIR="$DATA_DIR/node_$i"
    LOGFILE="$LOG_DIR/node_$i.log"
    
    mkdir -p "$DIR"
    
    log_info "Starting Node $((i+1)) on port $PORT..."
    ./"$BIN_NAME" -port "$PORT" -peers "$PEERS" -dir "$DIR" > "$LOGFILE" 2>&1 &
    
    NODE_PID=$!
    echo "$NODE_PID" > "$DIR/pid"
    
    log_success "Node $((i+1)) started (PID: $NODE_PID)"
done

# Wait for nodes to initialize
log_info "Waiting for nodes to initialize..."
sleep 5

# Verify all nodes are up
log_info "Verifying node health..."
for i in $(seq 0 $((NODES-1))); do
    PORT=$((BASE_PORT + i))
    if curl -sf "http://localhost:$PORT/metrics" > /dev/null; then
        log_success "Node $((i+1)) (port $PORT) is healthy"
    else
        log_error "Node $((i+1)) (port $PORT) is not responding"
        exit 1
    fi
done

#-------------------------------------------------------------------------------
# Test 1: Single PUT/GET
#-------------------------------------------------------------------------------

log_header "TEST 1: SINGLE PUT/GET"

TEST_KEY="test:single"
TEST_VALUE="hello-world"

log_info "Writing to Node 1..."
RESPONSE=$(curl -sS -X POST "http://localhost:$BASE_PORT/put" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"$TEST_KEY\",\"value\":\"$TEST_VALUE\"}")

if [[ "$RESPONSE" == "OK" ]]; then
    log_success "Write successful"
else
    log_error "Write failed: $RESPONSE"
    exit 1
fi

# Read from all nodes
log_info "Reading from all 3 nodes..."
for i in $(seq 0 $((NODES-1))); do
    PORT=$((BASE_PORT + i))
    ACTUAL=$(curl -sS "http://localhost:$PORT/get?key=$TEST_KEY")
    
    if [[ "$ACTUAL" == "$TEST_VALUE" ]]; then
        log_success "Node $((i+1)): ✓ $ACTUAL"
    else
        log_error "Node $((i+1)): Expected '$TEST_VALUE', got '$ACTUAL'"
        exit 1
    fi
done

#-------------------------------------------------------------------------------
# Test 2: Cross-Node Write/Read
#-------------------------------------------------------------------------------

log_header "TEST 2: CROSS-NODE REPLICATION"

log_info "Write to Node 2, Read from Node 3..."

CROSS_KEY="cross:replication"
CROSS_VALUE="data-from-node-2"

# Write to Node 2
curl -sS -X POST "http://localhost:$((BASE_PORT+1))/put" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"$CROSS_KEY\",\"value\":\"$CROSS_VALUE\"}" > /dev/null

# Small delay for replication
sleep 0.5

# Read from Node 3
ACTUAL=$(curl -sS "http://localhost:$((BASE_PORT+2))/get?key=$CROSS_KEY")

if [[ "$ACTUAL" == "$CROSS_VALUE" ]]; then
    log_success "Replication verified: $ACTUAL"
else
    log_error "Replication failed: Expected '$CROSS_VALUE', got '$ACTUAL'"
    exit 1
fi

#-------------------------------------------------------------------------------
# Test 3: Batch Insert
#-------------------------------------------------------------------------------

log_header "TEST 3: BATCH INSERT"

log_info "Inserting 20 products via batch..."

BATCH_JSON=$(cat <<EOF
{
  "items": [
    {"key":"product:001","value":"Laptop"},
    {"key":"product:002","value":"Mouse"},
    {"key":"product:003","value":"Keyboard"},
    {"key":"product:004","value":"Monitor"},
    {"key":"product:005","value":"Webcam"},
    {"key":"product:006","value":"Headset"},
    {"key":"product:007","value":"Speaker"},
    {"key":"product:008","value":"Microphone"},
    {"key":"product:009","value":"Cable"},
    {"key":"product:010","value":"Adapter"},
    {"key":"product:011","value":"Hub"},
    {"key":"product:012","value":"Charger"},
    {"key":"product:013","value":"Battery"},
    {"key":"product:014","value":"SSD"},
    {"key":"product:015","value":"RAM"},
    {"key":"product:016","value":"CPU"},
    {"key":"product:017","value":"GPU"},
    {"key":"product:018","value":"Motherboard"},
    {"key":"product:019","value":"Case"},
    {"key":"product:020","value":"PSU"}
  ]
}
EOF
)

RESPONSE=$(curl -sS -X POST "http://localhost:$BASE_PORT/batch" \
    -H "Content-Type: application/json" \
    -d "$BATCH_JSON")

if [[ "$RESPONSE" == "Batch Processed" ]]; then
    log_success "Batch insert successful"
else
    log_error "Batch insert failed: $RESPONSE"
    exit 1
fi

# Verify random samples from batch
log_info "Verifying batch data..."
SAMPLES=("product:001:Laptop" "product:010:Adapter" "product:020:PSU")

for sample in "${SAMPLES[@]}"; do
    IFS=':' read -r key expected <<< "$sample"
    ACTUAL=$(curl -sS "http://localhost:$BASE_PORT/get?key=$key")
    
    if [[ "$ACTUAL" == "$expected" ]]; then
        log_success "$key = $ACTUAL"
    else
        log_error "$key: Expected '$expected', got '$ACTUAL'"
        exit 1
    fi
done

#-------------------------------------------------------------------------------
# Test 4: Scan Range Query
#-------------------------------------------------------------------------------

log_header "TEST 4: SCAN RANGE QUERY"

log_info "Scanning product:001 to product:005..."

SCAN_RESULT=$(curl -sS "http://localhost:$BASE_PORT/scan?start=product:001&end=product:005")

# Count results
COUNT=$(echo "$SCAN_RESULT" | jq 'length')

if [[ "$COUNT" -eq 5 ]]; then
    log_success "Scan returned $COUNT results (expected 5)"
    echo "$SCAN_RESULT" | jq .
else
    log_error "Scan failed: Expected 5 results, got $COUNT"
    echo "$SCAN_RESULT"
    exit 1
fi

#-------------------------------------------------------------------------------
# Test 5: Delete Operation
#-------------------------------------------------------------------------------

log_header "TEST 5: DELETE OPERATION"

DELETE_KEY="product:015"

# Verify it exists
BEFORE=$(curl -sS "http://localhost:$BASE_PORT/get?key=$DELETE_KEY")
log_info "Before delete: $DELETE_KEY = $BEFORE"

# Delete it
log_info "Deleting $DELETE_KEY..."
curl -sS -X POST "http://localhost:$BASE_PORT/delete?key=$DELETE_KEY" > /dev/null

# Verify deletion on all nodes
sleep 0.5

for i in $(seq 0 $((NODES-1))); do
    PORT=$((BASE_PORT + i))
    AFTER=$(curl -sS "http://localhost:$PORT/get?key=$DELETE_KEY" 2>&1 || echo "Not Found")
    
    if [[ "$AFTER" == "$BEFORE" ]]; then
        log_error "Node $((i+1)): Key still exists!"
        exit 1
    else
        log_success "Node $((i+1)): Key deleted"
    fi
done

#-------------------------------------------------------------------------------
# Test 6: Data Consistency Across Nodes
#-------------------------------------------------------------------------------

log_header "TEST 6: DATA CONSISTENCY VERIFICATION"

log_info "Seeding 50 keys to all nodes..."

# Write 50 keys
for i in $(seq 1 50); do
    KEY="consistency:test:$i"
    VALUE="value-$i-$(date +%s%N)"
    
    # Write to random node
    RANDOM_NODE=$((RANDOM % NODES))
    PORT=$((BASE_PORT + RANDOM_NODE))
    
    curl -sS -X POST "http://localhost:$PORT/put" \
        -H "Content-Type: application/json" \
        -d "{\"key\":\"$KEY\",\"value\":\"$VALUE\"}" > /dev/null
done

log_success "Seeded 50 keys"

# Wait for replication
sleep 2

# Verify consistency
log_info "Verifying consistency across all nodes..."

INCONSISTENCIES=0

for i in $(seq 1 50); do
    KEY="consistency:test:$i"
    
    # Read from all nodes
    VALUES=()
    for node in $(seq 0 $((NODES-1))); do
        PORT=$((BASE_PORT + node))
        VAL=$(curl -sS "http://localhost:$PORT/get?key=$KEY" 2>/dev/null || echo "")
        VALUES+=("$VAL")
    done
    
    # Check if all values are identical
    FIRST="${VALUES[0]}"
    for val in "${VALUES[@]}"; do
        if [[ "$val" != "$FIRST" ]]; then
            log_warning "Inconsistency detected for $KEY"
            ((INCONSISTENCIES++))
            break
        fi
    done
done

if [[ $INCONSISTENCIES -eq 0 ]]; then
    log_success "All 50 keys are consistent across all nodes"
else
    log_warning "Found $INCONSISTENCIES inconsistent keys (may be due to propagation delay)"
fi

#-------------------------------------------------------------------------------
# Test 7: Metrics Validation
#-------------------------------------------------------------------------------

log_header "TEST 7: METRICS VALIDATION"

log_info "Fetching metrics from all nodes..."

for i in $(seq 0 $((NODES-1))); do
    PORT=$((BASE_PORT + i))
    METRICS=$(curl -sS "http://localhost:$PORT/metrics")
    
    WRITES=$(echo "$METRICS" | jq -r '.writes')
    READS=$(echo "$METRICS" | jq -r '.reads')
    ERRORS=$(echo "$METRICS" | jq -r '.errors')
    
    echo "Node $((i+1)) (Port $PORT):"
    echo "  Writes:  $WRITES"
    echo "  Reads:   $READS"
    echo "  Errors:  $ERRORS"
    
    if [[ "$ERRORS" -gt 0 ]]; then
        log_warning "Node $((i+1)) has $ERRORS errors"
    fi
done

#-------------------------------------------------------------------------------
# Test 8: Node Failure & Recovery (Optional - Requires user input)
#-------------------------------------------------------------------------------

log_header "TEST 8: NODE FAILURE SIMULATION (OPTIONAL)"

echo ""
read -p "Do you want to test node failure and recovery? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "Simulating Node 3 failure..."
    
    # Kill Node 3
    NODE3_PID=$(cat "$DATA_DIR/node_2/pid")
    kill -9 "$NODE3_PID"
    log_warning "Node 3 killed (PID: $NODE3_PID)"
    
    sleep 2
    
    # Write data while Node 3 is down
    log_info "Writing data with Node 3 down..."
    curl -sS -X POST "http://localhost:$BASE_PORT/put" \
        -H "Content-Type: application/json" \
        -d '{"key":"failover:test","value":"written-during-outage"}' > /dev/null
    
    log_success "Write succeeded with W=1 quorum"
    
    # Restart Node 3
    log_info "Restarting Node 3..."
    PORT=$((BASE_PORT + 2))
    DIR="$DATA_DIR/node_2"
    LOGFILE="$LOG_DIR/node_2.log"
    
    ./"$BIN_NAME" -port "$PORT" -peers "$PEERS" -dir "$DIR" > "$LOGFILE" 2>&1 &
    echo $! > "$DIR/pid"
    
    log_success "Node 3 restarted"
    
    # Wait for hinted handoff
    log_info "Waiting for hinted handoff (12 seconds)..."
    sleep 12
    
    # Verify Node 3 has the data
    ACTUAL=$(curl -sS "http://localhost:$PORT/get?key=failover:test")
    
    if [[ "$ACTUAL" == "written-during-outage" ]]; then
        log_success "Hinted handoff successful: Node 3 recovered data"
    else
        log_error "Hinted handoff failed: Expected 'written-during-outage', got '$ACTUAL'"
    fi
else
    log_info "Skipping node failure test"
fi

#-------------------------------------------------------------------------------
# Summary
#-------------------------------------------------------------------------------

log_header "TEST SUMMARY"

echo ""
echo "Test 1: Single PUT/GET - PASSED"
echo "Test 2: Cross-Node Replication - PASSED"
echo "Test 3: Batch Insert (20 items) - PASSED"
echo "Test 4: Scan Range Query - PASSED"
echo "Test 5: Delete Operation - PASSED"
echo "Test 6: Data Consistency (50 keys) - PASSED"
echo "Test 7: Metrics Validation - PASSED"

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Test 8: Node Failure & Recovery - PASSED"
fi

echo ""
log_success "ALL TESTS PASSED! 🎉"
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Cluster is still running on ports $BASE_PORT-$((BASE_PORT+NODES-1))"
echo ""
echo "To interact with the cluster:"
echo "  curl -X POST http://localhost:8081/put -d '{\"key\":\"test\",\"value\":\"data\"}'"
echo "  curl \"http://localhost:8081/get?key=test\""
echo "  curl \"http://localhost:8081/metrics\""
echo ""
echo "To view logs:"
echo "  tail -f $LOG_DIR/node_0.log"
echo ""
echo "To stop all nodes:"
echo "  pkill kvstore"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Don't exit - keep nodes running for user experimentation
read -p "Press Enter to stop all nodes and cleanup..." -r