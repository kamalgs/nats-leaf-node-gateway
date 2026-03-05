#!/usr/bin/env bash
# Leaf node benchmark: Rust leaf vs Go native leaf vs direct hub.
#
# Prerequisites:
#   - nats-server in PATH  (go install github.com/nats-io/nats-server/v2@main)
#   - nats CLI in PATH     (go install github.com/nats-io/natscli/nats@latest)
#   - cargo (Rust toolchain)
#
# Usage:
#   cd nats-server && ./bench.sh
#   ./bench.sh --msgs 2000000 --size 256    # override defaults

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults (overridable via CLI args)
MSGS=1000000
SIZE=128
RUNS=3

# Parse optional overrides
while [[ $# -gt 0 ]]; do
  case "$1" in
    --msgs)  MSGS="$2";  shift 2 ;;
    --size)  SIZE="$2";  shift 2 ;;
    --runs)  RUNS="$2";  shift 2 ;;
    *)       echo "Unknown arg: $1"; echo "Usage: $0 [--msgs N] [--size N] [--runs N]"; exit 1 ;;
  esac
done

# Ports
HUB_CLIENT_PORT=4333
HUB_LEAF_PORT=7422
GO_LEAF_PORT=4225
RUST_LEAF_PORT=5223

# PID tracking for cleanup
PIDS=()
cleanup() {
  echo ""
  echo "Cleaning up..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# Check prerequisites
for cmd in nats-server nats cargo; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: $cmd not found in PATH"
    exit 1
  fi
done

# Check ports are free
for port in $HUB_CLIENT_PORT $HUB_LEAF_PORT $GO_LEAF_PORT $RUST_LEAF_PORT; do
  if ss -tln 2>/dev/null | grep -q ":${port} "; then
    echo "ERROR: port $port already in use"
    exit 1
  fi
done

echo "=== Leaf Node Benchmark ==="
echo "  msgs=$MSGS  size=${SIZE}B  runs=$RUNS"
echo ""

# --- Build Rust leaf server ---
echo "Building Rust leaf server (release)..."
cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
  -p nats-server --release --example leaf_server 2>&1 | tail -1
RUST_BIN="$REPO_ROOT/target/release/examples/leaf_server"
echo ""

# --- Start hub ---
echo "Starting hub (client=$HUB_CLIENT_PORT, leafnode=$HUB_LEAF_PORT)..."
nats-server -c "$SCRIPT_DIR/configs/bench_hub.conf" &
PIDS+=($!)
sleep 1

# --- Start Go native leaf ---
echo "Starting Go native leaf (port=$GO_LEAF_PORT)..."
nats-server -c "$SCRIPT_DIR/configs/bench_go_leaf.conf" &
PIDS+=($!)
sleep 1

# --- Start Rust leaf ---
echo "Starting Rust leaf (port=$RUST_LEAF_PORT)..."
"$RUST_BIN" --port "$RUST_LEAF_PORT" --hub "nats://127.0.0.1:$HUB_LEAF_PORT" &
PIDS+=($!)
sleep 2

# Verify connections
echo ""
echo "Verifying connectivity..."
nats pub _bench.ping pong -s "nats://127.0.0.1:$HUB_CLIENT_PORT" >/dev/null 2>&1 || { echo "FAIL: hub"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$GO_LEAF_PORT"    >/dev/null 2>&1 || { echo "FAIL: go leaf"; exit 1; }
nats pub _bench.ping pong -s "nats://127.0.0.1:$RUST_LEAF_PORT"  >/dev/null 2>&1 || { echo "FAIL: rust leaf"; exit 1; }
echo "All servers responding."
echo ""

# --- Benchmark function ---
run_bench() {
  local label="$1"
  local port="$2"
  echo "--- $label (port $port) ---"
  for i in $(seq 1 "$RUNS"); do
    nats bench pub bench.test \
      --msgs "$MSGS" --size "$SIZE" --no-progress \
      -s "nats://127.0.0.1:$port" 2>&1 | grep "stats:"
  done
  echo ""
}

echo "=== Publish Throughput (${MSGS} msgs, ${SIZE}B payload, ${RUNS} runs) ==="
echo ""
run_bench "Direct Hub"       "$HUB_CLIENT_PORT"
run_bench "Go Native Leaf"   "$GO_LEAF_PORT"
run_bench "Rust Leaf"        "$RUST_LEAF_PORT"

echo "=== Done ==="
