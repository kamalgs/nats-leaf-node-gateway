#!/usr/bin/env bash
# Network-namespace benchmark: UDP vs TCP cluster over Docker bridge network.
#
# Simulates multi-node deployment by running each cluster node in a separate
# Docker container connected via a bridge network. Traffic goes through the
# full kernel network stack (not loopback), so GSO/GRO offload engages
# properly.
#
# Usage:
#   cd tests && ./bench_netns.sh                   # default: 500K msgs, 3 runs
#   cd tests && ./bench_netns.sh --msgs 100000     # custom msg count
#   cd tests && ./bench_netns.sh --runs 1          # single run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Use sg to get docker group permissions if not already in the group.
if ! docker ps >/dev/null 2>&1; then
  if sg docker -c "docker ps" >/dev/null 2>&1; then
    # Re-exec the entire script under the docker group.
    exec sg docker -c "$(printf '%q ' "$0" "$@")"
  else
    echo "ERROR: Cannot access Docker. Need docker group membership."; exit 1
  fi
fi

# ──────────────────────────────────────────────────────────────────────
# Parse args
# ──────────────────────────────────────────────────────────────────────
MSGS=500000
SIZE=128
RUNS=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --msgs) MSGS="$2"; shift 2 ;;
    --size) SIZE="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    *)      echo "Unknown arg: $1"; echo "Usage: $0 [--msgs N] [--size N] [--runs N]"; exit 1 ;;
  esac
done

# ──────────────────────────────────────────────────────────────────────
# Binaries
# ──────────────────────────────────────────────────────────────────────
RUST_BIN="$REPO_ROOT/target/release/open-wire"
NATS_CLI="$(which nats 2>/dev/null || true)"

if [[ ! -x "$RUST_BIN" ]]; then
  echo "Building open-wire with udp-transport feature..."
  (cd "$REPO_ROOT" && cargo build --release --features udp-transport,cluster)
fi
if [[ -z "$NATS_CLI" ]]; then
  echo "ERROR: nats CLI not found in PATH"; exit 1
fi

# ──────────────────────────────────────────────────────────────────────
# Docker network + containers
# ──────────────────────────────────────────────────────────────────────
NETWORK="bench-cluster-net"
NODE_A="bench-node-a"
NODE_B="bench-node-b"
IMAGE="debian:bookworm-slim"

CONF_DIR=""
cleanup() {
  echo ""
  echo "Cleaning up..."
  docker rm -f "$NODE_A" "$NODE_B" 2>/dev/null || true
  docker network rm "$NETWORK" 2>/dev/null || true
  [[ -n "$CONF_DIR" ]] && rm -rf "$CONF_DIR"
}
trap cleanup EXIT

# Clean any stale resources
docker rm -f "$NODE_A" "$NODE_B" 2>/dev/null || true
docker network rm "$NETWORK" 2>/dev/null || true

echo "Creating Docker bridge network: $NETWORK"
docker network create "$NETWORK" --subnet 172.30.0.0/24 >/dev/null

# Discover IPs (static assignment)
IP_A="172.30.0.10"
IP_B="172.30.0.11"

echo "Starting containers..."
# Run containers in background with the binary and nats CLI mounted.
# --privileged is needed for GSO/GRO setsockopt and sysctl.
for node_name in "$NODE_A" "$NODE_B"; do
  if [[ "$node_name" == "$NODE_A" ]]; then
    node_ip="$IP_A"
  else
    node_ip="$IP_B"
  fi
  docker run -d --rm \
    --name "$node_name" \
    --network "$NETWORK" \
    --ip "$node_ip" \
    --privileged \
    -v "$RUST_BIN:/usr/local/bin/open-wire:ro" \
    -v "$NATS_CLI:/usr/local/bin/nats:ro" \
    "$IMAGE" \
    sleep infinity >/dev/null
done

echo "  Node A: $IP_A"
echo "  Node B: $IP_B"

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
CLK_TCK=$(getconf CLK_TCK 2>/dev/null || echo 100)

# Extract msgs/sec from nats bench output
extract_rate() {
  grep -oP '[\d,]+(?= msgs/sec)' | head -1 | tr -d ','
}

fmt_num() {
  printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# Get container-internal PID for the open-wire process
get_server_pid() {
  local container="$1"
  docker exec "$container" pgrep -x open-wire 2>/dev/null | head -1
}

# CPU ticks inside container (via /proc inside the container)
cpu_ticks() {
  local container="$1" pid="$2"
  docker exec "$container" awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null || echo 0
}

# Run a command inside a container
dexec() {
  local container="$1"; shift
  docker exec "$container" "$@"
}

# Run a command inside a container in background, return host PID
dexec_bg() {
  local container="$1"; shift
  docker exec -d "$container" "$@"
}

# Kill all open-wire processes in a container and wait for exit.
# debian-slim has no pkill/pgrep so we scan /proc.
kill_servers() {
  local container="$1"
  docker exec "$container" sh -c '
    pids=""
    for p in /proc/[0-9]*/comm; do
      if [ "$(cat "$p" 2>/dev/null)" = "open-wire" ]; then
        pid=$(echo "$p" | cut -d/ -f3)
        kill -9 "$pid" 2>/dev/null
        pids="$pids $pid"
      fi
    done
    # Wait for processes to exit (up to 5s).
    for i in $(seq 1 50); do
      alive=0
      for pid in $pids; do
        [ -d "/proc/$pid" ] && alive=1
      done
      [ "$alive" = "0" ] && break
      sleep 0.1
    done
  ' 2>/dev/null || true
}

# ──────────────────────────────────────────────────────────────────────
# Generate config files inside containers
# ──────────────────────────────────────────────────────────────────────
echo ""
echo "Writing cluster configs..."

# Write config files on the host, then mount them into containers.
CONF_DIR=$(mktemp -d)

cat > "$CONF_DIR/tcp_a.conf" <<EOF
listen: 0.0.0.0:4222
server_name: tcp-node-a
cluster {
  name: bench-net-tcp
  listen: 0.0.0.0:5222
  routes = [
    "nats-route://$IP_B:5222"
  ]
}
EOF

cat > "$CONF_DIR/tcp_b.conf" <<EOF
listen: 0.0.0.0:4222
server_name: tcp-node-b
cluster {
  name: bench-net-tcp
  listen: 0.0.0.0:5222
  routes = [
    "nats-route://$IP_A:5222"
  ]
}
EOF

cat > "$CONF_DIR/udp_a.conf" <<EOF
listen: 0.0.0.0:4333
server_name: udp-node-a
cluster {
  name: bench-net-udp
  listen: 0.0.0.0:5333
  udp_port: 6333
  routes = [
    "nats-route://$IP_B:5333"
  ]
}
EOF

cat > "$CONF_DIR/udp_b.conf" <<EOF
listen: 0.0.0.0:4333
server_name: udp-node-b
cluster {
  name: bench-net-udp
  listen: 0.0.0.0:5333
  udp_port: 6333
  routes = [
    "nats-route://$IP_A:5333"
  ]
}
EOF

# Copy configs into running containers.
for f in tcp_a.conf tcp_b.conf udp_a.conf udp_b.conf; do
  docker cp "$CONF_DIR/$f" "$NODE_A:/tmp/$f" 2>/dev/null || true
  docker cp "$CONF_DIR/$f" "$NODE_B:/tmp/$f" 2>/dev/null || true
done

# ──────────────────────────────────────────────────────────────────────
# Run benchmark scenarios
# ──────────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  NETWORK-NAMESPACE BENCHMARK: UDP vs TCP over Docker bridge"
echo "  ${MSGS} msgs × ${SIZE}B payload, ${RUNS} runs"
echo "  Nodes: $IP_A ↔ $IP_B (bridge network, full kernel stack)"
echo "================================================================"
echo ""

# ── Scenario 1: TCP cluster pub/sub (pub on A, sub on B) ──
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  1. TCP CLUSTER PUB/SUB (pub on A, sub on B)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Start TCP servers
echo "Starting TCP cluster..."
dexec_bg "$NODE_A" env RUST_LOG=warn open-wire -c /tmp/tcp_a.conf
sleep 0.5
dexec_bg "$NODE_B" env RUST_LOG=warn open-wire -c /tmp/tcp_b.conf
sleep 2

# Verify connectivity
dexec "$NODE_A" nats pub _bench.ping pong -s "nats://127.0.0.1:4222" >/dev/null 2>&1 || { echo "FAIL: TCP node A not responding"; exit 1; }
dexec "$NODE_B" nats pub _bench.ping pong -s "nats://127.0.0.1:4222" >/dev/null 2>&1 || { echo "FAIL: TCP node B not responding"; exit 1; }
echo "  TCP cluster ready."

tcp_ps_rate_sum=0
for i in $(seq 1 "$RUNS"); do
  # Subscriber on B (background)
  dexec_bg "$NODE_B" nats bench sub "bench.cross.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4222"
  sleep 0.5

  # Publisher on A (foreground, capture output)
  output=$(dexec "$NODE_A" nats bench pub "bench.cross.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4222" 2>&1)
  echo "  Run $i: $(echo "$output" | grep -E "stats:" || echo "$output")"
  rate=$(echo "$output" | extract_rate)
  tcp_ps_rate_sum=$(( tcp_ps_rate_sum + ${rate:-0} ))

  # Wait for subscriber to finish
  sleep 2
done
tcp_ps_rate=$(( tcp_ps_rate_sum / RUNS ))
echo "  Avg: $(fmt_num $tcp_ps_rate) msgs/sec"
echo ""

# Stop TCP servers
kill_servers "$NODE_A"
kill_servers "$NODE_B"
sleep 1

# ── Scenario 2: UDP cluster pub/sub (pub on A, sub on B) ──
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  2. UDP CLUSTER PUB/SUB (pub on A, sub on B)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Start UDP servers
echo "Starting UDP cluster..."
dexec_bg "$NODE_A" env RUST_LOG=warn open-wire -c /tmp/udp_a.conf
sleep 0.5
dexec_bg "$NODE_B" env RUST_LOG=warn open-wire -c /tmp/udp_b.conf
sleep 2

# Retry connectivity check — server may need extra time after previous kill.
udp_ready=0
for attempt in $(seq 1 10); do
  if dexec "$NODE_A" nats pub _bench.ping pong -s "nats://127.0.0.1:4333" >/dev/null 2>&1; then
    udp_ready=1; break
  fi
  sleep 1
done
if [[ "$udp_ready" -eq 0 ]]; then
  echo "FAIL: UDP node A not responding after 10 retries"
  # Debug: show what's running in node A
  dexec "$NODE_A" sh -c 'for p in /proc/[0-9]*/comm; do echo "$(echo $p | cut -d/ -f3): $(cat $p 2>/dev/null)"; done' 2>/dev/null | grep -v "^[0-9]*: $" || true
  exit 1
fi
dexec "$NODE_B" nats pub _bench.ping pong -s "nats://127.0.0.1:4333" >/dev/null 2>&1 || { echo "FAIL: UDP node B not responding"; exit 1; }
echo "  UDP cluster ready."

udp_ps_rate_sum=0
for i in $(seq 1 "$RUNS"); do
  dexec_bg "$NODE_B" nats bench sub "bench.cross.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4333"
  sleep 0.5

  output=$(dexec "$NODE_A" nats bench pub "bench.cross.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4333" 2>&1)
  echo "  Run $i: $(echo "$output" | grep -E "stats:" || echo "$output")"
  rate=$(echo "$output" | extract_rate)
  udp_ps_rate_sum=$(( udp_ps_rate_sum + ${rate:-0} ))

  sleep 2
done
udp_ps_rate=$(( udp_ps_rate_sum / RUNS ))
echo "  Avg: $(fmt_num $udp_ps_rate) msgs/sec"
echo ""

# Stop UDP servers
kill_servers "$NODE_A"
kill_servers "$NODE_B"
sleep 1

# ── Scenario 3: TCP cluster fan-out (pub on A, sub on A + B) ──
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  3. TCP CLUSTER FAN-OUT (pub on A, sub on A + B)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting TCP cluster..."
dexec_bg "$NODE_A" env RUST_LOG=warn open-wire -c /tmp/tcp_a.conf
sleep 0.5
dexec_bg "$NODE_B" env RUST_LOG=warn open-wire -c /tmp/tcp_b.conf
sleep 2

tcp_fan_rate_sum=0
for i in $(seq 1 "$RUNS"); do
  dexec_bg "$NODE_A" nats bench sub "bench.fan.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4222"
  dexec_bg "$NODE_B" nats bench sub "bench.fan.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4222"
  sleep 0.5

  output=$(dexec "$NODE_A" nats bench pub "bench.fan.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4222" 2>&1)
  echo "  Run $i: $(echo "$output" | grep -E "stats:" || echo "$output")"
  rate=$(echo "$output" | extract_rate)
  tcp_fan_rate_sum=$(( tcp_fan_rate_sum + ${rate:-0} ))

  sleep 3
done
tcp_fan_rate=$(( tcp_fan_rate_sum / RUNS ))
echo "  Avg: $(fmt_num $tcp_fan_rate) msgs/sec"
echo ""

kill_servers "$NODE_A"
kill_servers "$NODE_B"
sleep 1

# ── Scenario 4: UDP cluster fan-out (pub on A, sub on A + B) ──
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  4. UDP CLUSTER FAN-OUT (pub on A, sub on A + B)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting UDP cluster..."
dexec_bg "$NODE_A" env RUST_LOG=warn open-wire -c /tmp/udp_a.conf
sleep 0.5
dexec_bg "$NODE_B" env RUST_LOG=warn open-wire -c /tmp/udp_b.conf
sleep 2

udp_fan_rate_sum=0
for i in $(seq 1 "$RUNS"); do
  dexec_bg "$NODE_A" nats bench sub "bench.fan.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4333"
  dexec_bg "$NODE_B" nats bench sub "bench.fan.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4333"
  sleep 0.5

  output=$(dexec "$NODE_A" nats bench pub "bench.fan.test" \
    --msgs "$MSGS" --size "$SIZE" --no-progress \
    -s "nats://127.0.0.1:4333" 2>&1)
  echo "  Run $i: $(echo "$output" | grep -E "stats:" || echo "$output")"
  rate=$(echo "$output" | extract_rate)
  udp_fan_rate_sum=$(( udp_fan_rate_sum + ${rate:-0} ))

  sleep 3
done
udp_fan_rate=$(( udp_fan_rate_sum / RUNS ))
echo "  Avg: $(fmt_num $udp_fan_rate) msgs/sec"
echo ""

kill_servers "$NODE_A"
kill_servers "$NODE_B"
sleep 1

# ── Scenario 5: TCP burst (10x messages) ──
BURST_MSGS=$(( MSGS * 10 ))
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  5. TCP BURST (pub on A, sub on B — ${BURST_MSGS} msgs)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting TCP cluster..."
dexec_bg "$NODE_A" env RUST_LOG=warn open-wire -c /tmp/tcp_a.conf
sleep 0.5
dexec_bg "$NODE_B" env RUST_LOG=warn open-wire -c /tmp/tcp_b.conf
sleep 2

dexec_bg "$NODE_B" nats bench sub "bench.burst.test" \
  --msgs "$BURST_MSGS" --size "$SIZE" --no-progress \
  -s "nats://127.0.0.1:4222"
sleep 0.5
output=$(dexec "$NODE_A" nats bench pub "bench.burst.test" \
  --msgs "$BURST_MSGS" --size "$SIZE" --no-progress \
  -s "nats://127.0.0.1:4222" 2>&1)
echo "  $(echo "$output" | grep -E "stats:" || echo "$output")"
tcp_burst_rate=$(echo "$output" | extract_rate)
sleep 3
echo ""

kill_servers "$NODE_A"
kill_servers "$NODE_B"
sleep 1

# ── Scenario 6: UDP burst (10x messages) ──
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  6. UDP BURST (pub on A, sub on B — ${BURST_MSGS} msgs)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "Starting UDP cluster..."
dexec_bg "$NODE_A" env RUST_LOG=warn open-wire -c /tmp/udp_a.conf
sleep 0.5
dexec_bg "$NODE_B" env RUST_LOG=warn open-wire -c /tmp/udp_b.conf
sleep 2

dexec_bg "$NODE_B" nats bench sub "bench.burst.test" \
  --msgs "$BURST_MSGS" --size "$SIZE" --no-progress \
  -s "nats://127.0.0.1:4333"
sleep 0.5
output=$(dexec "$NODE_A" nats bench pub "bench.burst.test" \
  --msgs "$BURST_MSGS" --size "$SIZE" --no-progress \
  -s "nats://127.0.0.1:4333" 2>&1)
echo "  $(echo "$output" | grep -E "stats:" || echo "$output")"
udp_burst_rate=$(echo "$output" | extract_rate)
sleep 3
echo ""

kill_servers "$NODE_A"
kill_servers "$NODE_B"

# ──────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  SUMMARY — Docker bridge network (full kernel stack)"
echo "================================================================"
echo ""
printf "%-20s %12s %12s %8s\n" "Scenario" "UDP msg/s" "TCP msg/s" "Ratio"
printf "%-20s %12s %12s %8s\n" "────────────────────" "────────────" "────────────" "────────"

for label_var in "A→B:$udp_ps_rate:$tcp_ps_rate" "Fan x2:$udp_fan_rate:$tcp_fan_rate" "Burst:${udp_burst_rate:-0}:${tcp_burst_rate:-0}"; do
  IFS=: read -r label udp_r tcp_r <<< "$label_var"
  if [[ "$tcp_r" -gt 0 ]]; then
    ratio=$(( udp_r * 100 / tcp_r ))
  else
    ratio=0
  fi
  printf "%-20s %12s %12s %7d%%\n" "$label" "$(fmt_num "$udp_r")" "$(fmt_num "$tcp_r")" "$ratio"
done

echo ""
echo "Done."
