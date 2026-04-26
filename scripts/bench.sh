#!/usr/bin/env bash
# Phase 11 — redis-benchmark comparison: loony-redis vs stock Redis
#
# Usage:
#   ./scripts/bench.sh              # run against loony-redis on :6379 and Redis on :6380
#   LOONY_PORT=7000 ./scripts/bench.sh
#   REDIS_PORT=6379 LOONY_PORT=6380 ./scripts/bench.sh
#
# Prerequisites:
#   redis-benchmark  (ships with Redis; `apt install redis-tools` or Homebrew)
#   redis-server     (for the baseline; optional if you already have one running)
#   cargo            (to build loony-redis)

set -euo pipefail

LOONY_PORT=${LOONY_PORT:-6379}
REDIS_PORT=${REDIS_PORT:-6380}
PIPELINE=${PIPELINE:-1}          # increase to 32 to see pipeline gains
REQUESTS=${REQUESTS:-100000}
CLIENTS=${CLIENTS:-50}
DATA_SIZE=${DATA_SIZE:-32}       # bytes per value

BINARY="./target/release/loony-redis"

RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'; NC='\033[0m'

banner() { echo -e "\n${YLW}=== $* ===${NC}"; }

# ── 1. Build loony-redis ──────────────────────────────────────────────────
banner "Building loony-redis (release)"
cargo build --release --quiet

# ── 2. Start loony-redis ──────────────────────────────────────────────────
banner "Starting loony-redis on :$LOONY_PORT"
"$BINARY" --port "$LOONY_PORT" --no-metrics &
LOONY_PID=$!
trap 'kill $LOONY_PID 2>/dev/null; kill $REDIS_PID 2>/dev/null; exit' INT TERM EXIT
sleep 0.3   # let it bind

# ── 3. Start stock Redis ──────────────────────────────────────────────────
banner "Starting stock Redis on :$REDIS_PORT"
redis-server --port "$REDIS_PORT" --loglevel warning --daemonize no &
REDIS_PID=$!
sleep 0.3

# ── 4. Common benchmark helper ────────────────────────────────────────────
run_bench() {
    local label="$1"
    local port="$2"
    echo -e "\n${GRN}--- $label (port $port, -P $PIPELINE, -n $REQUESTS, -c $CLIENTS, -d $DATA_SIZE) ---${NC}"
    redis-benchmark \
        -h 127.0.0.1 -p "$port"   \
        -P "$PIPELINE"             \
        -n "$REQUESTS"             \
        -c "$CLIENTS"              \
        -d "$DATA_SIZE"            \
        -t ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,mset \
        --csv 2>&1 | awk -F',' '
            NR==1 { next }
            {
                gsub(/"/, "", $1); gsub(/"/, "", $2)
                printf "  %-22s %8s req/s\n", $1, $2
            }
        '
}

# ── 5. Run benchmarks ─────────────────────────────────────────────────────
banner "Benchmark: $REQUESTS requests, $CLIENTS clients, pipeline=$PIPELINE"

run_bench "loony-redis" "$LOONY_PORT"
run_bench "stock Redis" "$REDIS_PORT"

# ── 6. Pipelined comparison (P=32) ────────────────────────────────────────
banner "Pipelined benchmark (P=32): ${REQUESTS} requests"

PIPELINE=32 run_bench "loony-redis P=32" "$LOONY_PORT"
PIPELINE=32 run_bench "stock Redis  P=32" "$REDIS_PORT"

banner "Done"
