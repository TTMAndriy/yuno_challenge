#!/usr/bin/env bash
# Boots the three mock PSPs and the router, waits for readiness, then holds.
#
# Note: first boot can take 20-40s because each service is a separate uvicorn
# process and Python's import of fastapi/httpx is not fast. The readiness gate
# below waits up to 120s and reports each service as it comes up, so a slow
# machine looks slow rather than broken.
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p logs
PIDS=()

cleanup() {
  echo ""
  echo "shutting down..."
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

start_psp() {
  local id=$1 name=$2 port=$3 rps=$4 fee=$5
  PSP_ID="$id" PSP_NAME="$name" PSP_MAX_RPS="$rps" PSP_FEE_PERCENT="$fee" \
    python3 -m uvicorn mock_psp.main:app --host 127.0.0.1 --port "$port" \
    --loop uvloop --http httptools --log-level warning > "logs/$id.log" 2>&1 &
  PIDS+=($!)
  printf "  %-20s :%s  %3s rps  %s%% fee\n" "$name" "$port" "$rps" "$fee"
}

wait_for() {
  local label=$1 url=$2
  for _ in $(seq 1 240); do
    if curl -sf --max-time 1 "$url" > /dev/null 2>&1; then
      echo "  up: $label"
      return 0
    fi
    sleep 0.5
  done
  echo "  FAILED: $label did not respond at $url"
  return 1
}

echo "starting mock processors..."
start_psp psp-atlas    "Atlas Pay"          9001 150 2.5
start_psp psp-borealis "Borealis Payments"  9002 100 2.9
start_psp psp-cygnus   "Cygnus Financial"   9003 200 2.1

echo "starting router on :8080..."
python3 -m uvicorn router.main:app --host 127.0.0.1 --port 8080 \
    --loop uvloop --http httptools --log-level info &
PIDS+=($!)

echo ""
echo "waiting for readiness (this can take ~30s on first boot)..."
wait_for "psp-atlas"    "http://127.0.0.1:9001/healthz" || cleanup
wait_for "psp-borealis" "http://127.0.0.1:9002/healthz" || cleanup
wait_for "psp-cygnus"   "http://127.0.0.1:9003/healthz" || cleanup
wait_for "router"       "http://127.0.0.1:8080/healthz" || cleanup

cat <<'BANNER'

================================================================
  READY -- Mercado Luna Smart Load Distribution Service
================================================================
  router       http://127.0.0.1:8080
  metrics      http://127.0.0.1:8080/metrics
  summary      http://127.0.0.1:8080/metrics/summary

  In a second terminal:
     python3 -m loadgen.demo        full 4-scenario demo
     python3 -m loadgen.generate --help
================================================================

BANNER
wait
