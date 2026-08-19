#!/usr/bin/env bash
#
# One command, one terminal, start to finish.
#
#   ./verify.sh              unit tests + full 4-scenario demo   (~5 min)
#   ./verify.sh --quick      unit tests + one 500-rps load test  (~1 min)
#   ./verify.sh --tests-only unit tests only                     (~5 s)
#
# Boots the three mock processors and the router, runs the checks, prints a
# verdict, and shuts everything down. Exit code 0 means every check passed, so
# this is safe to use as a CI gate.
set -uo pipefail
cd "$(dirname "$0")"

MODE="${1:---full}"
PIDS=()
FAILED=0

hr() { printf '%s\n' "================================================================"; }
step() { echo; hr; echo "  $1"; hr; }

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------- dependencies
step "DEPENDENCIES"
missing=()
for mod in fastapi uvicorn httpx pydantic uvloop httptools; do
  python3 -c "import $mod" 2>/dev/null || missing+=("$mod")
done
if [ ${#missing[@]} -gt 0 ]; then
  echo "  missing: ${missing[*]}"
  echo "  installing from requirements.txt..."
  pip install -q -r requirements.txt || { echo "  FAILED to install"; exit 1; }
fi
python3 -c "import pytest" 2>/dev/null || pip install -q pytest pytest-asyncio
echo "  ok: $(python3 --version), all imports present"

# ----------------------------------------------------------------- unit tests
step "UNIT TESTS"
echo "  Requirement 1 (rate limiting), Requirement 2 (health + breaker),"
echo "  retry classification, selection rule, and the shipped config."
echo
if python3 -m pytest -q 2>&1 | tail -20; then
  echo "  UNIT TESTS: PASS"
else
  echo "  UNIT TESTS: FAIL"
  FAILED=1
fi

if [ "$MODE" = "--tests-only" ]; then
  hr; [ "$FAILED" = 0 ] && echo "  RESULT: PASS (unit tests only)" || echo "  RESULT: FAIL"; hr
  exit "$FAILED"
fi

# --------------------------------------------------------------- boot services
step "STARTING SERVICES"
mkdir -p logs

start_psp() {
  PSP_ID="$1" PSP_NAME="$2" PSP_MAX_RPS="$4" PSP_FEE_PERCENT="$5" \
    python3 -m uvicorn mock_psp.main:app --host 127.0.0.1 --port "$3" \
    --loop uvloop --http httptools --log-level warning > "logs/$1.log" 2>&1 &
  PIDS+=($!)
  printf "  %-20s :%s  %3s rps  %s%% fee\n" "$2" "$3" "$4" "$5"
}
start_psp psp-atlas    "Atlas Pay"          9001 150 2.5
start_psp psp-borealis "Borealis Payments"  9002 100 2.9
start_psp psp-cygnus   "Cygnus Financial"   9003 200 2.1

python3 -m uvicorn router.main:app --host 127.0.0.1 --port 8080 \
  --loop uvloop --http httptools --log-level info > logs/router.log 2>&1 &
PIDS+=($!)
echo "  router               :8080"

echo
echo "  waiting for readiness (first boot can take ~30s)..."
for svc in "psp-atlas 9001" "psp-borealis 9002" "psp-cygnus 9003" "router 8080"; do
  set -- $svc
  up=0
  for _ in $(seq 1 240); do
    curl -sf --max-time 1 "http://127.0.0.1:$2/healthz" >/dev/null 2>&1 && { up=1; break; }
    sleep 0.5
  done
  if [ "$up" = 1 ]; then echo "  up: $1"; else
    echo "  FAILED: $1 never responded. See logs/"
    exit 1
  fi
done
echo
echo "  effective limits (rate + in-flight must fit inside each processor's cap):"
grep "router ready" logs/router.log | sed 's/.*| /    /'

# ----------------------------------------------------------------- the demo
if [ "$MODE" = "--quick" ]; then
  step "QUICK LOAD TEST -- 500 rps for 8s"
  python3 -m loadgen.generate --rps 500 --seconds 8 --reset 2>&1 | tail -32
  if python3 - <<'PY'
import asyncio, sys, httpx
async def main():
    async with httpx.AsyncClient(timeout=5.0) as c:
        bad = []
        for port in (9001, 9002, 9003):
            s = (await c.get(f"http://127.0.0.1:{port}/stats")).json()
            if s["rate_limit_rejections"] or s["peak_rps_observed"] > s["max_rps"]:
                bad.append(s["processor_id"])
        sys.exit(1 if bad else 0)
asyncio.run(main())
PY
  then echo; echo "  RATE LIMITS: PASS"; else echo; echo "  RATE LIMITS: FAIL"; FAILED=1; fi
else
  step "END-TO-END DEMO -- 4 scenarios (~4 min)"
  echo "  1. normal traffic   2. the midnight spike"
  echo "  3. processor failure mid-sale   4. automatic recovery"
  if python3 -m loadgen.demo; then
    echo "  DEMO: PASS"
  else
    echo "  DEMO: FAIL"
    FAILED=1
  fi
fi

# --------------------------------------------------------------------- verdict
step "VERDICT"
if [ "$FAILED" = 0 ]; then
  echo "  PASS -- unit tests and end-to-end checks all green."
  echo
  echo "  What was proven:"
  echo "    - no processor was ever sent more than its configured rate limit,"
  echo "      as reported by the processors' own 429 counters"
  if [ "$MODE" = "--quick" ]; then
    echo
    echo "  NOT covered by --quick: failover and recovery are only exercised by"
    echo "  the full run. Use ./verify.sh for those."
  else
    echo "    - a failing processor is detected and removed from rotation"
    echo "      with no manual intervention"
    echo "    - traffic redistributes to healthy processors, and the router"
    echo "      detects recovery on its own via synthetic probes"
  fi
else
  echo "  FAIL -- see the output above."
fi
hr
exit "$FAILED"
