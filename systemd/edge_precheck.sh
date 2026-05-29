#!/bin/bash
# Edge pre-start validation script.
# Checks .env, backend reachability, model file, and GPU availability
# before starting edge services.
# To run this file - bash edge_precheck.sh

set -euo pipefail

echo "[PRECHECK] Starting prechecks..."

PROJECT_ROOT="/home/neopeak/Desktop/WF-project/WF/Workforce-Detection"
ENV_FILE="$PROJECT_ROOT/backend/.env"
MODEL_PATH="$PROJECT_ROOT/models/WF_V1.4.2_best.engine"

# 1. Check env file
if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] .env file not found: $ENV_FILE"
    exit 1
fi

# Load env
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Validate required env
if [ -z "${EDGE_API_BASE:-}" ]; then
    echo "[ERROR] EDGE_API_BASE is missing or empty in .env"
    exit 1
fi

API_BASE="${EDGE_API_BASE%/}"
ROOT_BASE="${API_BASE%/api/v1}"

if ! command -v curl >/dev/null 2>&1; then
    echo "[ERROR] curl is not installed"
    exit 1
fi

# 2. Check backend reachable
echo "[PRECHECK] Checking backend..."

# Try practical endpoints in order:
# 1) API prefixed edge ping
# 2) root health
# 3) root docs
BACKEND_CANDIDATES=(
    "$API_BASE/edge/ping"
    "$ROOT_BASE/health"
    "$ROOT_BASE/docs"
)

check_backend_once() {
    local url
    for url in "${BACKEND_CANDIDATES[@]}"; do
        if curl -fsS --max-time 5 "$url" > /dev/null 2>&1; then
            REACHABLE_URL="$url"
            return 0
        fi
    done
    return 1
}

for i in {1..10}; do
    if check_backend_once; then
        echo "[OK] Backend reachable ($REACHABLE_URL)"
        break
    fi

    echo "[WAIT] Backend not ready... retry $i"
    sleep 3
done

# final check
if ! check_backend_once; then
    echo "[ERROR] Backend not reachable. Tried:"
    for url in "${BACKEND_CANDIDATES[@]}"; do
        echo "  - $url"
    done
    exit 1
fi

# 3. Check model
if [ ! -f "$MODEL_PATH" ]; then
    echo "[ERROR] Model file missing: $MODEL_PATH"
    exit 1
fi

# 4. GPU check
echo "[PRECHECK] Checking GPU..."
if ! command -v tegrastats >/dev/null 2>&1; then
    echo "[ERROR] tegrastats not found"
    exit 1
fi

# tegrastats is a long-running process; timeout confirms it can start.
set +e
timeout 2s tegrastats --interval 1000 > /dev/null 2>&1
tegrastats_status=$?
set -e

if [ "$tegrastats_status" -ne 0 ] && [ "$tegrastats_status" -ne 124 ]; then
    echo "[ERROR] GPU not accessible (tegrastats exit: $tegrastats_status)"
    exit 1
fi

echo "[PRECHECK] All checks passed"
exit 0
