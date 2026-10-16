#!/usr/bin/env bash
# One-command showcase: builds and boots plexus-flow, plexus-search (if the
# sibling repo exists), the Go tool worker and the Python agent worker, then
# runs every scenario. Services stay up afterwards so you can explore the
# dashboard; Ctrl+C tears everything down.
#
#   ./run_showcase.sh                    # all scenarios, automatic reviewer
#   ./run_showcase.sh --interactive      # you approve/reject in the terminal
#   ./run_showcase.sh --scenario happy   # one scenario
#   ./run_showcase.sh --no-block         # exit when the scenarios finish
#
# Use a real model:  PLEXUS_LLM=openai OPENAI_API_KEY=... ./run_showcase.sh
# or Ollama:         PLEXUS_LLM=openai OPENAI_BASE_URL=http://localhost:11434/v1 OPENAI_MODEL=llama3.1 ./run_showcase.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLEXUS_FLOW_DIR="${PLEXUS_FLOW_DIR:-${ROOT}/../plexus-flow}"
PLEXUS_SEARCH_DIR="${PLEXUS_SEARCH_DIR:-${ROOT}/../plexus-search}"
DATA="${DATA_DIR:-${ROOT}/data}"   # wiped on every run
BIN="${ROOT}/bin"

HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:8080}"
GRPC_ADDR="${GRPC_ADDR:-127.0.0.1:9090}"
RAFT_ADDR="${RAFT_ADDR:-127.0.0.1:9000}"
ACT_RAFT_ADDR="${ACT_RAFT_ADDR:-127.0.0.1:9001}"
SEARCH_ADDR="${SEARCH_ADDR:-127.0.0.1:8090}"
SEARCH_RAFT_ADDR="${SEARCH_RAFT_ADDR:-127.0.0.1:9100}"

SCENARIO="all"
LAUNCHER_ARGS=()
BLOCK=true
while [ $# -gt 0 ]; do
    case "$1" in
        --interactive) LAUNCHER_ARGS+=(--interactive) ;;
        --scenario) SCENARIO="$2"; shift ;;
        --scenario=*) SCENARIO="${1#*=}" ;;
        --no-block) BLOCK=false ;;
        *) echo "unknown option $1"; exit 2 ;;
    esac
    shift
done

CYAN="\033[36m"; GREEN="\033[32m"; YELLOW="\033[33m"; BOLD="\033[1m"; DIM="\033[2m"; RESET="\033[0m"
step() { echo -e "${CYAN}[setup]${RESET} $*"; }

PIDS=()
cleanup() {
    echo -e "\n${YELLOW}[teardown] stopping showcase services...${RESET}"
    [ -f "${DATA}/agents.pid" ] && kill "$(cat "${DATA}/agents.pid")" 2>/dev/null || true
    for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    echo -e "${GREEN}[teardown] done.${RESET}"
}
trap cleanup EXIT INT TERM

wait_http() { # url, name
    for _ in $(seq 1 100); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done
    echo "timed out waiting for $2 ($1)"; exit 1
}

# --- preflight -----------------------------------------------------------------
for addr in "${HTTP_ADDR}" "${GRPC_ADDR}" "${RAFT_ADDR}" "${ACT_RAFT_ADDR}" "${SEARCH_ADDR}" "${SEARCH_RAFT_ADDR}"; do
    if lsof -nP -iTCP:"${addr##*:}" -sTCP:LISTEN >/dev/null 2>&1; then
        echo -e "${YELLOW}port ${addr##*:} is already in use. Stop that process or override the address, e.g.${RESET}"
        echo "  HTTP_ADDR=127.0.0.1:18080 GRPC_ADDR=127.0.0.1:19090 RAFT_ADDR=127.0.0.1:19000 ACT_RAFT_ADDR=127.0.0.1:19001 ./run_showcase.sh"
        trap - EXIT; exit 1
    fi
done

# --- build -------------------------------------------------------------------
mkdir -p "${BIN}"
step "building plexus-flow from ${PLEXUS_FLOW_DIR}"
# built through this module: go.mod replaces plexus-flow and plexus with the sibling checkouts
(cd "${ROOT}" && go build -o "${BIN}/plexus-flow" github.com/avivklas/plexus-flow/cmd/plexus-flow)
step "building Go tool worker"
(cd "${ROOT}" && go build -o "${BIN}/tool-worker" ./go)

KB_URL=""
if [ -d "${PLEXUS_SEARCH_DIR}" ]; then
    step "building plexus-search from ${PLEXUS_SEARCH_DIR}"
    if (cd "${PLEXUS_SEARCH_DIR}" && go build -o "${BIN}/plexus-search" ./cmd/plexus-search); then
        KB_URL="http://${SEARCH_ADDR}"
    else
        echo -e "${YELLOW}  plexus-search failed to build; using the in-memory KB${RESET}"
    fi
else
    step "plexus-search not found at ${PLEXUS_SEARCH_DIR}; using the in-memory KB"
fi
step "syncing Python environment (uv)"
(cd "${ROOT}" && uv sync -q)

rm -rf "${DATA}" && mkdir -p "${DATA}/logs"

# --- boot ----------------------------------------------------------------------
if [ -n "${KB_URL}" ]; then
    step "starting plexus-search on ${SEARCH_ADDR}"
    "${BIN}/plexus-search" --id kb-1 --http-addr "${SEARCH_ADDR}" --raft-addr "${SEARCH_RAFT_ADDR}" \
        --data-dir "${DATA}/search" --bootstrap > "${DATA}/logs/plexus-search.log" 2>&1 &
    PIDS+=($!)
    wait_http "${KB_URL}/health" plexus-search
    sleep 1 # let it win its Raft election before writing
    (cd "${ROOT}" && uv run python python/kb.py seed --url "${KB_URL}")
fi
export KB_URL
export LLM_LEDGER_DIR="${DATA}/llm_calls"
export PYTHONUNBUFFERED=1

step "starting plexus-flow (dashboard http://${HTTP_ADDR}, workers gRPC ${GRPC_ADDR})"
"${BIN}/plexus-flow" --http-addr "${HTTP_ADDR}" --grpc-addr "${GRPC_ADDR}" \
    --raft-addr "${RAFT_ADDR}" --act-raft-addr "${ACT_RAFT_ADDR}" \
    --data-dir "${DATA}/flow" > "${DATA}/logs/plexus-flow.log" 2>&1 &
PIDS+=($!)
wait_http "http://${HTTP_ADDR}/api/v1/cluster/status" plexus-flow

step "starting Go tool worker"
"${BIN}/tool-worker" --grpc-addr "${GRPC_ADDR}" &
PIDS+=($!)

step "starting Python agent worker"
(cd "${ROOT}" && exec uv run python python/agents.py --grpc-addr "${GRPC_ADDR}" --pidfile "${DATA}/agents.pid") &
PIDS+=($!)
sleep 2

# --- run -----------------------------------------------------------------------
(cd "${ROOT}" && uv run python python/launcher.py --scenario "${SCENARIO}" \
    --http-addr "http://${HTTP_ADDR}" --grpc-addr "${GRPC_ADDR}" --pidfile "${DATA}/agents.pid" "${LAUNCHER_ARGS[@]+"${LAUNCHER_ARGS[@]}"}")

echo -e "\n👉 Explore the flow graph and every trace at ${BOLD}http://${HTTP_ADDR}${RESET}"
echo -e "${DIM}   logs: ${DATA}/logs   llm call ledgers: ${DATA}/llm_calls${RESET}"
if [ "${BLOCK}" = true ]; then
    echo -e "${YELLOW}Services are still running. Press Ctrl+C to stop.${RESET}"
    while true; do sleep 1; done
fi
