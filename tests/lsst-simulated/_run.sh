#!/usr/bin/env bash

set -euo pipefail

current_datetime() { TZ=utc date "+%Y-%m-%d %H:%M:%S"; }

KEEP_UP=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --keep-up) KEEP_UP=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "${BOOM_REPO_ROOT:-}" ]; then
    echo "Error: BOOM_REPO_ROOT is not set"
    exit 1
fi
if [ -z "${SIM_ID:-}" ]; then
    echo "Error: SIM_ID is not set"
    exit 1
fi

SIM_PROJECT="${SIM_PROJECT:-boom-sim-${SIM_ID}}"
SIM_COMPOSE=("-p" "$SIM_PROJECT" "-f" "$BOOM_REPO_ROOT/tests/lsst-simulated/sim/compose.yaml")
INFRA_COMPOSE=("-f" "$BOOM_REPO_ROOT/tests/lsst-simulated/infra/compose.yaml")

DB_NAME="${SIM_DB_NAME:-boom-simu-${SIM_ID}}"
EXPECTED_ALERTS="${EXPECTED_ALERTS:-9}"
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
BG_PIDS=()

cleanup() {
    if [ ${#BG_PIDS[@]} -gt 0 ]; then
        kill "${BG_PIDS[@]}" 2>/dev/null || true
        wait "${BG_PIDS[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Query MongoDB running in the shared infra stack
mongo_count() {
    local query="$1"
    local raw
    raw=$(docker compose "${INFRA_COMPOSE[@]}" exec -T mongo \
        mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "$query")
    raw=$(printf '%s\n' "$raw" | tail -n 1 | tr -d '\r')
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

docker compose --progress quiet "${SIM_COMPOSE[@]}" down --volumes 2>/dev/null || true

if ! docker compose --progress quiet "${SIM_COMPOSE[@]}" up -d; then
    echo "$(current_datetime) - ERROR: Failed to start simulation services"
    docker compose "${SIM_COMPOSE[@]}" logs mongo-init || true
    exit 1
fi

LOG_DIR="logs/lsst-simulated/${SIM_ID}"
mkdir -p "$LOG_DIR"
docker compose "${SIM_COMPOSE[@]}" logs -f producer  > "$LOG_DIR/producer.log"  &
BG_PIDS+=($!)
docker compose "${SIM_COMPOSE[@]}" logs -f consumer  > "$LOG_DIR/consumer.log"  &
BG_PIDS+=($!)
docker compose "${SIM_COMPOSE[@]}" logs -f scheduler > "$LOG_DIR/scheduler.log" &
BG_PIDS+=($!)

echo "$(current_datetime) - [${SIM_ID}] Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while ! docker compose "${SIM_COMPOSE[@]}" logs consumer | grep -q "Consumer received first message, continuing..."; do
    CURRENT_TIME=$(date +%s)
    if [ $((CURRENT_TIME - START_TIME)) -ge "$TIMEOUT_SECS" ]; then
        echo "$(current_datetime) - [${SIM_ID}] Timeout waiting for consumer"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - [${SIM_ID}] Consumer started"

echo "$(current_datetime) - [${SIM_ID}] Waiting for $EXPECTED_ALERTS alerts in MongoDB ($DB_NAME)"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments()")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    if [ $((CURRENT_TIME - START_TIME)) -ge "$TIMEOUT_SECS" ]; then
        ACTUAL=$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments()")
        echo "$(current_datetime) - [${SIM_ID}] Timeout: only $ACTUAL/$EXPECTED_ALERTS alerts ingested"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - [${SIM_ID}] All $EXPECTED_ALERTS alerts ingested"

echo "$(current_datetime) - [${SIM_ID}] Waiting for enrichment"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments({ properties: { \$exists: true } })")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    if [ $((CURRENT_TIME - START_TIME)) -ge "$TIMEOUT_SECS" ]; then
        ACTUAL=$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments({ properties: { \$exists: true } })")
        echo "$(current_datetime) - [${SIM_ID}] Timeout: only $ACTUAL/$EXPECTED_ALERTS alerts enriched"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - [${SIM_ID}] All $EXPECTED_ALERTS alerts enriched"

echo "$(current_datetime) - [${SIM_ID}] Pipeline completed successfully"

if [ "$KEEP_UP" = false ]; then
    docker compose --progress quiet "${SIM_COMPOSE[@]}" down --volumes
fi

exit 0
