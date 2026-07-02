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

COMPOSE_CONFIG=("-f" "$BOOM_REPO_ROOT/tests/lsst-simulated/compose.yaml")
DB_NAME="${DB_NAME:-boom-lsst-simulated}"
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

mongo_count() {
    local query="$1"
    local raw
    raw=$(docker compose "${COMPOSE_CONFIG[@]}" exec -T mongo \
        mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "$query")
    raw=$(printf '%s\n' "$raw" | tail -n 1 | tr -d '\r')
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

docker compose "${COMPOSE_CONFIG[@]}" down --volumes

if ! docker compose "${COMPOSE_CONFIG[@]}" up  -d; then #--build
    echo "$(current_datetime) - ERROR: Failed to start services"
    docker compose "${COMPOSE_CONFIG[@]}" logs mongo-init || true
    exit 1
fi

mkdir -p logs/lsst-simulated
docker compose "${COMPOSE_CONFIG[@]}" logs -f producer  > logs/lsst-simulated/producer.log  &
BG_PIDS+=($!)
docker compose "${COMPOSE_CONFIG[@]}" logs -f consumer  > logs/lsst-simulated/consumer.log  &
BG_PIDS+=($!)
docker compose "${COMPOSE_CONFIG[@]}" logs -f scheduler > logs/lsst-simulated/scheduler.log &
BG_PIDS+=($!)

# Wait for consumer to receive first message
echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while ! docker compose "${COMPOSE_CONFIG[@]}" logs consumer | grep -q "Consumer received first message, continuing..."; do
    CURRENT_TIME=$(date +%s)
    if [ $((CURRENT_TIME - START_TIME)) -ge "$TIMEOUT_SECS" ]; then
        echo "$(current_datetime) - Timeout waiting for consumer"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - Consumer started"

# Wait for all alerts in MongoDB
echo "$(current_datetime) - Waiting for $EXPECTED_ALERTS alerts in MongoDB"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments()")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    if [ $((CURRENT_TIME - START_TIME)) -ge "$TIMEOUT_SECS" ]; then
        ACTUAL=$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments()")
        echo "$(current_datetime) - Timeout: only $ACTUAL/$EXPECTED_ALERTS alerts ingested"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts ingested"

# Wait for enrichment (properties)
echo "$(current_datetime) - Waiting for enrichment"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments({ properties: { \$exists: true } })")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    if [ $((CURRENT_TIME - START_TIME)) -ge "$TIMEOUT_SECS" ]; then
        ACTUAL=$(mongo_count "db.getSiblingDB('$DB_NAME').LSST_alerts.countDocuments({ properties: { \$exists: true } })")
        echo "$(current_datetime) - Timeout: only $ACTUAL/$EXPECTED_ALERTS alerts enriched"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts enriched"

echo "$(current_datetime) - Pipeline completed successfully"

if [ "$KEEP_UP" = false ]; then
    docker compose "${COMPOSE_CONFIG[@]}" down --volumes
fi

exit 0
