#!/usr/bin/env bash

set -euo pipefail

COMPOSE="-f $(dirname "$0")/compose.yaml"
MONGO_URI="mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom-lsst-simulated?authSource=admin"

run() {
    BOOM_REPO_ROOT="${BOOM_REPO_ROOT:-.}" docker compose $COMPOSE exec -T mongo \
        mongosh "$MONGO_URI" --quiet --eval "$1"
}

echo "alerts:   $(run 'db.LSST_alerts.countDocuments()')"
echo "enriched: $(run 'db.LSST_alerts.countDocuments({properties:{$exists:true}})')"
echo "filters:  $(run 'db.filters.countDocuments()')"
