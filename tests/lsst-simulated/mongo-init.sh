#!/usr/bin/env bash

set -euo pipefail

MONGO_URI="mongodb://mongoadmin:mongoadminsecret@mongo:27017/${DB_NAME}?authSource=admin"

for i in $(seq 1 30); do
    if mongosh "$MONGO_URI" --quiet --eval "db.runCommand('ping').ok" > /dev/null 2>&1; then
        break
    fi
    echo "Waiting for MongoDB... ($i/30)"
    sleep 2
done

echo "Dropping LSST alert collections and filters"
mongosh "$MONGO_URI" --quiet --eval "
    db.LSST_alerts.drop();
    db.LSST_alerts_aux.drop();
    db.LSST_alerts_cutouts.drop();
    db.filters.drop();"

echo "Creating filters collection with index"
mongosh "$MONGO_URI" --eval "
    db.createCollection('filters');
    db.filters.createIndex({ filter_id: 1 });"

echo "MongoDB initialization completed successfully"
exit 0
