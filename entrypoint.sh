#!/bin/bash
set -e

cd /opt/hive/bin

# Wait for PostgreSQL
until nc -z postgres 5432; do
    echo "Waiting for PostgreSQL to start..."
    sleep 2
done

echo "Initializing Hive Metastore schema..."
./schematool -dbType postgres -initSchema --verbose || true

echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore
