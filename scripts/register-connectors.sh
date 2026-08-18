#!/usr/bin/env bash
# Register all Debezium connectors with Kafka Connect once it's up.
#   bash scripts/register-connectors.sh
set -e
CONNECT=http://localhost:8083

echo "waiting for Kafka Connect..."
until curl -sf "$CONNECT/connectors" >/dev/null; do sleep 2; done

for f in connectors/*.json; do
  echo "registering $f"
  curl -sf -X POST -H "Content-Type: application/json" --data @"$f" "$CONNECT/connectors" \
    && echo " ok" || echo " (already exists?)"
done

echo "--- connectors ---"
curl -s "$CONNECT/connectors" && echo
