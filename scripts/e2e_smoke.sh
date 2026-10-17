#!/usr/bin/env bash
# End-to-end smoke test of the Docker stack (needs Docker; run by the manual e2e workflow).
# Seeds Postgres, registers the Debezium connector (initial snapshot), generates changes,
# then waits until every Iceberg table has the same keys as Postgres.
set -euo pipefail

DSN="postgresql://postgres:postgres@postgres:5432/courses"
LOADGEN="cdc-lake loadgen --dsn $DSN --seed 7 --users 60 --courses 8 --steps 400"

docker compose up -d --build --wait
docker compose exec -T consumer $LOADGEN --phase seed

curl -fsS -X POST -H "Content-Type: application/json" \
  --data @connectors/courseplatform-postgres.json http://localhost:8083/connectors
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:8083/connectors/courseplatform-postgres/status | grep -q '"state":"RUNNING"'; then
    break
  fi
  sleep 2
done

docker compose exec -T consumer $LOADGEN --phase changes
docker compose exec -T consumer cdc-lake smoke --dsn "$DSN" --timeout 240
echo "e2e smoke test passed"
