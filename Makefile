.PHONY: install test lint typecheck format demo report up e2e down

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy src

format:
	ruff format .
	ruff check --fix .

# Offline end to end (no Docker): emulated Debezium stream -> Iceberg -> checks -> maintenance,
# then docs/results.md and docs/img/*.png. Takes about a minute and a half.
demo:
	rm -rf data/demo
	cdc-lake demo
	cdc-lake report

report:
	cdc-lake report

# Docker stack (Postgres, Redpanda, Debezium Connect, MinIO, consumer). Not run where this repo
# was built; the manual GitHub workflow "e2e" runs the same script.
up:
	docker compose up -d --build --wait

e2e:
	scripts/e2e_smoke.sh

down:
	docker compose down -v
