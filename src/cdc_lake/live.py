"""The Docker-stack side: Kafka consumer, Postgres load generator and a smoke check.

Needs the `stream` extra (confluent-kafka, psycopg) and running services, so it is not part of
the unit tests or coverage; the manual `e2e` workflow runs it (see README, "Docker stack").
The consumer reuses exactly the same `Lake.apply` as the offline demo.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from .config import LakeConfig, open_catalog
from .demo import quarantined_keys
from .lake import Lake
from .loadgen import Generator, GeneratorConfig
from .queries import current_state
from .sources import TABLE_ORDER

log = logging.getLogger("cdc_lake.live")
TOPIC_PATTERN = r"^courseplatform\.public\..*"


def _decode(raw: bytes | None) -> Any:
    return None if raw is None or raw == b"" else json.loads(raw)


def ensure_bucket(config: LakeConfig) -> None:
    """Create the MinIO bucket on first start (PyIceberg's FileIO does not create buckets)."""
    if not config.warehouse.startswith("s3://") or not config.s3_endpoint:
        return
    from pyarrow import fs

    endpoint = config.s3_endpoint.split("://", 1)[-1]
    s3 = fs.S3FileSystem(
        access_key=config.s3_access_key,
        secret_key=config.s3_secret_key,
        endpoint_override=endpoint,
        scheme="http" if config.s3_endpoint.startswith("http://") else "https",
        allow_bucket_creation=True,
    )
    s3.create_dir(config.warehouse.removeprefix("s3://").split("/", 1)[0])


def consume(bootstrap: str, group: str, batch_size: int = 500, idle_exit_s: float = 0.0) -> None:
    """Poll Debezium topics, apply each batch to Iceberg, then commit Kafka offsets.

    Offsets are committed only after the Iceberg commit (at-least-once delivery). A crash in
    between redelivers the batch, and the idempotent merge turns that into exactly-once state.
    """
    from confluent_kafka import Consumer

    config = LakeConfig.from_env()
    ensure_bucket(config)
    lake = Lake(open_catalog(config))
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            # Debezium creates topics after the consumer starts; pick them up quickly.
            "topic.metadata.refresh.interval.ms": 5000,
        }
    )
    consumer.subscribe([TOPIC_PATTERN])
    batch_id, idle_since = 0, time.time()
    try:
        while True:
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            records = [
                {
                    "topic": m.topic(),
                    "partition": m.partition(),
                    "offset": m.offset(),
                    "key": _decode(m.key()),
                    "value": _decode(m.value()),
                }
                for m in messages
                if m.error() is None
            ]
            if not records:
                if idle_exit_s and time.time() - idle_since > idle_exit_s:
                    return
                continue
            idle_since = time.time()
            result = lake.apply(records, batch_id=batch_id)
            consumer.commit(asynchronous=False)
            lags = [
                result.committed_at - r["value"]["source"]["ts_ms"] / 1000
                for r in records
                if r["value"] and isinstance(r["value"].get("source"), dict)
            ]
            log.info(
                "batch %d: %d events up to LSN %s, %d rows written, %d quarantined, %.2fs, "
                "max commit-to-lake %.1fs",
                batch_id,
                result.events,
                result.max_lsn,
                result.rows_written,
                result.quarantined,
                result.seconds,
                max(lags) if lags else 0.0,
            )
            batch_id += 1
    finally:
        consumer.close()


def _connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, autocommit=False)


def pg_loadgen(dsn: str, seed: int, users: int, courses: int, steps: int, phase: str) -> None:
    """Run the generator against Postgres. `phase` is `seed` (initial rows) or `changes`.

    Both phases replay the same seeded generator, so `changes` continues from the rows `seed`
    inserted.
    """
    gen = Generator(GeneratorConfig(seed=seed, users=users, courses=courses, steps=steps))
    db = gen.initial()
    with _connect(dsn) as conn, conn.cursor() as cur:
        if phase == "seed":
            for table in TABLE_ORDER:
                for row in db.tables[table].values():
                    cols = list(row)
                    cur.execute(
                        f"INSERT INTO {table} ({', '.join(cols)}) "
                        f"VALUES ({', '.join(['%s'] * len(cols))})",
                        [row[c] for c in cols],
                    )
            conn.commit()
            return
        current_tx = None
        for op in gen.changes():
            if current_tx is not None and op.tx != current_tx:
                conn.commit()
            current_tx = op.tx
            if op.kind == "ddl":
                cur.execute(op.sql)
            elif op.kind == "delete" and op.before:
                cur.execute(f"DELETE FROM {op.table} WHERE id = %s", [op.before["id"]])
            elif op.after is not None:
                cols = list(op.after)
                if op.kind == "insert":
                    cur.execute(
                        f"INSERT INTO {op.table} ({', '.join(cols)}) "
                        f"VALUES ({', '.join(['%s'] * len(cols))})",
                        [op.after[c] for c in cols],
                    )
                else:
                    sets = ", ".join(f"{c} = %s" for c in cols if c != "id")
                    cur.execute(
                        f"UPDATE {op.table} SET {sets} WHERE id = %s",
                        [op.after[c] for c in cols if c != "id"] + [op.after["id"]],
                    )
        conn.commit()


def smoke(dsn: str, timeout_s: float = 180.0) -> bool:
    """Wait until every lake table has the same primary keys as Postgres.

    Keys with a quarantined event are left out: the planted type change makes them differ on
    purpose (the same rule as the offline reconciliation).
    """
    config = LakeConfig.from_env()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        lake = Lake(open_catalog(config))
        skip = quarantined_keys(lake)
        with _connect(dsn) as conn, conn.cursor() as cur:
            ok = True
            for table in TABLE_ORDER:
                cur.execute(f"SELECT id FROM {table}")
                source = {r[0] for r in cur.fetchall()}
                lake_ids = set(current_state(lake, table).column("id").to_pylist())
                log.info("%s: postgres %d rows, lake %d rows", table, len(source), len(lake_ids))
                ok = ok and source - skip[table] == lake_ids - skip[table]
        if ok:
            return True
        time.sleep(5)
    return False
