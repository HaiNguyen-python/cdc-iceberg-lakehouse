# How it works

This page follows one change from a Postgres `UPDATE` to an Iceberg commit, and explains the
important code on the way. File references point to `src/cdc_lake/`.

## 0. The source: four tables in Postgres

`sources.py` declares the course platform: `users`, `courses`, `enrolments`, `payments`, each
with a `bigint id` primary key and `timestamptz` audit columns. `init_sql()` generates the DDL for
the Postgres container (`docker/postgres/init.sql`, kept identical by a test) and sets
`REPLICA IDENTITY FULL`, so Debezium sends the whole old row in `before` for updates and deletes.

`arrow_schema()` turns a spec into the first Iceberg schema of a lake table: the source columns
plus four metadata columns: `_lsn` (which version this is), `_op`, `_ts_ms` (commit time in
Postgres) and `_deleted`. After the table exists, **Iceberg is the source of truth for the
schema** (`Lake.kinds()` reads it back), because schema evolution can add columns.

## 1. Debezium: from WAL to Kafka records

With `wal_level=logical`, Postgres writes enough information into the WAL to rebuild row
changes. Debezium (`connectors/courseplatform-postgres.json`) uses the built-in `pgoutput` plugin
through a replication slot. On first start it takes a **snapshot**: every existing row is
emitted with `op = "r"` and the same LSN. Then it streams changes: `c` (insert), `u` (update),
`d` (delete). After each delete it also writes a **tombstone**: same key, null value, so Kafka
log compaction can drop the key later.

The connector uses the JSON converter **without schemas**, so a record value looks like:

```json
{"before": {...}, "after": {...}, "op": "u", "ts_ms": 1788250200140,
 "source": {"connector": "postgresql", "table": "users", "lsn": 24025112,
            "ts_ms": 1788250200000, "txId": 760, "snapshot": "false", ...}}
```

`decimal.handling.mode=string` turns `numeric` into strings (the default would be base64
bytes), and `time.precision.mode=adaptive` sends `timestamptz` as ISO-8601 strings. The test
fixtures in `tests/fixtures/*.jsonl` are written in this format, one Kafka record per line:
`{"topic", "partition", "offset", "key", "value"}`.

## 2. Parsing the envelope (`events.py`)

`parse_record` turns one record into a `ChangeEvent`:

1. `key` and `value` may be JSON objects or JSON strings (what `rpk topic consume` prints).
2. A null or empty value is a tombstone: the table comes from the topic name
   (`courseplatform.public.users` → `users`), the key from the record key, and there is no LSN.
3. Otherwise it checks `op` (only `c/u/d/r`; a truncate `t` is refused), the `source` block and
   `source.lsn`. A `schema`/`payload` envelope is refused with a hint to disable schemas.
4. The row image is `after` for `c/u/r` and `before` for `d`.

Anything refused raises `EnvelopeError` with a short reason that ends up in the quarantine.

## 3. Typing and schema drift (`coerce.py`)

JSON without schemas has no types, so each value is checked against the column's kind:

- `coerce_value` accepts `int` for `long`, `int`/`float` for `double` (widening), strings for
  `string`, ISO strings with an offset for `timestamptz`. Anything else raises `TypeChange`,
  e.g. `type change: amount_cents expected long, got string '49.00'`.
- `new_columns` finds keys the table does not have yet and infers their kind from the first
  non-null value. Null-only columns are skipped until a real value shows the type.

## 4. Applying a batch (`lake.py → Lake.apply`)

A batch is a list of records, as a consumer poll returns them. `apply`:

1. Parses every record. Bad envelopes and unknown tables are collected for quarantine.
2. Groups events by table and calls `_apply_table` for each one.

`_typed_changes` (per table):

1. Sorts events by LSN, so "the first value decides a new column's type" is deterministic.
2. Rejects events without a usable primary key, or whose known columns no longer fit.
3. Collects new columns from the surviving events and calls `add_columns`, which runs
   `table.update_schema().add_column(...)` on the state, changelog and history tables. This is a
   metadata commit: Iceberg tracks columns by id, old files read the new column as null.
4. Types every row against the evolved schema and builds `merge.Change` objects.

`_apply_table` then writes three things.

**Current state.** It reads `(id, _lsn, _deleted)` for the touched keys and asks
`merge.plan_state` what to write (next section). The result is written in **one transaction**:
`delete(id IN keys that already exist)` and `append(new rows)`. PyIceberg rewrites only the data
files that contain those keys (copy-on-write) and commits both steps atomically, so a reader
never sees a key missing or twice.

**Changelog.** `merge.new_versions` keeps one row per `(id, LSN)` that the changelog does not
have yet, sorted by key and LSN, and appends them. Replays add nothing.

**SCD2 history** (enrolments only). For keys that got new versions, `_rebuild_history` reads
their full changelog, builds history rows with `history.scd2_rows` and replaces those keys'
history in one transaction.

Finally `_quarantine` appends rejected records with their reason, skipping record ids
(`topic:partition:offset`) already there, so replays do not duplicate quarantine rows either.
Every data commit carries `cdc.batch` and `cdc.max-lsn` in its snapshot summary
(`cdc-lake snapshots <table>` shows them).

## 5. The merge rules (`merge.py`)

`plan_state(changes, stored, pk)` is a pure function, which is why most of the guarantees can
be tested without Iceberg:

- `latest_per_key` keeps the change with the highest `order_key` per key: LSN first, then a
  deterministic tie-break (only exact duplicates share an LSN).
- A stored row is replaced only by a strictly higher LSN. Equal or lower = duplicate or stale,
  counted as `superseded`.
- A delete is written as a row with `_deleted = true` and the last values from `before`. The row
  stays, so an older update arriving later sees a higher stored LSN and is skipped.
- A tombstone deletes the key at the highest LSN known for it, if it is still alive. A tombstone
  for an unknown or already deleted key changes nothing.

Why LSN order is safe: two transactions that touch the same row are serialised by the row lock,
so the second change is written to the WAL after the first one commits. Across different rows
and different topics there is no order, and the merge does not need one.

## 6. History and time travel (`history.py`, `queries.py`)

`scd2_rows` sorts each key's versions by LSN. Every non-delete version becomes a row valid from
its `_ts_ms` until the next version's `_ts_ms`; the last one is `is_current`. A delete closes the
previous version and adds nothing. Snapshot rows (`op = r`) get the snapshot time as
`valid_from`, because what happened before CDC was switched on is unknown.

`queries.py` reads the lake two ways:

- `state_at_snapshot` / `state_at_time` scan an older Iceberg snapshot (lake time).
- `enrolments_as_of` queries the SCD2 table with DuckDB:
  `valid_from <= t AND (valid_to IS NULL OR valid_to > t)` (source time).
- `sql()` registers the current state of every table (tombstones removed), the history and the
  quarantine in DuckDB for ad-hoc SQL (`cdc-lake query`).

## 7. Maintenance (`maintenance.py`)

Each batch adds at least one data file per touched table. `compact` reads the live rows and
overwrites the table with them: same rows, one file, atomic. `expire_snapshots` keeps the newest
N snapshots, removes the others from the metadata, then deletes data files on local disk that no
remaining snapshot references (orphans). `file_stats` reports live files, snapshots and files
on disk, before and after.

## 8. The load generator and the offline demo (`loadgen.py`, `demo.py`)

`Generator` builds an initial database (users, courses, enrolments, 70% paid) and then yields
`Op`s in commit order: new users, enrolments with pending payments, progress updates, payments
settling or failing, cancellations with refunds, price changes, deleted payments, GDPR-style user
deletes (children first, one transaction), new courses. It also plants:

- at 35% of the run: `ALTER TABLE courses ADD COLUMN level text`;
- at 60%: `amount_cents` becomes `numeric` for 30 steps, so Debezium sends strings, then goes
  back to `bigint` (the rewrite back emits no events);
- at 50%: a copy of the enrolments table, to check SCD2 "as of" later.

`DebeziumEmitter` wraps the rows in envelopes with growing LSNs, one topic per table, offsets,
and a tombstone after each delete. `deliver` interleaves the topics at random (Kafka keeps order
only inside a partition), can shuffle inside windows (more disorder than Kafka allows) and can
deliver a block twice (a consumer that crashed before committing offsets).

`run_demo` applies the snapshot, then the stream in fixed batches with a paced producer (record
*i* is available at `t0 + i / rate`), compacting every N batches. Then it checks: replay of the
last two batches writes nothing; each table at the snapshot after the initial load equals the
initial database; SCD2 as of the checkpoint equals the copy; the final state reconciles with the
generator's database, with every difference explained by a quarantined event. It finishes with
maintenance on all nine tables and writes `metrics.json`. `report.py` turns that into
`docs/results.md` and the two charts.

## 9. The Docker stack (`live.py`, `docker-compose.yml`)

`consume` subscribes to `^courseplatform\.public\..*`, polls up to 500 records, calls the same
`Lake.apply`, and commits Kafka offsets **after** the Iceberg commit. A crash in between means
the batch comes again, and the idempotent merge makes that harmless. `pg_loadgen` runs the same
generator as SQL against Postgres, and `smoke` waits until every lake table has the same keys as
Postgres (keys with quarantined events excepted). `scripts/e2e_smoke.sh` ties it together; the
manual `e2e` workflow runs it. None of this was run where the repo was built (no Docker).
