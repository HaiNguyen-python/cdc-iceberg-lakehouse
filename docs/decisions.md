# Design decisions

Short records of the main choices: what was decided, why, and what it costs. Format inspired by
Architecture Decision Records (ADRs).

## 1. Debezium with the JSON converter, schemas disabled

- **Decision:** Debezium's Postgres connector (`pgoutput`) with
  `value.converter.schemas.enable=false`, `decimal.handling.mode=string`,
  `time.precision.mode=adaptive`, one topic per table.
- **Why:** the records are small and readable, any tool (rpk, kcat, a test fixture) can produce
  them, and no schema registry is needed. Numeric as strings avoids Debezium's base64 decimal
  encoding.
- **Cost:** no types on the wire. The consumer must type values itself against the lake schema
  (ADR 6), and a type change in the source is only visible as "this value does not fit".
- **Alternatives:** Avro or Protobuf with a schema registry (typed, schema changes explicit, one
  more service); Debezium Server writing to Iceberg directly (less code, less control over merge
  rules and quarantine).

## 2. Merge in Python, write with PyIceberg copy-on-write transactions

- **Decision:** the merge decision is made by pure functions (`merge.py`); the write is, per
  table and batch, one transaction: `delete(id IN changed keys)` + `append(new rows)`.
- **Why:** the installed PyIceberg (0.12) can delete by filter and append atomically, and it has
  `upsert`, but `upsert` matches on columns only: it has no "keep the highest LSN" rule and does
  not handle deletes. Making the decision in Python keeps the rule in one small, fully tested
  function, and the commit stays atomic. The changelog table (ADR 4) provides the merge-on-read
  view of history.
- **Cost:** copy-on-write rewrites every data file that holds a changed key, so each batch writes
  more than the changed rows; stored LSNs are read back for every batch. Fine for thousands of
  changes per batch, not for millions.
- **Alternatives:** Spark or Flink with Iceberg `MERGE INTO` and merge-on-read delete files (the
  production answer at scale, with a JVM cluster); PyIceberg `upsert` (simpler, but wrong for
  out-of-order events).

## 3. Order by LSN, never by arrival

- **Decision:** per primary key, the version with the highest `source.lsn` wins; a stored row is
  replaced only by a strictly higher LSN.
- **Why:** for one row, LSN order is commit order (row locks serialise writers). One rule then
  gives three properties: replays are no-ops (equal LSN), duplicates collapse, and shuffled
  delivery converges. A test applies a fully shuffled stream with duplicates and compares every
  table with the ordered run.
- **Cost:** needs a total order per key. Snapshot rows share one LSN, which is fine because each
  key appears once in a snapshot. A primary-key update arrives as delete + create of two keys and
  works; composite keys are not supported (all tables here have `id`).

## 4. Soft deletes, an append-only changelog and rebuilt SCD2

- **Decision:** a delete is stored as a row with `_deleted = true` in the state table. Every
  distinct (key, LSN) version goes to `<table>_changelog`. SCD2 history for touched keys is
  rebuilt from the changelog on every batch.
- **Why:** if a delete removed the row, a late older update would recreate it; the tombstone row
  remembers the delete's LSN. The changelog is the full, ordered truth, so history built from it
  is correct even when a version arrives late, and a rebuild per key is idempotent.
- **Cost:** deleted keys stay in the state table (readers must filter `_deleted`); purging old
  tombstone rows would need a retention rule. The changelog grows forever. Rebuilding history
  reads each touched key's whole changelog.

## 5. Kafka tombstones delete only what is still alive

- **Decision:** a tombstone (null value, no LSN) deletes its key at the highest LSN known for
  it, if the key is alive; otherwise it is counted and ignored. It is not written to the
  changelog or history.
- **Why:** Debezium always sends the delete event (with LSN and `before`) first, so normally the
  tombstone has nothing left to do. Handling it anyway covers a compacted topic where only the
  tombstone survived.
- **Cost:** a tombstone-only delete has no LSN or timestamp, so it cannot close an SCD2 version.

## 6. Add new columns, quarantine type changes

- **Decision:** a key the table does not have becomes a new nullable Iceberg column, typed from
  its first non-null value (`update_schema().add_column`). A value that does not fit a known
  column sends the whole event to `lake.quarantine` with a reason. `int` into `double` is allowed.
- **Why:** adding a column is safe and Iceberg does it in metadata, without rewriting files.
  A type change is not safe: writing `"49.00"` into a `long` column either fails or needs a guess
  that might be wrong. A person should decide.
- **Cost:** the quarantined event's row stays behind the source until someone fixes and replays
  it (the demo shows exactly which keys). A new column that is always null is not added until a
  value appears. A string column that holds timestamps is added as `string`.

## 7. Two clocks for "as of" queries

- **Decision:** Iceberg snapshots answer "what did the lake contain after commit X"; the SCD2
  table answers "what was true in the source at time T" (Postgres commit time).
- **Why:** they are different questions. Snapshots are free with Iceberg and great for debugging
  and reproducible reads, but they disappear on expiry and their timestamps are lake commit
  times, not business times.
- **Cost:** two mechanisms to explain; SCD2 exists only for enrolments here.

## 8. Maintenance: whole-table compaction, keep-last-N expiry, local orphan cleanup

- **Decision:** `compact` overwrites a table with its live rows; `expire_snapshots` keeps the
  newest N snapshots and then deletes data files no snapshot references.
- **Why:** each micro-batch adds files (the chart shows `enrolments` growing to 19 files); a scan
  opens every file, and per-batch apply time fell from 2.5 s to 0.7 s after the first compaction
  in the demo run. PyIceberg 0.12 has snapshot expiry but no data-file rewrite or orphan removal,
  so both are small functions here.
- **Cost:** a full rewrite is fine for these table sizes but wasteful for large tables
  (partition-level bin-packing is the real answer). Orphan removal assumes a single writer and
  works on the local filesystem only. Expired snapshots are gone for time travel.

## 9. SQL catalog on SQLite, local warehouse by default

- **Decision:** PyIceberg's `SqlCatalog` with a SQLite file and a `file://` warehouse; the Docker
  stack points the warehouse at MinIO (`s3://lake/warehouse`).
- **Why:** runs in the sandbox, in CI and on a laptop with no services, and the tables are real
  Iceberg tables (metadata JSON, manifests, Parquet) that any Iceberg reader can open.
- **Cost:** SQLite allows one writer at a time and only local processes can share it; a team
  would use a REST catalog (Nessie, Polaris, Glue).

## 10. An offline emulator, a paced demo and a manual e2e workflow

- **Decision:** `cdc-lake demo` emulates Debezium from the load generator; CI runs unit tests,
  validates the compose file and builds the image; the full Docker stack runs only in the
  manual `e2e` workflow.
- **Why:** the emulator makes every guarantee testable and reproducible from a seed, and CI does
  not depend on pulling five images and waiting for Debezium. Marking a flaky job
  `continue-on-error` would hide failures; a manual job is honest about what it is.
- **Cost:** the emulator is my model of Debezium's output, checked against the documented
  format, not captured from a running connector. Freshness in the demo excludes Postgres →
  Debezium → Kafka latency. The Docker stack was not run in the environment this repo was built
  in.
