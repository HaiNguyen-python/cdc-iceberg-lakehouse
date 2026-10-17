# Learning guide (for me, Hai)

Goal: explain change data capture and Iceberg table mechanics in an interview, change this code
with confidence, and defend every number in the README. This is my "streaming / CDC" data
engineering project: logical replication, Kafka delivery semantics, idempotent merges, table
formats, schema evolution, history and maintenance.

Start with [docs/how-it-works.md](docs/how-it-works.md) (the code walkthrough) and
[docs/decisions.md](docs/decisions.md) (the reasons). This file is the practice plan.

## 1. Run it and look inside (1-2 hours)

```bash
make install && make test
cdc-lake --lake data/try apply tests/fixtures/users.jsonl
ls -R data/try/warehouse/lake/users          # metadata/*.json, manifests (avro), data/*.parquet
cdc-lake --lake data/try snapshots users     # cdc.batch / cdc.max-lsn in each summary
cdc-lake --lake data/try apply tests/fixtures/users.jsonl      # again: "0 rows written". Why?
cdc-lake --lake data/try apply tests/fixtures/payments_type_change.jsonl
cdc-lake --lake data/try query "SELECT record_id, reason FROM quarantine"
make demo                                    # about a minute and a half
```

Then:

- Open one line of `tests/fixtures/enrolments.jsonl` and name every field of the envelope.
  Which ones does the code use, and which does it ignore?
- In `data/demo/lake`, compare `cdc-lake as-of --table enrolments --snapshot-id <old id>` with
  `cdc-lake as-of --business-time ...`. Which clock does each one use?
- Break something on purpose: in `merge.plan_state`, change `winner.lsn > lsn` to `>=` or remove
  the `_deleted` check, run `pytest tests/test_merge_apply.py`, and read which test fails and why.
- Run the demo with `--compact-every 0` (no compaction) and compare the apply time chart.

## 2. Read the code in this order

| File | What to understand | Question to answer myself |
| --- | --- | --- |
| `tests/fixtures/*.jsonl` | Real Debezium envelope shape, tombstones | Why is `before` null for an insert but full for a delete here? |
| `events.py` | Envelope parsing and refusal reasons | What does Debezium send for a truncate, and why refuse it? |
| `coerce.py` | Typing JSON values, new vs changed columns | Why is int → double allowed but double → long not? |
| `merge.py` | Highest LSN wins, soft deletes, tombstones | Why is LSN order equal to commit order for one row, but not across rows? |
| `lake.py` | Batch apply, transactions, schema evolution, quarantine | What happens if the process dies between the state commit and the changelog commit? |
| `history.py` | SCD2 from the changelog | Why rebuild per key instead of updating `valid_to` in place? |
| `queries.py` | Snapshot time travel vs SCD2 as-of | When would the two answers differ? |
| `maintenance.py` | Compaction, expiry, orphans | Why does compaction alone not free disk space? |
| `loadgen.py` | Generator, emulator, `deliver` | Which disorder is realistic for Kafka and which is not? |
| `demo.py` | Paced freshness, reconciliation | What exactly does "freshness" measure here, and what does it leave out? |
| `live.py` | Consumer loop, offset commit after Iceberg commit | Why is at-least-once plus an idempotent merge "effectively once"? |
| `docker-compose.yml`, `connectors/` | `wal_level=logical`, replication slot, converter settings | What happens to the WAL if the consumer stops for a day? |

## 3. Concepts in my own words (fill these in)

- **Change data capture vs batch extracts:** ...
- **Postgres WAL, logical decoding, replication slots, `pgoutput`:** ...
- **`REPLICA IDENTITY` (default vs FULL):** ...
- **Debezium snapshot (`op = r`) and streaming; tombstones and log compaction:** ...
- **Kafka delivery: at-most-once, at-least-once, exactly-once; partitions and ordering:** ...
- **Idempotency and the "highest version wins" merge:** ...
- **Iceberg: metadata file, manifest list, manifests, data files, snapshots:** ...
- **Copy-on-write vs merge-on-read:** ...
- **Schema evolution by column id; safe and unsafe changes:** ...
- **SCD type 1 vs type 2:** ...
- **Small-file problem, compaction, snapshot expiry, orphan files:** ...

## 4. Interview questions this repo answers

1. *How do you get changes out of Postgres without full dumps?* Logical decoding through a
   replication slot, Debezium snapshot then stream; show the connector config.
2. *Kafka can deliver twice and out of order. How is the lake still correct?* Merge by primary
   key on the LSN; equal or lower is skipped; soft deletes. Show the shuffled-stream test.
3. *How do you handle deletes in a lake?* Delete events with `before`, tombstone rows with
   `_deleted`, filtered on read; Kafka tombstones only for compaction.
4. *What happens when someone adds or changes a column in production?* Add: metadata-only
   Iceberg schema change. Change: quarantine with a reason, row stays behind until replayed;
   show the reconciliation table.
5. *How would you answer "what did this enrolment look like last Tuesday"?* SCD2 history from
   the changelog; versus Iceberg time travel for "what did the lake show".
6. *Why do streaming writes need table maintenance?* Files per batch, the chart; compaction,
   expiry, orphan cleanup, and what each one costs.
7. *What is your freshness and throughput?* Give the local numbers and say exactly what they
   include. Then: what would dominate in production (poll interval, Debezium lag, commit time).
8. *How would you run this at scale?* Partitioned tables, merge-on-read with Spark/Flink, a REST
   catalog, offsets in the snapshot summary, monitoring replication slot lag.

## 5. Make it mine (do at least three)

- [ ] Run `make e2e` (or the GitHub `e2e` workflow), fix whatever breaks, and add the real
      Postgres-commit-to-Iceberg latency to the README.
- [ ] Add a `requeue` command: read quarantined records, apply a fix (e.g. cast `"49.00"` to
      4900 cents), re-apply, and show the payments reconcile to 0 differing keys.
- [ ] Partition `enrolments` by `bucket(16, id)` and compact only partitions with many files.
- [ ] Store Kafka offsets in the snapshot summary and resume the consumer from them.
- [ ] Purge tombstone rows older than a retention window during compaction, and prove with a
      test that late events older than the window are now the only risk.
- [ ] Read the same warehouse with DuckDB's `iceberg` extension or Spark, and compare results.

## 6. Quality checks I should be able to explain

- `make lint` (ruff), `mypy src`, `pytest` (32 tests, coverage report), `pre-commit install`.
- Why `live.py` is excluded from coverage, and how the `e2e` workflow covers it instead.
- Why the shuffled-stream test compares changelog, history and quarantine too, not only state.
- Why the demo has a positive control (a row deleted behind the pipeline's back must be found).

## 7. Honesty checklist before pushing

- [ ] I can explain every file without looking at it.
- [ ] README and results numbers are from `make demo` runs I did myself.
- [ ] I say clearly that the data is synthetic and the problems are planted.
- [ ] I say that the Docker stack and the `e2e` workflow were not run where the repo was built.
- [ ] I say that the fixtures and emulator follow Debezium's documented format; they were not
      captured from a running connector.
- [ ] Freshness is described as "offered to consumer → Iceberg commit", nothing more.
