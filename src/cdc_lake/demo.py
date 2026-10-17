"""`cdc-lake demo`: the whole pipeline offline, from load generator to maintained Iceberg tables.

No Docker: the Debezium stream is emulated from the generator (`loadgen.py`), so the run is
reproducible from a seed. Row counts, quarantine and reconciliation are deterministic; timings
are measured on this machine (local, single process) and vary between runs.

Freshness is measured with a paced producer: record *i* becomes available at
`t0 + i / rate`, the consumer takes fixed-size batches (waiting until a batch is full), and
freshness is the time from a record becoming available to the Iceberg commit that contains it.
It covers batching and apply time, not Postgres -> Debezium -> Kafka latency.
"""

from __future__ import annotations

import json
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from .coerce import coerce_row
from .config import LakeConfig, open_catalog
from .history import ms_to_datetime
from .lake import Lake
from .loadgen import DebeziumEmitter, Generator, GeneratorConfig, deliver
from .maintenance import FileStats, compact, expire_snapshots, file_stats
from .queries import current_snapshot_id, current_state, enrolments_as_of, state_at_snapshot
from .sources import TABLE_ORDER


@dataclass
class DemoConfig:
    seed: int = 7
    users: int = 300
    courses: int = 24
    steps: int = 3000
    batch_size: int = 200
    rate: float = 75.0  # events/s offered by the simulated producer; 0 = as fast as possible
    shuffle_window: int = 40
    redeliver_count: int = 300
    compact_every: int = 10
    retain_last: int = 3


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[index]


def diff_rows(
    lake_rows: pa.Table, source: dict[int, dict[str, Any]], kinds: dict[str, str]
) -> dict[str, list[int]]:
    """Keys missing from the lake, extra in the lake, and present in both with other values."""
    lake = {row["id"]: row for row in lake_rows.to_pylist()}
    missing = sorted(set(source) - set(lake))
    extra = sorted(set(lake) - set(source))
    changed = []
    for pk in sorted(set(source) & set(lake)):
        expected = coerce_row(source[pk], kinds)
        actual = {c: lake[pk].get(c) for c in expected}
        if actual != expected:
            changed.append(pk)
    return {"missing": missing, "extra": extra, "changed": changed}


def quarantined_keys(lake: Lake) -> dict[str, set[int]]:
    """Primary keys of quarantined change events, per table."""
    out: dict[str, set[int]] = {t: set() for t in lake.specs}
    for row in lake.table("quarantine").scan().to_arrow().to_pylist():
        record = json.loads(row["record"])
        value = record.get("value") or {}
        image = value.get("after") or value.get("before") or record.get("key") or {}
        if row["table_name"] in out and isinstance(image.get("id"), int):
            out[row["table_name"]].add(image["id"])
    return out


def reconcile(lake: Lake, source: dict[str, dict[int, dict[str, Any]]]) -> dict[str, Any]:
    """Compare the lake's current state with the source database, table by table.

    Every difference must be explained by a quarantined event for the same key; anything else
    is a bug in the pipeline.
    """
    explained = quarantined_keys(lake)
    report: dict[str, Any] = {}
    for name in TABLE_ORDER:
        lake_rows = current_state(lake, name)
        diff = diff_rows(lake_rows, source[name], lake.kinds(name))
        differing = set(diff["missing"]) | set(diff["extra"]) | set(diff["changed"])
        report[name] = {
            "source_rows": len(source[name]),
            "lake_rows": lake_rows.num_rows,
            "differing": len(differing),
            "explained_by_quarantine": len(differing & explained[name]),
            "unexplained": sorted(differing - explained[name]),
        }
    return report


def _wait_until(when: float) -> None:
    delay = when - time.time()
    if delay > 0:
        time.sleep(delay)


def run_phase(
    lake: Lake,
    records: list[dict[str, Any]],
    cfg: DemoConfig,
    phase: str,
    first_batch: int,
    paced: bool,
    compact_every: int = 0,
) -> tuple[list[dict[str, Any]], list[float]]:
    """Apply records in fixed-size batches; return per-batch metrics and per-event freshness."""
    batches: list[dict[str, Any]] = []
    freshness: list[float] = []
    t0 = time.time()
    rate = cfg.rate if paced and cfg.rate > 0 else 0.0
    for n, start in enumerate(range(0, len(records), cfg.batch_size)):
        chunk = records[start : start + cfg.batch_size]
        available = [t0 + (start + i) / rate for i in range(len(chunk))] if rate else []
        if available:
            _wait_until(available[-1])
        result = lake.apply(chunk, batch_id=first_batch + n)
        lags = [result.committed_at - a for a in available]
        freshness += lags
        live_files = {name: file_stats(lake.table(name)).live_files for name in lake.specs}
        compacted = bool(compact_every) and (n + 1) % compact_every == 0
        if compacted:
            # Inline, like a consumer that compacts between batches: the next batch waits.
            for source in lake.specs:
                for name in lake.related_tables(source):
                    compact(lake.table(name))
        batches.append(
            {
                "batch": first_batch + n,
                "phase": phase,
                "events": result.events,
                "seconds": round(result.seconds, 4),
                "rows_written": result.rows_written,
                "superseded": sum(t.superseded for t in result.tables.values()),
                "quarantined": result.quarantined,
                "columns_added": result.columns_added,
                "compacted_after": compacted,
                "freshness_p50_s": round(statistics.median(lags), 3) if lags else None,
                "freshness_max_s": round(max(lags), 3) if lags else None,
                "live_files": live_files,
            }
        )
    return batches, freshness


def _sum_stats(stats: list[FileStats]) -> dict[str, int]:
    return {
        "live_files": sum(s.live_files for s in stats),
        "snapshots": sum(s.snapshots for s in stats),
        "files_on_disk": sum(s.files_on_disk for s in stats),
    }


def run_demo(out_dir: Path, cfg: DemoConfig | None = None) -> dict[str, Any]:
    """Run the offline pipeline end to end and write `metrics.json` to `out_dir`."""
    cfg = cfg or DemoConfig()
    out_dir = Path(out_dir)
    lake = Lake(open_catalog(LakeConfig.local(out_dir / "lake")))

    gen = Generator(
        GeneratorConfig(seed=cfg.seed, users=cfg.users, courses=cfg.courses, steps=cfg.steps)
    )
    initial = gen.initial().snapshot()
    emitter = DebeziumEmitter(seed=cfg.seed)
    snapshot_records = deliver(emitter.snapshot(gen.db), seed=cfg.seed)
    change_records = emitter.stream(gen.changes())
    arrivals = deliver(
        change_records,
        seed=cfg.seed,
        shuffle_window=cfg.shuffle_window,
        redeliver_at=0.8,
        redeliver_count=cfg.redeliver_count,
    )

    # 1. initial snapshot (op = r)
    snap_batches, _ = run_phase(lake, snapshot_records, cfg, "snapshot", 0, paced=False)
    after_snapshot = {name: current_snapshot_id(lake, name) for name in lake.specs}

    # 2. streaming changes, paced, with periodic compaction
    stream_batches, freshness = run_phase(
        lake,
        arrivals,
        cfg,
        "stream",
        len(snap_batches),
        paced=True,
        compact_every=cfg.compact_every,
    )

    # 3. checks
    replay = lake.apply(arrivals[-2 * cfg.batch_size :], batch_id=-1)
    time_travel = {}
    for name in TABLE_ORDER:
        snapshot_id = after_snapshot[name]
        old = (
            state_at_snapshot(lake, name, snapshot_id) if snapshot_id else current_state(lake, name)
        )
        diff = diff_rows(old, initial[name], lake.kinds(name))
        time_travel[name] = {
            "rows": old.num_rows,
            "source_rows": len(initial[name]),
            "differing": sum(len(v) for v in diff.values()),
        }
    as_of = ms_to_datetime(gen.checkpoint_ms) or datetime.now()
    scd2_rows = enrolments_as_of(lake, as_of)
    scd2_diff = diff_rows(scd2_rows, gen.checkpoint, lake.kinds("enrolments"))
    reconciliation = reconcile(lake, gen.db.tables)

    # 4. maintenance on every table
    all_tables = [name for source in lake.specs for name in lake.related_tables(source)]
    before_compaction = [file_stats(lake.table(n)) for n in all_tables]
    for name in all_tables:
        compact(lake.table(name))
    after_compaction = [file_stats(lake.table(n)) for n in all_tables]
    for name in all_tables:
        expire_snapshots(lake.table(name), retain_last=cfg.retain_last)
    after_expiry = [file_stats(lake.table(n)) for n in all_tables]
    rows_after_maintenance = {n: current_state(lake, n).num_rows for n in TABLE_ORDER}

    stream_events = sum(b["events"] for b in stream_batches)
    stream_seconds = sum(b["seconds"] for b in stream_batches)
    snap_events = sum(b["events"] for b in snap_batches)
    snap_seconds = sum(b["seconds"] for b in snap_batches)
    quarantine = lake.table("quarantine").scan().to_arrow().to_pylist()
    reasons: dict[str, int] = {}
    for row in quarantine:
        label = row["reason"].split(",")[0].split(" got ")[0]
        reasons[label] = reasons.get(label, 0) + 1

    metrics: dict[str, Any] = {
        "data": "synthetic",
        "config": asdict(cfg),
        "source_ops": cfg.steps,
        "records": {
            "snapshot": len(snapshot_records),
            "change_records": len(change_records),
            "tombstones": sum(r["value"] is None for r in change_records),
            "delivered_stream": len(arrivals),
            "redelivered": len(arrivals) - len(change_records),
        },
        "throughput": {
            "snapshot_events_per_s": round(snap_events / snap_seconds, 1) if snap_seconds else 0,
            "stream_events_per_s": round(stream_events / stream_seconds, 1)
            if stream_seconds
            else 0,
            "stream_apply_seconds": round(stream_seconds, 2),
            "batches": len(snap_batches) + len(stream_batches),
        },
        "freshness_s": {
            "rate_events_per_s": cfg.rate,
            "p50": round(percentile(freshness, 0.5), 3),
            "p95": round(percentile(freshness, 0.95), 3),
            "max": round(max(freshness), 3) if freshness else 0.0,
        },
        "columns_added": sorted({c for b in stream_batches for c in b["columns_added"]}),
        "quarantine": {"rows": len(quarantine), "reasons": reasons},
        "replay_last_two_batches": {
            "events": replay.events,
            "rows_written": replay.rows_written,
            "versions_added": sum(t.versions_added for t in replay.tables.values()),
        },
        "time_travel_to_initial_load": time_travel,
        "scd2_as_of_checkpoint": {
            "as_of": as_of.isoformat(),
            "rows": scd2_rows.num_rows,
            "source_rows": len(gen.checkpoint),
            "differing": sum(len(v) for v in scd2_diff.values()),
        },
        "reconciliation": reconciliation,
        "maintenance": {
            "tables": len(all_tables),
            "retain_last": cfg.retain_last,
            "before": _sum_stats(before_compaction),
            "after_compaction": _sum_stats(after_compaction),
            "after_expiry": _sum_stats(after_expiry),
            "rows_after_maintenance": rows_after_maintenance,
        },
        "batches": snap_batches + stream_batches,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str))
    return metrics
