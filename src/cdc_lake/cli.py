"""`cdc-lake` command line."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from .config import LakeConfig, open_catalog
from .demo import DemoConfig, run_demo
from .events import read_jsonl
from .lake import Lake
from .loadgen import DebeziumEmitter, Generator, GeneratorConfig, deliver


def _lake(args: argparse.Namespace) -> Lake:
    return Lake(open_catalog(LakeConfig.from_env(args.lake)))


def cmd_demo(args: argparse.Namespace) -> int:
    cfg = DemoConfig(
        seed=args.seed,
        users=args.users,
        courses=args.courses,
        steps=args.steps,
        batch_size=args.batch_size,
        rate=args.rate,
        shuffle_window=args.shuffle_window,
        redeliver_count=args.redeliver,
        compact_every=args.compact_every,
        retain_last=args.retain_last,
    )
    out = Path(args.out)
    if (out / "lake").exists():
        print(f"{out / 'lake'} exists; remove it first for a clean run", file=sys.stderr)
        return 2
    metrics = run_demo(out, cfg)
    summary = {k: metrics[k] for k in ("records", "throughput", "freshness_s", "quarantine")}
    print(json.dumps(summary, indent=2))
    unexplained = sum(len(r["unexplained"]) for r in metrics["reconciliation"].values())
    return 1 if unexplained else 0


def cmd_report(args: argparse.Namespace) -> int:
    from .report import write_report

    print(write_report(Path(args.metrics), Path(args.docs)))
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    """Write an emulated Debezium stream to JSONL, to inspect or to feed `apply`."""
    gen = Generator(GeneratorConfig(seed=args.seed, users=args.users, steps=args.steps))
    emitter = DebeziumEmitter(seed=args.seed)
    records = emitter.snapshot(gen.initial()) + emitter.stream(gen.changes())
    records = deliver(records, seed=args.seed)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")
    print(f"{len(records)} records -> {args.out}")
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    lake = _lake(args)
    for path in args.files:
        records = read_jsonl(path)
        for n, start in enumerate(range(0, len(records), args.batch_size)):
            result = lake.apply(records[start : start + args.batch_size], batch_id=n)
            print(
                f"{path} batch {n}: {result.events} events, {result.rows_written} rows written, "
                f"{result.quarantined} quarantined, columns added {result.columns_added or '-'}"
            )
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    from .queries import sql

    print(sql(_lake(args), args.sql).to_pandas().to_string(index=False))
    return 0


def cmd_as_of(args: argparse.Namespace) -> int:
    from .queries import enrolments_as_of, state_at_snapshot, state_at_time

    lake = _lake(args)
    if args.snapshot_id is not None:
        table = state_at_snapshot(lake, args.table, args.snapshot_id)
    elif args.business_time:
        table = enrolments_as_of(lake, datetime.fromisoformat(args.business_time))
    else:
        table = state_at_time(lake, args.table, datetime.fromisoformat(args.time))
    print(table.to_pandas().to_string(index=False))
    return 0


def cmd_snapshots(args: argparse.Namespace) -> int:
    for snap in _lake(args).table(args.table).snapshots():
        summary = snap.summary.additional_properties if snap.summary else {}
        print(snap.snapshot_id, datetime.fromtimestamp(snap.timestamp_ms / 1000), summary)
    return 0


def cmd_maintain(args: argparse.Namespace) -> int:
    from .maintenance import compact, expire_snapshots

    lake = _lake(args)
    for source in lake.specs:
        for name in lake.related_tables(source):
            before, _ = compact(lake.table(name))
            _, after = expire_snapshots(lake.table(name), retain_last=args.retain_last)
            print(f"{name}: {before.as_dict()} -> {after.as_dict()}")
    return 0


def cmd_consume(args: argparse.Namespace) -> int:
    from .live import consume

    consume(args.bootstrap, args.group, args.batch_size, args.idle_exit)
    return 0


def cmd_loadgen(args: argparse.Namespace) -> int:
    from .live import pg_loadgen

    pg_loadgen(args.dsn, args.seed, args.users, args.courses, args.steps, args.phase)
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    from .live import smoke

    return 0 if smoke(args.dsn, args.timeout) else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cdc-lake", description=__doc__)
    p.add_argument("--lake", default="data/lake", help="local lake folder (catalog + warehouse)")
    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser("demo", help="offline end-to-end run on the emulated Debezium stream")
    d.add_argument("--out", default="data/demo")
    defaults = DemoConfig()
    for flag, value in (
        ("--seed", defaults.seed),
        ("--users", defaults.users),
        ("--courses", defaults.courses),
        ("--steps", defaults.steps),
        ("--batch-size", defaults.batch_size),
        ("--shuffle-window", defaults.shuffle_window),
        ("--redeliver", defaults.redeliver_count),
        ("--compact-every", defaults.compact_every),
        ("--retain-last", defaults.retain_last),
    ):
        d.add_argument(flag, type=int, default=value)
    d.add_argument("--rate", type=float, default=defaults.rate, help="events/s, 0 = unpaced")
    d.set_defaults(func=cmd_demo)

    r = sub.add_parser("report", help="docs/results.md and charts from metrics.json")
    r.add_argument("--metrics", default="data/demo/metrics.json")
    r.add_argument("--docs", default="docs")
    r.set_defaults(func=cmd_report)

    g = sub.add_parser("generate", help="write an emulated Debezium stream as JSONL")
    g.add_argument("--out", default="data/stream.jsonl")
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--users", type=int, default=50)
    g.add_argument("--steps", type=int, default=500)
    g.set_defaults(func=cmd_generate)

    a = sub.add_parser("apply", help="apply Kafka records from JSONL files")
    a.add_argument("files", nargs="+")
    a.add_argument("--batch-size", type=int, default=500)
    a.set_defaults(func=cmd_apply)

    q = sub.add_parser("query", help="DuckDB SQL over the current state")
    q.add_argument("sql")
    q.set_defaults(func=cmd_query)

    t = sub.add_parser("as-of", help="time travel by snapshot id / wall time, or SCD2 time")
    t.add_argument("--table", default="enrolments")
    group = t.add_mutually_exclusive_group(required=True)
    group.add_argument("--snapshot-id", type=int)
    group.add_argument("--time", help="ISO time with offset; Iceberg commit time")
    group.add_argument("--business-time", help="ISO time with offset; enrolments SCD2 history")
    t.set_defaults(func=cmd_as_of)

    s = sub.add_parser("snapshots", help="list a table's Iceberg snapshots")
    s.add_argument("table")
    s.set_defaults(func=cmd_snapshots)

    m = sub.add_parser("maintain", help="compact every table, expire snapshots, drop orphans")
    m.add_argument("--retain-last", type=int, default=3)
    m.set_defaults(func=cmd_maintain)

    c = sub.add_parser("consume", help="Kafka -> Iceberg consumer (Docker stack)")
    c.add_argument("--bootstrap", default="redpanda:9092")
    c.add_argument("--group", default="cdc-lake")
    c.add_argument("--batch-size", type=int, default=500)
    c.add_argument("--idle-exit", type=float, default=0.0, help="stop after N idle seconds")
    c.set_defaults(func=cmd_consume)

    lg = sub.add_parser("loadgen", help="run the load generator against Postgres")
    lg.add_argument("--dsn", required=True)
    lg.add_argument("--phase", choices=("seed", "changes"), required=True)
    lg.add_argument("--seed", type=int, default=7)
    lg.add_argument("--users", type=int, default=100)
    lg.add_argument("--courses", type=int, default=12)
    lg.add_argument("--steps", type=int, default=1000)
    lg.set_defaults(func=cmd_loadgen)

    sm = sub.add_parser("smoke", help="wait until the lake matches Postgres (Docker stack)")
    sm.add_argument("--dsn", required=True)
    sm.add_argument("--timeout", type=float, default=180.0)
    sm.set_defaults(func=cmd_smoke)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
