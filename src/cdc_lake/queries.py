"""Read the lake: current state, time travel, SCD2 "as of" and ad-hoc DuckDB SQL.

Two different clocks, on purpose:

* **Iceberg snapshots** answer "what did the lake contain after commit X / at wall-clock time
  T?" (`state_at_snapshot`, `state_at_time`). Good for debugging and reproducible reads; limited
  by snapshot expiry.
* **SCD2 history** answers "what was true in the source at business time T?"
  (`enrolments_as_of`), using Postgres commit times. It survives snapshot expiry.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from .lake import Lake


def _live_rows(table: pa.Table, pk: str) -> pa.Table:
    live = table.filter(pc.invert(pc.fill_null(table.column("_deleted"), False)))
    return live.sort_by(pk)


def current_state(lake: Lake, name: str, snapshot_id: int | None = None) -> pa.Table:
    """Rows that exist (tombstones removed), sorted by primary key."""
    pk = lake.specs[name].primary_key
    scan = lake.table(name).scan(snapshot_id=snapshot_id)
    return _live_rows(scan.to_arrow(), pk)


def state_at_snapshot(lake: Lake, name: str, snapshot_id: int) -> pa.Table:
    return current_state(lake, name, snapshot_id=snapshot_id)


def state_at_time(lake: Lake, name: str, when: datetime) -> pa.Table:
    """Lake state as of a wall-clock time (the latest snapshot committed at or before it)."""
    table = lake.table(name)
    snapshot = table.snapshot_as_of_timestamp(int(when.timestamp() * 1000))
    if snapshot is None:
        return current_state(lake, name).slice(0, 0)
    return current_state(lake, name, snapshot_id=snapshot.snapshot_id)


def current_snapshot_id(lake: Lake, name: str) -> int | None:
    snapshot = lake.table(name).current_snapshot()
    return snapshot.snapshot_id if snapshot else None


def enrolments_as_of(lake: Lake, when: datetime) -> pa.Table:
    """Enrolments as they were in the source at `when`, from the SCD2 history (DuckDB)."""
    history = lake.table("enrolments_history").scan().to_arrow()
    con = duckdb.connect()
    con.register("enrolments_history", history)
    columns = ", ".join(f'"{c}"' for c in lake.kinds("enrolments"))
    return con.execute(
        f"""
        SELECT {columns} FROM enrolments_history
        WHERE valid_from <= $when AND (valid_to IS NULL OR valid_to > $when)
        ORDER BY id
        """,
        {"when": when},
    ).to_arrow_table()


def sql(lake: Lake, query: str) -> pa.Table:
    """Run DuckDB SQL over the current state of every source table (tombstones removed)."""
    con = duckdb.connect()
    for name in lake.specs:
        con.register(name, current_state(lake, name))
    con.register("enrolments_history", lake.table("enrolments_history").scan().to_arrow())
    con.register("quarantine", lake.table("quarantine").scan().to_arrow())
    return con.execute(query).to_arrow_table()
