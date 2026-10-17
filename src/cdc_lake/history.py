"""SCD type 2 history built from the changelog.

Every version of a row in the changelog becomes one history row, valid from its commit time
until the next version of the same key (or a delete). Because the history is rebuilt from the
full, LSN-sorted changelog of each touched key, a version that arrives late still lands in the
right place, and replaying a batch produces the same rows.

Times come from `source.ts_ms` (the commit time in Postgres). For snapshot rows (`op = r`) that
is the time the snapshot read the row, not when the row was first written: history before CDC
was switched on is unknown, and the table says so instead of guessing.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

HISTORY_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("valid_from", pa.timestamp("us", tz="UTC")),
    ("valid_to", pa.timestamp("us", tz="UTC")),
    ("valid_from_lsn", pa.int64()),
    ("valid_to_lsn", pa.int64()),
    ("is_current", pa.bool_()),
    ("change_op", pa.string()),
)


def ms_to_datetime(ms: int | None) -> datetime | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=UTC)


def scd2_rows(
    versions: list[dict[str, Any]], pk_column: str, source_columns: list[str]
) -> list[dict[str, Any]]:
    """History rows for every key in `versions` (changelog rows with `_lsn`, `_op`, `_ts_ms`).

    A delete closes the previous version and adds no row of its own, so a deleted key has no
    current row.
    """
    by_key: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for version in versions:
        by_key[version[pk_column]].append(version)

    rows: list[dict[str, Any]] = []
    for pk in sorted(by_key):
        ordered = sorted(by_key[pk], key=lambda v: v["_lsn"])
        for i, version in enumerate(ordered):
            if version["_op"] == "d":
                continue
            nxt = ordered[i + 1] if i + 1 < len(ordered) else None
            rows.append(
                {
                    **{c: version.get(c) for c in source_columns},
                    "valid_from": ms_to_datetime(version["_ts_ms"]),
                    "valid_to": ms_to_datetime(nxt["_ts_ms"]) if nxt else None,
                    "valid_from_lsn": version["_lsn"],
                    "valid_to_lsn": nxt["_lsn"] if nxt else None,
                    "is_current": nxt is None,
                    "change_op": version["_op"],
                }
            )
    return rows
