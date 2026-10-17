"""Pure merge rules: which version of a row wins, and what to write.

The rules only look at the Postgres LSN, never at arrival order, so the result is the same
whether events come in order, shuffled, or twice:

* per primary key, the change with the **highest LSN** wins (ties are exact duplicates; a
  deterministic tie-break keeps the result stable even if they were not);
* a stored row is only replaced by a change with a **strictly higher** LSN, so replaying old
  events is a no-op;
* a delete is stored as a row with `_deleted = true` (a soft tombstone), so an older update that
  arrives after the delete cannot resurrect the row;
* a Kafka tombstone (null value, no LSN) deletes the key at the highest LSN known for it. In a
  real topic it always follows the delete event for the same key, which already did the work.

For the same row, LSN order is commit order in Postgres: a second writer must wait for the
first one's row lock, so its change is written to the WAL after the first one commits.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

OP_RANK = {"r": 0, "c": 1, "u": 2, "d": 3}


@dataclass(frozen=True)
class Change:
    """A typed change for one primary key, ready to merge."""

    pk: int
    op: str
    lsn: int | None
    ts_ms: int | None
    row: dict[str, Any]

    @property
    def is_tombstone(self) -> bool:
        return self.op == "tombstone"


@dataclass(frozen=True)
class Stored:
    """What the merge needs to know about the row currently in the lake."""

    lsn: int
    deleted: bool


def _canonical(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, default=str)


def order_key(change: Change) -> tuple[int, int, str]:
    """Total order over versions of one key: LSN first, then a stable tie-break."""
    return (change.lsn or 0, OP_RANK.get(change.op, 9), _canonical(change.row))


def latest_per_key(changes: Iterable[Change]) -> dict[int, Change]:
    """Highest-LSN data change per key (tombstones are handled separately)."""
    best: dict[int, Change] = {}
    for change in changes:
        if change.is_tombstone:
            continue
        current = best.get(change.pk)
        if current is None or order_key(change) > order_key(current):
            best[change.pk] = change
    return best


def state_row(change: Change) -> dict[str, Any]:
    return {
        **change.row,
        "_lsn": change.lsn,
        "_op": change.op,
        "_ts_ms": change.ts_ms,
        "_deleted": change.op == "d",
    }


def plan_state(
    changes: list[Change], stored: dict[int, Stored], pk_column: str
) -> dict[int, dict[str, Any]]:
    """Rows to (re)write in the current-state table, keyed by primary key.

    Keys that are absent from the result are left untouched: either the batch only had stale
    or duplicate versions for them, or a tombstone for a key that is already deleted or unknown.
    """
    winners = latest_per_key(changes)
    tombstoned = {c.pk for c in changes if c.is_tombstone}
    out: dict[int, dict[str, Any]] = {}
    for pk in sorted(set(winners) | tombstoned):
        old = stored.get(pk)
        lsn = old.lsn if old else None
        deleted = old.deleted if old else None
        winner = winners.get(pk)
        if winner is not None and winner.lsn is not None and (lsn is None or winner.lsn > lsn):
            out[pk] = state_row(winner)
            lsn, deleted = winner.lsn, winner.op == "d"
        if pk in tombstoned and lsn is not None and not deleted:
            out[pk] = {
                pk_column: pk,
                "_lsn": lsn,
                "_op": "tombstone",
                "_ts_ms": None,
                "_deleted": True,
            }
    return out


def new_versions(changes: list[Change], known: dict[int, set[int]]) -> list[Change]:
    """Changelog rows to append: one per (key, LSN) that the changelog does not have yet.

    Sorted by key and LSN so the appended file is the same whatever the arrival order.
    """
    seen: dict[tuple[int, int], Change] = {}
    for change in changes:
        if change.is_tombstone or change.lsn is None:
            continue
        if change.lsn in known.get(change.pk, set()):
            continue
        ident = (change.pk, change.lsn)
        current = seen.get(ident)
        if current is None or order_key(change) > order_key(current):
            seen[ident] = change
    return [seen[k] for k in sorted(seen)]
