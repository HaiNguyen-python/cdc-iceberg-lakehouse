"""Parse Debezium Postgres change events (JSON converter, `schemas.enable=false`).

Input is one Kafka record as a dict: `{"topic", "partition", "offset", "key", "value"}`. `key`
and `value` may be JSON objects or JSON-encoded strings (what `rpk topic consume` prints). A
record whose value is null or empty is a **tombstone**: Debezium writes one after every delete so
that Kafka log compaction can drop the key.

Only the envelope is interpreted here; column values are typed later (`coerce.py`), because the
right type depends on the lake table's current schema, which can change.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

Op = Literal["c", "u", "d", "r", "tombstone"]
DATA_OPS = frozenset({"c", "u", "d", "r"})


class EnvelopeError(ValueError):
    """The record is not a change event this consumer can apply; the reason is user-facing."""


@dataclass(frozen=True)
class ChangeEvent:
    """One row change. `row` is the state the change leaves behind (`before` for a delete)."""

    table: str
    op: Op
    key: dict[str, Any]
    row: dict[str, Any]
    lsn: int | None
    ts_ms: int | None
    snapshot: bool
    topic: str
    partition: int
    offset: int
    before: dict[str, Any] | None = field(default=None, compare=False)

    @property
    def is_tombstone(self) -> bool:
        return self.op == "tombstone"

    @property
    def is_delete(self) -> bool:
        return self.op == "d"


def _maybe_json(value: Any) -> Any:
    """Accept both decoded JSON and the JSON text that CLI consumers print."""
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        if value == "":
            return None
        return json.loads(value)
    return value


def table_from_topic(topic: str) -> str:
    """Debezium topic names are `<topic.prefix>.<schema>.<table>`."""
    parts = topic.split(".")
    if len(parts) < 3:
        raise EnvelopeError(f"topic '{topic}' is not <prefix>.<schema>.<table>")
    return parts[-1]


def parse_record(record: dict[str, Any]) -> ChangeEvent:
    """Turn one Kafka record into a `ChangeEvent`, or raise `EnvelopeError` with a reason."""
    topic = str(record.get("topic", ""))
    partition = int(record.get("partition", 0))
    offset = int(record.get("offset", -1))
    try:
        key = _maybe_json(record.get("key"))
        value = _maybe_json(record.get("value"))
    except json.JSONDecodeError as exc:
        raise EnvelopeError(f"invalid JSON: {exc.msg}") from exc

    if value is None:
        if not isinstance(key, dict) or not key:
            raise EnvelopeError("tombstone without a key")
        return ChangeEvent(
            table=table_from_topic(topic),
            op="tombstone",
            key=key,
            row={},
            lsn=None,
            ts_ms=None,
            snapshot=False,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    if not isinstance(value, dict):
        raise EnvelopeError("value is not a JSON object")
    if "schema" in value and "payload" in value:
        raise EnvelopeError("schema+payload envelope: set value.converter.schemas.enable=false")
    op = value.get("op")
    if op not in DATA_OPS:
        raise EnvelopeError(f"unsupported op '{op}'")
    source = value.get("source")
    if not isinstance(source, dict):
        raise EnvelopeError("missing source block")
    lsn = source.get("lsn")
    if not isinstance(lsn, int) or isinstance(lsn, bool):
        raise EnvelopeError("missing source.lsn")

    before = value.get("before")
    after = value.get("after")
    row = before if op == "d" else after
    if not isinstance(row, dict):
        side = "before" if op == "d" else "after"
        raise EnvelopeError(f"op '{op}' without '{side}'")
    if not isinstance(key, dict) or not key:
        # A missing key is unusual but recoverable: the primary key is also in the row.
        key = {}

    snapshot_flag = str(source.get("snapshot", "false")).lower()
    return ChangeEvent(
        table=str(source.get("table") or table_from_topic(topic)),
        op=op,
        key=key,
        row=row,
        lsn=lsn,
        ts_ms=source.get("ts_ms") if isinstance(source.get("ts_ms"), int) else None,
        snapshot=snapshot_flag not in ("false", "", "none"),
        topic=topic,
        partition=partition,
        offset=offset,
        before=before if isinstance(before, dict) else None,
    )


def read_jsonl(path: str) -> list[dict[str, Any]]:
    """Read Kafka records stored one JSON object per line (blank lines ignored)."""
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]
