"""Type JSON column values against the lake schema, and detect schema drift.

The JSON converter without schemas sends bare values, so the lake has to decide what a value
means. Two kinds of drift are told apart on purpose:

* a **new column** (a key the table does not have yet) is additive and safe: its type is inferred
  from the first non-null value and the column is added to the Iceberg schema;
* a **type change** (a known column whose value no longer fits) is not safe to apply silently:
  writing it would either fail or corrupt the column, so the event goes to quarantine with a
  reason a person can act on.

Widening that Iceberg allows without a rewrite (int -> double) is accepted.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


class TypeChange(ValueError):
    """A value does not fit its column's type. The message is the quarantine reason."""


def json_kind(value: Any) -> str | None:
    """Lake kind for a JSON value, or None for null (a null tells us nothing about the type)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    return "json"


def parse_timestamptz(value: str) -> datetime:
    """Debezium `ZonedTimestamp`: ISO-8601 with offset, usually `Z`."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp without offset")
    return parsed.astimezone(UTC)


def coerce_value(column: str, kind: str, value: Any) -> Any:
    """Return the value as the Python type for `kind`, or raise `TypeChange`."""
    if value is None:
        return None
    got = json_kind(value)
    if kind == "long" and got == "long":
        return value
    if kind == "double" and got in ("long", "double"):
        return float(value)
    if kind in ("string", "boolean") and got == kind:
        return value
    if kind == "timestamptz" and got == "string":
        try:
            return parse_timestamptz(value)
        except ValueError as exc:
            raise TypeChange(f"type change: {column} expected timestamptz, got '{value}'") from exc
    shown = repr(value) if len(repr(value)) <= 40 else repr(value)[:37] + "..."
    raise TypeChange(f"type change: {column} expected {kind}, got {got} {shown}")


def coerce_row(row: dict[str, Any], kinds: dict[str, str]) -> dict[str, Any]:
    """Type every known column of `row`. Unknown columns are ignored here (see `new_columns`)."""
    return {
        column: coerce_value(column, kind, row.get(column))
        for column, kind in kinds.items()
        if column in row
    }


def new_columns(row: dict[str, Any], kinds: dict[str, str]) -> dict[str, str]:
    """Columns in `row` the table does not have yet, with their inferred kind.

    Columns whose value is null are skipped: their type is unknown, and adding them later loses
    nothing because a missing column reads as null anyway.
    """
    found: dict[str, str] = {}
    for column, value in row.items():
        if column in kinds:
            continue
        kind = json_kind(value)
        if kind is not None:
            found[column] = kind
    return found
