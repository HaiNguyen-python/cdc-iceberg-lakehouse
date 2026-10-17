"""Apply batches of Debezium records to Iceberg tables.

Per source table the lake keeps:

* `lake.<table>` - current state, one row per primary key, deleted keys kept as tombstone rows
  (`_deleted = true`); readers filter them out (`queries.current_state`);
* `lake.<table>_changelog` - every distinct version (key, LSN) ever seen, append-only;
* `lake.enrolments_history` - SCD2 history rebuilt from the changelog for touched keys;
* `lake.quarantine` - records that could not be applied, with a reason.

PyIceberg can delete rows by filter and append in one transaction (copy-on-write), but its
`upsert` has no notion of "keep the highest LSN" or of deletes, so the merge decision is made in
Python (`merge.py`) and only the result is written: delete the affected keys, append their new
rows, one atomic commit per table. See docs/decisions.md (ADR 2).
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, cast

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import BooleanExpression, In
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IcebergType,
    LongType,
    StringType,
    TimestamptzType,
)

from .coerce import TypeChange, coerce_row, coerce_value, new_columns
from .config import NAMESPACE
from .events import ChangeEvent, EnvelopeError, parse_record
from .history import HISTORY_FIELDS, scd2_rows
from .merge import Change, Stored, new_versions, plan_state, state_row
from .sources import ARROW_TYPES, KINDS, SOURCE_TABLES, TableSpec, arrow_schema

HISTORY_TABLES = ("enrolments",)
ICEBERG_TYPES: dict[str, IcebergType] = {
    "long": LongType(),
    "double": DoubleType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "timestamptz": TimestamptzType(),
}
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("record_id", pa.string(), nullable=False),
        pa.field("table_name", pa.string()),
        pa.field("op", pa.string()),
        pa.field("lsn", pa.int64()),
        pa.field("reason", pa.string()),
        pa.field("record", pa.string()),
    ]
)


def is_in(column: str, values: Iterable[Any]) -> BooleanExpression:
    """`column IN values`. PyIceberg's `In` builds itself in `__new__`, which its type hints
    do not describe, hence the cast."""
    return cast(BooleanExpression, cast(Any, In)(column, list(values)))


def kind_of(iceberg_type: IcebergType) -> str:
    for kind, candidate in ICEBERG_TYPES.items():
        if iceberg_type == candidate:
            return kind
    raise ValueError(f"unsupported Iceberg type {iceberg_type}")


def arrow_for(table: Table) -> pa.Schema:
    """Arrow schema matching the table's current Iceberg schema (after any evolution)."""
    extra = dict(HISTORY_FIELDS)
    fields = []
    for f in table.schema().fields:
        dtype = extra[f.name] if f.name in extra else ARROW_TYPES[kind_of(f.field_type)]
        fields.append(pa.field(f.name, dtype, nullable=not f.required))
    return pa.schema(fields)


def to_arrow(table: Table, rows: list[dict[str, Any]]) -> pa.Table:
    schema = arrow_for(table)
    return pa.Table.from_pylist(
        [{name: row.get(name) for name in schema.names} for row in rows], schema=schema
    )


@dataclass
class TableStats:
    events: int = 0
    rows_written: int = 0
    versions_added: int = 0
    # data changes not written to the state table: an equal or higher LSN already won
    # (older versions in the same batch, out-of-order arrivals, redeliveries)
    superseded: int = 0
    tombstones: int = 0
    quarantined: int = 0


@dataclass
class ApplyResult:
    events: int = 0
    quarantined: int = 0
    columns_added: list[str] = field(default_factory=list)
    tables: dict[str, TableStats] = field(default_factory=dict)
    max_lsn: int | None = None
    seconds: float = 0.0
    committed_at: float = 0.0

    @property
    def rows_written(self) -> int:
        return sum(t.rows_written for t in self.tables.values())


@dataclass(frozen=True)
class Rejected:
    record: dict[str, Any]
    table: str
    op: str
    lsn: int | None
    reason: str


def _record_id(record: dict[str, Any]) -> str:
    return f"{record.get('topic', '')}:{record.get('partition', 0)}:{record.get('offset', -1)}"


class Lake:
    """The Iceberg side of the pipeline: table creation, batch apply and schema evolution."""

    def __init__(self, catalog: Catalog, specs: dict[str, TableSpec] | None = None) -> None:
        self.catalog = catalog
        self.specs = specs or SOURCE_TABLES
        self.ensure_tables()

    # ---- tables -------------------------------------------------------------------------

    def ident(self, name: str) -> str:
        return f"{NAMESPACE}.{name}"

    def table(self, name: str) -> Table:
        return self.catalog.load_table(self.ident(name))

    def _create(self, name: str, schema: pa.Schema) -> None:
        try:
            self.catalog.load_table(self.ident(name))
        except NoSuchTableError:
            self.catalog.create_table(self.ident(name), schema=schema)

    def ensure_tables(self) -> None:
        for spec in self.specs.values():
            self._create(spec.name, arrow_schema(spec))
            self._create(f"{spec.name}_changelog", arrow_schema(spec))
            if spec.name in HISTORY_TABLES:
                base = arrow_schema(spec, with_meta=False)
                fields = list(base) + [pa.field(n, t) for n, t in HISTORY_FIELDS]
                self._create(f"{spec.name}_history", pa.schema(fields))
        self._create("quarantine", QUARANTINE_SCHEMA)

    def related_tables(self, name: str) -> list[str]:
        names = [name, f"{name}_changelog"]
        if name in HISTORY_TABLES:
            names.append(f"{name}_history")
        return names

    def kinds(self, name: str) -> dict[str, str]:
        """Source columns and their kinds, read from the live Iceberg schema."""
        return {
            f.name: kind_of(f.field_type)
            for f in self.table(name).schema().fields
            if not f.name.startswith("_")
        }

    def add_columns(self, name: str, columns: dict[str, str]) -> None:
        """Schema evolution: add nullable columns to state, changelog and history tables.

        Iceberg tracks columns by id, so this is a metadata-only commit: no data file is
        rewritten and old files simply read the new column as null.
        """
        for table_name in self.related_tables(name):
            table = self.table(table_name)
            existing = set(table.schema().column_names)
            todo = {c: k for c, k in columns.items() if c not in existing}
            if not todo:
                continue
            with table.update_schema() as update:
                for column, kind in sorted(todo.items()):
                    update.add_column(column, ICEBERG_TYPES[kind], doc="added by schema evolution")

    # ---- apply --------------------------------------------------------------------------

    def apply(self, records: Iterable[dict[str, Any]], batch_id: int = 0) -> ApplyResult:
        """Apply one micro-batch. Safe to call again with the same records (idempotent)."""
        started = time.perf_counter()
        result = ApplyResult()
        rejected: list[Rejected] = []
        by_table: dict[str, list[tuple[dict[str, Any], ChangeEvent]]] = defaultdict(list)

        for record in records:
            result.events += 1
            try:
                event = parse_record(record)
            except EnvelopeError as exc:
                topic = str(record.get("topic", ""))
                table = topic.rsplit(".", 1)[-1] if topic.count(".") >= 2 else ""
                rejected.append(Rejected(record, table, "", None, f"envelope: {exc}"))
                continue
            if event.table not in self.specs:
                rejected.append(Rejected(record, event.table, event.op, event.lsn, "unknown table"))
                continue
            by_table[event.table].append((record, event))

        for name in sorted(by_table):
            stats, table_rejected, added, max_lsn = self._apply_table(
                self.specs[name], by_table[name], batch_id
            )
            result.tables[name] = stats
            rejected += table_rejected
            result.columns_added += [f"{name}.{c}" for c in added]
            if max_lsn is not None:
                result.max_lsn = max(max_lsn, result.max_lsn or max_lsn)

        result.quarantined = len(rejected)
        self._quarantine(rejected)
        result.seconds = time.perf_counter() - started
        result.committed_at = time.time()
        return result

    def _typed_changes(
        self, spec: TableSpec, items: list[tuple[dict[str, Any], ChangeEvent]]
    ) -> tuple[list[Change], list[Rejected], list[str]]:
        """Validate and type events; evolve the schema for new columns first."""
        pk = spec.primary_key
        kinds = self.kinds(spec.name)
        rejected: list[Rejected] = []
        valid: list[tuple[dict[str, Any], ChangeEvent, int]] = []
        # LSN order makes "first value decides a new column's type" deterministic.
        items = sorted(items, key=lambda it: (it[1].lsn or -1, it[1].offset))
        for record, event in items:
            key_value = event.key.get(pk, event.row.get(pk))
            if not isinstance(key_value, int) or isinstance(key_value, bool):
                rejected.append(Rejected(record, spec.name, event.op, event.lsn, "missing key"))
                continue
            try:
                coerce_row(event.row, kinds)
            except TypeChange as exc:
                rejected.append(Rejected(record, spec.name, event.op, event.lsn, str(exc)))
                continue
            valid.append((record, event, key_value))

        additions: dict[str, str] = {}
        for _, event, _ in valid:
            for column, kind in new_columns(event.row, kinds).items():
                additions.setdefault(column, kind)
        unsupported = {c for c, k in additions.items() if k not in KINDS}
        additions = {c: k for c, k in additions.items() if c not in unsupported}
        if additions:
            self.add_columns(spec.name, additions)
            kinds = self.kinds(spec.name)

        changes: list[Change] = []
        for record, event, key_value in valid:
            bad = sorted(unsupported & set(event.row))
            if bad and any(event.row[c] is not None for c in bad):
                reason = f"unsupported nested value in new column {bad[0]}"
                rejected.append(Rejected(record, spec.name, event.op, event.lsn, reason))
                continue
            try:
                row = coerce_row(event.row, kinds)
            except TypeChange as exc:
                rejected.append(Rejected(record, spec.name, event.op, event.lsn, str(exc)))
                continue
            row[pk] = coerce_value(pk, kinds[pk], key_value)
            changes.append(Change(key_value, event.op, event.lsn, event.ts_ms, row))
        return changes, rejected, sorted(additions)

    def _stored(self, table: Table, pk: str, keys: list[int], deleted: bool) -> pa.Table:
        fields = (pk, "_lsn", "_deleted") if deleted else (pk, "_lsn")
        return table.scan(row_filter=is_in(pk, keys), selected_fields=fields).to_arrow()

    def _apply_table(
        self, spec: TableSpec, items: list[tuple[dict[str, Any], ChangeEvent]], batch_id: int
    ) -> tuple[TableStats, list[Rejected], list[str], int | None]:
        pk = spec.primary_key
        stats = TableStats(events=len(items))
        changes, rejected, added = self._typed_changes(spec, items)
        stats.quarantined = len(rejected)
        stats.tombstones = sum(c.is_tombstone for c in changes)
        if not changes:
            return stats, rejected, added, None

        keys = sorted({c.pk for c in changes})
        lsns = [c.lsn for c in changes if c.lsn is not None]
        max_lsn = max(lsns) if lsns else None
        props = {"cdc.batch": str(batch_id), "cdc.max-lsn": str(max_lsn or "")}

        # 1. current state
        state = self.table(spec.name)
        found = self._stored(state, pk, keys, deleted=True).to_pylist()
        stored = {r[pk]: Stored(r["_lsn"], bool(r["_deleted"])) for r in found}
        planned = plan_state(changes, stored, pk)
        data_changes = sum(not c.is_tombstone for c in changes)
        stats.superseded = data_changes - sum(
            1 for row in planned.values() if row["_op"] != "tombstone"
        )
        if planned:
            replace = sorted(k for k in planned if k in stored)
            with state.transaction() as tx:
                if replace:
                    tx.delete(is_in(pk, replace), snapshot_properties=props)
                tx.append(to_arrow(state, list(planned.values())), snapshot_properties=props)
            stats.rows_written = len(planned)

        # 2. changelog (append only versions it has not seen)
        changelog = self.table(f"{spec.name}_changelog")
        known: dict[int, set[int]] = defaultdict(set)
        for r in self._stored(changelog, pk, keys, deleted=False).to_pylist():
            known[r[pk]].add(r["_lsn"])
        versions = new_versions(changes, known)
        if versions:
            changelog.append(
                to_arrow(changelog, [state_row(v) for v in versions]), snapshot_properties=props
            )
            stats.versions_added = len(versions)

        # 3. SCD2 history for the keys that got new versions
        if spec.name in HISTORY_TABLES and versions:
            self._rebuild_history(spec, sorted({v.pk for v in versions}), props)
        return stats, rejected, added, max_lsn

    def _rebuild_history(self, spec: TableSpec, keys: list[int], props: dict[str, str]) -> None:
        pk = spec.primary_key
        changelog = self.table(f"{spec.name}_changelog")
        versions = changelog.scan(row_filter=is_in(pk, keys)).to_arrow().to_pylist()
        history = self.table(f"{spec.name}_history")
        columns = list(self.kinds(spec.name))
        rows = scd2_rows(versions, pk, columns)
        existing = history.scan(row_filter=is_in(pk, keys), selected_fields=(pk,)).to_arrow()
        with history.transaction() as tx:
            if existing.num_rows:
                tx.delete(is_in(pk, keys), snapshot_properties=props)
            tx.append(to_arrow(history, rows), snapshot_properties=props)

    def _quarantine(self, rejected: list[Rejected]) -> None:
        if not rejected:
            return
        table = self.table("quarantine")
        ids = sorted({_record_id(r.record) for r in rejected})
        seen = set(
            table.scan(row_filter=is_in("record_id", ids), selected_fields=("record_id",))
            .to_arrow()
            .column("record_id")
            .to_pylist()
        )
        rows: dict[str, dict[str, Any]] = {}
        for r in rejected:
            rid = _record_id(r.record)
            if rid in seen or rid in rows:
                continue
            rows[rid] = {
                "record_id": rid,
                "table_name": r.table,
                "op": r.op,
                "lsn": r.lsn,
                "reason": r.reason,
                "record": json.dumps(r.record, sort_keys=True, default=str),
            }
        if rows:
            table.append(pa.Table.from_pylist(list(rows.values()), schema=QUARANTINE_SCHEMA))
