"""The operational schema of the course platform, as the lake first sees it.

This is the starting contract only. Once a table exists in Iceberg, the Iceberg schema is the
source of truth (new columns are added there by schema evolution), so the applier always reads
column types back from the table instead of from this module.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

# Column kinds the lake understands. They are the JSON-visible types of the Debezium Postgres
# connector with `decimal.handling.mode=string` and `time.precision.mode=adaptive`:
# integers arrive as JSON numbers, timestamptz as ISO-8601 strings.
KINDS = ("long", "double", "string", "boolean", "timestamptz")

ARROW_TYPES: dict[str, pa.DataType] = {
    "long": pa.int64(),
    "double": pa.float64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
    "timestamptz": pa.timestamp("us", tz="UTC"),
}

POSTGRES_TYPES = {
    "long": "bigint",
    "double": "double precision",
    "string": "text",
    "boolean": "boolean",
    "timestamptz": "timestamptz",
}

# Metadata columns the applier adds to every lake table. They are what makes the merge
# order-independent: `_lsn` decides which version wins, `_deleted` keeps a tombstone row so a
# late, older update cannot bring a deleted row back.
META_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("_lsn", pa.int64()),
    ("_op", pa.string()),
    ("_ts_ms", pa.int64()),
    ("_deleted", pa.bool_()),
)


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: str
    columns: tuple[tuple[str, str], ...]

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def kinds(self) -> dict[str, str]:
        return dict(self.columns)


SOURCE_TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        TableSpec(
            "users",
            "id",
            (
                ("id", "long"),
                ("email", "string"),
                ("full_name", "string"),
                ("country", "string"),
                ("created_at", "timestamptz"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "courses",
            "id",
            (
                ("id", "long"),
                ("title", "string"),
                ("category", "string"),
                ("price_cents", "long"),
                ("published", "boolean"),
                ("created_at", "timestamptz"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "enrolments",
            "id",
            (
                ("id", "long"),
                ("user_id", "long"),
                ("course_id", "long"),
                ("status", "string"),
                ("progress_pct", "long"),
                ("enrolled_at", "timestamptz"),
                ("updated_at", "timestamptz"),
            ),
        ),
        TableSpec(
            "payments",
            "id",
            (
                ("id", "long"),
                ("enrolment_id", "long"),
                ("amount_cents", "long"),
                ("currency", "string"),
                ("status", "string"),
                ("created_at", "timestamptz"),
                ("updated_at", "timestamptz"),
            ),
        ),
    )
}

# Parents before children: the order the load generator creates and deletes rows in.
TABLE_ORDER = ("users", "courses", "enrolments", "payments")


def arrow_schema(spec: TableSpec, with_meta: bool = True) -> pa.Schema:
    """Arrow schema for a lake table: source columns (PK required) plus metadata columns."""
    fields = [
        pa.field(name, ARROW_TYPES[kind], nullable=name != spec.primary_key)
        for name, kind in spec.columns
    ]
    if with_meta:
        fields += [pa.field(name, dtype) for name, dtype in META_FIELDS]
    return pa.schema(fields)


def postgres_ddl(spec: TableSpec) -> str:
    """CREATE TABLE for the source database; `docker/postgres/init.sql` must match it."""
    cols = [
        f"    {name} {POSTGRES_TYPES[kind]}" + (" PRIMARY KEY" if name == spec.primary_key else "")
        for name, kind in spec.columns
    ]
    return f"CREATE TABLE {spec.name} (\n" + ",\n".join(cols) + "\n);"


def init_sql() -> str:
    """Init script for the Postgres container; a test keeps docker/postgres/init.sql in sync.

    REPLICA IDENTITY FULL makes Debezium send the whole old row in `before` for updates and
    deletes, so a delete in the lake keeps the row's last values instead of only its key.
    """
    parts = [postgres_ddl(SOURCE_TABLES[t]) for t in TABLE_ORDER]
    parts += [f"ALTER TABLE {t} REPLICA IDENTITY FULL;" for t in TABLE_ORDER]
    return "\n\n".join(parts) + "\n"
