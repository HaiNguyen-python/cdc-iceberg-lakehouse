"""Envelope parsing and value typing against fixtures in real Debezium Postgres JSON shape."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from conftest import records

from cdc_lake.coerce import TypeChange, coerce_value, new_columns
from cdc_lake.events import EnvelopeError, parse_record


def test_parses_snapshot_create_update_delete():
    events = [parse_record(r) for r in records("users")]
    assert [e.op for e in events] == ["r", "r", "c", "u", "d", "tombstone"]
    snap, _, create, update, delete, _ = events
    assert snap.snapshot and not create.snapshot
    assert snap.table == "users" and snap.lsn == 24023928 and snap.ts_ms == 1788249600000
    assert create.row["email"] == "sara@example.com"
    assert update.row["email"] == "aino.virtanen@example.com"
    assert update.before is not None and update.before["email"] == "aino@example.com"
    # A delete's row image is `before` (REPLICA IDENTITY FULL sends the whole old row).
    assert delete.is_delete and delete.row["full_name"] == "Minh Tran"


def test_tombstone_has_key_and_table_but_no_lsn():
    tombstone = parse_record(records("users")[-1])
    assert tombstone.is_tombstone
    assert tombstone.key == {"id": 2} and tombstone.table == "users"
    assert tombstone.lsn is None and tombstone.row == {}


def test_accepts_string_encoded_records_like_rpk_output():
    raw = records("enrolments")[1]
    encoded = {**raw, "key": json.dumps(raw["key"]), "value": json.dumps(raw["value"])}
    assert parse_record(encoded) == parse_record(raw)
    empty = parse_record({**raw, "value": ""})
    assert empty.is_tombstone and empty.key == {"id": 11}


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda v: {**v, "op": "t"}, "unsupported op 't'"),
        (lambda v: {"schema": {}, "payload": v}, "schemas.enable=false"),
        (lambda v: {**v, "source": {**v["source"], "lsn": None}}, "missing source.lsn"),
        (lambda v: {**v, "after": None}, "without 'after'"),
    ],
)
def test_rejects_records_it_cannot_apply(mutate, reason):
    raw = records("users")[2]
    with pytest.raises(EnvelopeError, match=reason.replace("(", r"\(")):
        parse_record({**raw, "value": mutate(raw["value"])})


def test_value_typing_widens_but_refuses_type_changes():
    assert coerce_value("price", "double", 3) == 3.0
    assert coerce_value("at", "timestamptz", "2026-09-01T08:00:00.5Z") == datetime(
        2026, 9, 1, 8, 0, 0, 500000, tzinfo=UTC
    )
    assert coerce_value("x", "long", None) is None
    for kind, value in (("long", "49.00"), ("long", 4.5), ("long", True), ("timestamptz", 17)):
        with pytest.raises(TypeChange, match="type change"):
            coerce_value("col", kind, value)
    # A new column with only nulls has no type yet, so it is not proposed.
    assert new_columns({"id": 1, "level": None, "tag": "x"}, {"id": "long"}) == {"tag": "string"}
