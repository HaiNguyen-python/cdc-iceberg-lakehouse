"""Schema evolution, quarantine, SCD2 history and time travel."""

from __future__ import annotations

from datetime import UTC, datetime

from conftest import records

from cdc_lake.lake import Lake
from cdc_lake.maintenance import file_stats
from cdc_lake.queries import (
    current_snapshot_id,
    current_state,
    enrolments_as_of,
    sql,
    state_at_snapshot,
    state_at_time,
)


def _live_paths(lake: Lake, name: str) -> set[str]:
    return set(lake.table(name).inspect.data_files().column("file_path").to_pylist())


def test_new_column_is_added_without_rewriting_existing_files(lake: Lake):
    snapshot, update, create = records("courses_add_column")
    lake.apply([snapshot])
    result = lake.apply([create])  # carries `level`, but only as null: type still unknown
    assert result.columns_added == []
    files_before = _live_paths(lake, "courses")

    result = lake.apply([update])  # `level` = "beginner": now the column is added
    assert result.columns_added == ["courses.level"]
    for name in ("courses", "courses_changelog"):
        assert lake.table(name).schema().find_field("level").required is False

    rows = {r["id"]: r for r in current_state(lake, "courses").to_pylist()}
    assert rows[100]["level"] == "beginner" and rows[100]["price_cents"] == 3900
    assert rows[101]["level"] is None
    # The file holding course 101 was written before the column existed and is still live:
    # schema evolution is a metadata change, not a rebuild.
    assert files_before & _live_paths(lake, "courses")


def test_type_change_goes_to_quarantine_with_a_reason(lake: Lake):
    result = lake.apply(records("payments_type_change"))
    assert result.quarantined == 3
    payment = current_state(lake, "payments").to_pylist()
    assert [(p["id"], p["amount_cents"], p["status"]) for p in payment] == [(500, 4900, "paid")]
    reasons = sql(lake, "SELECT record_id, reason FROM quarantine ORDER BY record_id").to_pylist()
    assert reasons == [
        {
            "record_id": "courseplatform.public.payments:0:1",
            "reason": "type change: amount_cents expected long, got string '49.00'",
        },
        {
            "record_id": "courseplatform.public.payments:0:2",
            "reason": "type change: amount_cents expected long, got string '29.00'",
        },
        {
            "record_id": "courseplatform.public.payments:0:3",
            "reason": "envelope: unsupported op 't'",
        },
    ]
    assert lake.table("payments").schema().find_field("amount_cents").field_type.__str__() == "long"


def test_scd2_history_chains_versions_and_closes_deleted_keys(lake: Lake):
    lake.apply(records("enrolments"))
    rows = lake.table("enrolments_history").scan().to_arrow()
    history = sorted(rows.to_pylist(), key=lambda r: (r["id"], r["valid_from_lsn"]))
    ten = [r for r in history if r["id"] == 10]
    assert [(r["status"], r["progress_pct"], r["is_current"]) for r in ten] == [
        ("active", 0, False),
        ("active", 40, False),
        ("completed", 100, True),
    ]
    for older, newer in zip(ten, ten[1:], strict=False):
        assert older["valid_to"] == newer["valid_from"]
        assert older["valid_to_lsn"] == newer["valid_from_lsn"]
    assert ten[-1]["valid_to"] is None

    eleven = [r for r in history if r["id"] == 11]
    assert len(eleven) == 1 and not eleven[0]["is_current"]
    assert eleven[0]["valid_to"] == datetime(2026, 9, 6, 7, 45, tzinfo=UTC)  # the delete


def test_scd2_history_is_the_same_when_events_arrive_backwards(lake: Lake, tmp_path):
    from conftest import make_lake

    stream = records("enrolments")
    lake.apply(stream)
    backwards = make_lake(tmp_path / "backwards")
    for record in reversed(stream):
        backwards.apply([record])

    def history(target: Lake) -> list[dict]:
        rows = target.table("enrolments_history").scan().to_arrow().to_pylist()
        return sorted(rows, key=lambda r: (r["id"], r["valid_from_lsn"]))

    assert history(backwards) == history(lake)


def test_time_travel_and_business_time_return_old_state(lake: Lake):
    stream = records("enrolments")
    lake.apply(stream[:2])
    first = current_snapshot_id(lake, "enrolments")
    after_first = datetime.now(UTC)
    lake.apply(stream[2:])

    assert first is not None
    then = state_at_snapshot(lake, "enrolments", first).to_pylist()
    assert [(r["id"], r["status"], r["progress_pct"]) for r in then] == [
        (10, "active", 0),
        (11, "active", 0),
    ]
    assert state_at_time(lake, "enrolments", after_first).num_rows == 2
    assert state_at_time(lake, "enrolments", datetime(2000, 1, 1, tzinfo=UTC)).num_rows == 0
    now = current_state(lake, "enrolments").to_pylist()
    assert [(r["id"], r["status"]) for r in now] == [(10, "completed")]

    # Business time from SCD2: on 3 Sep enrolment 10 was at 40 %, and 11 still existed.
    sept3 = enrolments_as_of(lake, datetime(2026, 9, 3, tzinfo=UTC)).to_pylist()
    assert [(r["id"], r["progress_pct"]) for r in sept3] == [(10, 40), (11, 0)]
    assert file_stats(lake.table("enrolments")).snapshots >= 3
