"""The merge contract: highest LSN wins, replays are no-ops, deletes stick, order is irrelevant."""

from __future__ import annotations

import copy
from pathlib import Path

from conftest import make_lake, records, sorted_rows

from cdc_lake.lake import Lake
from cdc_lake.loadgen import DebeziumEmitter, Generator, GeneratorConfig, deliver
from cdc_lake.merge import Change, Stored, plan_state
from cdc_lake.queries import current_state


def _apply_in_batches(lake: Lake, stream: list[dict], size: int) -> None:
    for n, start in enumerate(range(0, len(stream), size)):
        lake.apply(stream[start : start + size], batch_id=n)


def _everything(lake: Lake) -> dict[str, list[dict]]:
    """Full content of every table (tombstone rows included), in a stable order."""
    out: dict[str, list[dict]] = {}
    for source in lake.specs:
        out[source] = sorted_rows(lake.table(source).scan().to_arrow(), "id")
        out[f"{source}_changelog"] = sorted_rows(
            lake.table(f"{source}_changelog").scan().to_arrow(), "id", "_lsn"
        )
    out["enrolments_history"] = sorted_rows(
        lake.table("enrolments_history").scan().to_arrow(), "id", "valid_from_lsn"
    )
    out["quarantine"] = sorted_rows(lake.table("quarantine").scan().to_arrow(), "record_id")
    return out


def test_plan_state_keeps_highest_lsn_and_never_resurrects():
    stored = {1: Stored(lsn=300, deleted=True), 2: Stored(lsn=100, deleted=False)}
    changes = [
        Change(1, "u", 200, 0, {"id": 1, "v": "late, older update"}),
        Change(2, "u", 150, 0, {"id": 2, "v": "older"}),
        Change(2, "u", 180, 0, {"id": 2, "v": "newest"}),
        Change(3, "tombstone", None, None, {}),  # unknown key: nothing to delete
    ]
    planned = plan_state(changes, stored, "id")
    assert set(planned) == {2}
    assert planned[2]["v"] == "newest" and planned[2]["_lsn"] == 180


def test_shuffled_duplicated_stream_gives_same_lake_as_ordered(tmp_path: Path):
    gen = Generator(GeneratorConfig(seed=3, users=12, courses=4, steps=160))
    emitter = DebeziumEmitter(seed=3)
    stream = emitter.snapshot(gen.initial()) + emitter.stream(gen.changes())

    ordered = make_lake(tmp_path / "ordered")
    _apply_in_batches(ordered, stream, 60)

    chaos = deliver(
        copy.deepcopy(stream),
        seed=11,
        shuffle_window=len(stream),
        redeliver_at=0.5,
        redeliver_count=80,
    )
    assert chaos != stream and len(chaos) == len(stream) + 80
    shuffled = make_lake(tmp_path / "shuffled")
    _apply_in_batches(shuffled, chaos, 37)

    assert _everything(shuffled) == _everything(ordered)
    assert current_state(ordered, "courses").schema.names[-1] == "level"


def test_replaying_the_same_events_changes_nothing(lake: Lake):
    for name in ("users", "enrolments", "courses_add_column", "payments_type_change"):
        lake.apply(records(name))
    before = _everything(lake)
    snapshots = {t: len(lake.table(t).snapshots()) for t in before}

    for name in ("users", "enrolments", "courses_add_column", "payments_type_change"):
        result = lake.apply(records(name))
        assert result.rows_written == 0

    assert _everything(lake) == before
    assert {t: len(lake.table(t).snapshots()) for t in before} == snapshots


def test_delete_is_kept_as_tombstone_row_and_blocks_late_updates(lake: Lake):
    stream = records("users")
    lake.apply(stream)
    assert [r["id"] for r in current_state(lake, "users").to_pylist()] == [1, 3]
    stored = {r["id"]: r for r in lake.table("users").scan().to_arrow().to_pylist()}
    assert stored[2]["_deleted"] and stored[2]["_op"] == "d"
    assert stored[2]["full_name"] == "Minh Tran"  # last values kept from `before`

    # The snapshot row of user 2 arrives again, late (lower LSN than the delete).
    lake.apply([stream[1]])
    assert [r["id"] for r in current_state(lake, "users").to_pylist()] == [1, 3]


def test_tombstone_alone_deletes_a_live_key(lake: Lake):
    stream = records("users")
    lake.apply(stream[:3])  # users 1, 2, 3 exist
    lake.apply([stream[-1]])  # tombstone for user 2, its delete event never seen
    assert [r["id"] for r in current_state(lake, "users").to_pylist()] == [1, 3]
    row = next(r for r in lake.table("users").scan().to_arrow().to_pylist() if r["id"] == 2)
    assert row["_op"] == "tombstone" and row["_lsn"] == 24023928
