"""Compaction and snapshot expiry: fewer files, same rows."""

from __future__ import annotations

import pytest
from conftest import sorted_rows

from cdc_lake.lake import Lake
from cdc_lake.loadgen import DebeziumEmitter, Generator, GeneratorConfig
from cdc_lake.maintenance import compact, expire_snapshots, file_stats
from cdc_lake.queries import current_snapshot_id, state_at_snapshot


@pytest.fixture
def busy_lake(lake: Lake) -> Lake:
    """A lake after many small batches: many small files, many snapshots."""
    gen = Generator(GeneratorConfig(seed=5, users=10, courses=3, steps=120))
    emitter = DebeziumEmitter(seed=5)
    stream = emitter.snapshot(gen.initial()) + emitter.stream(gen.changes())
    for n, start in enumerate(range(0, len(stream), 25)):
        lake.apply(stream[start : start + 25], batch_id=n)
    return lake


def test_compaction_rewrites_into_one_file_with_identical_rows(busy_lake: Lake):
    table = busy_lake.table("enrolments")
    rows_before = sorted_rows(table.scan().to_arrow(), "id")
    before, after = compact(table)
    assert before.live_files > 3
    assert after.live_files == 1
    assert after.snapshots == before.snapshots + 2  # overwrite = delete + append snapshot
    assert sorted_rows(busy_lake.table("enrolments").scan().to_arrow(), "id") == rows_before
    # Nothing to do the second time.
    again_before, again_after = compact(busy_lake.table("enrolments"))
    assert again_before == again_after


def test_expiry_drops_old_snapshots_and_orphan_files(busy_lake: Lake):
    old_snapshot = busy_lake.table("users").snapshots()[0].snapshot_id
    compact(busy_lake.table("users"))
    rows = sorted_rows(busy_lake.table("users").scan().to_arrow(), "id")
    current = current_snapshot_id(busy_lake, "users")

    before, after = expire_snapshots(busy_lake.table("users"), retain_last=1)
    assert after.snapshots == 1 and before.snapshots > 1
    assert after.files_on_disk == after.live_files == 1 < before.files_on_disk
    assert current_snapshot_id(busy_lake, "users") == current
    assert sorted_rows(busy_lake.table("users").scan().to_arrow(), "id") == rows
    # Time travel to an expired snapshot is no longer possible: that is the trade-off.
    with pytest.raises(ValueError):
        state_at_snapshot(busy_lake, "users", old_snapshot)
    assert file_stats(busy_lake.table("users")).live_files == 1
