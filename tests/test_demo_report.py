"""Offline end-to-end run: reconciliation with the source, the generator, and the report."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cdc_lake.demo import DemoConfig, reconcile, run_demo
from cdc_lake.lake import is_in
from cdc_lake.loadgen import DebeziumEmitter, Generator, GeneratorConfig, deliver
from cdc_lake.report import write_report

SMALL = DemoConfig(
    seed=7,
    users=25,
    courses=5,
    steps=260,
    batch_size=70,
    rate=0,
    shuffle_window=30,
    redeliver_count=60,
    compact_every=3,
    retain_last=2,
)


@pytest.fixture(scope="module")
def demo(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Any]]:
    out = tmp_path_factory.mktemp("demo")
    return out, run_demo(out, SMALL)


def test_lake_reconciles_with_the_source_database(demo):
    _, m = demo
    for name, r in m["reconciliation"].items():
        assert r["unexplained"] == [], name
    # The planted type change is caught, and is the only reason rows differ.
    assert m["quarantine"]["rows"] > 0
    assert set(m["quarantine"]["reasons"]) == {"type change: amount_cents expected long"}
    assert m["reconciliation"]["payments"]["differing"] > 0
    assert m["columns_added"] == ["courses.level"]


def test_demo_checks_time_travel_history_replay_and_maintenance(demo):
    _, m = demo
    assert all(t["differing"] == 0 for t in m["time_travel_to_initial_load"].values())
    assert m["scd2_as_of_checkpoint"]["differing"] == 0
    assert m["scd2_as_of_checkpoint"]["rows"] == m["scd2_as_of_checkpoint"]["source_rows"]
    assert m["replay_last_two_batches"]["rows_written"] == 0
    assert m["replay_last_two_batches"]["versions_added"] == 0
    mt = m["maintenance"]
    assert mt["after_compaction"]["live_files"] == mt["tables"]
    assert mt["after_expiry"]["files_on_disk"] < mt["before"]["files_on_disk"]
    assert mt["rows_after_maintenance"] == {
        n: r["lake_rows"] for n, r in m["reconciliation"].items()
    }
    assert any(b["compacted_after"] for b in m["batches"])


def test_reconciliation_catches_a_row_lost_by_the_pipeline(demo):
    """Positive control: break the lake behind the pipeline's back and expect a finding."""
    from cdc_lake.config import LakeConfig, open_catalog
    from cdc_lake.lake import Lake

    out, _ = demo
    lake = Lake(open_catalog(LakeConfig.local(out / "lake")))
    users = lake.table("users")
    victim = users.scan().to_arrow().column("id").to_pylist()[0]
    users.delete(is_in("id", [victim]))
    gen = Generator(
        GeneratorConfig(
            seed=SMALL.seed, users=SMALL.users, courses=SMALL.courses, steps=SMALL.steps
        )
    )
    gen.initial()
    for _ in gen.changes():
        pass
    report = reconcile(lake, gen.db.tables)
    assert victim in report["users"]["unexplained"]


def test_generator_and_delivery_are_deterministic():
    def stream(seed: int) -> list[dict]:
        gen = Generator(GeneratorConfig(seed=seed, users=8, courses=3, steps=80))
        emitter = DebeziumEmitter(seed=seed)
        return emitter.snapshot(gen.initial()) + emitter.stream(gen.changes())

    assert stream(1) == stream(1) and stream(1) != stream(2)
    records = stream(1)
    lsns = [r["value"]["source"]["lsn"] for r in records if r["value"]]
    assert lsns == sorted(lsns)  # WAL order
    arrived = deliver(records, seed=1)
    for topic in {r["topic"] for r in records}:
        # Interleaving topics keeps each partition's order, as Kafka does.
        assert [r for r in arrived if r["topic"] == topic] == [
            r for r in records if r["topic"] == topic
        ]
    assert len(deliver(records, seed=1, redeliver_at=0.5, redeliver_count=10)) == len(records) + 10


def test_report_writes_results_and_charts(demo, tmp_path: Path):
    out, _ = demo
    docs = tmp_path / "docs"
    results = write_report(out / "metrics.json", docs)
    text = results.read_text()
    assert "synthetic" in text and "Unexplained" in text
    assert "courses.level" in text
    for chart in ("files_per_batch.png", "apply_time_per_batch.png"):
        assert 10_000 < (docs / "img" / chart).stat().st_size < 300_000


def test_report_handles_a_run_without_compaction(demo, tmp_path: Path):
    import copy
    import json

    metrics = copy.deepcopy(demo[1])
    for batch in metrics["batches"]:
        batch["compacted_after"] = False
    path = tmp_path / "metrics.json"
    path.write_text(json.dumps(metrics, default=str))
    write_report(path, tmp_path / "docs")
    assert (tmp_path / "docs" / "img" / "apply_time_per_batch.png").exists()
