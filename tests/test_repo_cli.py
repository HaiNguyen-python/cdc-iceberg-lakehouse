"""Repository contracts (Docker files stay in sync with the code) and the CLI."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from conftest import FIXTURES

from cdc_lake.cli import main
from cdc_lake.sources import SOURCE_TABLES, init_sql

ROOT = Path(__file__).resolve().parents[1]


def test_postgres_init_script_matches_the_source_contract():
    assert (ROOT / "docker/postgres/init.sql").read_text() == init_sql()


def test_connector_config_matches_what_the_parser_expects():
    config = json.loads((ROOT / "connectors/courseplatform-postgres.json").read_text())["config"]
    tables = {t.removeprefix("public.") for t in config["table.include.list"].split(",")}
    assert tables == set(SOURCE_TABLES)
    # The parser and the type rules assume bare JSON, numeric as strings, ISO timestamps.
    assert config["value.converter.schemas.enable"] == "false"
    assert config["key.converter.schemas.enable"] == "false"
    assert config["decimal.handling.mode"] == "string"
    assert config["time.precision.mode"] == "adaptive"
    assert config["topic.prefix"] == "courseplatform"


def test_compose_pins_every_image_and_enables_logical_decoding():
    compose = (ROOT / "docker-compose.yml").read_text()
    images = re.findall(r"^\s+image:\s*(\S+)", compose, flags=re.M)
    assert len(images) == 4  # the consumer is built from the Dockerfile
    for image in images:
        name, _, tag = image.rpartition(":")
        assert name and tag and tag != "latest", image
    assert "wal_level=logical" in compose


def test_cli_apply_query_time_travel_and_maintain(tmp_path: Path, capsys):
    lake = str(tmp_path / "lake")
    files = [str(FIXTURES / f"{n}.jsonl") for n in ("enrolments", "payments_type_change")]
    assert main(["--lake", lake, "apply", *files]) == 0
    assert "3 quarantined" in capsys.readouterr().out

    assert main(["--lake", lake, "query", "SELECT id, status FROM enrolments"]) == 0
    assert "completed" in capsys.readouterr().out

    assert main(["--lake", lake, "as-of", "--business-time", "2026-09-03T00:00:00+00:00"]) == 0
    assert "40" in capsys.readouterr().out

    assert main(["--lake", lake, "snapshots", "enrolments"]) == 0
    first_snapshot = int(capsys.readouterr().out.split()[0])
    assert main(["--lake", lake, "as-of", "--snapshot-id", str(first_snapshot)]) == 0
    assert main(["--lake", lake, "as-of", "--time", "2000-01-01T00:00:00+00:00"]) == 0

    assert main(["--lake", lake, "maintain", "--retain-last", "1"]) == 0
    assert "enrolments_history" in capsys.readouterr().out


def test_cli_generate_then_apply(tmp_path: Path, capsys):
    stream = tmp_path / "stream.jsonl"
    assert main(["generate", "--out", str(stream), "--users", "5", "--steps", "40"]) == 0
    assert main(["--lake", str(tmp_path / "lake"), "apply", str(stream)]) == 0
    assert "rows written" in capsys.readouterr().out


def test_cli_demo_refuses_to_reuse_an_old_lake(tmp_path: Path):
    (tmp_path / "lake").mkdir()
    assert main(["demo", "--out", str(tmp_path)]) == 2
    with pytest.raises(SystemExit):
        main(["as-of"])  # one of the time options is required
