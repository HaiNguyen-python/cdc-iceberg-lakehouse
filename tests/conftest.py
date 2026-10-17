from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from cdc_lake.config import LakeConfig, open_catalog
from cdc_lake.events import read_jsonl
from cdc_lake.lake import Lake

FIXTURES = Path(__file__).parent / "fixtures"


def records(name: str) -> list[dict[str, Any]]:
    """Kafka records from tests/fixtures/<name>.jsonl (Debezium Postgres, schemas disabled)."""
    return read_jsonl(str(FIXTURES / f"{name}.jsonl"))


def make_lake(root: Path) -> Lake:
    return Lake(open_catalog(LakeConfig.local(root)))


def sorted_rows(table: pa.Table, *keys: str) -> list[dict[str, Any]]:
    return table.sort_by([(k, "ascending") for k in keys]).to_pylist()


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    return make_lake(tmp_path / "lake")
