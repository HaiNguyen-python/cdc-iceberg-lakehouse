"""Table maintenance: small-file compaction, snapshot expiry and orphan file cleanup.

Every CDC micro-batch writes at least one new data file per touched table, and copy-on-write
deletes rewrite more. After a few dozen batches a table that fits in one file is spread over
dozens; every scan then opens all of them. Compaction rewrites the live rows into one file.

Old snapshots keep old files reachable (that is what makes time travel work), so compaction
alone does not shrink storage. Expiring snapshots drops them from the metadata, and removing
orphans deletes the data files no remaining snapshot references.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

from pyiceberg.table import Table


@dataclass(frozen=True)
class FileStats:
    live_files: int
    snapshots: int
    files_on_disk: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def _local_data_dir(table: Table) -> Path | None:
    location = urlparse(table.location())
    if location.scheme not in ("", "file"):
        return None
    return Path(unquote(location.path)) / "data"


def _disk_files(table: Table) -> set[str]:
    data_dir = _local_data_dir(table)
    if data_dir is None or not data_dir.exists():
        return set()
    return {str(p) for p in data_dir.rglob("*.parquet")}


def file_stats(table: Table) -> FileStats:
    """Data files in the current snapshot, snapshot count, and data files on local disk."""
    live = len(table.inspect.data_files()) if table.current_snapshot() else 0
    return FileStats(live, len(table.snapshots()), len(_disk_files(table)))


def compact(table: Table, min_files: int = 2) -> tuple[FileStats, FileStats]:
    """Rewrite the live rows into as few files as the target file size allows.

    Uses an overwrite of the whole table: simple and atomic, and the row content is unchanged.
    For a large partitioned table one would rewrite only the partitions with many small files.
    """
    before = file_stats(table)
    if before.live_files < min_files:
        return before, before
    rows = table.scan().to_arrow()
    table.overwrite(rows, snapshot_properties={"cdc.maintenance": "compaction"})
    return before, file_stats(table)


def expire_snapshots(table: Table, retain_last: int = 5) -> tuple[FileStats, FileStats]:
    """Keep the newest `retain_last` snapshots, expire the rest, delete unreferenced files.

    Time travel to an expired snapshot stops working; that is the price of reclaiming space.
    Orphan removal assumes a single writer (this consumer); with concurrent writers only files
    older than the longest possible commit should be deleted.
    """
    before = file_stats(table)
    snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)
    expired = [s.snapshot_id for s in snapshots[: max(0, len(snapshots) - retain_last)]]
    current = table.current_snapshot()
    expired = [sid for sid in expired if current is None or sid != current.snapshot_id]
    if expired:
        table.maintenance.expire_snapshots().by_ids(expired).commit()
        table.refresh()
    referenced = {
        unquote(urlparse(p).path) for p in table.inspect.all_files().column("file_path").to_pylist()
    }
    for orphan in sorted(_disk_files(table) - referenced):
        Path(orphan).unlink()
    return before, file_stats(table)
