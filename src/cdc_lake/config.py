"""Where the lake lives: an Iceberg SQL catalog (SQLite file) and a warehouse location.

Defaults are a local folder, so tests, CI and `cdc-lake demo` need no services. The Docker stack
sets `CDC_LAKE_WAREHOUSE=s3://...` plus the `CDC_LAKE_S3_*` variables to write to MinIO.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

NAMESPACE = "lake"


@dataclass(frozen=True)
class LakeConfig:
    root: Path
    warehouse: str
    catalog_uri: str
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    @classmethod
    def local(cls, root: Path | str) -> LakeConfig:
        root = Path(root).resolve()
        return cls(
            root=root,
            warehouse=(root / "warehouse").as_uri(),
            catalog_uri=f"sqlite:///{root / 'catalog.db'}",
        )

    @classmethod
    def from_env(cls, default_root: Path | str = "data/lake") -> LakeConfig:
        base = cls.local(os.environ.get("CDC_LAKE_ROOT", str(default_root)))
        return cls(
            root=base.root,
            warehouse=os.environ.get("CDC_LAKE_WAREHOUSE", base.warehouse),
            catalog_uri=os.environ.get("CDC_LAKE_CATALOG_URI", base.catalog_uri),
            s3_endpoint=os.environ.get("CDC_LAKE_S3_ENDPOINT"),
            s3_access_key=os.environ.get("CDC_LAKE_S3_ACCESS_KEY"),
            s3_secret_key=os.environ.get("CDC_LAKE_S3_SECRET_KEY"),
        )

    def catalog_properties(self) -> dict[str, str]:
        props = {"uri": self.catalog_uri, "warehouse": self.warehouse}
        if self.s3_endpoint:
            props["s3.endpoint"] = self.s3_endpoint
        if self.s3_access_key and self.s3_secret_key:
            props["s3.access-key-id"] = self.s3_access_key
            props["s3.secret-access-key"] = self.s3_secret_key
        return props


def open_catalog(config: LakeConfig) -> Catalog:
    """Open (and create on first use) the catalog and the `lake` namespace."""
    config.root.mkdir(parents=True, exist_ok=True)
    if config.warehouse.startswith("file:"):
        Path(config.warehouse.removeprefix("file://")).mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("cdc_lake", **config.catalog_properties())
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog
