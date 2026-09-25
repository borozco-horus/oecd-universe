from __future__ import annotations

from pathlib import Path

import pytest

from oecd_universe.settings import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    for relative in (
        "data/bronze/oecd",
        "data/bronze/dbnomics",
        "data/manifests",
        "data/silver/observations",
        "data/silver/datasets",
        "data/silver/dimensions",
        "data/silver/codelists",
        "data/quarantine",
        "warehouse",
        "snapshots",
    ):
        (tmp_path / relative).mkdir(parents=True, exist_ok=True)
    return Settings(
        root=tmp_path,
        oecd_dataflow_url="https://example.test/dataflow",
        dbnomics_catalog_url="https://example.test/datasets",
        dbnomics_series_url="https://example.test/series/{dataset_id}",
        dbnomics_repository="https://example.test/repo.git",
        user_agent="test",
        priority_terms=["employment"],
    )
