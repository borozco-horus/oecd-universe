from __future__ import annotations

import json
from pathlib import Path

import duckdb

from oecd_universe.settings import Settings


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def build_warehouse(settings: Settings) -> dict[str, object]:
    target = settings.path("warehouse/oecd.duckdb")
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(target))
    views = {
        "datasets": settings.path("data/silver/datasets/catalog.parquet"),
        "observations": settings.path("data/silver/observations/observations.parquet"),
        "observation_dimensions": settings.path(
            "data/silver/observations/observation_dimensions.parquet"
        ),
        "dimensions": settings.path("data/silver/dimensions/dimensions.parquet"),
        "codelists": settings.path("data/silver/codelists/codelists.parquet"),
        "artifacts": settings.path("data/manifests/artifacts.parquet"),
        "mexico_coverage": settings.path("data/silver/datasets/mexico_coverage.parquet"),
        "quality_issues": settings.path("data/quarantine/issues.parquet"),
    }
    for name, path in views.items():
        if path.exists():
            connection.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{_sql_path(path)}')"
            )
    connection.execute(
        """
        CREATE OR REPLACE TABLE snapshots (
          snapshot_id VARCHAR,
          sync_timestamp VARCHAR,
          software_git_commit VARCHAR,
          dbnomics_commit VARCHAR,
          metadata_path VARCHAR
        )
        """
    )
    for metadata in settings.path("snapshots").glob("*/snapshot.json"):
        payload = json.loads(metadata.read_text())
        connection.execute(
            "INSERT INTO snapshots VALUES (?, ?, ?, ?, ?)",
            [
                payload.get("snapshot_id"),
                payload.get("sync_timestamp"),
                payload.get("software_git_commit"),
                payload.get("dbnomics_commit"),
                str(metadata.relative_to(settings.root)),
            ],
        )
    count = connection.execute("SELECT count(*) FROM observations").fetchone()[0]
    mexico = connection.execute(
        "SELECT count(*) FROM observations WHERE ref_area = 'MEX'"
    ).fetchone()[0]
    connection.close()
    return {"observations": count, "mexico_observations": mexico, "bytes": target.stat().st_size}
