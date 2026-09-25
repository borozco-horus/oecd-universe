from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime

import polars as pl

from oecd_universe.provenance.artifacts import read_artifacts
from oecd_universe.settings import Settings


def _git_commit(settings: Settings) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=settings.root, text=True, timeout=10
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def create_snapshot(settings: Settings, snapshot_id: str | None = None) -> dict[str, object]:
    snapshot_id = snapshot_id or f"oecd-{date.today().isoformat()}"
    target = settings.path(f"snapshots/{snapshot_id}")
    target.mkdir(parents=True, exist_ok=True)
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    db_snapshot_path = settings.path("data/manifests/dbnomics_snapshot.json")
    db_snapshot = json.loads(db_snapshot_path.read_text()) if db_snapshot_path.exists() else {}
    artifacts = read_artifacts(settings)
    payload = {
        "snapshot_id": snapshot_id,
        "sync_timestamp": datetime.now(UTC).isoformat(),
        "software_git_commit": _git_commit(settings),
        "dbnomics_commit": db_snapshot.get("commit"),
        "dataset_versions": [
            {
                "agency": row["agency"],
                "dataset_id": row["dataset_id"],
                "version": row["version"],
                "last_updated": row["last_updated"],
                "dbnomics_dir_hash": row["dbnomics_dir_hash"],
            }
            for row in catalog.select(
                ["agency", "dataset_id", "version", "last_updated", "dbnomics_dir_hash"]
            ).to_dicts()
        ],
        "artifacts": artifacts.select(
            ["sha256", "provider", "source_url", "dataset_id", "dataset_version"]
        ).to_dicts(),
    }
    output = target / "snapshot.json"
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output.exists() and output.read_text() != serialized:
        # Snapshot IDs are immutable; a same-day rerun receives a timestamp suffix.
        snapshot_id = f"{snapshot_id}-{datetime.now(UTC).strftime('%H%M%S')}"
        target = settings.path(f"snapshots/{snapshot_id}")
        target.mkdir(parents=True, exist_ok=False)
        payload["snapshot_id"] = snapshot_id
        output = target / "snapshot.json"
        serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    output.write_text(serialized)
    return {"snapshot_id": snapshot_id, "artifacts": artifacts.height, "datasets": catalog.height}
