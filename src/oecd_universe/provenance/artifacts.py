from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from oecd_universe.settings import Settings

ARTIFACT_COLUMNS = {
    "sha256": pl.String,
    "provider": pl.String,
    "source_url": pl.String,
    "retrieved_at": pl.String,
    "last_modified": pl.String,
    "etag": pl.String,
    "dataset_id": pl.String,
    "dataset_version": pl.String,
    "file_name": pl.String,
    "relative_path": pl.String,
    "media_type": pl.String,
    "file_size": pl.Int64,
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest(settings: Settings) -> Path:
    return settings.path("data/manifests/artifacts.parquet")


def read_artifacts(settings: Settings) -> pl.DataFrame:
    path = _manifest(settings)
    if path.exists():
        return pl.read_parquet(path)
    return pl.DataFrame(schema=ARTIFACT_COLUMNS)


def register_bytes(
    settings: Settings,
    *,
    provider: str,
    source_url: str,
    content: bytes,
    file_name: str,
    media_type: str = "application/octet-stream",
    last_modified: str | None = None,
    etag: str | None = None,
    dataset_id: str | None = None,
    dataset_version: str | None = None,
) -> tuple[Path, str]:
    """Store immutable content and append a unique manifest row."""
    digest = sha256_bytes(content)
    suffixes = "".join(Path(file_name).suffixes) or ".bin"
    target = settings.path(f"data/bronze/{provider}/{digest[:2]}/{digest}{suffixes}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if sha256_file(target) != digest:
            raise RuntimeError(f"checksum mismatch for immutable artifact: {target}")
    else:
        target.write_bytes(content)

    row = {
        "sha256": digest,
        "provider": provider,
        "source_url": source_url,
        "retrieved_at": utc_now(),
        "last_modified": last_modified,
        "etag": etag,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "file_name": file_name,
        "relative_path": str(target.relative_to(settings.root)),
        "media_type": media_type,
        "file_size": len(content),
    }
    old = read_artifacts(settings)
    new = pl.DataFrame([row], schema=ARTIFACT_COLUMNS)
    out = pl.concat([old, new], how="vertical_relaxed").unique(
        subset=["sha256", "provider", "source_url"], keep="first"
    )
    manifest = _manifest(settings)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(manifest, compression="zstd")
    return target, digest


def latest_for_url(settings: Settings, source_url: str) -> dict[str, object] | None:
    artifacts = read_artifacts(settings)
    if artifacts.is_empty():
        return None
    rows = artifacts.filter(pl.col("source_url") == source_url).sort("retrieved_at")
    return rows.tail(1).to_dicts()[0] if rows.height else None
