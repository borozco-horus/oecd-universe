from __future__ import annotations

import hashlib
import re
from pathlib import Path

import polars as pl

from oecd_universe.provenance.artifacts import read_artifacts, sha256_file
from oecd_universe.settings import Settings

ISSUE_SCHEMA = {
    "issue_id": pl.String,
    "classification": pl.String,
    "category": pl.String,
    "dataset_id": pl.String,
    "observation_id": pl.String,
    "details": pl.String,
}

PERIOD = re.compile(
    r"^(?:\d{4}|\d{4}-(?:Q[1-4]|M?(?:0[1-9]|1[0-2])|S[12]|W\d{2}|\d{2}-\d{2}|\d{2}))$"
)


def _issue(
    classification: str,
    category: str,
    details: str,
    dataset_id: str | None = None,
    observation_id: str | None = None,
) -> dict[str, object]:
    raw = f"{classification}|{category}|{dataset_id}|{observation_id}|{details}"
    return {
        "issue_id": hashlib.sha256(raw.encode()).hexdigest(),
        "classification": classification,
        "category": category,
        "dataset_id": dataset_id,
        "observation_id": observation_id,
        "details": details,
    }


def validate(settings: Settings) -> dict[str, int]:
    observations = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    obs_dimensions = pl.read_parquet(
        settings.path("data/silver/observations/observation_dimensions.parquet")
    )
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    dimensions = pl.read_parquet(settings.path("data/silver/dimensions/dimensions.parquet"))
    artifacts = read_artifacts(settings)
    issues: list[dict[str, object]] = []

    duplicate_ids = observations.group_by("observation_id").len().filter(pl.col("len") > 1)
    for row in duplicate_ids.to_dicts():
        issues.append(
            _issue(
                "QUARANTINE",
                "duplicate_observation",
                f"count={row['len']}",
                observation_id=row["observation_id"],
            )
        )
    duplicate_catalog = catalog.group_by(["agency", "dataset_id"]).len().filter(pl.col("len") > 1)
    for row in duplicate_catalog.to_dicts():
        issues.append(
            _issue(
                "QUARANTINE", "duplicate_dataset_id", f"count={row['len']}", str(row["dataset_id"])
            )
        )

    artifact_hashes = set(artifacts["sha256"].to_list()) if artifacts.height else set()
    for row in (
        observations.filter(
            pl.col("source_artifact_sha256").is_null()
            | ~pl.col("source_artifact_sha256").is_in(list(artifact_hashes))
        )
        .select(["dataset_id", "observation_id"])
        .to_dicts()
    ):
        issues.append(
            _issue(
                "QUARANTINE",
                "missing_provenance",
                "source artifact is absent from manifest",
                str(row["dataset_id"]),
                str(row["observation_id"]),
            )
        )

    for artifact in artifacts.to_dicts():
        path = settings.path(str(artifact["relative_path"]))
        if not path.exists() or sha256_file(path) != artifact["sha256"]:
            issues.append(
                _issue(
                    "QUARANTINE",
                    "checksum_mismatch",
                    str(Path(str(artifact["relative_path"]))),
                    str(artifact.get("dataset_id") or "") or None,
                )
            )

    malformed = observations.filter(
        pl.col("time_period").is_null() | ~pl.col("time_period").str.contains(PERIOD.pattern)
    )
    for row in malformed.select(["dataset_id", "observation_id", "time_period"]).to_dicts():
        issues.append(
            _issue(
                "WARN",
                "malformed_or_unsupported_period",
                f"time_period={row['time_period']!r}",
                str(row["dataset_id"]),
                str(row["observation_id"]),
            )
        )

    known = dimensions.select(["dataset_id", "dimension_id"]).unique()
    unknown = (
        obs_dimensions.select(["dataset_id", "dimension_id"])
        .unique()
        .join(known, on=["dataset_id", "dimension_id"], how="anti")
    )
    for row in unknown.to_dicts():
        issues.append(
            _issue(
                "WARN",
                "unknown_dimension",
                f"dimension_id={row['dimension_id']}",
                str(row["dataset_id"]),
            )
        )

    missing_unit = (
        observations.filter(pl.col("obs_value").is_not_null() & pl.col("unit_measure").is_null())
        .group_by("dataset_id")
        .len()
    )
    for row in missing_unit.to_dicts():
        issues.append(
            _issue("WARN", "missing_unit", f"observations={row['len']}", str(row["dataset_id"]))
        )

    malformed_area = observations.filter(
        pl.col("ref_area").is_not_null()
        & ~pl.col("ref_area").str.contains(r"^[A-Za-z0-9_\-]{2,16}$")
    )
    for row in malformed_area.select(["dataset_id", "observation_id", "ref_area"]).to_dicts():
        issues.append(
            _issue(
                "WARN",
                "malformed_area_code",
                f"ref_area={row['ref_area']!r}",
                str(row["dataset_id"]),
                str(row["observation_id"]),
            )
        )

    catalog_titles = catalog.select(["dataset_id", "title"]).unique("dataset_id")
    enriched = observations.join(catalog_titles, on="dataset_id", how="left")
    economic_text = pl.concat_str(
        [pl.col("title").fill_null(""), pl.col("measure").fill_null("")], separator=" "
    ).str.to_lowercase()
    negative = enriched.filter(
        economic_text.str.contains("employment|hours") & (pl.col("obs_value") < 0)
    )
    for row in negative.select(["dataset_id", "observation_id", "obs_value"]).to_dicts():
        issues.append(
            _issue(
                "WARN",
                "negative_economic_value",
                f"obs_value={row['obs_value']}",
                str(row["dataset_id"]),
                str(row["observation_id"]),
            )
        )
    percentage = enriched.filter(
        economic_text.str.contains("share|percent|percentage")
        & pl.col("obs_value").is_not_null()
        & ((pl.col("obs_value") < -10) | (pl.col("obs_value") > 200))
    )
    for row in percentage.select(["dataset_id", "observation_id", "obs_value"]).to_dicts():
        issues.append(
            _issue(
                "WARN",
                "implausible_percentage_like_value",
                f"obs_value={row['obs_value']}; diagnostic only",
                str(row["dataset_id"]),
                str(row["observation_id"]),
            )
        )

    unit_changes = (
        observations.group_by(["dataset_id", "source_row_or_series_id"])
        .agg(pl.col("unit_measure").drop_nulls().n_unique().alias("units"))
        .filter(pl.col("units") > 1)
    )
    for row in unit_changes.to_dicts():
        issues.append(
            _issue(
                "WARN",
                "unit_change_within_series",
                f"distinct_units={row['units']}",
                str(row["dataset_id"]),
            )
        )

    issue_frame = (
        pl.DataFrame(issues, schema=ISSUE_SCHEMA) if issues else pl.DataFrame(schema=ISSUE_SCHEMA)
    )
    issue_frame.write_parquet(settings.path("data/quarantine/issues.parquet"), compression="zstd")
    quarantine_ids = set(
        issue_frame.filter(pl.col("classification") == "QUARANTINE")["observation_id"]
        .drop_nulls()
        .to_list()
    )
    warnings = issue_frame.filter(pl.col("classification") == "WARN").height
    return {
        "valid_rows": max(0, observations.height - len(quarantine_ids)),
        "warnings": warnings,
        "quarantined_rows": len(quarantine_ids),
        "issues": issue_frame.height,
        "duplicates": duplicate_ids.height,
    }
