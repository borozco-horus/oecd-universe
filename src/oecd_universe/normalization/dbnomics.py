from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import polars as pl

from oecd_universe.provenance.artifacts import read_artifacts
from oecd_universe.settings import Settings

CORE_DIMENSION_MAP = {
    "ref_area": ("REF_AREA", "LOCATION", "COUNTRY", "geo"),
    "frequency": ("FREQ", "FREQUENCY", "frequency"),
    "measure": ("MEASURE", "SUBJECT", "INDICATOR", "TRANSACT"),
    "unit_measure": ("UNIT_MEASURE", "UNIT", "unit"),
    "unit_multiplier": ("UNIT_MULT", "POWERCODE"),
    "price_base": ("PRICE_BASE", "PRICES", "PRICE"),
    "adjustment": ("ADJUSTMENT", "SEASONAL_ADJUSTMENT"),
    "activity": ("ACTIVITY", "INDUSTRY", "ECONOMIC_ACTIVITY"),
    "counterpart_area": ("COUNTERPART_AREA", "PARTNER", "PARTNER_AREA"),
    "sex": ("SEX",),
    "age": ("AGE",),
    "employment_status": ("EMPLOYMENT_STATUS", "STATUS"),
    "education_level": ("EDUCATION_LEVEL", "EDUCATION", "ISCED11"),
}

OBS_SCHEMA = {
    "observation_id": pl.String,
    "provider": pl.String,
    "agency": pl.String,
    "dataset_id": pl.String,
    "dataset_version": pl.String,
    "ref_area": pl.String,
    "time_period": pl.String,
    "period_start": pl.Date,
    "period_end": pl.Date,
    "frequency": pl.String,
    "measure": pl.String,
    "unit_measure": pl.String,
    "unit_multiplier": pl.Int64,
    "price_base": pl.String,
    "adjustment": pl.String,
    "activity": pl.String,
    "counterpart_area": pl.String,
    "sex": pl.String,
    "age": pl.String,
    "employment_status": pl.String,
    "education_level": pl.String,
    "obs_value": pl.Float64,
    "obs_status": pl.String,
    "conf_status": pl.String,
    "break_status": pl.String,
    "decimals": pl.Int64,
    "additional_dimensions": pl.String,
    "source_artifact_sha256": pl.String,
    "source_row_or_series_id": pl.String,
    "retrieved_at": pl.String,
    "source_last_updated": pl.String,
}

DIM_SCHEMA = {
    "observation_id": pl.String,
    "provider": pl.String,
    "dataset_id": pl.String,
    "dimension_id": pl.String,
    "code": pl.String,
}


def _first(dimensions: dict[str, Any], names: tuple[str, ...]) -> str | None:
    upper = {str(key).upper(): value for key, value in dimensions.items()}
    for name in names:
        if name.upper() in upper and upper[name.upper()] is not None:
            return str(upper[name.upper()])
    return None


def _int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except ValueError:
        return None


def _float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "NA", "NaN", "") else None
    except (TypeError, ValueError):
        return None


def _period_bounds(period: str, frequency: str | None) -> tuple[object | None, object | None]:
    # Only deterministic Gregorian annual periods are materialized in v0.1.
    if (frequency in ("A", "Y", "Annual", None)) and len(period) == 4 and period.isdigit():
        from datetime import date

        year = int(period)
        return date(year, 1, 1), date(year, 12, 31)
    return None, None


def _obs_id(dataset_id: str, series_id: str, period: str) -> str:
    raw = f"DBnomics\x1fOECD\x1f{dataset_id}\x1f{series_id}\x1f{period}".encode()
    return hashlib.sha256(raw).hexdigest()


def _attributes(doc: dict[str, Any], index: int) -> dict[str, Any]:
    output: dict[str, Any] = {}
    values = doc.get("observations_attributes") or doc.get("attributes") or []
    if isinstance(values, list) and index < len(values) and isinstance(values[index], dict):
        output.update(values[index])
    return output


def normalize(settings: Settings) -> dict[str, int]:
    artifacts = read_artifacts(settings).filter(
        (pl.col("provider") == "dbnomics")
        & pl.col("dataset_id").is_not_null()
        & pl.col("file_name").str.contains("-series-")
    )
    observations: list[dict[str, object]] = []
    observation_dimensions: list[dict[str, object]] = []
    seen_artifacts: set[str] = set()
    for artifact in artifacts.sort("retrieved_at").to_dicts():
        digest = str(artifact["sha256"])
        if digest in seen_artifacts:
            continue
        seen_artifacts.add(digest)
        payload = json.loads(settings.path(str(artifact["relative_path"])).read_bytes())
        for doc in (payload.get("series") or {}).get("docs") or []:
            dataset_id = str(doc.get("dataset_code") or artifact["dataset_id"])
            series_id = str(doc.get("series_code") or "")
            dimensions = {str(k): str(v) for k, v in (doc.get("dimensions") or {}).items()}
            frequency = str(doc.get("@frequency") or _first(dimensions, ("FREQ",)) or "") or None
            periods = list(doc.get("period") or [])
            values = list(doc.get("value") or [])
            for index, period_value in enumerate(periods):
                period = str(period_value)
                oid = _obs_id(dataset_id, series_id, period)
                attrs = _attributes(doc, index)
                start, end = _period_bounds(period, frequency)
                projected = {
                    field: _first(dimensions, aliases)
                    for field, aliases in CORE_DIMENSION_MAP.items()
                }
                observations.append(
                    {
                        "observation_id": oid,
                        "provider": "DBnomics",
                        "agency": "OECD",
                        "dataset_id": dataset_id,
                        "dataset_version": artifact.get("dataset_version"),
                        "ref_area": projected["ref_area"],
                        "time_period": period,
                        "period_start": start,
                        "period_end": end,
                        "frequency": projected["frequency"] or frequency,
                        "measure": projected["measure"],
                        "unit_measure": projected["unit_measure"],
                        "unit_multiplier": _int(projected["unit_multiplier"]),
                        "price_base": projected["price_base"],
                        "adjustment": projected["adjustment"],
                        "activity": projected["activity"],
                        "counterpart_area": projected["counterpart_area"],
                        "sex": projected["sex"],
                        "age": projected["age"],
                        "employment_status": projected["employment_status"],
                        "education_level": projected["education_level"],
                        "obs_value": _float(values[index] if index < len(values) else None),
                        "obs_status": attrs.get("OBS_STATUS"),
                        "conf_status": attrs.get("CONF_STATUS"),
                        "break_status": attrs.get("BREAK") or attrs.get("BREAK_STATUS"),
                        "decimals": _int(str(attrs.get("DECIMALS")))
                        if attrs.get("DECIMALS") is not None
                        else None,
                        "additional_dimensions": json.dumps(
                            dimensions, sort_keys=True, separators=(",", ":")
                        ),
                        "source_artifact_sha256": digest,
                        "source_row_or_series_id": series_id,
                        "retrieved_at": artifact["retrieved_at"],
                        "source_last_updated": doc.get("indexed_at"),
                    }
                )
                observation_dimensions.extend(
                    {
                        "observation_id": oid,
                        "provider": "DBnomics",
                        "dataset_id": dataset_id,
                        "dimension_id": key,
                        "code": value,
                    }
                    for key, value in dimensions.items()
                )
    obs_frame = (
        pl.DataFrame(observations, schema=OBS_SCHEMA)
        if observations
        else pl.DataFrame(schema=OBS_SCHEMA)
    )
    dim_frame = (
        pl.DataFrame(observation_dimensions, schema=DIM_SCHEMA)
        if observation_dimensions
        else pl.DataFrame(schema=DIM_SCHEMA)
    )
    obs_path = settings.path("data/silver/observations/observations.parquet")
    obs_path.parent.mkdir(parents=True, exist_ok=True)
    obs_frame.write_parquet(obs_path, compression="zstd", statistics=True)
    dim_frame.write_parquet(
        settings.path("data/silver/observations/observation_dimensions.parquet"),
        compression="zstd",
        statistics=True,
    )
    return {
        "observations": obs_frame.height,
        "observation_dimensions": dim_frame.height,
        "datasets": obs_frame["dataset_id"].n_unique() if obs_frame.height else 0,
        "normalized_at": int(datetime.now(UTC).timestamp()),
    }
