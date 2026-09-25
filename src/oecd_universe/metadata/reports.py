from __future__ import annotations

import csv
import io
import json
import shutil
import time
from collections import Counter
from pathlib import Path

import httpx
import polars as pl

from oecd_universe.acquisition.http import SourceClient
from oecd_universe.provenance.artifacts import read_artifacts
from oecd_universe.settings import Settings

DOMAINS = {
    "capital stock": "capital stock|fixed assets",
    "labour compensation": "labour compensation|compensation of employees",
    "wages": "wage|earnings",
    "hours": "hours worked|working hours",
    "employment": "employment|labour force",
    "productivity": "productivity",
    "value added": "value added",
    "investment": "investment|gross fixed capital",
    "taxes": "tax|taxation",
    "inequality": "inequality|income distribution",
    "trade": "trade|export|import",
    "digitalisation": "digital|ict|information technolog",
    "business dynamics": "business dynamics|enterprise demography|business demography",
}


def _file_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def estimate_storage(settings: Settings) -> dict[str, int | None]:
    resources_path = settings.path("data/silver/datasets/official_bulk_resources.parquet")
    rows: list[dict[str, object]] = []
    if resources_path.exists():
        resources = pl.read_parquet(resources_path).filter(pl.col("format") == "parquet")
        client = httpx.Client(
            headers={"User-Agent": settings.user_agent}, timeout=30, follow_redirects=True
        )
        try:
            for row in resources.unique("url").to_dicts():
                size = None
                status = None
                error = None
                try:
                    response = client.head(str(row["url"]))
                    status = response.status_code
                    if response.is_success and response.headers.get("content-length"):
                        size = int(response.headers["content-length"])
                except httpx.HTTPError as exc:
                    error = repr(exc)
                rows.append({**row, "content_length": size, "http_status": status, "error": error})
                time.sleep(settings.request_delay_seconds)
        finally:
            client.close()
    schema = {
        "agency": pl.String,
        "dataset_id": pl.String,
        "version": pl.String,
        "label": pl.String,
        "url": pl.String,
        "format": pl.String,
        "content_length": pl.Int64,
        "http_status": pl.Int64,
        "error": pl.String,
    }
    estimates = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    estimates.write_parquet(
        settings.path("data/silver/datasets/official_bulk_estimates.parquet"), compression="zstd"
    )
    disk = shutil.disk_usage(settings.root)
    known_bulk = int(estimates["content_length"].sum() or 0) if estimates.height else 0
    unknown_bulk = estimates.filter(pl.col("content_length").is_null()).height
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    series_count = int(catalog["dbnomics_series"].drop_nulls().sum() or 0)
    assumed_rows = series_count * 20
    projected_silver = assumed_rows * 120
    result = {
        "disk_total": disk.total,
        "disk_free": disk.free,
        "known_official_bulk_bytes": known_bulk,
        "unknown_official_bulk_files": unknown_bulk,
        "dbnomics_series": series_count,
        "assumed_observations": assumed_rows,
        "projected_silver_bytes": projected_silver,
    }
    settings.path("data/manifests/storage_estimate.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    return result


def build_mexico_coverage(settings: Settings) -> dict[str, int]:
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    observations = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    known = catalog.filter(pl.col("contains_mexico").fill_null(False))
    mex = observations.filter(pl.col("ref_area") == "MEX")
    stats = (
        mex.group_by("dataset_id").agg(
            pl.len().alias("observation_count"),
            pl.col("time_period").min().alias("observed_start_period"),
            pl.col("time_period").max().alias("observed_end_period"),
            pl.col("frequency")
            .drop_nulls()
            .unique()
            .sort()
            .str.join(",")
            .alias("observed_frequencies"),
            pl.col("activity").drop_nulls().n_unique().alias("available_industries"),
        )
        if mex.height
        else pl.DataFrame(
            schema={
                "dataset_id": pl.String,
                "observation_count": pl.UInt32,
                "observed_start_period": pl.String,
                "observed_end_period": pl.String,
                "observed_frequencies": pl.String,
                "available_industries": pl.UInt32,
            }
        )
    )
    coverage = known.join(stats, on="dataset_id", how="left").with_columns(
        pl.col("observation_count").fill_null(0),
        pl.concat_str(
            [
                pl.col("dataset_id"),
                pl.col("title").fill_null(""),
                pl.col("description").fill_null(""),
            ],
            separator=" ",
        ).alias("search_text"),
    )
    for domain, pattern in DOMAINS.items():
        coverage = coverage.with_columns(
            pl.col("search_text")
            .str.to_lowercase()
            .str.contains(pattern)
            .alias(domain.replace(" ", "_"))
        )
    coverage = coverage.drop("search_text")
    coverage.write_parquet(
        settings.path("data/silver/datasets/mexico_coverage.parquet"), compression="zstd"
    )
    return {
        "known_datasets": coverage.height,
        "observations": mex.height,
        "materialized_datasets": mex["dataset_id"].n_unique() if mex.height else 0,
    }


def compare_sources(settings: Settings, limit: int = 3) -> dict[str, int]:
    observations = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    candidates = observations.join(
        catalog.filter(pl.col("sdmx_available") & pl.col("version").is_not_null()).select(
            ["dataset_id", "agency", "version"]
        ),
        on="dataset_id",
        how="inner",
    ).unique(["dataset_id", "source_row_or_series_id"])
    results: list[dict[str, object]] = []
    client = SourceClient(settings)
    try:
        for row in candidates.head(limit).to_dicts():
            dataset_id = str(row["dataset_id"])
            agency = str(row["agency"])
            version = str(row["version"])
            series = str(row["source_row_or_series_id"])
            url = (
                "https://sdmx.oecd.org/public/rest/data/"
                f"{agency},{dataset_id},{version}/{series}"
                "?dimensionAtObservation=AllDimensions&format=csvfile"
            )
            try:
                acquired = client.get(
                    url,
                    provider="oecd",
                    file_name=f"comparison-{dataset_id.replace('/', '_')}.csv",
                    dataset_id=dataset_id,
                    dataset_version=version,
                    accept="text/csv",
                )
                official_rows = list(
                    csv.DictReader(io.StringIO(acquired.content.decode("utf-8-sig")))
                )
                official = {
                    str(item.get("TIME_PERIOD")): item.get("OBS_VALUE") for item in official_rows
                }
                mirrored = observations.filter(
                    (pl.col("dataset_id") == dataset_id)
                    & (pl.col("source_row_or_series_id") == series)
                )
                checked = 0
                mismatches = 0
                for item in mirrored.select(["time_period", "obs_value"]).to_dicts():
                    if item["time_period"] not in official:
                        continue
                    checked += 1
                    try:
                        if (
                            abs(float(official[item["time_period"]]) - float(item["obs_value"]))
                            > 1e-12
                        ):
                            mismatches += 1
                    except (TypeError, ValueError):
                        mismatches += 1
                results.append(
                    {
                        "dataset_id": dataset_id,
                        "series_id": series,
                        "status": "compared",
                        "values_checked": checked,
                        "value_mismatches": mismatches,
                        "official_rows": len(official_rows),
                        "error": None,
                    }
                )
            except Exception as error:
                results.append(
                    {
                        "dataset_id": dataset_id,
                        "series_id": series,
                        "status": "failed",
                        "values_checked": 0,
                        "value_mismatches": 0,
                        "official_rows": 0,
                        "error": repr(error),
                    }
                )
    finally:
        client.close()
    schema = {
        "dataset_id": pl.String,
        "series_id": pl.String,
        "status": pl.String,
        "values_checked": pl.Int64,
        "value_mismatches": pl.Int64,
        "official_rows": pl.Int64,
        "error": pl.String,
    }
    frame = pl.DataFrame(results, schema=schema) if results else pl.DataFrame(schema=schema)
    frame.write_parquet(
        settings.path("data/manifests/source_comparison.parquet"), compression="zstd"
    )
    return {"series": frame.height, "compared": frame.filter(pl.col("status") == "compared").height}


def _fmt_bytes(value: int) -> str:
    return f"{value / (1024**3):,.3f} GiB"


def generate_reports(settings: Settings) -> dict[str, object]:
    report_dir = settings.path("reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    observations = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    dimensions = pl.read_parquet(settings.path("data/silver/dimensions/dimensions.parquet"))
    issues = pl.read_parquet(settings.path("data/quarantine/issues.parquet"))
    artifacts = read_artifacts(settings)
    mexico = pl.read_parquet(settings.path("data/silver/datasets/mexico_coverage.parquet"))
    failures = pl.read_parquet(settings.path("data/manifests/failed_datasets.parquet"))
    storage = json.loads(settings.path("data/manifests/storage_estimate.json").read_text())
    db_snapshot = json.loads(settings.path("data/manifests/dbnomics_snapshot.json").read_text())
    comparison_path = settings.path("data/manifests/source_comparison.parquet")
    comparison = pl.read_parquet(comparison_path) if comparison_path.exists() else pl.DataFrame()

    official_count = catalog.filter(pl.col("sdmx_available")).height
    db_count = catalog.filter(pl.col("dbnomics_available")).height
    bulk_count = catalog.filter(pl.col("bulk_available")).height
    materialized = observations["dataset_id"].n_unique() if observations.height else 0
    universe = f"""# OECD Universe

Generated from live source metadata.

- OECD-agency SDMX dataflows: {official_count:,}
- DBnomics OECD datasets: {db_count:,}
- Union catalog rows: {catalog.height:,}
- Dataflows with official Parquet related files: {bulk_count:,}
- Datasets known to expose MEX: {mexico.height:,}
- Materialized datasets in this constrained run: {materialized:,}

Fields not exposed by a catalog source remain null. They are not inferred. The union catalog
does not assert semantic identity unless the source dataset identifiers match exactly.
"""
    (report_dir / "UNIVERSE.md").write_text(universe)

    domain_counts = {
        domain: mexico.filter(pl.col(domain.replace(" ", "_"))).height for domain in DOMAINS
    }
    mexico_rows = "\n".join(
        f"- {domain}: {count:,} catalog datasets" for domain, count in domain_counts.items()
    )
    mex_obs = observations.filter(pl.col("ref_area") == "MEX")
    mex_start = mex_obs["time_period"].min() if mex_obs.height else None
    mex_end = mex_obs["time_period"].max() if mex_obs.height else None
    mex_report = f"""# Mexico Coverage

- Catalog datasets whose DBnomics codelists contain `MEX`: {mexico.height:,}
- Materialized MEX observations: {int(mexico["observation_count"].sum() or 0):,}
- Materialized MEX datasets: {mexico.filter(pl.col("observation_count") > 0).height:,}
- Earliest/latest materialized periods: {mex_start} / {mex_end}

## Keyword coverage (metadata mapping only)

{mexico_rows}

No economic variables or interpretations were constructed. `available_industries` is the count of
distinct materialized activity codes, not a harmonized industry classification.
"""
    (report_dir / "MEXICO_COVERAGE.md").write_text(mex_report)

    classes = Counter(issues["classification"].to_list()) if issues.height else Counter()
    categories = Counter(issues["category"].to_list()) if issues.height else Counter()
    category_rows = (
        "\n".join(f"- {key}: {value:,}" for key, value in sorted(categories.items())) or "- None"
    )
    quarantine_count = (
        issues.filter(pl.col("classification") == "QUARANTINE")["observation_id"]
        .drop_nulls()
        .n_unique()
    )
    quality = f"""# Data Quality

- Observations inspected: {observations.height:,}
- Valid (not linked to a quarantine-class issue): {observations.height - quarantine_count:,}
- Warning issues: {classes["WARN"]:,}
- Quarantined observation IDs: {quarantine_count:,}

## Issues

{category_rows}

Flags are non-destructive. Source observations remain in SILVER and issues point back to them.
"""
    (report_dir / "DATA_QUALITY.md").write_text(quality)

    known_required = storage["known_official_bulk_bytes"] + storage["projected_silver_bytes"]
    storage_report = f"""# Storage Plan

- Current BRONZE: {_fmt_bytes(_file_bytes(settings.path("data/bronze")))}
- Current SILVER: {_fmt_bytes(_file_bytes(settings.path("data/silver")))}
- Free local disk: {_fmt_bytes(storage["disk_free"])}
- Known official Parquet content lengths: {_fmt_bytes(storage["known_official_bulk_bytes"])}
- Official Parquet files with unknown size: {storage["unknown_official_bulk_files"]:,}
- DBnomics series in catalog: {storage["dbnomics_series"]:,}
- Planning assumption: 20 observations per series and 120 bytes per normalized observation
- Approximate SILVER under that explicit assumption: {_fmt_bytes(storage["projected_silver_bytes"])}
- Known/assumed lower-bound requirement: {_fmt_bytes(known_required)}

The full-universe size is not knowable from source catalogs because many bulk files omit
content length and DBnomics reports series rather than observations. With only
{_fmt_bytes(storage["disk_free"])} free, the executed run uses selective Priority-1
materialization. Expansion should use external object storage for content-addressed BRONZE
and retain local Parquet metadata/cache.
"""
    (report_dir / "STORAGE_PLAN.md").write_text(storage_report)

    stale = catalog.filter(
        pl.col("dbnomics_available") & pl.col("sdmx_available") & pl.col("last_updated").is_null()
    ).height
    freshness = f"""# Freshness

- DBnomics repository commit: `{db_snapshot.get("commit")}`
- DBnomics snapshot retrieved: {db_snapshot.get("retrieved_at")}
- Exact-match OECD/DBnomics datasets without a mirror update timestamp: {stale:,}
- OECD versions are recorded per catalog row and snapshot.

DBnomics is treated as a mirror, never as evidence that OECD has not changed.
Research-critical series must be rechecked against official SDMX before use.
"""
    (report_dir / "FRESHNESS.md").write_text(freshness)

    failed_rows = (
        "\n".join(
            f"- `{r['dataset_id']}` ({r['stage']}): {r['error']}" for r in failures.to_dicts()
        )
        or "No acquisition failures in the executed subset."
    )
    (report_dir / "FAILED_DATASETS.md").write_text(f"# Failed Datasets\n\n{failed_rows}\n")

    drift = issues.filter(pl.col("category") == "unknown_dimension")
    drift_rows = (
        "\n".join(f"- `{r['dataset_id']}`: {r['details']}" for r in drift.to_dicts())
        or "No dimension drift detected in the materialized subset."
    )
    (report_dir / "SCHEMA_DRIFT.md").write_text(f"# Schema Drift\n\n{drift_rows}\n")

    if comparison.height:
        comparison_rows = "\n".join(
            f"- `{r['dataset_id']}` / `{r['series_id']}`: {r['status']}; "
            f"checked={r['values_checked']}, value mismatches={r['value_mismatches']}, "
            f"official rows={r['official_rows']}, error={r['error']}"
            for r in comparison.to_dicts()
        )
    else:
        comparison_rows = "No exact-match materialized series were available for comparison."
    comparison_report = f"""# Source Comparison: OECD Official vs DBnomics

The comparison uses exact dataset identifiers and exact DBnomics series keys; it does not
guess mappings.

{comparison_rows}

Dimensions and dates are preserved in the normalized mirror records. A successful value comparison
matches overlapping `TIME_PERIOD` and numeric values. Failed or zero-overlap checks remain explicit.
"""
    (report_dir / "SOURCE_COMPARISON.md").write_text(comparison_report)
    return {
        "catalog": catalog.height,
        "observations": observations.height,
        "mexico_datasets": mexico.height,
        "dimensions": dimensions["dimension_id"].n_unique() if dimensions.height else 0,
        "artifacts": artifacts.height,
    }
