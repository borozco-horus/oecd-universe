from __future__ import annotations

import json
from urllib.parse import quote

import polars as pl
from rich.console import Console

from oecd_universe.acquisition.http import SourceClient
from oecd_universe.settings import Settings

CONSOLE = Console()


def select_priority_datasets(settings: Settings) -> list[dict[str, object]]:
    catalog = pl.read_parquet(settings.path("data/silver/datasets/catalog.parquet"))
    candidates = catalog.filter(
        pl.col("dbnomics_available").fill_null(False)
        & pl.col("contains_mexico").fill_null(False)
        & pl.col("dbnomics_series").is_not_null()
        & (pl.col("dbnomics_series") <= settings.max_series_per_dataset)
    ).to_dicts()
    chosen: list[dict[str, object]] = []
    used: set[str] = set()
    for term in settings.priority_terms:
        needle = term.lower()
        matches = [
            row
            for row in candidates
            if row["dataset_id"] not in used
            and needle
            in (
                f"{row.get('dataset_id', '')} {row.get('title', '')} {row.get('description', '')}"
            ).lower()
        ]
        if matches:
            row = sorted(matches, key=lambda item: int(item.get("dbnomics_series") or 0))[0]
            chosen.append(row)
            used.add(str(row["dataset_id"]))
        if len(chosen) >= settings.max_acquisition_datasets:
            break
    if len(chosen) < settings.max_acquisition_datasets:
        for row in sorted(candidates, key=lambda item: int(item.get("dbnomics_series") or 0)):
            if row["dataset_id"] not in used:
                chosen.append(row)
                used.add(str(row["dataset_id"]))
            if len(chosen) >= settings.max_acquisition_datasets:
                break
    return chosen


def acquire(settings: Settings) -> dict[str, int]:
    selected = select_priority_datasets(settings)
    pl.DataFrame(selected).write_parquet(
        settings.path("data/manifests/acquisition_plan.parquet"), compression="zstd"
    )
    failures: list[dict[str, object]] = []
    pages = 0
    acquired_datasets = 0
    client = SourceClient(settings)
    try:
        for row in selected:
            dataset_id = str(row["dataset_id"])
            offset = 0
            total = 1
            try:
                while offset < total:
                    encoded = quote(dataset_id, safe="")
                    url = (
                        settings.dbnomics_series_url.format(dataset_id=encoded)
                        + f"?limit={settings.series_page_size}&offset={offset}&observations=1"
                    )
                    page = client.get(
                        url,
                        provider="dbnomics",
                        file_name=f"{dataset_id.replace('/', '_')}-series-{offset:07d}.json",
                        dataset_id=dataset_id,
                        dataset_version=str(row.get("version") or "") or None,
                    )
                    payload = json.loads(page.content)
                    series = payload.get("series") or {}
                    docs = list(series.get("docs") or [])
                    total = int(series.get("num_found") or 0)
                    offset += len(docs)
                    pages += 1
                    CONSOLE.print(f"{dataset_id}: {min(offset, total)}/{total} series")
                    if not docs:
                        break
                acquired_datasets += 1
            except Exception as error:  # preserve failure and continue universe acquisition
                failures.append(
                    {"dataset_id": dataset_id, "stage": "acquire", "error": repr(error)}
                )
    finally:
        client.close()
    failure_path = settings.path("data/manifests/failed_datasets.parquet")
    if failures:
        pl.DataFrame(failures).write_parquet(failure_path, compression="zstd")
    else:
        pl.DataFrame(
            schema={"dataset_id": pl.String, "stage": pl.String, "error": pl.String}
        ).write_parquet(failure_path, compression="zstd")
    return {
        "selected": len(selected),
        "acquired": acquired_datasets,
        "pages": pages,
        "failed": len(failures),
    }
