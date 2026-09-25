from __future__ import annotations

import json
import subprocess
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import polars as pl
from rich.console import Console

from oecd_universe.acquisition.http import SourceClient
from oecd_universe.settings import Settings

CONSOLE = Console()
COMMON = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
STRUCTURE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
MESSAGE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

CATALOG_SCHEMA = {
    "dataset_id": pl.String,
    "agency": pl.String,
    "dataflow": pl.String,
    "version": pl.String,
    "title": pl.String,
    "description": pl.String,
    "source_primary": pl.String,
    "source_secondary": pl.String,
    "bulk_available": pl.Boolean,
    "bulk_format": pl.String,
    "dbnomics_available": pl.Boolean,
    "sdmx_available": pl.Boolean,
    "last_updated": pl.String,
    "contains_mexico": pl.Boolean,
    "frequency_codes": pl.String,
    "start_period": pl.String,
    "end_period": pl.String,
    "estimated_rows": pl.Int64,
    "estimated_bytes": pl.Int64,
    "acquisition_strategy": pl.String,
    "dbnomics_series": pl.Int64,
    "dbnomics_dir_hash": pl.String,
}


def _localized(node: ET.Element, tag: str) -> str | None:
    values = node.findall(COMMON + tag)
    for value in values:
        if value.attrib.get(XML_LANG) == "en":
            return value.text
    return values[0].text if values else None


def parse_oecd_dataflows(content: bytes) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    root = ET.fromstring(content)
    records: list[dict[str, object]] = []
    resources: list[dict[str, object]] = []
    for flow in root.findall(f".//{STRUCTURE}Dataflow"):
        agency = flow.attrib.get("agencyID", "")
        if not agency.startswith("OECD"):
            continue
        flow_id = flow.attrib.get("id", "")
        version = flow.attrib.get("version")
        has_parquet = False
        for annotation in flow.findall(f".//{COMMON}Annotation"):
            if annotation.findtext(COMMON + "AnnotationType") != "EXT_RESOURCE":
                continue
            text = _localized(annotation, "AnnotationText") or ""
            if "|" not in text:
                continue
            label, url = (part.strip() for part in text.split("|", 1))
            if not url.startswith("http"):
                continue
            resource_format = (
                "parquet" if "parquet" in label.lower() else Path(url).suffix.lower().lstrip(".")
            )
            has_parquet = has_parquet or resource_format == "parquet"
            resources.append(
                {
                    "agency": agency,
                    "dataset_id": flow_id,
                    "version": version,
                    "label": label,
                    "url": url,
                    "format": resource_format or None,
                }
            )
        records.append(
            {
                "dataset_id": flow_id,
                "agency": agency,
                "dataflow": flow_id,
                "version": version,
                "title": _localized(flow, "Name"),
                "description": _localized(flow, "Description"),
                "bulk_available": has_parquet,
                "bulk_format": "parquet" if has_parquet else None,
                "sdmx_available": True,
            }
        )
    return records, resources


def parse_dbnomics_documents(
    documents: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    datasets: list[dict[str, object]] = []
    dimensions: list[dict[str, object]] = []
    codelists: list[dict[str, object]] = []
    for doc in documents:
        dataset_id = str(doc["code"])
        order = list(doc.get("dimensions_codes_order") or [])
        labels = dict(doc.get("dimensions_labels") or {})
        values = dict(doc.get("dimensions_values_labels") or {})
        contains_mexico = any(
            "MEX" in dict(dimension_values) for dimension_values in values.values()
        )
        freq_values = dict(values.get("FREQ") or values.get("frequency") or {})
        datasets.append(
            {
                "dataset_id": dataset_id,
                "title": doc.get("name"),
                "description": doc.get("description"),
                "contains_mexico": contains_mexico,
                "frequency_codes": ",".join(sorted(freq_values)),
                "dbnomics_series": doc.get("nb_series"),
                "dbnomics_dir_hash": doc.get("dir_hash"),
                "last_updated": doc.get("indexed_at"),
            }
        )
        for position, dimension_id in enumerate(order):
            dimensions.append(
                {
                    "provider": "DBnomics",
                    "agency": "OECD",
                    "dataset_id": dataset_id,
                    "dimension_id": dimension_id,
                    "label": labels.get(dimension_id),
                    "description": None,
                    "position": position,
                    "source": "DBnomics dataset metadata",
                }
            )
            for code, label in dict(values.get(dimension_id) or {}).items():
                codelists.append(
                    {
                        "provider": "DBnomics",
                        "agency": "OECD",
                        "dataset_id": dataset_id,
                        "dimension_id": dimension_id,
                        "code": str(code),
                        "label": str(label),
                        "description": None,
                        "valid_from": None,
                        "valid_to": None,
                        "source": "DBnomics dataset metadata",
                    }
                )
    return datasets, dimensions, codelists


def _dbnomics_commit(settings: Settings) -> str | None:
    try:
        output = subprocess.check_output(
            ["git", "ls-remote", settings.dbnomics_repository, "HEAD"],
            text=True,
            timeout=60,
        )
        return output.split()[0]
    except (OSError, subprocess.SubprocessError, IndexError):
        return None


def _write_snapshot(settings: Settings, commit: str | None, count: int) -> None:
    snapshot = {
        "commit": commit,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "provider": "OECD",
        "repository": settings.dbnomics_repository,
        "coverage": {"datasets_reported_by_api": count},
    }
    path = settings.path("data/manifests/dbnomics_snapshot.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")


def discover(settings: Settings) -> dict[str, int]:
    client = SourceClient(settings)
    try:
        official = client.get(
            settings.oecd_dataflow_url,
            provider="oecd",
            file_name="dataflows.xml",
            accept="application/vnd.sdmx.structure+xml;version=2.1",
        )
        official_rows, bulk_rows = parse_oecd_dataflows(official.content)

        documents: list[dict[str, object]] = []
        offset = 0
        total = 1
        while offset < total:
            url = (
                f"{settings.dbnomics_catalog_url}"
                f"?limit={settings.catalog_page_size}&offset={offset}"
            )
            page = client.get(
                url,
                provider="dbnomics",
                file_name=f"datasets-{offset:05d}.json",
            )
            payload = json.loads(page.content)
            block = payload["datasets"]
            total = int(block["num_found"])
            docs = list(block["docs"])
            documents.extend(docs)
            offset += len(docs)
            if not docs:
                break
            CONSOLE.print(f"DBnomics catalog: {min(offset, total)}/{total}")
    finally:
        client.close()

    db_rows, dimension_rows, codelist_rows = parse_dbnomics_documents(documents)
    db_by_id = {str(row["dataset_id"]): row for row in db_rows}
    official_ids = {str(row["dataset_id"]) for row in official_rows}
    catalog: list[dict[str, object]] = []
    for row in official_rows:
        dataset_id = str(row["dataset_id"])
        mirror = db_by_id.get(dataset_id)
        db_available = mirror is not None
        bulk_available = bool(row["bulk_available"])
        strategy = (
            "official_bulk" if bulk_available else "dbnomics" if db_available else "sdmx_fallback"
        )
        catalog.append(
            {
                **row,
                "source_primary": "OECD official bulk"
                if bulk_available
                else "DBnomics"
                if db_available
                else "OECD SDMX",
                "source_secondary": "OECD SDMX",
                "dbnomics_available": db_available,
                "last_updated": mirror.get("last_updated") if mirror else None,
                "contains_mexico": mirror.get("contains_mexico") if mirror else None,
                "frequency_codes": mirror.get("frequency_codes") if mirror else None,
                "start_period": None,
                "end_period": None,
                "estimated_rows": None,
                "estimated_bytes": None,
                "acquisition_strategy": strategy,
                "dbnomics_series": mirror.get("dbnomics_series") if mirror else None,
                "dbnomics_dir_hash": mirror.get("dbnomics_dir_hash") if mirror else None,
            }
        )
    for row in db_rows:
        dataset_id = str(row["dataset_id"])
        if dataset_id in official_ids:
            continue
        catalog.append(
            {
                "dataset_id": dataset_id,
                "agency": "OECD",
                "dataflow": None,
                "version": None,
                "title": row.get("title"),
                "description": row.get("description"),
                "source_primary": "DBnomics",
                "source_secondary": None,
                "bulk_available": False,
                "bulk_format": None,
                "dbnomics_available": True,
                "sdmx_available": False,
                "last_updated": row.get("last_updated"),
                "contains_mexico": row.get("contains_mexico"),
                "frequency_codes": row.get("frequency_codes"),
                "start_period": None,
                "end_period": None,
                "estimated_rows": None,
                "estimated_bytes": None,
                "acquisition_strategy": "dbnomics",
                "dbnomics_series": row.get("dbnomics_series"),
                "dbnomics_dir_hash": row.get("dbnomics_dir_hash"),
            }
        )

    catalog_path = settings.path("data/silver/datasets/catalog.parquet")
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(catalog, schema=CATALOG_SCHEMA).sort(["agency", "dataset_id"]).write_parquet(
        catalog_path, compression="zstd"
    )
    pl.DataFrame(bulk_rows).write_parquet(
        settings.path("data/silver/datasets/official_bulk_resources.parquet"), compression="zstd"
    )
    pl.DataFrame(dimension_rows).write_parquet(
        settings.path("data/silver/dimensions/dimensions.parquet"), compression="zstd"
    )
    pl.DataFrame(codelist_rows).write_parquet(
        settings.path("data/silver/codelists/codelists.parquet"), compression="zstd"
    )
    commit = _dbnomics_commit(settings)
    _write_snapshot(settings, commit, len(db_rows))
    return {
        "official_dataflows": len(official_rows),
        "dbnomics_datasets": len(db_rows),
        "catalog_rows": len(catalog),
        "official_bulk_dataflows": len(
            {r["dataset_id"] for r in bulk_rows if r["format"] == "parquet"}
        ),
        "mexico_datasets_known": sum(bool(r.get("contains_mexico")) for r in catalog),
    }


def encoded_dataset_id(dataset_id: str) -> str:
    return quote(dataset_id, safe="")
