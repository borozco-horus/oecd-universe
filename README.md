# oecd-universe

![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-2ea44f)
![Data](https://img.shields.io/badge/data-local%20%26%20auditable-0f766e)

An auditable, storage-aware foundation for discovering and structuring the OECD public-data universe.
It turns OECD/DBnomics metadata and observations into reproducible BRONZE/SILVER artifacts and a
queryable DuckDB view—without pretending that raw data is a research-ready GOLD panel.

> **Status:** early foundation (v0.1). The repository contains code and configuration, not downloaded
> datasets. Acquisition is deliberately selective and guarded by a storage estimate.

## Design

The canonical model uses an observation fact table plus an observation-dimension bridge. Selected common
dimensions are projected into nullable columns for efficient filtering, while every source dimension is
also retained in `observation_dimensions.parquet` and as deterministic JSON in `additional_dimensions`.
This avoids hundreds of sparse columns without collapsing dataset-specific semantics. Codes such as
`TOT` are always scoped by provider, dataset, and dimension.

BRONZE files are content-addressed by SHA-256 and never overwritten. `artifacts.parquet` records source
URL, retrieval time, HTTP validators, dataset/version, size, and checksum. Snapshots contain metadata and
hash references only; they do not duplicate artifacts.

## Quick start

```bash
make discover
make estimate
uv sync
make test
make discover
make estimate
```

The complete workflow is available with `make oecd-universe`. Discovery always precedes downloads, and
`configs/sync/policy.json` controls selective acquisition so a run can stop safely before filling a disk.

## What it provides

- Source-aware catalog discovery across OECD SDMX and DBnomics.
- Content-addressed BRONZE artifacts with checksums and retrieval metadata.
- Typed SILVER observations plus a dimension bridge that preserves dataset-specific semantics.
- Validation, Mexico coverage reports, snapshots, and DuckDB/Parquet querying.
- Tests and configuration designed for reproducible local runs.

## Warehouse examples

```sql
SELECT * FROM observations WHERE ref_area = 'MEX';

SELECT dataset_id, activity, measure, time_period, obs_value, unit_measure
FROM observations
WHERE ref_area = 'MEX'
  AND dataset_id = 'DATASET_ID'
ORDER BY time_period;

SELECT o.dataset_id, d.dimension_id, d.code, o.time_period, o.obs_value
FROM observations o
JOIN observation_dimensions d USING (observation_id, provider, dataset_id)
WHERE o.ref_area = 'MEX' AND d.dimension_id = 'ACTIVITY';
```

DuckDB views scan Parquet directly; observations are not redundantly materialized in the database.

## Scientific boundary

DBnomics is used for broad normalized acquisition and discovery, while official OECD SDMX metadata is the
authority for dataflow identity, version, descriptions, and related bulk resources. Exact source identity
is never inferred from similar titles. Research-critical datasets must be verified against official OECD
values before variable construction.

## Technical walkthrough

[Bruno Orozco Mejía's technical walkthrough](https://brunoorozco.com/proyectos/oecd-universe/)
explains the artifact hashes, dimension model, DuckDB views, reproducible tests and limits of v0.1
in Spanish. For professional background and contact, visit [Bruno Orozco Mejía](https://brunoorozco.com/).

## Contributing

Focused issues and pull requests are welcome. Please keep provenance intact, avoid committing generated
data, and include tests for changes to the canonical model or validation rules.
