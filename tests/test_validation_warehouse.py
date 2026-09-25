from __future__ import annotations

import polars as pl

from oecd_universe.normalization.dbnomics import DIM_SCHEMA, OBS_SCHEMA
from oecd_universe.settings import Settings
from oecd_universe.warehouse.build import build_warehouse


def test_duckdb_queries_mexico(settings: Settings) -> None:
    observation = {name: None for name in OBS_SCHEMA}
    observation.update(
        {
            "observation_id": "1",
            "provider": "DBnomics",
            "agency": "OECD",
            "dataset_id": "X",
            "ref_area": "MEX",
            "time_period": "2020",
            "frequency": "A",
            "obs_value": 1.0,
            "additional_dimensions": "{}",
            "source_artifact_sha256": "a" * 64,
            "source_row_or_series_id": "MEX.A",
            "retrieved_at": "2026-01-01T00:00:00+00:00",
        }
    )
    pl.DataFrame([observation], schema=OBS_SCHEMA).write_parquet(
        settings.path("data/silver/observations/observations.parquet")
    )
    pl.DataFrame(schema=DIM_SCHEMA).write_parquet(
        settings.path("data/silver/observations/observation_dimensions.parquet")
    )
    result = build_warehouse(settings)
    assert result["observations"] == 1
    assert result["mexico_observations"] == 1
