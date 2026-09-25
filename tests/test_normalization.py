from __future__ import annotations

import json

import polars as pl

from oecd_universe.normalization.dbnomics import normalize
from oecd_universe.provenance.artifacts import register_bytes
from oecd_universe.settings import Settings


def test_normalization_preserves_all_dimensions(settings: Settings) -> None:
    payload = {
        "series": {
            "docs": [
                {
                    "dataset_code": "DSD_X@DF_X",
                    "series_code": "MEX.A.TOT.SPECIAL",
                    "@frequency": "A",
                    "dimensions": {
                        "REF_AREA": "MEX",
                        "FREQ": "A",
                        "MEASURE": "TOT",
                        "SPECIAL_DIMENSION": "X",
                    },
                    "period": ["2020", "2021-Q1"],
                    "value": [1.25, 2.5],
                    "indexed_at": "2026-01-01",
                }
            ]
        }
    }
    register_bytes(
        settings,
        provider="dbnomics",
        source_url="https://example.test/series",
        content=json.dumps(payload).encode(),
        file_name="DSD_X-series-0000000.json",
        dataset_id="DSD_X@DF_X",
        dataset_version="1.0",
    )
    result = normalize(settings)
    observations = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    dimensions = pl.read_parquet(
        settings.path("data/silver/observations/observation_dimensions.parquet")
    )
    assert result["observations"] == 2
    assert observations["ref_area"].to_list() == ["MEX", "MEX"]
    assert observations["period_start"].to_list()[0].isoformat() == "2020-01-01"
    assert observations["period_start"].to_list()[1] is None
    assert json.loads(observations["additional_dimensions"][0])["SPECIAL_DIMENSION"] == "X"
    assert dimensions.filter(pl.col("dimension_id") == "SPECIAL_DIMENSION").height == 2


def test_normalization_is_reproducible(settings: Settings) -> None:
    payload = {
        "series": {
            "docs": [
                {
                    "dataset_code": "X",
                    "series_code": "MEX.A",
                    "dimensions": {"LOCATION": "MEX", "FREQ": "A"},
                    "period": ["2020"],
                    "value": [1],
                }
            ]
        }
    }
    register_bytes(
        settings,
        provider="dbnomics",
        source_url="https://example.test/x",
        content=json.dumps(payload).encode(),
        file_name="X-series-0000000.json",
        dataset_id="X",
    )
    normalize(settings)
    first = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    normalize(settings)
    second = pl.read_parquet(settings.path("data/silver/observations/observations.parquet"))
    assert first["observation_id"].to_list() == second["observation_id"].to_list()
