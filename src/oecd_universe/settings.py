from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseModel):
    root: Path = ROOT
    oecd_dataflow_url: str
    dbnomics_catalog_url: str
    dbnomics_series_url: str
    dbnomics_repository: str
    user_agent: str
    catalog_page_size: int = 100
    series_page_size: int = 1000
    max_acquisition_datasets: int = 10
    max_series_per_dataset: int = 5000
    safe_free_space_fraction: float = 0.70
    request_delay_seconds: float = 0.25
    priority_terms: list[str]

    @classmethod
    def load(cls, root: Path | None = None) -> Settings:
        root = root or ROOT
        values: dict[str, Any] = {"root": root}
        for rel in ("configs/sources/sources.json", "configs/sync/policy.json"):
            values.update(json.loads((root / rel).read_text()))
        return cls.model_validate(values)

    def path(self, relative: str) -> Path:
        return self.root / relative
