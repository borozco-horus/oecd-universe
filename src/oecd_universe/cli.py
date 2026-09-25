from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from typing import Any

from rich.console import Console

from oecd_universe.acquisition.dbnomics import acquire
from oecd_universe.discovery.catalog import discover
from oecd_universe.metadata.reports import (
    build_mexico_coverage,
    compare_sources,
    estimate_storage,
    generate_reports,
)
from oecd_universe.metadata.snapshot import create_snapshot
from oecd_universe.normalization.dbnomics import normalize
from oecd_universe.settings import Settings
from oecd_universe.validation.checks import validate
from oecd_universe.warehouse.build import build_warehouse

CONSOLE = Console()


def _run(name: str, function: Callable[[Settings], dict[str, Any]], settings: Settings) -> None:
    CONSOLE.rule(name)
    result = function(settings)
    CONSOLE.print_json(json.dumps(result, default=str))


def run_all(settings: Settings) -> None:
    for name, function in (
        ("discover", discover),
        ("estimate", estimate_storage),
        ("acquire", acquire),
        ("normalize", normalize),
        ("validate", validate),
        ("mexico-report", build_mexico_coverage),
        ("source-comparison", compare_sources),
        ("snapshot", create_snapshot),
        ("warehouse", build_warehouse),
        ("report", generate_reports),
    ):
        _run(name, function, settings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="oecd-universe")
    parser.add_argument(
        "command",
        choices=[
            "discover",
            "estimate",
            "acquire",
            "normalize",
            "validate",
            "warehouse",
            "report",
            "mexico-report",
            "snapshot",
            "compare",
            "sync",
            "all",
        ],
    )
    args = parser.parse_args(argv)
    settings = Settings.load()
    commands: dict[str, Callable[[Settings], dict[str, Any]]] = {
        "discover": discover,
        "estimate": estimate_storage,
        "acquire": acquire,
        "normalize": normalize,
        "validate": validate,
        "warehouse": build_warehouse,
        "mexico-report": build_mexico_coverage,
        "snapshot": create_snapshot,
        "compare": compare_sources,
    }
    try:
        if args.command in ("all", "sync"):
            run_all(settings)
        elif args.command == "report":
            _run("mexico-report", build_mexico_coverage, settings)
            _run("source-comparison", compare_sources, settings)
            _run("report", generate_reports, settings)
        else:
            _run(args.command, commands[args.command], settings)
    except Exception:
        CONSOLE.print_exception()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
