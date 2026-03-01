from __future__ import annotations

from src.discover_excel import discover_excel
from src.ingest_bronze import ingest_bronze
from src.model_gold import build_gold
from src.transform_silver import build_silver


def run_discovery(source_root: str, fy: str) -> list[dict]:
    return [row for row in discover_excel(source_root) if row["fy"] == fy]


def run_bronze(discovered: list[dict], bronze_root: str, run_month: str) -> list[dict]:
    return ingest_bronze(discovered, bronze_root, run_month)


def run_silver(bronze_root: str, silver_root: str, fy: str) -> dict:
    return build_silver(bronze_root, silver_root, fy)


def run_gold(silver_root: str, gold_root: str, fy: str) -> dict:
    return build_gold(silver_root, gold_root, fy)
