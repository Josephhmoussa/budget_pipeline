from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from dagster import Definitions, RunRequest, asset, define_asset_job, schedule

from src.discover_excel import discover_excel
from src.ingest_bronze import ingest_bronze
from src.model_gold import build_gold
from src.serve_gold import build_gold_serving
from src.transform_silver import build_silver


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_config(config_path: str = "config/pipeline_config.json") -> dict:
    target = (PROJECT_ROOT / config_path).resolve()
    with target.open("r", encoding="utf-8") as f:
        return json.load(f)


def _default_run_month() -> str:
    return datetime.now().strftime("%Y-%m")


def run_discovery(source_root: str, fy: str) -> list[dict]:
    return [row for row in discover_excel(source_root) if row["fy"] == fy]


def run_bronze(discovered: list[dict], bronze_root: str, run_month: str) -> list[dict]:
    return ingest_bronze(discovered, bronze_root, run_month)


def run_silver(bronze_root: str, silver_root: str, fy: str, program_lookup: str = "", contractor_lookup: str = "") -> dict:
    return build_silver(bronze_root, silver_root, fy, program_lookup, contractor_lookup)


def run_gold(silver_root: str, gold_root: str, fy: str) -> dict:
    return build_gold(silver_root, gold_root, fy)


@asset(config_schema={"fy": str, "config_path": str})
def discovered_files(context) -> list[dict]:
    cfg = _load_config(context.op_config.get("config_path", "config/pipeline_config.json"))
    fy = context.op_config["fy"]
    rows = run_discovery(cfg["source_root"], fy)
    context.add_output_metadata({"selected_fy": len(rows)})
    return rows


@asset(config_schema={"fy": str, "run_month": str, "config_path": str}, deps=[discovered_files])
def bronze_loaded(context, discovered_files: list[dict]) -> list[dict]:
    cfg = _load_config(context.op_config.get("config_path", "config/pipeline_config.json"))
    bronze_root = str(PROJECT_ROOT / cfg["data_paths"]["bronze"])
    run_month = context.op_config.get("run_month", _default_run_month())
    loaded = run_bronze(discovered_files, bronze_root, run_month)
    context.add_output_metadata({"new_bronze": len(loaded)})
    return loaded


@asset(config_schema={"fy": str, "config_path": str}, deps=[bronze_loaded])
def silver_history(context) -> dict:
    cfg = _load_config(context.op_config.get("config_path", "config/pipeline_config.json"))
    fy = context.op_config["fy"]
    paths = run_silver(
        str(PROJECT_ROOT / cfg["data_paths"]["bronze"]),
        str(PROJECT_ROOT / cfg["data_paths"]["silver"]),
        fy,
        cfg.get("reference_files", {}).get("project_codes_lookup", ""),
        cfg.get("reference_files", {}).get("contractor_codes_lookup", ""),
    )
    context.add_output_metadata(paths)
    return paths


@asset(config_schema={"fy": str, "config_path": str}, deps=[silver_history])
def gold_fact(context) -> dict:
    cfg = _load_config(context.op_config.get("config_path", "config/pipeline_config.json"))
    fy = context.op_config["fy"]
    out = run_gold(str(PROJECT_ROOT / cfg["data_paths"]["silver"]), str(PROJECT_ROOT / cfg["data_paths"]["gold"]), fy)
    context.add_output_metadata(out)
    return out


@asset(config_schema={"config_path": str}, deps=[gold_fact])
def gold_serving(context) -> str:
    cfg = _load_config(context.op_config.get("config_path", "config/pipeline_config.json"))
    path = build_gold_serving(str(PROJECT_ROOT / cfg["data_paths"]["gold"]))
    context.add_output_metadata({"path": path})
    return path


budget_pipeline_job = define_asset_job("budget_pipeline_job")


@schedule(job=budget_pipeline_job, cron_schedule="0 6 * * *", execution_timezone="Europe/Paris")
def daily_budget_pipeline_schedule(_context):
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {
                "discovered_files": {"config": {"fy": "2026", "config_path": "config/pipeline_config.json"}},
                "bronze_loaded": {"config": {"fy": "2026", "run_month": _default_run_month(), "config_path": "config/pipeline_config.json"}},
                "silver_history": {"config": {"fy": "2026", "config_path": "config/pipeline_config.json"}},
                "gold_fact": {"config": {"fy": "2026", "config_path": "config/pipeline_config.json"}},
                "gold_serving": {"config": {"config_path": "config/pipeline_config.json"}},
            }
        },
    )


defs = Definitions(
    assets=[discovered_files, bronze_loaded, silver_history, gold_fact, gold_serving],
    jobs=[budget_pipeline_job],
    schedules=[daily_budget_pipeline_schedule],
)
