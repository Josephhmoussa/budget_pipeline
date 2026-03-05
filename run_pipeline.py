from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from src.discover_excel import discover_excel
from src.ingest_bronze import ingest_bronze
from src.io_utils import ensure_dir, write_rows_csv
from src.model_gold import build_gold
from src.serve_gold import build_gold_serving
from src.transform_silver import build_silver


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def append_run_report(project_root: Path, run_row: dict) -> None:
    logs_dir = ensure_dir(project_root / "logs")
    report_path = logs_dir / "run_report.csv"
    fields = [
        "run_ts",
        "fy",
        "run_month",
        "discovered",
        "selected_fy",
        "new_bronze",
        "silver_history_path",
        "gold_fact_path",
        "gold_serving_path",
    ]
    write_rows_csv(report_path, [run_row], fields)


def main() -> None:
    project_root = Path(__file__).parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--fy", required=True)
    parser.add_argument("--config", default=str(project_root / "config" / "pipeline_config.json"))
    parser.add_argument("--run-month", default=datetime.now().strftime("%Y-%m"))
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    source_root = cfg["source_root"]
    bronze = project_root / cfg["data_paths"]["bronze"]
    silver = project_root / cfg["data_paths"]["silver"]
    gold = project_root / cfg["data_paths"]["gold"]
    program_lookup = cfg.get("reference_files", {}).get("project_codes_lookup", "")
    contractor_lookup = cfg.get("reference_files", {}).get("contractor_codes_lookup", "")

    discovered = discover_excel(source_root)
    selected = [r for r in discovered if r["fy"] == args.fy]
    loaded = ingest_bronze(selected, bronze, args.run_month)
    silver_paths = build_silver(bronze, silver, args.fy, program_lookup, contractor_lookup)
    gold_paths = build_gold(silver, gold, args.fy)
    serving_path = build_gold_serving(gold)
    summary = {
        "discovered": len(discovered),
        "selected_fy": len(selected),
        "new_bronze": len(loaded),
        "silver": silver_paths,
        "gold": gold_paths,
        "gold_serving": serving_path,
    }
    append_run_report(
        project_root,
        {
            "run_ts": datetime.now(UTC).isoformat(),
            "fy": args.fy,
            "run_month": args.run_month,
            "discovered": summary["discovered"],
            "selected_fy": summary["selected_fy"],
            "new_bronze": summary["new_bronze"],
            "silver_history_path": silver_paths.get("history", ""),
            "gold_fact_path": gold_paths.get("fact", ""),
            "gold_serving_path": serving_path,
        },
    )

    print(summary)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[run] cancelled by user")
        raise SystemExit(130)
    except Exception as exc:
        print(f"[run] failed: {exc}")
        raise
