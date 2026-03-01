from __future__ import annotations
from pathlib import Path
from zipfile import BadZipFile

import pandas as pd

from .io_utils import ensure_dir, read_rows_csv
from .silver_mapping import TARGET_COLS, load_program_lookup, map_actuals, map_budget, norm_cols


def _excel_engine(path: str) -> str | None:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xlsm"}:
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    if ext == ".xlsb":
        return "pyxlsb"
    return None

def build_silver(bronze_root: str | Path, silver_root: str | Path, fy: str, program_lookup_file: str = "") -> dict:
    manifest = read_rows_csv(Path(bronze_root) / "manifest.csv")
    rows = [r for r in manifest if r.get("fy") == fy and not str(r.get("file_name", "")).startswith("~$")]
    try:
        program_lookup = load_program_lookup(program_lookup_file) if program_lookup_file else pd.DataFrame()
    except (FileNotFoundError, ValueError) as exc:
        print(f"[silver] warning: program lookup unavailable ({exc}); using empty lookup")
        program_lookup = pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for row in rows:
        engine = _excel_engine(str(row["bronze_path"]))
        if engine is None:
            print(f"[silver] warning: skipping unsupported extension {row['bronze_path']}")
            continue
        try:
            df = norm_cols(pd.read_excel(row["bronze_path"], engine=engine))
        except ImportError as exc:
            print(f"[silver] warning: missing Excel engine '{engine}' for {row['bronze_path']} ({exc})")
            continue
        except (FileNotFoundError, BadZipFile, ValueError) as exc:
            print(f"[silver] warning: skipping unreadable source {row['bronze_path']} ({exc})")
            continue
        out = map_actuals(df, fy, program_lookup) if row["source"] == "actuals" else map_budget(df, fy)
        out["snapshot_key"] = row.get("snapshot_month") or f"{row.get('snapshot_fy', fy)}-12"
        frames.append(out)
    if not frames:
        return {"history": "", "latest": ""}
    history = pd.concat(frames, ignore_index=True)
    history["amount"] = pd.to_numeric(history["amount"], errors="coerce").fillna(0.0)
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    keys = ["cost_center_code", "cost_center_name", "group_cost_nature", "cost_nature", "account", "account_name", "currency", "supplier_name", "cpx_opx", "details", "bubble", "portfolio", "product_code", "product_name", "date", "scenario", "program"]
    latest = history.sort_values("snapshot_key").drop_duplicates(subset=keys, keep="last")
    history = history[TARGET_COLS]
    latest = latest[TARGET_COLS]
    target = ensure_dir(Path(silver_root) / f"fy={fy}")
    history_path = target / "silver_finance_history.parquet"
    latest_path = target / "silver_finance_latest.parquet"
    history.to_parquet(history_path, index=False)
    latest.to_parquet(latest_path, index=False)
    return {"history": str(history_path), "latest": str(latest_path)}
