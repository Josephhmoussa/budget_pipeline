from __future__ import annotations
from pathlib import Path
from zipfile import BadZipFile

import pandas as pd

from .io_utils import ensure_dir, read_rows_csv
from .silver_mapping import TARGET_COLS, load_contractor_lookup, load_program_lookup, map_actuals, map_budget, norm_cols


SOURCE_SHEETS = {"actuals": "OS extract", "budget": "Database"}


def _excel_engine(path: str) -> str | None:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xlsm"}:
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    if ext == ".xlsb":
        return "pyxlsb"
    return None


def _source_sheet(source: str) -> str:
    return SOURCE_SHEETS.get(source, "OS extract")


def _source_header(source: str) -> int:
    return 8 if source == "budget" else 0

def build_silver(bronze_root: str | Path, silver_root: str | Path, fy: str, program_lookup_file: str = "", contractor_lookup_file: str = "") -> dict:
    manifest = read_rows_csv(Path(bronze_root) / "manifest.csv")
    rows = [r for r in manifest if r.get("fy") == fy and not str(r.get("file_name", "")).startswith("~$")]
    try:
        program_lookup = load_program_lookup(program_lookup_file) if program_lookup_file else pd.DataFrame()
    except (FileNotFoundError, ValueError) as exc:
        print(f"[silver] warning: program lookup unavailable ({exc}); using empty lookup")
        program_lookup = pd.DataFrame()
    try:
        contractor_lookup = load_contractor_lookup(contractor_lookup_file) if contractor_lookup_file else pd.DataFrame()
    except (FileNotFoundError, ValueError) as exc:
        print(f"[silver] warning: contractor lookup unavailable ({exc}); using empty lookup")
        contractor_lookup = pd.DataFrame()
    loaded: list[tuple[dict, pd.DataFrame]] = []
    for row in rows:
        source = str(row.get("source", ""))
        engine = _excel_engine(str(row["bronze_path"]))
        if engine is None:
            print(f"[silver] warning: skipping unsupported extension {row['bronze_path']}")
            continue
        try:
            df = norm_cols(
                pd.read_excel(
                    row["bronze_path"],
                    engine=engine,
                    sheet_name=_source_sheet(source),
                    header=_source_header(source),
                )
            )
        except ImportError as exc:
            print(f"[silver] warning: missing Excel engine '{engine}' for {row['bronze_path']} ({exc})")
            continue
        except (FileNotFoundError, BadZipFile, ValueError) as exc:
            print(f"[silver] warning: skipping unreadable source {row['bronze_path']} ({exc})")
            continue
        loaded.append((row, df))

    frames: list[pd.DataFrame] = []
    account_lookup = pd.DataFrame(columns=["account_code", "account_name"])
    for row, df in loaded:
        if row["source"] != "actuals":
            continue
        out = map_actuals(df, fy, program_lookup)
        out["snapshot_key"] = row.get("snapshot_month") or f"{row.get('snapshot_fy', fy)}-12"
        frames.append(out)
        account_lookup = pd.concat([account_lookup, out[["account_code", "account_name"]]], ignore_index=True)

    if not account_lookup.empty:
        account_lookup = account_lookup[(account_lookup["account_code"] != "") & (account_lookup["account_name"] != "")]
        account_lookup = account_lookup.drop_duplicates(subset=["account_code"], keep="first")

    for row, df in loaded:
        if row["source"] != "budget":
            continue
        out = map_budget(df, fy, account_lookup, contractor_lookup)
        out["snapshot_key"] = row.get("snapshot_month") or f"{row.get('snapshot_fy', fy)}-12"
        frames.append(out)
    if not frames:
        return {"history": "", "latest": ""}
    history = pd.concat(frames, ignore_index=True)
    history["amount"] = pd.to_numeric(history["amount"], errors="coerce").fillna(0.0)
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    keys = ["cost_center_code", "cost_center_name", "group_cost_nature", "cost_nature", "account_code", "account_name", "currency", "supplier_name", "cpx_opx", "details", "bubble", "portfolio", "product_code", "product_name", "date", "scenario", "program"]
    latest = history.sort_values("snapshot_key").drop_duplicates(subset=keys, keep="last")
    history = history[TARGET_COLS]
    latest = latest[TARGET_COLS]
    target = ensure_dir(Path(silver_root) / f"fy={fy}")
    history_path = target / "silver_finance_history.parquet"
    latest_path = target / "silver_finance_latest.parquet"
    history.to_parquet(history_path, index=False)
    latest.to_parquet(latest_path, index=False)
    return {"history": str(history_path), "latest": str(latest_path)}
