from __future__ import annotations
import re
from pathlib import Path
import pandas as pd
from .budget_program_mapping import derive_budget_program

TARGET_COLS = [
    "cost_center_code", "cost_center_name", "group_cost_nature", "cost_nature", "account", "account_name",
    "currency", "supplier_name", "cpx_opx", "details", "bubble", "portfolio", "product_code", "product_name",
    "date", "amount", "scenario", "program",
]
MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]; MONTH_NUM = {m: f"{i:02d}" for i, m in enumerate(MONTHS, start=1)}

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower())).strip("_") for c in df.columns]
    return df


def pick(df: pd.DataFrame, names: list[str], default: str = "") -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series([default] * len(df), index=df.index)


def split_code_name(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    parts = series.astype(str).str.split(" - ", n=1, expand=True)
    return (parts[0], parts[1]) if parts.shape[1] > 1 else (parts[0], parts[0])


def load_program_lookup(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)
    if not path.exists():
        return pd.DataFrame(columns=["project_code", "task_id", "project_name", "program"])
    lookup = norm_cols(pd.read_excel(path, engine="openpyxl"))
    out = pd.DataFrame()
    out["project_code"] = pick(lookup, ["project_code", "project"], "").astype(str).str.strip()
    out["program"] = pick(lookup, ["program"], "").astype(str).str.strip()
    return out[(out["program"] != "") & (out["project_code"] != "")][["project_code", "program"]].drop_duplicates()


def derive_program(df: pd.DataFrame, lookup: pd.DataFrame) -> pd.Series:
    if lookup.empty:
        return pd.Series(["other"] * len(df), index=df.index)
    keys = pd.DataFrame({"project_code": pick(df, ["project", "project_code"], "").astype(str).str.strip()})
    merged = keys.merge(lookup, on="project_code", how="left")
    return merged["program"].fillna("other")


def actuals_date(df: pd.DataFrame, fy: str) -> pd.Series:
    raw = pick(df, ["time", "date", "period", "month"], "")
    parsed = pd.to_datetime(raw, errors="coerce", format="mixed")
    if parsed.notna().any():
        return parsed
    year = pd.to_numeric(pick(df, ["year"], fy), errors="coerce").fillna(int(fy)).astype(int).astype(str)
    month = raw.astype(str).str.strip().str.lower().str[:3]
    m = month.map({k[:3]: v for k, v in MONTH_NUM.items()}).fillna("01")
    return pd.to_datetime(year + "-" + m + "-01", errors="coerce")


def map_actuals(df: pd.DataFrame, fy: str, lookup: pd.DataFrame) -> pd.DataFrame:
    acct_code, acct_name = split_code_name(pick(df, ["account"], ""))
    _, cc_name = split_code_name(pick(df, ["cost_center_name"], ""))
    prod_code, prod_name = split_code_name(pick(df, ["business_unit_final"], ""))
    cpx = pick(df, ["cpx_opx_n_or_y", "cpx_opx"], "").astype(str).str.strip().str.lower().map({"y": "Capex", "n": "Opex", "capex": "Capex", "opex": "Opex"}).fillna("")
    return pd.DataFrame({
        "cost_center_code": pick(df, ["cost_center_code"], ""), "cost_center_name": cc_name.fillna(""),
        "group_cost_nature": pick(df, ["group_cost_nature"], ""), "cost_nature": pick(df, ["cost_nature"], ""),
        "account": acct_code.fillna(""), "account_name": acct_name.fillna(""),
        "currency": pick(df, ["transaction_currency", "currency"], ""), "supplier_name": pick(df, ["supplier_name", "supplier"], ""),
        "cpx_opx": cpx, "details": pick(df, ["line_description", "details"], ""),
        "bubble": pick(df, ["bubble"], ""), "portfolio": pick(df, ["portfolio"], ""),
        "product_code": prod_code.fillna(""), "product_name": prod_name.fillna(""),
        "date": actuals_date(df, fy), "amount": pick(df, ["amount"], 0), "scenario": "ACTUALS", "program": derive_program(df, lookup),
    })


def map_budget(df: pd.DataFrame, fy: str) -> pd.DataFrame:
    month_cols = [m for m in MONTHS if m in df.columns]
    long = df.melt(id_vars=[c for c in df.columns if c not in month_cols], value_vars=month_cols, var_name="month_name", value_name="amount")
    acct_code, acct_name = split_code_name(pick(long, ["account"], ""))
    _, cc_name = split_code_name(pick(long, ["cost_center_name"], ""))
    prod_code_raw, prod_name = split_code_name(pick(long, ["product"], ""))
    details = pick(long, ["details"], "")
    return pd.DataFrame({
        "cost_center_code": pick(long, ["cost_center_code"], ""), "cost_center_name": cc_name.fillna(""),
        "group_cost_nature": pick(long, ["group_cost_nature", "group_cost_center"], ""), "cost_nature": pick(long, ["cost_nature"], ""),
        "account": acct_code.fillna(""), "account_name": acct_name.fillna(""),
        "currency": pick(long, ["currency"], ""), "supplier_name": pick(long, ["supplier_name", "supplier"], ""),
        "cpx_opx": pick(long, ["cpx_opx_capex_opex", "cpx_opx"], ""), "details": details,
        "bubble": pick(long, ["bubble"], ""), "portfolio": pick(long, ["portfolio"], ""),
        "product_code": pick(long, ["product_code"], prod_code_raw.fillna("")), "product_name": prod_name.fillna(""),
        "date": pd.to_datetime(fy + "-" + long["month_name"].map(MONTH_NUM).fillna("01") + "-01", errors="coerce"),
        "amount": long["amount"], "scenario": "BUDGET", "program": derive_budget_program(details),
    })
