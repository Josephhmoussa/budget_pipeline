from __future__ import annotations
from pathlib import Path
import pandas as pd
from .io_utils import ensure_dir


def build_gold(silver_root: str | Path, gold_root: str | Path, fy: str) -> dict:
    silver_latest = Path(silver_root) / f"fy={fy}" / "silver_finance_latest.parquet"
    if not silver_latest.exists():
        return {"fact": ""}

    try:
        fact = pd.read_parquet(silver_latest)
    except (FileNotFoundError, OSError, ValueError) as exc:
        print(f"[gold] warning: cannot read {silver_latest} ({exc})")
        return {"fact": ""}

    target = ensure_dir(Path(gold_root) / f"fy={fy}")
    fact_path = target / "fact_finance.parquet"
    try:
        fact.to_parquet(fact_path, index=False)
    except (OSError, ValueError) as exc:
        print(f"[gold] warning: cannot write {fact_path} ({exc})")
        return {"fact": ""}
    return {"fact": str(fact_path)}
