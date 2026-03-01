from __future__ import annotations
from pathlib import Path
import pandas as pd


def build_gold_serving(gold_root: str | Path) -> str:
    root = Path(gold_root)
    parts = sorted(root.glob("fy=*/fact_finance.parquet"))
    if not parts:
        return ""
    frames = [pd.read_parquet(path) for path in parts]
    merged = pd.concat(frames, ignore_index=True)
    out_path = root / "fact_finance_all.parquet"
    merged.to_parquet(out_path, index=False)
    return str(out_path)
