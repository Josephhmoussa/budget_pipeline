from pathlib import Path

import pandas as pd


root = Path(__file__).resolve().parents[1]
fy = "2026"
out_dir = root / "output" / "excel" / f"fy={fy}"
out_dir.mkdir(parents=True, exist_ok=True)

silver_dir = root / "data" / "silver" / f"fy={fy}"
gold_dir = root / "data" / "gold" / f"fy={fy}"

silver_history = pd.read_parquet(silver_dir / "silver_finance_history.parquet")
with pd.ExcelWriter(out_dir / "silver_outputs.xlsx", engine="openpyxl") as writer:
    silver_history.to_excel(writer, index=False, sheet_name="history")

gold_fact = pd.read_parquet(gold_dir / "fact_finance.parquet")
with pd.ExcelWriter(out_dir / "gold_outputs.xlsx", engine="openpyxl") as writer:
    gold_fact.to_excel(writer, index=False, sheet_name="fact_finance")

print("silver_file", out_dir / "silver_outputs.xlsx")
print("gold_file", out_dir / "gold_outputs.xlsx")
print("rows", {
    "silver_history": len(silver_history),
    "gold_fact": len(gold_fact),
})
