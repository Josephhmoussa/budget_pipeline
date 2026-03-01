# Budget Pipeline Roadmap (Bronze → Silver → Gold)

## 1) Goal
Build a lightweight pipeline that:
1. Crawls `mock_structure/**` for Excel files.
2. Copies raw files into `budget_pipeline` Bronze storage.
3. Transforms to Silver (clean, standardized, deduplicated business records).
4. Models Gold datasets for reporting.

This roadmap follows your constraints:
- efficient code
- each module ≤ 100 lines
- easy Dagster integration later
- no overengineering

## 2) Recommended Medallion Design
### Bronze (raw, immutable)
- Store exact source files + minimal ingest metadata.
- Partition by fiscal year (`fy`) and source type (`actuals|budget`).
- Keep source snapshots because files are cumulative.

Path pattern:
- `data/bronze/fy=YYYY/source=actuals/ingest_month=YYYY-MM/*.xlsx`
- `data/bronze/fy=YYYY/source=budget/ingest_year=YYYY/*.xlsx`

Metadata file per ingested file (JSON/CSV):
- `file_path`, `file_name`, `file_hash`, `ingest_ts`, `fy`, `source`, `snapshot_month`

### Silver (clean + conformed)
- Read Bronze files, standardize schema, data types, and column names.
- Deduplicate by business key + period + metric + cost_center (or your key).
- Because data is cumulative, keep latest record per key-period using max `snapshot_month`.

Path pattern:
- `data/silver/fy=YYYY/*.parquet`

Core Silver outputs:
- `silver_actuals_latest`
- `silver_budget_latest`
- optional `silver_actuals_history` / `silver_budget_history` (if audit needed)

### Gold (business-ready marts)
- Build analysis tables from Silver latest datasets.

Path pattern:
- `data/gold/fy=YYYY/*.parquet`

Core Gold outputs (MVP):
- `gold_budget_vs_actual_monthly`
- `gold_variance_ytd`
- `gold_cost_center_summary`

## 3) Handling Cumulative Source Correctly (Important)
Best practice for your source pattern:
1. **Actuals:** ingest every monthly file as a snapshot from month folders (do not overwrite).
2. **Budget:** ingest one annual cumulative file from each FY folder (do not overwrite).
3. In Silver, compute **latest valid row per business grain** using `snapshot_month` for actuals and `snapshot_fy` for budget.
4. Keep optional history table for traceability.

Why this is best here:
- preserves lineage and audit
- prevents accidental data loss when source restates old months
- stays simple (no heavy CDC framework)

## 4) Folder Structure in `budget_pipeline`
```
budget_pipeline/
  src/
    discover_excel.py        # crawl + list files
    ingest_bronze.py         # copy files + metadata
    transform_silver.py      # standardize + dedupe latest
    model_gold.py            # marts
    io_utils.py              # shared file/io helpers
    schema_utils.py          # column mappings/types
  data/
    bronze/
    silver/
    gold/
  orchestration/
    dagster_assets.py        # later, map each step to assets
  logs/
  ROADMAP.md
```

## 5) Dagster-Ready Strategy (without implementing Dagster now)
Design each script as pure functions with clear inputs/outputs:
- `discover_excel(root_path) -> list[dict]`
- `ingest_bronze(file_records, bronze_root, run_month) -> manifest`
- `build_silver(bronze_root, silver_root, fy) -> table_paths`
- `build_gold(silver_root, gold_root, fy) -> table_paths`

Later, each function becomes one Dagster asset/op with minimal rewrite.

## 6) Implementation Plan (MVP first)
1. Build file discovery for:
  - actuals: `mock_structure/**/fy*_actuals/*/*.xlsx` (month folders)
  - budget: `mock_structure/**/fy*_budget/*.xlsx` (FY folder)
2. Add Bronze ingest with hash-based idempotency (skip unchanged files).
3. Build Silver standardization + latest-per-key logic.
4. Build Gold `budget vs actual` + variance outputs.
5. Add simple run script + logging.
6. Then wrap in Dagster assets.

## 7) Practical Rules to Stay Within Your Constraints
- Keep each module < 100 lines; split by responsibility, not by layer complexity.
- Prefer `pandas + pyarrow` only (avoid extra frameworks now).
- Use append-only Bronze; deterministic rebuild for Silver/Gold per `fy`.
- Start with one robust business key contract and document it.

## 8) Recommended Partitioning Decision
Use fiscal-year partitions in all layers, and ingest-month snapshots in Bronze:
- Bronze: `fy + source + ingest_month`
- Silver: `fy` (and optionally `scenario=actual|budget`)
- Gold: `fy`

This balances performance, simplicity, and downstream Dagster partitioning.

## 9) Gold Modeling Recommendation (Unified Fact)
Yes—**a unified fact schema is the best option** for your Gold layer in this case.

Recommended Gold star model:
- `fact_finance` (grain: `fy, period_month, cost_center, account, scenario`)
- `scenario` values: `ACTUAL`, `BUDGET`
- measures: `amount`, `ytd_amount`, optional `variance_to_budget`
- small conformed dimensions: `dim_date`, `dim_cost_center`, `dim_account`

Why this is best here:
- one table supports both reporting and variance logic efficiently
- easy to partition by `fy` and filter by `scenario`
- maps cleanly to Dagster assets and keeps implementation simple

## 10) First Deliverable (1–2 days)
- One command runs end-to-end for a target FY.
- Bronze populated from mock source.
- Silver latest tables produced.
- Gold `fact_finance` (+ a thin variance view/table) produced.
- Ready to map into Dagster asset graph.
