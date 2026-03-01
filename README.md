# Budget Pipeline

## Run
- From project root: `python3 run_pipeline.py --fy 2026`
- Optional args:
  - `python3 run_pipeline.py --fy 2026 --run-month 2026-03`
  - `python3 run_pipeline.py --fy 2026 --config config/pipeline_config.json`

## Config
- Main config: `config/pipeline_config.json`
- `source_root`: source files root path
- `data_paths`: bronze/silver/gold output folders (relative to project root)
- `reference_files.project_codes_lookup`: lookup Excel used to enrich actuals `program`

## Outputs
- Bronze: `data/bronze/fy=YYYY/...`
- Silver: `data/silver/fy=YYYY/silver_finance_{history,latest}.parquet`
- Gold: `data/gold/fy=YYYY/fact_finance.parquet`
- Serving (all FY): `data/gold/fact_finance_all.parquet`
- Run log: `logs/run_report.csv`

## Tools
- Populate sample project-code mapping: `python3 tools/populate_project_codes_mapping.py`
- Refresh Excel exports: `python3 tools/regenerate_output_excels.py`
