# Budget Pipeline

## Run
- From project root: `python3 run_pipeline.py --fy 2026`
- Optional args:
  - `python3 run_pipeline.py --fy 2026 --run-month 2026-03`
  - `python3 run_pipeline.py --fy 2026 --config config/pipeline_config.json`

## Config
- Copy the template: `cp config/pipeline_config.example.json config/pipeline_config.json`
- Main local config (git-ignored): `config/pipeline_config.json`
- Repo-safe template: `config/pipeline_config.example.json`
- `source_root`: source files root path
- `data_paths`: bronze/silver/gold output folders (relative to project root)
- `reference_files.project_codes_lookup`: lookup Excel used to enrich actuals `program`
- `reference_files.contractor_codes_lookup`: contractor lookup Excel used to enrich budget `program` from supplier keyword matching

## Outputs
- Bronze: `data/bronze/fy=YYYY/...`
- Silver: `data/silver/fy=YYYY/silver_finance_history.parquet`
- Gold: `data/gold/fy=YYYY/fact_finance.parquet`
- Serving (all FY): `data/gold/fact_finance_all.parquet`
- Run log: `logs/run_report.csv`

## Dagster
- Install deps: `pip install -r requirements.txt`
- Start UI from project root:
  - `dagster dev -m orchestration.dagster_assets`
- Materialize all assets using job `budget_pipeline_job`
- Enable schedule `daily_budget_pipeline_schedule` to trigger daily at 06:00 (Europe/Paris)
- Provide run config in Dagster launchpad, for example:
  - `discovered_files`: `{"config": {"fy": "2026", "config_path": "config/pipeline_config.json"}}`
  - `bronze_loaded`: `{"config": {"fy": "2026", "run_month": "2026-03", "config_path": "config/pipeline_config.json"}}`
  - `silver_history`: `{"config": {"fy": "2026", "config_path": "config/pipeline_config.json"}}`
  - `gold_fact`: `{"config": {"fy": "2026", "config_path": "config/pipeline_config.json"}}`
  - `gold_serving`: `{"config": {"config_path": "config/pipeline_config.json"}}`

## Tools
- Populate sample project-code mapping: `python3 tools/populate_project_codes_mapping.py`
- Refresh Excel exports: `python3 tools/regenerate_output_excels.py`
