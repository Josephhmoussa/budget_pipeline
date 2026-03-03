import pandas as pd
from src.silver_mapping import norm_cols, pick

actuals_path = '/Users/josephmoussa/Desktop/mock_structure/2026/atil/fy26_actuals/january_2026/actuals_2026_data.xlsx'
lookup_path = '/Users/josephmoussa/Desktop/Cost Centers/Project Codes Consolidation/ATIL Project Codes.xlsx'

actuals = norm_cols(pd.read_excel(actuals_path, sheet_name='OS extract', header=0))
actuals_code = pick(actuals, ['project', 'project_code'], '').astype(str).str.strip()
actuals_code = actuals_code.str.upper().str.replace(r'\.0$', '', regex=True)

lookup = norm_cols(pd.read_excel(lookup_path, sheet_name='program_reference'))
lookup_code = pick(lookup, ['project_code', 'project'], '').astype(str).str.strip()
lookup_code = lookup_code.str.upper().str.replace(r'\.0$', '', regex=True)
lookup_program = pick(lookup, ['program1', 'program'], '').astype(str).str.strip()

lookup_df = pd.DataFrame({'project_code': lookup_code, 'program': lookup_program})
lookup_df = lookup_df[(lookup_df['project_code'] != '') & (lookup_df['program'] != '')].drop_duplicates('project_code')

actuals_df = pd.DataFrame({'project_code': actuals_code})
actuals_df = actuals_df[actuals_df['project_code'] != '']
merged = actuals_df.merge(lookup_df, on='project_code', how='left')

print('actual_rows', len(actuals_df))
print('lookup_keys', lookup_df['project_code'].nunique())
print('matched_rows', int(merged['program'].notna().sum()))
print('unmatched_rows', int(merged['program'].isna().sum()))
print('sample_unmatched', merged[merged['program'].isna()]['project_code'].drop_duplicates().head(20).tolist())

silver = pd.read_parquet('data/silver/fy=2026/silver_finance_latest.parquet')
a = silver[silver['scenario'] == 'ACTUALS']
print('silver_actuals_other', int((a['program'].astype(str).str.lower() == 'other').sum()), 'of', len(a))
