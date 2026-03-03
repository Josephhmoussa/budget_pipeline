from pathlib import Path
import random

import pandas as pd

actuals_path = Path('/Users/josephmoussa/Desktop/mock_structure/2026/atil/fy26_actuals/january_2026/actuals_2026_data.xlsx')
map_path = Path('/Users/josephmoussa/Desktop/Cost Centers/Project Codes Consolidation/ATIL Project Codes.xlsx')

src = pd.read_excel(actuals_path)
keys = src[['cost center code', 'project', 'task id', 'project name']].dropna().drop_duplicates().head(40)

programs = ['Network Modernization', 'Ops Transformation', 'Cloud Migration', 'Security Hardening', 'Data Platform']
statuses = ['Open', 'In Progress', 'Closed']
ref_statuses = ['Active', 'On Hold', 'Cancelled']
capex_opex = ['Capex', 'Opex']

rows = []
for _, row in keys.iterrows():
    rows.append({
        'cost center': row['cost center code'],
        'project code': row['project'],
        'task id': row['task id'],
        'project name': row['project name'],
        'project description': f"{row['project name']} description",
        'capex/opex': random.choice(capex_opex),
        'referential project status': random.choice(ref_statuses),
        'project status': random.choice(statuses),
        'comments': 'auto-generated sample mapping',
        'costs description': 'standard operating cost',
        'Program1': random.choice(programs),
    })

for i in range(5):
    rows.append({
        'cost center': f'CCC-X{i:03d}',
        'project code': f'PRJ-X-{i:03d}',
        'task id': f'TASK-X{i:04d}',
        'project name': f'Non Matching Project {i}',
        'project description': 'non matching sample',
        'capex/opex': random.choice(capex_opex),
        'referential project status': random.choice(ref_statuses),
        'project status': random.choice(statuses),
        'comments': 'non matching row',
        'costs description': 'other',
        'Program1': random.choice(programs),
    })

out = pd.DataFrame(rows)
map_path.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(map_path, engine='openpyxl') as writer:
    out.to_excel(writer, index=False, sheet_name='Reference')

print('mapping_file', map_path)
print('rows', len(out))
