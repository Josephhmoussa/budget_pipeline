from pathlib import Path
from openpyxl import load_workbook

BUDGET_FILE = Path('/Users/josephmoussa/Desktop/mock_structure/2026/atil/fy26_budget/budget_mock_data_2026.xlsx')
SHEET_NAME = 'Database'

if __name__ == '__main__':
    wb = load_workbook(BUDGET_FILE)
    ws = wb[SHEET_NAME]
    
    # Insert 8 blank rows at the top
    ws.insert_rows(1, 8)
    
    wb.save(BUDGET_FILE)
    print(f'UPDATED: {BUDGET_FILE}')
    print(f'Inserted 8 blank rows - header now at row 9')
