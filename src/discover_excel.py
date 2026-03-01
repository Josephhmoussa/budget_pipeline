from __future__ import annotations

import re
from pathlib import Path


FY_RE = re.compile(r"fy(\d{2,4})", re.IGNORECASE)
MONTH_RE = re.compile(r"(20\d{2})[-_](0[1-9]|1[0-2])")
MONTH_NAME_RE = re.compile(
    r"(january|february|march|april|may|june|july|august|september|october|november|december)[-_](20\d{2})",
    re.IGNORECASE,
)

MONTH_NUM = {
    "january": "01",
    "february": "02",
    "march": "03",
    "april": "04",
    "may": "05",
    "june": "06",
    "july": "07",
    "august": "08",
    "september": "09",
    "october": "10",
    "november": "11",
    "december": "12",
}
EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xls", ".xlsb"}


def _normalize_fy(raw: str) -> str:
    return f"20{raw}" if len(raw) == 2 else raw


def _extract_snapshot_month(path_text: str) -> str | None:
    numeric = MONTH_RE.search(path_text)
    if numeric:
        return f"{numeric.group(1)}-{numeric.group(2)}"
    named = MONTH_NAME_RE.search(path_text)
    if named:
        month = MONTH_NUM[named.group(1).lower()]
        year = named.group(2)
        return f"{year}-{month}"
    return None


def discover_excel(source_root: str | Path) -> list[dict]:
    root = Path(source_root)
    records: list[dict] = []
    for excel_file in root.rglob("*"):
        if not excel_file.is_file() or excel_file.suffix.lower() not in EXCEL_EXTENSIONS:
            continue
        path_text = str(excel_file).lower()
        source = "actuals" if "actuals" in path_text else "budget" if "budget" in path_text else None
        if source is None:
            continue
        fy_match = FY_RE.search(path_text)
        if not fy_match:
            continue
        fy = _normalize_fy(fy_match.group(1))
        snapshot_month = _extract_snapshot_month(path_text)
        records.append(
            {
                "source_path": str(excel_file),
                "file_name": excel_file.name,
                "file_ext": excel_file.suffix.lower(),
                "fy": fy,
                "source": source,
                "snapshot_month": snapshot_month,
                "snapshot_fy": fy,
            }
        )
    return sorted(records, key=lambda r: (r["fy"], r["source"], r["file_name"]))
