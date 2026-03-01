from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from typing import Iterable


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def file_hash(file_path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(file_path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_rows_csv(file_path: str | Path, rows: Iterable[dict], fieldnames: list[str]) -> None:
    path = Path(file_path)
    ensure_dir(path.parent)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})


def read_rows_csv(file_path: str | Path) -> list[dict]:
    path = Path(file_path)
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
