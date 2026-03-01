from __future__ import annotations

from datetime import datetime
from pathlib import Path
from shutil import copy2

from .io_utils import ensure_dir, file_hash, read_rows_csv, write_rows_csv


MANIFEST_COLS = [
    "file_name",
    "source_path",
    "bronze_path",
    "file_hash",
    "ingest_ts",
    "fy",
    "source",
    "snapshot_month",
    "snapshot_fy",
]


def ingest_bronze(file_records: list[dict], bronze_root: str | Path, run_month: str) -> list[dict]:
    bronze = ensure_dir(bronze_root)
    manifest_path = bronze / "manifest.csv"
    existing = read_rows_csv(manifest_path)
    known_hashes = {row["file_hash"] for row in existing}
    new_rows: list[dict] = []

    for rec in file_records:
        src = Path(rec["source_path"])
        try:
            digest = file_hash(src)
        except (FileNotFoundError, PermissionError, OSError) as e:
            print(f"[bronze] warning: cannot hash {src} ({e})")
            continue
        if digest in known_hashes:
            continue
        if rec["source"] == "actuals":
            partition = bronze / f"fy={rec['fy']}" / "source=actuals" / f"ingest_month={run_month}"
        else:
            partition = bronze / f"fy={rec['fy']}" / "source=budget" / f"ingest_year={rec['snapshot_fy']}"
        ensure_dir(partition)
        target = partition / src.name
        try:
            copy2(src, target)
        except (FileNotFoundError, PermissionError, OSError) as e:
            print(f"[bronze] warning: cannot copy {src} -> {target} ({e})")
            continue
        new_rows.append(
            {
                "file_name": src.name,
                "source_path": str(src),
                "bronze_path": str(target),
                "file_hash": digest,
                "ingest_ts": datetime.utcnow().isoformat(),
                "fy": rec["fy"],
                "source": rec["source"],
                "snapshot_month": rec.get("snapshot_month") or "",
                "snapshot_fy": rec["snapshot_fy"],
            }
        )
        known_hashes.add(digest)

    if new_rows:
        try:
            write_rows_csv(manifest_path, new_rows, MANIFEST_COLS)
        except OSError as e:
            print(f"[bronze] warning: cannot write manifest {manifest_path} ({e})")
    return new_rows
