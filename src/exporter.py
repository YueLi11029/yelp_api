"""CSV export helpers for borough and master datasets."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[Mapping[str, object]]) -> int:
    """Write rows to CSV and return number of rows written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count
