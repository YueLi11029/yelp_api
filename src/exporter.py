"""JSON export helpers for borough and master datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping


def write_json(path: Path, rows: Iterable[Mapping[str, object]]) -> int:
    """Write rows to JSON array and return number of rows written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    row_list = [dict(row) for row in rows]
    with path.open("w", encoding="utf-8") as f:
        json.dump(row_list, f, ensure_ascii=False, indent=2)
    return len(row_list)
