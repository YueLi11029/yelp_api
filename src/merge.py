"""Merge borough CSV exports into a single weekly master CSV."""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (  # noqa: E402
    CSV_FIELDNAMES,
    EXPORT_DIR,
    current_week_number,
    get_master_export_path,
)
from src.exporter import write_csv  # noqa: E402


def parse_review_count(row: dict[str, str]) -> int:
    value = (row.get("review_count") or "").strip()
    try:
        return int(value)
    except ValueError:
        return -1


def load_weekly_files(week_number: int) -> list[Path]:
    pattern = f"*_week{week_number}.csv"
    return sorted(EXPORT_DIR.glob(pattern))


def merge_rows(csv_files: list[Path]) -> dict[str, dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}

    for csv_file in csv_files:
        with csv_file.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                business_id = (row.get("id") or "").strip()
                if not business_id:
                    continue

                if business_id not in merged:
                    merged[business_id] = row
                    continue

                if parse_review_count(row) > parse_review_count(merged[business_id]):
                    merged[business_id] = row

    return merged


def run_merge(week_number: int) -> None:
    csv_files = load_weekly_files(week_number)
    if not csv_files:
        logging.warning("No borough exports found for week %d in %s", week_number, EXPORT_DIR)
        return

    merged_map = merge_rows(csv_files)
    output_path = get_master_export_path(week_number)

    ordered_rows = []
    for business_id in sorted(merged_map.keys()):
        row = merged_map[business_id]
        standardized = {field: row.get(field, "") for field in CSV_FIELDNAMES}
        ordered_rows.append(standardized)

    total = write_csv(output_path, CSV_FIELDNAMES, ordered_rows)

    logging.info("Merged %d CSV files.", len(csv_files))
    logging.info("Master CSV saved: %s", output_path)
    logging.info("Total unique restaurants: %d", total)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge borough CSV files into a weekly master CSV")
    parser.add_argument(
        "--week",
        type=int,
        default=current_week_number(),
        help="ISO week number to merge (default: current week)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    run_merge(args.week)


if __name__ == "__main__":
    main()
