"""Merge borough CSV exports into weekly master restaurant/review/image CSV files."""

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
    EXPORT_DIR,
    IMAGES_CSV_FIELDNAMES,
    RESTAURANTS_CSV_FIELDNAMES,
    REVIEWS_CSV_FIELDNAMES,
    current_week_number,
    get_master_images_export_path,
    get_master_restaurants_export_path,
    get_master_reviews_export_path,
)
from src.exporter import write_csv  # noqa: E402


def parse_review_count(row: dict[str, str]) -> int:
    value = (row.get("review_count") or "").strip()
    try:
        return int(value)
    except ValueError:
        return -1


def load_weekly_files(week_number: int, data_type: str) -> list[Path]:
    pattern = f"*_{data_type}_week{week_number}.csv"
    return sorted(EXPORT_DIR.glob(pattern))


def merge_restaurants(csv_files: list[Path]) -> list[dict[str, str]]:
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

    ordered_rows: list[dict[str, str]] = []
    for business_id in sorted(merged.keys()):
        row = merged[business_id]
        ordered_rows.append({field: row.get(field, "") for field in RESTAURANTS_CSV_FIELDNAMES})
    return ordered_rows


def merge_reviews(csv_files: list[Path]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}

    for csv_file in csv_files:
        with csv_file.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                review_id = (row.get("review_id") or "").strip()
                if not review_id:
                    continue
                if review_id not in merged:
                    merged[review_id] = row

    ordered_rows: list[dict[str, str]] = []
    for review_id in sorted(merged.keys()):
        row = merged[review_id]
        ordered_rows.append({field: row.get(field, "") for field in REVIEWS_CSV_FIELDNAMES})
    return ordered_rows


def merge_images(csv_files: list[Path]) -> list[dict[str, str]]:
    seen_urls: set[str] = set()
    merged_rows: list[dict[str, str]] = []

    for csv_file in csv_files:
        with csv_file.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                image_url = (row.get("image_url") or "").strip()
                if not image_url or image_url in seen_urls:
                    continue
                seen_urls.add(image_url)
                merged_rows.append(
                    {
                        "business_id": row.get("business_id", ""),
                        "image_url": image_url,
                    }
                )

    return merged_rows


def run_merge(week_number: int) -> None:
    restaurant_files = load_weekly_files(week_number, "restaurants")
    review_files = load_weekly_files(week_number, "reviews")
    image_files = load_weekly_files(week_number, "images")

    if not restaurant_files and not review_files and not image_files:
        logging.warning("No borough exports found for week %d in %s", week_number, EXPORT_DIR)
        return

    merged_restaurants = merge_restaurants(restaurant_files)
    merged_reviews = merge_reviews(review_files)
    merged_images = merge_images(image_files)

    restaurants_master_path = get_master_restaurants_export_path(week_number)
    reviews_master_path = get_master_reviews_export_path(week_number)
    images_master_path = get_master_images_export_path(week_number)

    restaurants_total = write_csv(restaurants_master_path, RESTAURANTS_CSV_FIELDNAMES, merged_restaurants)
    reviews_total = write_csv(reviews_master_path, REVIEWS_CSV_FIELDNAMES, merged_reviews)
    images_total = write_csv(images_master_path, IMAGES_CSV_FIELDNAMES, merged_images)

    logging.info("Merged restaurant files: %d", len(restaurant_files))
    logging.info("Merged review files: %d", len(review_files))
    logging.info("Merged image files: %d", len(image_files))
    logging.info("Master restaurants CSV: %s (%d rows)", restaurants_master_path, restaurants_total)
    logging.info("Master reviews CSV: %s (%d rows)", reviews_master_path, reviews_total)
    logging.info("Master images CSV: %s (%d rows)", images_master_path, images_total)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge borough CSV files into weekly master CSV files")
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
