"""Merge daily export files safely with de-duplication.

Primary use case:
- Merge legacy day17 files into day1 files in data/daily_exports.
- If day1 files already exist, merge instead of overwrite.

Examples:
    python src/merge_daily_data.py
    python src/merge_daily_data.py --from-day 17 --to-day 1
    python src/merge_daily_data.py --from-day 17 --to-day 1 --dry-run
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAILY_EXPORTS_DIR = PROJECT_ROOT / "data" / "daily_exports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge daily export files with de-duplication")
    parser.add_argument("--from-day", type=int, default=17, help="Source day number (default: 17)")
    parser.add_argument("--to-day", type=int, default=1, help="Target day number (default: 1)")
    parser.add_argument(
        "--exports-dir",
        type=str,
        default=str(DAILY_EXPORTS_DIR),
        help="Directory containing daily export files",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing files")
    return parser.parse_args()


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"Expected list JSON in {path}, got {type(data).__name__}")

    return [item for item in data if isinstance(item, dict)]


def _write_json_list(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def _dedupe_restaurants(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: dict[str, dict[str, Any]] = {}
    invalid_rows = 0

    for row in rows:
        business_id = row.get("business_id") or row.get("id")
        if not business_id:
            invalid_rows += 1
            continue
        deduped[str(business_id)] = row

    output = sorted(deduped.values(), key=lambda item: str(item.get("business_id") or item.get("id") or ""))
    return output, invalid_rows


def _dedupe_images(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    invalid_rows = 0

    for row in rows:
        business_id = str((row.get("business_id") or row.get("id") or "")).strip()
        image_url = str((row.get("image_url") or "")).strip()
        if not business_id or not image_url:
            invalid_rows += 1
            continue
        deduped[(business_id, image_url)] = row

    output = sorted(
        deduped.values(),
        key=lambda item: (
            str(item.get("business_id") or item.get("id") or ""),
            str(item.get("image_url") or ""),
        ),
    )
    return output, invalid_rows


def merge_dataset(
    source_path: Path,
    target_path: Path,
    dataset_name: str,
    dedupe_fn,
    dry_run: bool,
) -> None:
    source_rows = _load_json_list(source_path)
    target_rows = _load_json_list(target_path)

    if not source_rows and not target_rows:
        print(f"[{dataset_name}] No source/target data found. Skipping.")
        return

    if not source_rows and target_rows:
        print(f"[{dataset_name}] Source missing, target already exists. Nothing to merge.")
        return

    if source_rows and not target_rows:
        total_before = len(source_rows)
        total_after = len(source_rows)
        duplicates_removed = 0
        print(f"[{dataset_name}] total before merge: {total_before}")
        print(f"[{dataset_name}] total after merge:  {total_after}")
        print(f"[{dataset_name}] duplicates removed: {duplicates_removed}")
        print(f"[{dataset_name}] Target missing, renaming source -> target.")
        if dry_run:
            print(f"[{dataset_name}] DRY RUN: would rename {source_path.name} -> {target_path.name}")
            return

        source_path.rename(target_path)
        print(f"[{dataset_name}] Renamed {source_path.name} -> {target_path.name}")
        return

    total_before = len(source_rows) + len(target_rows)
    merged_rows, invalid_rows = dedupe_fn(target_rows + source_rows)
    total_after = len(merged_rows)
    duplicates_removed = total_before - total_after - invalid_rows

    print(f"[{dataset_name}] total before merge: {total_before}")
    print(f"[{dataset_name}] total after merge:  {total_after}")
    print(f"[{dataset_name}] duplicates removed: {max(duplicates_removed, 0)}")
    if invalid_rows:
        print(f"[{dataset_name}] invalid rows skipped: {invalid_rows}")

    if dry_run:
        print(f"[{dataset_name}] DRY RUN: would write merged data to {target_path.name}")
        print(f"[{dataset_name}] DRY RUN: would remove {source_path.name}")
        return

    _write_json_list(target_path, merged_rows)
    source_path.unlink(missing_ok=True)
    print(f"[{dataset_name}] Merged result saved to {target_path}")


def main() -> None:
    args = parse_args()
    exports_dir = Path(args.exports_dir)

    source_restaurants = exports_dir / f"day{args.from_day}_restaurants.json"
    source_images = exports_dir / f"day{args.from_day}_images.json"

    target_restaurants = exports_dir / f"day{args.to_day}_restaurants.json"
    target_images = exports_dir / f"day{args.to_day}_images.json"

    print("=" * 64)
    print("Daily Export Merge")
    print("=" * 64)
    print(f"From day: {args.from_day}")
    print(f"To day:   {args.to_day}")
    print(f"Dir:      {exports_dir}")
    print(f"Dry run:  {args.dry_run}")
    print("=" * 64)

    try:
        merge_dataset(
            source_path=source_restaurants,
            target_path=target_restaurants,
            dataset_name="restaurants",
            dedupe_fn=_dedupe_restaurants,
            dry_run=args.dry_run,
        )
        print("-" * 64)
        merge_dataset(
            source_path=source_images,
            target_path=target_images,
            dataset_name="images",
            dedupe_fn=_dedupe_images,
            dry_run=args.dry_run,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: merge failed: {exc}")
        raise SystemExit(1) from exc

    print("=" * 64)
    print("Merge completed.")
    print("=" * 64)


if __name__ == "__main__":
    main()
