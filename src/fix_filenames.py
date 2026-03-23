"""
Utility script to rename daily export files.

This script helps rename files from one day number to another, or from one date to another.
Supports renaming files in data/daily_exports/ directory.

Usage Examples:

# Rename day17 exports to day1
python fix_filenames.py --from-day 17 --to-day 1

# Rename day17 exports to a specific date (YYYY-MM-DD)
python fix_filenames.py --from-day 17 --to-date 2026-03-23

# Rename files from one date to another
python fix_filenames.py --from-date 2026-03-22 --to-date 2026-03-23

# Scan what would be renamed (dry run)
python fix_filenames.py --from-day 17 --to-day 1 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DAILY_EXPORTS_DIR = DATA_DIR / "daily_exports"
BOROUGH_EXPORTS_DIR = DATA_DIR / "borough_exports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rename daily export files from one day/date to another"
    )
    
    # Source specification
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--from-day",
        type=int,
        help="Source day number (1-31)",
    )
    source_group.add_argument(
        "--from-date",
        type=str,
        help="Source date (YYYY-MM-DD format)",
    )
    
    # Target specification
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--to-day",
        type=int,
        help="Target day number (1-31)",
    )
    target_group.add_argument(
        "--to-date",
        type=str,
        help="Target date (YYYY-MM-DD format)",
    )
    
    # Options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be renamed without actually renaming",
    )
    parser.add_argument(
        "--scope",
        choices=["daily_exports", "borough_exports", "both"],
        default="daily_exports",
        help="Which directory to scan for files (default: daily_exports)",
    )
    
    return parser.parse_args()


def get_source_pattern(from_day: int | None = None, from_date: str | None = None) -> str:
    """Generate source pattern for file matching."""
    if from_day is not None:
        return f"day{from_day}_"
    elif from_date is not None:
        return f"{from_date}_"
    else:
        raise ValueError("Either from_day or from_date must be provided")


def get_target_pattern(to_day: int | None = None, to_date: str | None = None) -> str:
    """Generate target pattern for file renaming."""
    if to_day is not None:
        return f"day{to_day}_"
    elif to_date is not None:
        return f"{to_date}_"
    else:
        raise ValueError("Either to_day or to_date must be provided")


def rename_files_in_directory(
    directory: Path,
    source_pattern: str,
    target_pattern: str,
    dry_run: bool = False,
) -> int:
    """
    Rename all files in directory matching source_pattern to use target_pattern.
    
    Returns count of renamed files.
    """
    if not directory.exists():
        logging.warning(f"Directory does not exist: {directory}")
        return 0
    
    renamed_count = 0
    files_to_rename = list(directory.glob(f"{source_pattern}*"))
    
    if not files_to_rename:
        logging.info(f"No files matching pattern '{source_pattern}' found in {directory}")
        return 0
    
    logging.info(f"Found {len(files_to_rename)} file(s) to rename in {directory}:")
    
    for old_file in files_to_rename:
        # Construct new filename
        stem = old_file.name[len(source_pattern):]  # Remove source pattern prefix
        new_filename = target_pattern + stem
        new_file = directory / new_filename
        
        logging.info(f"  {old_file.name} -> {new_filename}")
        
        if not dry_run:
            # Check if target already exists
            if new_file.exists():
                logging.error(f"  ERROR: Target file already exists: {new_file}")
                continue
            
            try:
                old_file.rename(new_file)
                renamed_count += 1
            except OSError as e:
                logging.error(f"  ERROR: Failed to rename: {e}")
    
    return renamed_count


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    
    args = parse_args()
    
    # Generate patterns
    source_pattern = get_source_pattern(args.from_day, args.from_date)
    target_pattern = get_target_pattern(args.to_day, args.to_date)
    
    # Display what we're doing
    print(f"\n{'='*60}")
    print(f"File Rename Operation")
    print(f"{'='*60}")
    print(f"Source pattern: {source_pattern}")
    print(f"Target pattern: {target_pattern}")
    if args.dry_run:
        print(f"Mode: DRY RUN (no files will be modified)")
    else:
        print(f"Mode: LIVE (files will be renamed)")
    print(f"{'='*60}\n")
    
    total_renamed = 0
    
    # Rename files in specified directories
    directories_to_process = []
    if args.scope in ["daily_exports", "both"]:
        directories_to_process.append(("daily_exports", DAILY_EXPORTS_DIR))
    if args.scope in ["borough_exports", "both"]:
        directories_to_process.append(("borough_exports", BOROUGH_EXPORTS_DIR))
    
    for dir_label, directory in directories_to_process:
        print(f"Processing: {dir_label}/")
        renamed = rename_files_in_directory(
            directory,
            source_pattern,
            target_pattern,
            args.dry_run,
        )
        total_renamed += renamed
        print()
    
    # Summary
    print(f"{'='*60}")
    if args.dry_run:
        print(f"Dry run complete. {total_renamed} file(s) would be renamed.")
    else:
        print(f"Operation complete. {total_renamed} file(s) renamed.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
