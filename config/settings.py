"""Application settings for the NYC restaurant collection system."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = DATA_DIR / "borough_exports"
MASTER_DIR = DATA_DIR / "master"

YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"

# Yelp Fusion pagination constraints.
PAGE_LIMIT = 50
MAX_OFFSET = 1000  # valid offsets: 0..950 when PAGE_LIMIT is 50

# Per Yelp key request budget.
MAX_REQUESTS_PER_KEY_PER_DAY = 500

# Retry behavior.
REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1

BOROUGH_CHOICES = [
    "Manhattan",
    "Brooklyn",
    "Queens",
    "Bronx",
    "Staten_Island",
]

CSV_FIELDNAMES = [
    "id",
    "name",
    "rating",
    "review_count",
    "price",
    "categories",
    "latitude",
    "longitude",
    "borough",
    "is_closed",
    "transactions",
    "last_updated",
]


def current_week_number() -> int:
    """Return ISO week number for weekly exports."""
    return datetime.now().isocalendar().week


def borough_slug(borough: str) -> str:
    """Normalize borough name for file paths."""
    return borough.lower().replace(" ", "_")


def get_database_path(borough: str) -> Path:
    return DATA_DIR / f"local_{borough_slug(borough)}.db"


def get_borough_export_path(borough: str, week_number: int | None = None) -> Path:
    week = week_number if week_number is not None else current_week_number()
    return EXPORT_DIR / f"{borough_slug(borough)}_week{week}.csv"


def get_master_export_path(week_number: int | None = None) -> Path:
    week = week_number if week_number is not None else current_week_number()
    return MASTER_DIR / f"master_week{week}.csv"
