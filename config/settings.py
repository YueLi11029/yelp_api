"""Application settings for the NYC restaurant collection system."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = DATA_DIR / "borough_exports"
MASTER_DIR = DATA_DIR / "master"

YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
YELP_BUSINESS_DETAILS_URL = "https://api.yelp.com/v3/businesses/{business_id}"
YELP_REVIEWS_URL = "https://api.yelp.com/v3/businesses/{business_id}/reviews"

# Yelp Fusion pagination constraints.
PAGE_LIMIT = 50
MAX_OFFSET = 1000  # valid offsets: 0..950 when PAGE_LIMIT is 50

# Per Yelp key request budget.
MAX_REQUESTS_PER_KEY_PER_DAY = 500
KEY_ROTATION_REQUEST_THRESHOLD = 480

# Per-run safety limits for expensive endpoints.
MAX_DETAIL_CALLS_PER_RUN = 200
MAX_REVIEW_CALLS_PER_RUN = 200

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

RESTAURANTS_EXPORT_FIELDS = [
    "id",
    "name",
    "rating",
    "review_count",
    "price",
    "categories",
    "latitude",
    "longitude",
    "address",
    "phone",
    "transactions",
    "image_url",
    "url",
    "is_closed",
]

REVIEWS_EXPORT_FIELDS = [
    "review_id",
    "business_id",
    "rating",
    "text",
    "time_created",
    "user_id",
    "user_name",
]

IMAGES_EXPORT_FIELDS = [
    "business_id",
    "image_url",
]


def current_day_number() -> int:
    """Return day-of-month number for daily exports (1-31)."""
    return datetime.now().day


def get_today_date_string() -> str:
    """Return today's date as YYYY-MM-DD string."""
    return datetime.now().strftime("%Y-%m-%d")


def borough_slug(borough: str) -> str:
    """Normalize borough name for file paths."""
    return borough.lower().replace(" ", "_")


def get_database_path(borough: str) -> Path:
    return DATA_DIR / f"local_{borough_slug(borough)}.db"


def get_borough_restaurants_export_path(borough: str, day_number: int | None = None) -> Path:
    day = day_number if day_number is not None else current_day_number()
    return EXPORT_DIR / f"{borough}_restaurants_day{day}.json"


def get_borough_reviews_export_path(borough: str, day_number: int | None = None) -> Path:
    day = day_number if day_number is not None else current_day_number()
    return EXPORT_DIR / f"{borough}_reviews_day{day}.json"


def get_borough_images_export_path(borough: str, day_number: int | None = None) -> Path:
    day = day_number if day_number is not None else current_day_number()
    return EXPORT_DIR / f"{borough}_images_day{day}.json"


def get_master_restaurants_export_path(day_number: int | None = None) -> Path:
    day = day_number if day_number is not None else current_day_number()
    return MASTER_DIR / f"master_restaurants_day{day}.json"


def get_master_reviews_export_path(day_number: int | None = None) -> Path:
    day = day_number if day_number is not None else current_day_number()
    return MASTER_DIR / f"master_reviews_day{day}.json"


def get_master_images_export_path(day_number: int | None = None) -> Path:
    day = day_number if day_number is not None else current_day_number()
    return MASTER_DIR / f"master_images_day{day}.json"


# ============================================================================
# DATE-BASED EXPORT PATHS (YYYY-MM-DD format)
# ============================================================================


def get_borough_restaurants_export_path_date(borough: str, date_str: str | None = None) -> Path:
    """Get borough restaurants export path using date format (YYYY-MM-DD)."""
    date = date_str if date_str is not None else get_today_date_string()
    return EXPORT_DIR / f"{borough}_restaurants_{date}.json"


def get_borough_reviews_export_path_date(borough: str, date_str: str | None = None) -> Path:
    """Get borough reviews export path using date format (YYYY-MM-DD)."""
    date = date_str if date_str is not None else get_today_date_string()
    return EXPORT_DIR / f"{borough}_reviews_{date}.json"


def get_borough_images_export_path_date(borough: str, date_str: str | None = None) -> Path:
    """Get borough images export path using date format (YYYY-MM-DD)."""
    date = date_str if date_str is not None else get_today_date_string()
    return EXPORT_DIR / f"{borough}_images_{date}.json"


def get_master_restaurants_export_path_date(date_str: str | None = None) -> Path:
    """Get master restaurants export path using date format (YYYY-MM-DD)."""
    date = date_str if date_str is not None else get_today_date_string()
    return MASTER_DIR / f"master_restaurants_{date}.json"


def get_master_reviews_export_path_date(date_str: str | None = None) -> Path:
    """Get master reviews export path using date format (YYYY-MM-DD)."""
    date = date_str if date_str is not None else get_today_date_string()
    return MASTER_DIR / f"master_reviews_{date}.json"


def get_master_images_export_path_date(date_str: str | None = None) -> Path:
    """Get master images export path using date format (YYYY-MM-DD)."""
    date = date_str if date_str is not None else get_today_date_string()
    return MASTER_DIR / f"master_images_{date}.json"
