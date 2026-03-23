"""Small Yelp Fusion reviews endpoint smoke test.

Usage:
  python src/test_reviews_api.py --borough Manhattan
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

import requests

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import REQUEST_TIMEOUT_SECONDS, YELP_REVIEWS_URL, get_database_path  # noqa: E402


def load_api_key() -> str:
    # Prefer indexed keys, fall back to single key.
    indexed = sorted(
        (
            (name, value.strip())
            for name, value in os.environ.items()
            if name.startswith("YELP_API_KEY_") and value.strip()
        ),
        key=lambda item: item[0],
    )
    if indexed:
        return indexed[0][1]

    fallback = os.environ.get("YELP_API_KEY", "").strip()
    if fallback:
        return fallback

    raise RuntimeError("No Yelp API key found in YELP_API_KEY_* or YELP_API_KEY")


def pick_business_alias(borough: str) -> str:
    db_path = get_database_path(borough)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT id, alias FROM restaurants WHERE alias IS NOT NULL ORDER BY id LIMIT 1").fetchone()

    if not row or not row[1]:
        raise RuntimeError(f"No restaurants with alias found in {db_path}")

    return str(row[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Yelp reviews endpoint with a real collected business alias")
    parser.add_argument("--borough", default="Manhattan")
    args = parser.parse_args()

    business_alias = pick_business_alias(args.borough)
    api_key = load_api_key()

    url = YELP_REVIEWS_URL.format(business_id=business_alias)
    headers = {"Authorization": f"Bearer {api_key}"}

    print(f"Testing business_alias: {business_alias}")
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    print(f"HTTP status: {response.status_code}")

    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}

    print("Raw API JSON response:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    reviews = payload.get("reviews") if isinstance(payload, dict) else None
    if isinstance(reviews, list):
        print(f"reviews array found with {len(reviews)} item(s)")
    else:
        print("reviews array NOT found in API payload")


if __name__ == "__main__":
    main()
