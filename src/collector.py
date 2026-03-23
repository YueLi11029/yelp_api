"""Borough-level Yelp Fusion collector with metadata, reviews, and image URL exports."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (  # noqa: E402
    BACKOFF_BASE_SECONDS,
    BOROUGH_CHOICES,
    EXPORT_DIR,
    KEY_ROTATION_REQUEST_THRESHOLD,
    MAX_DETAIL_CALLS_PER_RUN,
    MAX_OFFSET,
    MAX_REQUESTS_PER_KEY_PER_DAY,
    MAX_RETRIES,
    MAX_REVIEW_CALLS_PER_RUN,
    PAGE_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    YELP_BUSINESS_DETAILS_URL,
    YELP_REVIEWS_URL,
    YELP_SEARCH_URL,
    current_day_number,
    get_borough_images_export_path,
    get_borough_images_export_path_date,
    get_borough_restaurants_export_path,
    get_borough_restaurants_export_path_date,
    get_borough_reviews_export_path,
    get_borough_reviews_export_path_date,
    get_database_path,
)
from src.database import (  # noqa: E402
    connect_database,
    fetch_all_images,
    fetch_all_restaurants,
    fetch_all_reviews,
    initialize_database,
    insert_images,
    insert_reviews,
    update_restaurant_from_details,
    upsert_restaurant,
)
from src.exporter import write_json  # noqa: E402


class KeyExhaustedError(RuntimeError):
    """Raised when no API keys are available for further requests."""


GLOBAL_COLLECTED_IDS_PATH = PROJECT_ROOT / "data" / "master" / "collected_business_ids.json"


def load_global_collected_business_ids(path: Path = GLOBAL_COLLECTED_IDS_PATH) -> set[str]:
    """Load global collected business IDs from disk."""
    if not path.exists():
        return set()

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(item).strip() for item in data if str(item).strip()}
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Failed to load global business ID set from %s: %s", path, exc)

    return set()


def save_global_collected_business_ids(
    ids: set[str], path: Path = GLOBAL_COLLECTED_IDS_PATH
) -> None:
    """Persist global collected business IDs to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted(ids), f, ensure_ascii=False, indent=2)


class ApiKeyManager:
    def __init__(self, max_requests_per_key: int, rotation_threshold: int) -> None:
        self.keys = self._load_keys()
        if not self.keys:
            raise ValueError("No Yelp API keys found in environment variables YELP_API_KEY_*.")

        self.max_requests_per_key = max_requests_per_key
        self.rotation_threshold = rotation_threshold
        self.usage = [0 for _ in self.keys]
        self.exhausted = [False for _ in self.keys]
        self.current_index = 0

    @staticmethod
    def _load_keys() -> list[str]:
        key_pairs: list[tuple[int, str]] = []
        fallback_key = os.environ.get("YELP_API_KEY", "").strip()
        for env_name, env_value in os.environ.items():
            if not env_name.startswith("YELP_API_KEY_"):
                continue
            if not env_value:
                continue
            suffix = env_name.removeprefix("YELP_API_KEY_")
            if suffix.isdigit():
                key_pairs.append((int(suffix), env_value.strip()))

        key_pairs.sort(key=lambda item: item[0])
        keys = [value for _, value in key_pairs]
        if not keys and fallback_key:
            keys.append(fallback_key)
            logging.warning("Using fallback environment variable YELP_API_KEY as key index 1.")
        return keys

    def active_key_with_index(self) -> tuple[int, str]:
        if self.all_exhausted():
            raise KeyExhaustedError("All Yelp API keys are exhausted.")
        return self.current_index, self.keys[self.current_index]

    def active_display_index(self) -> int:
        return self.current_index + 1

    def all_exhausted(self) -> bool:
        return all(self.exhausted)

    def record_request(self, key_index: int) -> None:
        self.usage[key_index] += 1

        if self.usage[key_index] > self.rotation_threshold:
            self.mark_exhausted(key_index, "local_threshold")
            return

        if self.usage[key_index] >= self.max_requests_per_key:
            self.mark_exhausted(key_index, "daily_limit")

    def rotate(self, reason: str) -> None:
        if self.all_exhausted():
            raise KeyExhaustedError("All Yelp API keys are exhausted.")

        old_idx = self.current_index
        for step in range(1, len(self.keys) + 1):
            candidate = (old_idx + step) % len(self.keys)
            if not self.exhausted[candidate]:
                self.current_index = candidate
                logging.warning(
                    "Rotated API key due to %s: %d -> %d",
                    reason,
                    old_idx + 1,
                    self.current_index + 1,
                )
                return

        raise KeyExhaustedError("All Yelp API keys are exhausted.")

    def mark_exhausted(self, key_index: int, reason: str) -> None:
        if self.exhausted[key_index]:
            return

        self.exhausted[key_index] = True
        logging.warning("Marked API key index %d exhausted (%s).", key_index + 1, reason)
        if key_index == self.current_index and not self.all_exhausted():
            self.rotate(reason)


def build_search_params(borough: str, offset: int) -> dict[str, Any]:
    return {
        "term": "restaurants",
        "location": f"{borough.replace('_', ' ')}, NYC, NY",
        "limit": PAGE_LIMIT,
        "offset": offset,
    }


def yelp_request_with_retry(
    session: requests.Session,
    key_manager: ApiKeyManager,
    url: str,
    params: dict[str, Any],
    endpoint_label: str,
    total_requests_counter: dict[str, int],
) -> dict[str, Any] | None:
    attempt = 0
    while attempt <= MAX_RETRIES:
        if key_manager.all_exhausted():
            raise KeyExhaustedError("All Yelp API keys exhausted before request completion.")

        key_index, key_value = key_manager.active_key_with_index()
        headers = {"Authorization": f"Bearer {key_value}"}

        try:
            response = session.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            key_manager.record_request(key_index)
            total_requests_counter["count"] += 1

            logging.info(
                "Request #%d | endpoint=%s | key=%d | status=%d",
                total_requests_counter["count"],
                endpoint_label,
                key_index + 1,
                response.status_code,
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                key_manager.mark_exhausted(key_index, "http_429")
                continue

            if 500 <= response.status_code < 600:
                delay = BACKOFF_BASE_SECONDS * (2**attempt)
                logging.warning(
                    "Server error %d on %s. Retrying in %ss.",
                    response.status_code,
                    endpoint_label,
                    delay,
                )
                time.sleep(delay)
                attempt += 1
                continue

            logging.error(
                "Non-retriable response %d on %s: %s",
                response.status_code,
                endpoint_label,
                response.text,
            )
            return None

        except requests.RequestException as exc:
            delay = BACKOFF_BASE_SECONDS * (2**attempt)
            logging.warning(
                "Request error on %s with key %d: %s. Retrying in %ss.",
                endpoint_label,
                key_index + 1,
                exc,
                delay,
            )
            time.sleep(delay)
            attempt += 1

    logging.error("Max retries exceeded on %s.", endpoint_label)
    return None


def fetch_search_page(
    session: requests.Session,
    key_manager: ApiKeyManager,
    borough: str,
    offset: int,
    total_requests_counter: dict[str, int],
) -> list[dict[str, Any]]:
    payload = yelp_request_with_retry(
        session=session,
        key_manager=key_manager,
        url=YELP_SEARCH_URL,
        params=build_search_params(borough, offset),
        endpoint_label=f"search_offset_{offset}",
        total_requests_counter=total_requests_counter,
    )
    if not payload:
        return []
    return payload.get("businesses", [])


def fetch_business_details(
    session: requests.Session,
    key_manager: ApiKeyManager,
    business_id: str,
    total_requests_counter: dict[str, int],
) -> dict[str, Any] | None:
    payload = yelp_request_with_retry(
        session=session,
        key_manager=key_manager,
        url=YELP_BUSINESS_DETAILS_URL.format(business_id=business_id),
        params={},
        endpoint_label=f"details_{business_id}",
        total_requests_counter=total_requests_counter,
    )
    if payload is not None:
        logging.info("Fetched details for business %s", business_id)
        logging.info("Photos returned: %d", len(payload.get("photos") or []))
    return payload


def fetch_business_reviews(
    session: requests.Session,
    key_manager: ApiKeyManager,
    business_alias: str,
    total_requests_counter: dict[str, int],
) -> list[dict[str, Any]]:
    normalized_alias = str(business_alias).strip()
    reviews_url = YELP_REVIEWS_URL.format(business_id=normalized_alias)
    print("Business alias:", normalized_alias)
    print("Reviews URL:", reviews_url)
    logging.info("Reviews request alias=%r", normalized_alias)
    logging.info("Reviews request url=%s", reviews_url)
    payload = yelp_request_with_retry(
        session=session,
        key_manager=key_manager,
        url=reviews_url,
        params={},
        endpoint_label=f"reviews_{normalized_alias}",
        total_requests_counter=total_requests_counter,
    )
    if not payload:
        print("Reviews returned:", 0)
        return []
    reviews = payload.get("reviews", [])
    print("Reviews returned:", len(reviews))
    logging.info("Fetched reviews for business alias %s", normalized_alias)
    logging.info("Reviews returned: %d", len(reviews))
    return reviews


def _serialize_restaurants(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        categories = [part.strip() for part in str(row.get("categories") or "").split(",") if part.strip()]
        transactions = [part.strip() for part in str(row.get("transactions") or "").split(",") if part.strip()]

        payload = {
            "business_id": row.get("id"),
            "name": row.get("name"),
            "rating": row.get("rating"),
            "review_count": row.get("review_count"),
            "price": row.get("price"),
            "categories": categories,
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "address": row.get("address"),
            "phone": row.get("phone"),
            "transactions": transactions,
            "image_url": row.get("image_url"),
            "url": row.get("url"),
            "is_closed": row.get("is_closed"),
        }
        serialized.append(payload)

    return serialized


def _serialize_reviews(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        serialized.append(
            {
                "review_id": row.get("review_id"),
                "business_id": row.get("business_id"),
                "rating": row.get("rating"),
                "text": row.get("text"),
                "time_created": row.get("time_created"),
                "user_id": row.get("user_id"),
                "user_name": row.get("user_name"),
            }
        )
    return serialized


def _serialize_images(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        serialized.append(
            {
                "business_id": row.get("business_id"),
                "image_url": row.get("image_url"),
            }
        )
    return serialized


def collect_borough(
    borough: str,
    day_number: int | None = None,
    date_str: str | None = None,
    skip_seen_business_ids: bool = True,
) -> None:
    logging.info("Starting collection for borough=%s", borough)
    db_path = get_database_path(borough)
    global_seen_business_ids = load_global_collected_business_ids()
    logging.info("Loaded %d globally seen business IDs", len(global_seen_business_ids))

    with connect_database(db_path) as conn:
        initialize_database(conn)
        key_manager = ApiKeyManager(
            max_requests_per_key=MAX_REQUESTS_PER_KEY_PER_DAY,
            rotation_threshold=KEY_ROTATION_REQUEST_THRESHOLD,
        )
        session = requests.Session()

        total_requests_counter = {"count": 0}
        write_ops_since_commit = 0
        seen_ids: set[str] = set()
        collected_business_ids: list[str] = []
        newly_collected_business_ids: set[str] = set()
        search_image_url_by_business_id: dict[str, str] = {}

        restaurants_collected = 0
        reviews_collected = 0
        images_collected = 0
        detail_calls = 0
        review_calls = 0

        try:
            # STEP 1: collect restaurant metadata from /businesses/search.
            for offset in range(0, MAX_OFFSET, PAGE_LIMIT):
                businesses = fetch_search_page(
                    session=session,
                    key_manager=key_manager,
                    borough=borough,
                    offset=offset,
                    total_requests_counter=total_requests_counter,
                )
                if not businesses:
                    logging.info("No results at offset=%d. Stopping pagination.", offset)
                    break

                for business in businesses:
                    business_id = business.get("id")
                    if not business_id or business_id in seen_ids:
                        continue

                    seen_ids.add(business_id)

                    if skip_seen_business_ids and business_id in global_seen_business_ids:
                        continue

                    collected_business_ids.append(business_id)
                    newly_collected_business_ids.add(business_id)
                    search_image_url_by_business_id[business_id] = (business.get("image_url") or "").strip()

                    if upsert_restaurant(conn, business):
                        restaurants_collected += 1
                        write_ops_since_commit += 1

                    if write_ops_since_commit >= 50:
                        conn.commit()
                        write_ops_since_commit = 0

            # STEP 2 and STEP 3: collect details/photos and reviews with per-run limits.
            for business_id in collected_business_ids:
                business_alias = None
                if detail_calls < MAX_DETAIL_CALLS_PER_RUN:
                    detail_calls += 1
                    details = fetch_business_details(
                        session=session,
                        key_manager=key_manager,
                        business_id=business_id,
                        total_requests_counter=total_requests_counter,
                    )
                    if details:
                        business_alias = details.get("alias")
                        if update_restaurant_from_details(conn, business_id, details):
                            write_ops_since_commit += 1

                        detail_photos = details.get("photos") or []
                        fallback_image = search_image_url_by_business_id.get(business_id, "")
                        if not detail_photos and fallback_image:
                            # Yelp can return empty photos for some businesses; keep at least primary image URL.
                            detail_photos = [fallback_image]

                        new_images = insert_images(conn, business_id, detail_photos)
                        images_collected += new_images
                        write_ops_since_commit += new_images
                        logging.info("Inserted images for business %s: %d", business_id, new_images)

                if business_alias and review_calls < MAX_REVIEW_CALLS_PER_RUN:
                    review_calls += 1
                    reviews = fetch_business_reviews(
                        session=session,
                        key_manager=key_manager,
                        business_alias=business_alias,
                        total_requests_counter=total_requests_counter,
                    )
                    new_reviews = insert_reviews(conn, business_id, reviews)
                    reviews_collected += new_reviews
                    write_ops_since_commit += new_reviews
                    logging.info("Inserted reviews for business %s: %d", business_id, new_reviews)

                if write_ops_since_commit >= 50:
                    conn.commit()
                    write_ops_since_commit = 0

            conn.commit()

        except KeyExhaustedError:
            logging.error("All API keys exhausted. Stopping safely.")
            conn.commit()
        finally:
            session.close()

        if newly_collected_business_ids:
            global_seen_business_ids.update(newly_collected_business_ids)
            save_global_collected_business_ids(global_seen_business_ids)
            logging.info(
                "Saved global seen business IDs: +%d (total=%d)",
                len(newly_collected_business_ids),
                len(global_seen_business_ids),
            )

        all_restaurant_rows = [dict(row) for row in fetch_all_restaurants(conn)]
        all_review_rows = [dict(row) for row in fetch_all_reviews(conn)]
        all_image_rows = [dict(row) for row in fetch_all_images(conn)]

        restaurant_rows = [
            row for row in all_restaurant_rows if row.get("id") in newly_collected_business_ids
        ]
        review_rows = [
            row for row in all_review_rows if row.get("business_id") in newly_collected_business_ids
        ]
        image_rows = [
            row for row in all_image_rows if row.get("business_id") in newly_collected_business_ids
        ]

        # Use date-based paths if date_str provided, otherwise use day_number paths
        if date_str:
            restaurants_export_path = get_borough_restaurants_export_path_date(borough, date_str)
            reviews_export_path = get_borough_reviews_export_path_date(borough, date_str)
            images_export_path = get_borough_images_export_path_date(borough, date_str)
        else:
            restaurants_export_path = get_borough_restaurants_export_path(borough, day_number)
            reviews_export_path = get_borough_reviews_export_path(borough, day_number)
            images_export_path = get_borough_images_export_path(borough, day_number)

        exported_restaurants = write_json(
            restaurants_export_path,
            _serialize_restaurants(restaurant_rows),
        )
        exported_reviews = write_json(
            reviews_export_path,
            _serialize_reviews(review_rows),
        )
        exported_images = write_json(
            images_export_path,
            _serialize_images(image_rows),
        )

    logging.info("Borough collection complete: %s", borough)
    logging.info("restaurants collected: %d", restaurants_collected)
    logging.info("reviews collected: %d", reviews_collected)
    logging.info("images collected: %d", images_collected)
    logging.info("API requests used: %d", total_requests_counter["count"])
    logging.info("active API key index: %d", key_manager.active_display_index())
    logging.info("detail calls used: %d/%d", detail_calls, MAX_DETAIL_CALLS_PER_RUN)
    logging.info("review calls used: %d/%d", review_calls, MAX_REVIEW_CALLS_PER_RUN)
    logging.info("Exported restaurants JSON rows: %d | %s", exported_restaurants, restaurants_export_path)
    logging.info("Exported reviews JSON rows: %d | %s", exported_reviews, reviews_export_path)
    logging.info("Exported images JSON rows: %d | %s", exported_images, images_export_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Yelp restaurants by NYC borough")
    parser.add_argument("--borough", required=True, choices=BOROUGH_CHOICES)
    parser.add_argument("--day", type=int, default=None, help="Day number for export filename (1-31)")
    parser.add_argument("--date", type=str, default=None, help="Date for export filename (YYYY-MM-DD format)")
    parser.add_argument(
        "--skip-seen-business-ids",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip businesses already collected in previous days (default: enabled)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    
    # Validate and determine date/day parameters
    if args.date and args.day:
        logging.error("Cannot specify both --day and --date. Please use one or the other.")
        sys.exit(1)
    
    day_number = args.day if args.day is not None else (current_day_number() if not args.date else None)
    date_str = args.date if args.date else None
    
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    collect_borough(
        args.borough,
        day_number=day_number,
        date_str=date_str,
        skip_seen_business_ids=args.skip_seen_business_ids,
    )


if __name__ == "__main__":
    main()
