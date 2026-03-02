"""Borough-level Yelp Fusion collector with key rotation and weekly CSV export."""

from __future__ import annotations

import argparse
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
    CSV_FIELDNAMES,
    EXPORT_DIR,
    MAX_OFFSET,
    MAX_REQUESTS_PER_KEY_PER_DAY,
    MAX_RETRIES,
    PAGE_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    YELP_SEARCH_URL,
    current_week_number,
    get_borough_export_path,
    get_database_path,
)
from src.database import connect_database, fetch_all_restaurants, initialize_database, upsert_restaurant  # noqa: E402
from src.exporter import write_csv  # noqa: E402


class KeyExhaustedError(RuntimeError):
    """Raised when no API keys are available for further requests."""


class ApiKeyManager:
    def __init__(self, max_requests_per_key: int) -> None:
        self.keys = self._load_keys()
        if not self.keys:
            raise ValueError("No Yelp API keys found in environment variables YELP_API_KEY_*.")

        self.max_requests_per_key = max_requests_per_key
        self.usage = [0 for _ in self.keys]
        self.exhausted = [False for _ in self.keys]
        self.current_index = 0

    @staticmethod
    def _load_keys() -> list[str]:
        key_pairs: list[tuple[int, str]] = []
        for env_name, env_value in os.environ.items():
            if not env_name.startswith("YELP_API_KEY_"):
                continue
            if not env_value:
                continue
            suffix = env_name.removeprefix("YELP_API_KEY_")
            if suffix.isdigit():
                key_pairs.append((int(suffix), env_value.strip()))

        key_pairs.sort(key=lambda item: item[0])
        return [value for _, value in key_pairs]

    def active_key(self) -> str:
        if self.all_exhausted():
            raise KeyExhaustedError("All Yelp API keys are exhausted.")
        return self.keys[self.current_index]

    def active_display_index(self) -> int:
        return self.current_index + 1

    def all_exhausted(self) -> bool:
        return all(self.exhausted)

    def record_request(self) -> None:
        self.usage[self.current_index] += 1
        if self.usage[self.current_index] >= self.max_requests_per_key:
            self.exhausted[self.current_index] = True
            logging.warning(
                "API key index %d reached request limit (%d).",
                self.active_display_index(),
                self.max_requests_per_key,
            )
            self.rotate("daily_limit")

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

    def exhaust_current(self, reason: str) -> None:
        self.exhausted[self.current_index] = True
        logging.warning(
            "Marked API key index %d exhausted (%s).",
            self.active_display_index(),
            reason,
        )
        self.rotate(reason)


def build_params(borough: str, offset: int) -> dict[str, Any]:
    return {
        "term": "restaurants",
        "location": f"{borough.replace('_', ' ')}, NYC, NY",
        "limit": PAGE_LIMIT,
        "offset": offset,
    }


def yelp_request_with_retry(
    session: requests.Session,
    key_manager: ApiKeyManager,
    params: dict[str, Any],
    total_requests_counter: dict[str, int],
) -> list[dict[str, Any]]:
    attempt = 0
    while attempt <= MAX_RETRIES:
        if key_manager.all_exhausted():
            raise KeyExhaustedError("All Yelp API keys exhausted before request completion.")

        headers = {"Authorization": f"Bearer {key_manager.active_key()}"}
        try:
            response = session.get(
                YELP_SEARCH_URL,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            key_manager.record_request()
            total_requests_counter["count"] += 1
            logging.info(
                "Request #%d with key index %d | offset=%s | status=%d",
                total_requests_counter["count"],
                key_manager.active_display_index(),
                params["offset"],
                response.status_code,
            )

            if response.status_code == 200:
                return response.json().get("businesses", [])

            if response.status_code == 429:
                key_manager.exhaust_current("http_429")
                continue

            if 500 <= response.status_code < 600:
                delay = BACKOFF_BASE_SECONDS * (2**attempt)
                logging.warning(
                    "Server error %d on offset=%s. Retrying in %ss.",
                    response.status_code,
                    params["offset"],
                    delay,
                )
                time.sleep(delay)
                attempt += 1
                continue

            logging.error(
                "Non-retriable response %d at offset=%s: %s",
                response.status_code,
                params["offset"],
                response.text,
            )
            return []

        except requests.RequestException as exc:
            delay = BACKOFF_BASE_SECONDS * (2**attempt)
            logging.warning(
                "Request error on offset=%s with key index %d: %s. Retrying in %ss.",
                params["offset"],
                key_manager.active_display_index(),
                exc,
                delay,
            )
            time.sleep(delay)
            attempt += 1

    logging.error("Max retries exceeded at offset=%s.", params["offset"])
    return []


def collect_borough(borough: str, week_number: int | None = None) -> None:
    logging.info("Starting collection for borough=%s", borough)
    db_path = get_database_path(borough)

    with connect_database(db_path) as conn:
        initialize_database(conn)
        key_manager = ApiKeyManager(max_requests_per_key=MAX_REQUESTS_PER_KEY_PER_DAY)
        session = requests.Session()

        total_requests_counter = {"count": 0}
        seen_ids: set[str] = set()
        inserted = 0
        updated = 0

        try:
            for offset in range(0, MAX_OFFSET, PAGE_LIMIT):
                params = build_params(borough, offset)
                businesses = yelp_request_with_retry(session, key_manager, params, total_requests_counter)
                if not businesses:
                    logging.info("No results at offset=%d. Stopping pagination.", offset)
                    break

                for business in businesses:
                    business_id = business.get("id")
                    if not business_id or business_id in seen_ids:
                        continue
                    seen_ids.add(business_id)

                    result = upsert_restaurant(conn, business, borough)
                    if result == "inserted":
                        inserted += 1
                    elif result == "updated":
                        updated += 1

                conn.commit()

        except KeyExhaustedError:
            logging.error("All API keys exhausted. Stopping safely.")
        finally:
            session.close()

        rows = fetch_all_restaurants(conn)
        export_path = get_borough_export_path(borough, week_number)
        exported_count = write_csv(export_path, CSV_FIELDNAMES, [dict(row) for row in rows])

    logging.info("Borough collection complete: %s", borough)
    logging.info("Total requests made: %d", total_requests_counter["count"])
    logging.info("Inserted=%d Updated=%d", inserted, updated)
    logging.info("Total restaurants collected (db total): %d", exported_count)
    logging.info("Exported weekly CSV: %s", export_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Yelp restaurants by NYC borough")
    parser.add_argument("--borough", required=True, choices=BOROUGH_CHOICES)
    parser.add_argument(
        "--week",
        type=int,
        default=current_week_number(),
        help="ISO week number for export filename (default: current week)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    collect_borough(args.borough, args.week)


if __name__ == "__main__":
    main()
