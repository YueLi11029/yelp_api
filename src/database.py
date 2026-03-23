"""SQLite data access layer for local restaurant storage."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CREATE_RESTAURANTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS restaurants (
    id TEXT PRIMARY KEY,
    alias TEXT,
    name TEXT,
    url TEXT,
    image_url TEXT,
    rating REAL,
    review_count INTEGER,
    price TEXT,
    phone TEXT,
    categories TEXT,
    latitude REAL,
    longitude REAL,
    address TEXT,
    transactions TEXT,
    is_closed INTEGER,
    last_updated TIMESTAMP
);
"""

CREATE_REVIEWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reviews (
    review_id TEXT PRIMARY KEY,
    business_id TEXT,
    rating REAL,
    text TEXT,
    time_created TEXT,
    user_id TEXT,
    user_name TEXT
);
"""

CREATE_IMAGES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id TEXT,
    image_url TEXT,
    UNIQUE (business_id, image_url)
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_database(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_database(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_RESTAURANTS_TABLE_SQL)
    conn.execute(CREATE_REVIEWS_TABLE_SQL)
    conn.execute(CREATE_IMAGES_TABLE_SQL)
    _migrate_restaurants_table(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_business_id ON reviews (business_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_images_business_id ON images (business_id)")
    conn.commit()


def _migrate_restaurants_table(conn: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(restaurants)").fetchall()
    }
    expected_columns = {
        "id": "TEXT",
        "alias": "TEXT",
        "name": "TEXT",
        "url": "TEXT",
        "image_url": "TEXT",
        "rating": "REAL",
        "review_count": "INTEGER",
        "price": "TEXT",
        "phone": "TEXT",
        "categories": "TEXT",
        "latitude": "REAL",
        "longitude": "REAL",
        "address": "TEXT",
        "transactions": "TEXT",
        "is_closed": "INTEGER",
        "last_updated": "TIMESTAMP",
    }

    for column_name, column_type in expected_columns.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE restaurants ADD COLUMN {column_name} {column_type}")


def _extract_categories(business: dict[str, Any]) -> str:
    names = [c.get("title", "") for c in business.get("categories", []) if c.get("title")]
    return ",".join(names)


def _extract_transactions(business: dict[str, Any]) -> str:
    txns = [t for t in business.get("transactions", []) if t]
    return ",".join(txns)


def _extract_address(business: dict[str, Any]) -> str:
    location = business.get("location") or {}
    display_address = location.get("display_address") or []
    if isinstance(display_address, list):
        return ", ".join([line for line in display_address if line])
    return ""


def _extract_record(business: dict[str, Any]) -> dict[str, Any]:
    coords = business.get("coordinates") or {}
    return {
        "id": business.get("id"),
        "alias": business.get("alias"),
        "name": business.get("name"),
        "url": business.get("url"),
        "image_url": business.get("image_url"),
        "rating": business.get("rating"),
        "review_count": business.get("review_count"),
        "price": business.get("price"),
        "phone": business.get("phone") or business.get("display_phone"),
        "categories": _extract_categories(business),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "address": _extract_address(business),
        "transactions": _extract_transactions(business),
        "is_closed": int(bool(business.get("is_closed", False))),
        "last_updated": utc_now_iso(),
    }


def upsert_restaurant(conn: sqlite3.Connection, business: dict[str, Any]) -> bool:
    """Upsert a restaurant row and return True when a valid row was processed."""
    record = _extract_record(business)
    if not record["id"]:
        return False

    conn.execute(
        """
        INSERT INTO restaurants (
            id, alias, name, url, image_url, rating, review_count, price, phone,
            latitude, longitude, address, transactions, categories, is_closed, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            alias = excluded.alias,
            name = excluded.name,
            url = excluded.url,
            image_url = excluded.image_url,
            rating = excluded.rating,
            review_count = excluded.review_count,
            price = excluded.price,
            phone = excluded.phone,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            address = excluded.address,
            transactions = excluded.transactions,
            categories = excluded.categories,
            is_closed = excluded.is_closed,
            last_updated = excluded.last_updated
        """,
        (
            record["id"],
            record["alias"],
            record["name"],
            record["url"],
            record["image_url"],
            record["rating"],
            record["review_count"],
            record["price"],
            record["phone"],
            record["latitude"],
            record["longitude"],
            record["address"],
            record["transactions"],
            record["categories"],
            record["is_closed"],
            record["last_updated"],
        ),
    )
    return True


def update_restaurant_from_details(conn: sqlite3.Connection, business_id: str, details: dict[str, Any]) -> bool:
    if not business_id:
        return False

    location = details.get("location") or {}
    display_address = location.get("display_address") or []
    address = ", ".join([line for line in display_address if line]) if isinstance(display_address, list) else None
    phone = details.get("phone") or details.get("display_phone")

    cur = conn.execute(
        """
        UPDATE restaurants
        SET
            phone = COALESCE(?, phone),
            address = COALESCE(?, address),
            last_updated = ?
        WHERE id = ?
        """,
        (phone, address, utc_now_iso(), business_id),
    )
    return cur.rowcount > 0


def insert_images(conn: sqlite3.Connection, business_id: str, image_urls: list[str]) -> int:
    if not business_id or not image_urls:
        return 0

    cleaned_urls = []
    seen_urls: set[str] = set()
    for image_url in image_urls:
        normalized = (image_url or "").strip()
        if not normalized or normalized in seen_urls:
            continue
        seen_urls.add(normalized)
        cleaned_urls.append((business_id, normalized))

    if not cleaned_urls:
        return 0

    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO images (business_id, image_url)
        VALUES (?, ?)
        """,
        cleaned_urls,
    )
    return conn.total_changes - before


def insert_reviews(conn: sqlite3.Connection, business_id: str, reviews: list[dict[str, Any]]) -> int:
    if not business_id or not reviews:
        return 0

    review_rows: list[tuple[Any, ...]] = []
    for review in reviews:
        review_id = review.get("id")
        user = review.get("user") or {}
        if not review_id:
            continue
        print("Inserting review:", review_id)
        review_rows.append(
            (
                review_id,
                business_id,
                review.get("rating"),
                review.get("text"),
                review.get("time_created"),
                user.get("id"),
                user.get("name"),
            )
        )

    if not review_rows:
        return 0

    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO reviews (
            review_id, business_id, rating, text, time_created, user_id, user_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        review_rows,
    )
    return conn.total_changes - before


def fetch_all_restaurants(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            id, name, rating, review_count, price, categories,
            latitude, longitude, address, phone, transactions,
            image_url, url, is_closed
        FROM restaurants
        ORDER BY id
        """
    ).fetchall()


def fetch_all_reviews(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT review_id, business_id, rating, text, time_created, user_id, user_name
        FROM reviews
        ORDER BY review_id
        """
    ).fetchall()
    print("Total reviews in database:", len(rows))
    return rows


def fetch_all_images(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT business_id, image_url
        FROM images
        ORDER BY business_id, image_url
        """
    ).fetchall()
