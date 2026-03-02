"""SQLite data access layer for local restaurant storage."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS restaurants (
    id TEXT PRIMARY KEY,
    name TEXT,
    rating REAL,
    review_count INTEGER,
    price TEXT,
    categories TEXT,
    latitude REAL,
    longitude REAL,
    borough TEXT,
    is_closed INTEGER,
    transactions TEXT,
    last_updated TIMESTAMP
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
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()


def _extract_categories(business: dict[str, Any]) -> str:
    names = [c.get("title", "") for c in business.get("categories", []) if c.get("title")]
    return ",".join(names)


def _extract_transactions(business: dict[str, Any]) -> str:
    txns = [t for t in business.get("transactions", []) if t]
    return ",".join(txns)


def _extract_record(business: dict[str, Any], borough: str) -> dict[str, Any]:
    coords = business.get("coordinates") or {}
    return {
        "id": business.get("id"),
        "name": business.get("name"),
        "rating": business.get("rating"),
        "review_count": business.get("review_count"),
        "price": business.get("price"),
        "categories": _extract_categories(business),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "borough": borough,
        "is_closed": int(bool(business.get("is_closed", False))),
        "transactions": _extract_transactions(business),
        "last_updated": utc_now_iso(),
    }


def upsert_restaurant(conn: sqlite3.Connection, business: dict[str, Any], borough: str) -> str:
    """Insert new record or update changed incremental fields for existing IDs.

    Returns one of: "inserted", "updated", "unchanged", "skipped".
    """
    record = _extract_record(business, borough)
    if not record["id"]:
        return "skipped"

    existing = conn.execute(
        "SELECT rating, review_count, is_closed FROM restaurants WHERE id = ?",
        (record["id"],),
    ).fetchone()

    if existing is None:
        conn.execute(
            """
            INSERT INTO restaurants (
                id, name, rating, review_count, price, categories,
                latitude, longitude, borough, is_closed, transactions, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["id"],
                record["name"],
                record["rating"],
                record["review_count"],
                record["price"],
                record["categories"],
                record["latitude"],
                record["longitude"],
                record["borough"],
                record["is_closed"],
                record["transactions"],
                record["last_updated"],
            ),
        )
        return "inserted"

    rating_changed = existing["rating"] != record["rating"]
    reviews_changed = existing["review_count"] != record["review_count"]
    is_closed_changed = existing["is_closed"] != record["is_closed"]

    if rating_changed or reviews_changed or is_closed_changed:
        conn.execute(
            """
            UPDATE restaurants
            SET rating = ?, review_count = ?, is_closed = ?, last_updated = ?
            WHERE id = ?
            """,
            (
                record["rating"],
                record["review_count"],
                record["is_closed"],
                record["last_updated"],
                record["id"],
            ),
        )
        return "updated"

    return "unchanged"


def fetch_all_restaurants(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            id, name, rating, review_count, price, categories,
            latitude, longitude, borough, is_closed, transactions, last_updated
        FROM restaurants
        ORDER BY id
        """
    ).fetchall()
