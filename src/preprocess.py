"""Prepare merged Yelp datasets for ML, semantic search, and embeddings."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MASTER_DIR = PROJECT_ROOT / "data" / "master"
ML_DIR = PROJECT_ROOT / "data" / "ml"

RESTAURANTS_PATTERN = "master_restaurants_week*.csv"
REVIEWS_PATTERN = "master_reviews_week*.csv"
IMAGES_PATTERN = "master_images_week*.csv"


def clean_text(text: object) -> str:
    """Normalize review text for downstream NLP use."""
    if text is None:
        return ""

    cleaned = str(text).lower().replace("\n", " ")

    # Remove common emoji ranges and pictographs.
    emoji_pattern = re.compile(
        "["
        "\U0001F1E0-\U0001F1FF"
        "\U0001F300-\U0001F5FF"
        "\U0001F600-\U0001F64F"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FAFF"
        "\U00002700-\U000027BF"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    cleaned = emoji_pattern.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _extract_week_number(path: Path, prefix: str) -> int | None:
    stem = path.stem
    marker = f"{prefix}_week"
    if not stem.startswith(marker):
        return None
    suffix = stem.replace(marker, "", 1)
    if not suffix.isdigit():
        return None
    return int(suffix)


def _resolve_master_paths() -> tuple[Path, Path, Path]:
    if not MASTER_DIR.exists():
        raise FileNotFoundError(f"Master directory not found: {MASTER_DIR}")

    restaurants_files = list(MASTER_DIR.glob(RESTAURANTS_PATTERN))
    reviews_files = list(MASTER_DIR.glob(REVIEWS_PATTERN))
    images_files = list(MASTER_DIR.glob(IMAGES_PATTERN))

    if not restaurants_files or not reviews_files or not images_files:
        raise FileNotFoundError(
            "Missing one or more master CSV file groups in data/master. "
            "Run merge.py first to generate restaurants/reviews/images master files."
        )

    restaurants_map = {
        week: path
        for path in restaurants_files
        if (week := _extract_week_number(path, "master_restaurants")) is not None
    }
    reviews_map = {
        week: path
        for path in reviews_files
        if (week := _extract_week_number(path, "master_reviews")) is not None
    }
    images_map = {
        week: path
        for path in images_files
        if (week := _extract_week_number(path, "master_images")) is not None
    }

    common_weeks = sorted(set(restaurants_map) & set(reviews_map) & set(images_map))
    if not common_weeks:
        raise FileNotFoundError(
            "No matching week found across master restaurants/reviews/images CSV files."
        )

    latest_week = common_weeks[-1]
    return restaurants_map[latest_week], reviews_map[latest_week], images_map[latest_week]


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the latest matching weekly master CSV triplet."""
    restaurants_path, reviews_path, images_path = _resolve_master_paths()

    restaurants_df = pd.read_csv(restaurants_path)
    reviews_df = pd.read_csv(reviews_path)
    images_df = pd.read_csv(images_path)

    return restaurants_df, reviews_df, images_df


def build_restaurant_documents(
    restaurants_df: pd.DataFrame,
    reviews_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build document dataset and cleaned review dataset."""
    required_restaurant_cols = [
        "id",
        "name",
        "categories",
        "rating",
        "price",
        "latitude",
        "longitude",
    ]
    required_review_cols = ["review_id", "business_id", "text", "rating"]

    missing_restaurant = [c for c in required_restaurant_cols if c not in restaurants_df.columns]
    missing_review = [c for c in required_review_cols if c not in reviews_df.columns]

    if missing_restaurant:
        raise ValueError(f"Restaurants CSV missing columns: {missing_restaurant}")
    if missing_review:
        raise ValueError(f"Reviews CSV missing columns: {missing_review}")

    reviews_cleaned = reviews_df.copy()
    reviews_cleaned["cleaned_text"] = reviews_cleaned["text"].apply(clean_text)

    reviews_output = reviews_cleaned[["review_id", "business_id", "cleaned_text", "rating"]].copy()

    non_empty_reviews = reviews_cleaned[reviews_cleaned["cleaned_text"].str.len() > 0].copy()
    non_empty_reviews["review_order"] = non_empty_reviews.groupby("business_id").cumcount()
    non_empty_reviews = non_empty_reviews[non_empty_reviews["review_order"] < 10]

    review_summary = (
        non_empty_reviews.groupby("business_id", sort=False)["cleaned_text"]
        .apply(lambda parts: " ".join(parts.tolist()))
        .reset_index(name="review_summary")
    )

    merged = restaurants_df.merge(
        review_summary,
        how="left",
        left_on="id",
        right_on="business_id",
    )

    merged["review_summary"] = merged["review_summary"].fillna("")
    merged["name"] = merged["name"].fillna("")
    merged["categories"] = merged["categories"].fillna("")

    merged["document"] = (
        merged["name"].astype(str)
        + " "
        + merged["categories"].astype(str)
        + " "
        + merged["review_summary"].astype(str)
    ).apply(clean_text)

    restaurant_documents = merged[
        ["id", "name", "document", "rating", "price", "latitude", "longitude"]
    ].rename(columns={"id": "restaurant_id"})

    return restaurant_documents, reviews_output


def save_datasets(
    restaurant_documents: pd.DataFrame,
    reviews_cleaned: pd.DataFrame,
) -> tuple[Path, Path]:
    """Save ML-ready datasets under data/ml."""
    ML_DIR.mkdir(parents=True, exist_ok=True)

    documents_path = ML_DIR / "restaurant_documents.csv"
    reviews_path = ML_DIR / "reviews_cleaned.csv"

    restaurant_documents.to_csv(documents_path, index=False)
    reviews_cleaned.to_csv(reviews_path, index=False)

    return documents_path, reviews_path


def main() -> None:
    restaurants_df, reviews_df, _images_df = load_data()
    restaurant_documents, reviews_cleaned = build_restaurant_documents(restaurants_df, reviews_df)
    documents_path, reviews_path = save_datasets(restaurant_documents, reviews_cleaned)

    avg_review_length = reviews_cleaned["cleaned_text"].fillna("").str.split().str.len().mean()
    avg_review_length = float(avg_review_length) if pd.notna(avg_review_length) else 0.0

    print(f"Saved: {documents_path}")
    print(f"Saved: {reviews_path}")
    print(f"number of restaurants: {len(restaurant_documents)}")
    print(f"number of reviews: {len(reviews_cleaned)}")
    print(f"average review length: {avg_review_length:.2f}")


if __name__ == "__main__":
    main()
