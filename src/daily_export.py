"""
Daily export pipeline: Create separate JSON outputs for restaurants, images, and reviews.
Restaurants and images come from Yelp API.
Reviews come from Kaggle Yelp Academic Dataset.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DAILY_EXPORTS_DIR = DATA_DIR / "daily_exports"
BOROUGH_EXPORTS_DIR = DATA_DIR / "borough_exports"


def get_today_number() -> int:
    """Return day-of-month number for daily exports."""
    return datetime.now().day


def get_today_date_string() -> str:
    """Return today's date as YYYY-MM-DD string."""
    return datetime.now().strftime("%Y-%m-%d")


def load_yelp_api_exports(
    day_number: int | None = None, date_str: str | None = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Load restaurants and images from Yelp API exports.
    Looks for existing day exports from borough collector.
    Adds borough field based on filename.
    
    Accepts either day_number (for day{N} format) or date_str (for YYYY-MM-DD format).
    """
    restaurants = []
    images = []

    # Determine what suffix to look for
    if date_str:
        suffix_pattern = f"_{date_str}"
        file_pattern = f"{date_str}"
    else:
        day = day_number if day_number is not None else get_today_number()
        suffix_pattern = f"_day{day}"
        file_pattern = f"day{day}"

    # Look for existing day exports from borough collector
    for borough in ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten_Island"]:
        restaurants_file = BOROUGH_EXPORTS_DIR / f"{borough}_restaurants{suffix_pattern}.json"
        images_file = BOROUGH_EXPORTS_DIR / f"{borough}_images{suffix_pattern}.json"

        if restaurants_file.exists():
            with open(restaurants_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    # Add borough field based on filename
                    for rest in data:
                        rest["borough"] = borough.replace("_", " ")
                    restaurants.extend(data)

        if images_file.exists():
            with open(images_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    # Add borough field based on filename
                    for img in data:
                        if "borough" not in img:
                            img["borough"] = borough.replace("_", " ")
                    images.extend(data)

    return restaurants, images


def load_kaggle_reviews(
    review_file: Path | str, business_file: Path | str | None = None
) -> pd.DataFrame:
    """
    Load Kaggle Yelp reviews dataset.
    Each line is a JSON object, so we read line-by-line.
    """
    if isinstance(review_file, str):
        review_file = Path(review_file)

    if not review_file.exists():
        raise FileNotFoundError(f"Reviews file not found: {review_file}")

    print(f"Loading Kaggle reviews from {review_file}...")
    reviews = []
    with open(review_file, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx % 50000 == 0 and idx > 0:
                print(f"  Loaded {idx} reviews...")
            try:
                review = json.loads(line)
                reviews.append(review)
            except json.JSONDecodeError:
                continue

    print(f"Total reviews loaded: {len(reviews)}")
    reviews_df = pd.DataFrame(reviews)
    return reviews_df


def filter_reviews_to_nyc(reviews_df: pd.DataFrame, business_file: Path | str | None = None) -> pd.DataFrame:
    """
    Filter reviews to NYC businesses.
    If business_file is provided, use it to get city info.
    Otherwise, assume all reviews are already from the same city.
    """
    if business_file and Path(business_file).exists():
        print(f"\nLoading business data from {business_file}...")
        businesses = []
        with open(business_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    business = json.loads(line)
                    businesses.append(business)
                except json.JSONDecodeError:
                    continue

        business_df = pd.DataFrame(businesses)
        nyc_business_ids = business_df[business_df["city"] == "New York"]["business_id"].unique()
        print(f"Found {len(nyc_business_ids)} NYC businesses")

        reviews_df = reviews_df[reviews_df["business_id"].isin(nyc_business_ids)]
        print(f"Filtered reviews to {len(reviews_df)} NYC business reviews")
    else:
        print("No business file provided - using all reviews (assume NYC or pre-filtered)")

    return reviews_df


def match_reviews_to_restaurants(
    reviews_df: pd.DataFrame, restaurant_ids: set[str]
) -> pd.DataFrame:
    """
    Filter reviews to only those matching collected restaurants.
    """
    print(f"\nMatching reviews to {len(restaurant_ids)} collected restaurants...")
    matched = reviews_df[reviews_df["business_id"].isin(restaurant_ids)]
    print(f"Matched {len(matched)} reviews to restaurants")
    return matched


def clean_review_text(text: str | None) -> str:
    """
    Clean review text:
    - Convert to lowercase
    - Remove newlines
    - Remove extra spaces
    """
    if not isinstance(text, str):
        return ""

    text = text.lower()
    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def limit_reviews_per_business(
    reviews_df: pd.DataFrame, max_per_business: int = 10
) -> list[dict[str, Any]]:
    """
    Limit reviews per business and clean text.
    Returns list of cleaned reviews.
    """
    print(f"\nLimiting to {max_per_business} reviews per business...")
    reviews_by_business = reviews_df.groupby("business_id")

    limited_reviews = []
    for business_id, group in reviews_by_business:
        # Take first max_per_business reviews for this business
        sampled = group.head(max_per_business)
        for _, review in sampled.iterrows():
            cleaned = {
                "review_id": review.get("review_id"),
                "business_id": review.get("business_id"),
                "rating": int(review.get("stars", 5)) if "stars" in review else review.get("rating", 5),
                "text": clean_review_text(review.get("text")),
                "user_id": review.get("user_id"),
                "date": review.get("date"),
            }
            limited_reviews.append(cleaned)

    print(f"Output: {len(limited_reviews)} reviews from {reviews_by_business.ngroups} businesses")
    avg_per_business = len(limited_reviews) / max(reviews_by_business.ngroups, 1)
    print(f"Average reviews per business: {avg_per_business:.2f}")
    return limited_reviews


def format_restaurants_for_export(restaurants: list[dict[str, Any]], day_number: int) -> list[dict[str, Any]]:
    """Format restaurants for daily export."""
    formatted = []
    for rest in restaurants:
        business_id = rest.get("id") or rest.get("business_id")
        if not business_id:
            continue  # Skip restaurants without ID
        
        formatted_rest = {
            "business_id": business_id,
            "name": rest.get("name"),
            "borough": rest.get("borough", "Unknown"),
            "rating": rest.get("rating"),
            "price": rest.get("price"),
            "categories": rest.get("categories"),
            "latitude": rest.get("latitude"),
            "longitude": rest.get("longitude"),
        }
        formatted.append(formatted_rest)
    return formatted


def format_images_for_export(images: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Format images for daily export."""
    formatted = []
    for img in images:
        formatted_img = {
            "business_id": img.get("business_id") or img.get("id"),
            "image_url": img.get("image_url"),
        }
        formatted.append(formatted_img)
    return formatted


def save_daily_exports(
    restaurants: list[dict[str, Any]],
    images: list[dict[str, Any]],
    reviews: list[dict[str, Any]],
    day_number: int | None = None,
    date_str: str | None = None,
) -> None:
    """Save all three exports to daily_exports directory.
    
    Accepts either day_number (for day{N} format) or date_str (for YYYY-MM-DD format).
    """
    DAILY_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Determine filename pattern
    if date_str:
        file_suffix = date_str
    else:
        day = day_number if day_number is not None else get_today_number()
        file_suffix = f"day{day}"

    # Save restaurants
    restaurants_file = DAILY_EXPORTS_DIR / f"{file_suffix}_restaurants.json"
    with open(restaurants_file, "w", encoding="utf-8") as f:
        json.dump(restaurants, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(restaurants)} restaurants to {restaurants_file}")

    # Save images
    images_file = DAILY_EXPORTS_DIR / f"{file_suffix}_images.json"
    with open(images_file, "w", encoding="utf-8") as f:
        json.dump(images, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(images)} images to {images_file}")

    # Save reviews
    reviews_file = DAILY_EXPORTS_DIR / f"{file_suffix}_reviews.json"
    with open(reviews_file, "w", encoding="utf-8") as f:
        json.dump(reviews, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(reviews)} reviews to {reviews_file}")


def print_statistics(
    restaurants: list[dict[str, Any]],
    images: list[dict[str, Any]],
    reviews: list[dict[str, Any]],
) -> None:
    """Print dataset statistics."""
    print("\n" + "=" * 60)
    print("DAILY EXPORT STATISTICS")
    print("=" * 60)
    print(f"Restaurants: {len(restaurants)}")
    print(f"Images: {len(images)}")
    print(f"Reviews: {len(reviews)}")

    if reviews:
        unique_businesses = len(set(r["business_id"] for r in reviews))
        avg_reviews = len(reviews) / max(unique_businesses, 1)
        print(f"Unique businesses with reviews: {unique_businesses}")
        print(f"Average reviews per business: {avg_reviews:.2f}")

    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create daily export with separate restaurants, images, and reviews JSON files"
    )
    parser.add_argument("--day", type=int, default=None, help="Day number (default: today's day of month)")
    parser.add_argument("--date", type=str, default=None, help="Date in YYYY-MM-DD format (default: today)")
    parser.add_argument(
        "--kaggle-reviews",
        type=str,
        help="Path to yelp_academic_dataset_review.json",
    )
    parser.add_argument(
        "--kaggle-business",
        type=str,
        help="Path to yelp_academic_dataset_business.json (optional, for NYC filtering)",
    )
    parser.add_argument(
        "--max-reviews-per-business",
        type=int,
        default=10,
        help="Maximum reviews per business",
    )
    args = parser.parse_args()

    # Validate arguments
    if args.day and args.date:
        print("Error: Cannot specify both --day and --date. Please use one or the other.")
        sys.exit(1)

    # Determine which format to use
    date_str = args.date if args.date else None
    day_number = args.day if args.day is not None else (None if date_str else get_today_number())

    if date_str:
        print(f"Creating daily export for date {date_str}...")
    else:
        print(f"Creating daily export for day {day_number}...")

    # Load Yelp API exports
    restaurants, images = load_yelp_api_exports(day_number=day_number, date_str=date_str)
    print(f"Loaded {len(restaurants)} restaurants from Yelp API")
    print(f"Loaded {len(images)} images from Yelp API")

    # Format restaurants and images
    formatted_restaurants = format_restaurants_for_export(restaurants, day_number or 0)
    formatted_images = format_images_for_export(images)

    # Load Kaggle reviews if provided
    if args.kaggle_reviews:
        reviews_df = load_kaggle_reviews(args.kaggle_reviews, args.kaggle_business)

        # Filter to NYC
        reviews_df = filter_reviews_to_nyc(reviews_df, args.kaggle_business)

        # Match to collected restaurants
        restaurant_ids = set(r["business_id"] for r in formatted_restaurants if r.get("business_id"))
        reviews_df = match_reviews_to_restaurants(reviews_df, restaurant_ids)

        # Clean and limit reviews
        cleaned_reviews = limit_reviews_per_business(reviews_df, args.max_reviews_per_business)
    else:
        print("\nNo Kaggle reviews file provided. Creating empty reviews list.")
        cleaned_reviews = []

    # Save all exports
    save_daily_exports(
        formatted_restaurants,
        formatted_images,
        cleaned_reviews,
        day_number=day_number,
        date_str=date_str,
    )

    # Print statistics
    print_statistics(formatted_restaurants, formatted_images, cleaned_reviews)

    # Print example
    if cleaned_reviews:
        print("\nExample review:")
        print(json.dumps(cleaned_reviews[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
