"""
Filter Kaggle Yelp reviews to only those matching collected restaurants.

Input:
  - Collected restaurants: data/daily_exports/day1_restaurants.json
  - Kaggle reviews: data/kaggle/yelp_academic_dataset_review.json

Output:
  - Filtered reviews: data/processed/day1_filtered_reviews.json

Uses streaming (line-by-line) to avoid loading entire Kaggle file into memory.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"


def load_restaurant_ids(restaurants_file: Path | str) -> set[str]:
    """Load all business_ids from restaurants file into a set."""
    if isinstance(restaurants_file, str):
        restaurants_file = Path(restaurants_file)

    if not restaurants_file.exists():
        raise FileNotFoundError(f"Restaurants file not found: {restaurants_file}")

    print(f"Loading restaurants from {restaurants_file}...")
    restaurant_ids = set()

    with open(restaurants_file, "r", encoding="utf-8") as f:
        restaurants = json.load(f)
        if not isinstance(restaurants, list):
            raise ValueError("Restaurants file must contain a JSON array")

        for restaurant in restaurants:
            business_id = restaurant.get("business_id")
            if business_id:
                restaurant_ids.add(business_id)

    print(f"Loaded {len(restaurant_ids)} unique restaurants")
    return restaurant_ids


def filter_reviews_streaming(
    reviews_file: Path | str,
    restaurant_ids: set[str],
) -> list[dict]:
    """
    Stream read Kaggle reviews and filter to matching restaurants.

    Reads line-by-line to avoid loading entire file into memory.
    Expects JSONL format (one JSON object per line).
    """
    if isinstance(reviews_file, str):
        reviews_file = Path(reviews_file)

    if not reviews_file.exists():
        raise FileNotFoundError(f"Reviews file not found: {reviews_file}")

    print(f"\nStreaming reviews from {reviews_file}...")
    print("(Expected format: JSONL - one JSON object per line)")
    filtered_reviews = []
    total_reviews = 0
    matched_reviews = 0
    skipped_reviews = 0

    try:
        with open(reviews_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line in ("[]", "[", "]"):
                    # Skip empty lines and JSON array markers
                    continue

                total_reviews += 1

                # Progress indicator every 50k lines
                if total_reviews % 50000 == 0:
                    print(f"  Processed {total_reviews:,} reviews... ({matched_reviews:,} matched)")

                try:
                    review = json.loads(line)
                except json.JSONDecodeError as e:
                    skipped_reviews += 1
                    if line_num <= 5 or skipped_reviews <= 3:
                        print(f"  ⚠ Skipped line {line_num}: {str(e)[:50]}")
                    continue

                if not isinstance(review, dict):
                    skipped_reviews += 1
                    continue

                # Check if this review matches a restaurant we collected
                business_id = review.get("business_id")
                if business_id and business_id in restaurant_ids:
                    # Extract only the fields we need
                    filtered_review = {
                        "review_id": review.get("review_id"),
                        "business_id": review.get("business_id"),
                        "stars": review.get("stars", review.get("rating", 5)),
                        "text": review.get("text", ""),
                        "user_id": review.get("user_id"),
                        "date": review.get("date"),
                    }
                    filtered_reviews.append(filtered_review)
                    matched_reviews += 1

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)

    print(f"\nProcessing complete:")
    print(f"  Total reviews read: {total_reviews:,}")
    print(f"  Matched reviews: {matched_reviews:,}")
    print(f"  Skipped (parse error): {skipped_reviews}")

    return filtered_reviews


def save_filtered_reviews(reviews: list[dict], output_file: Path | str) -> None:
    """Save filtered reviews to JSON file."""
    if isinstance(output_file, str):
        output_file = Path(output_file)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nSaving {len(reviews)} filtered reviews to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(reviews, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved successfully")


def print_statistics(
    restaurant_ids: set[str],
    filtered_reviews: list[dict],
) -> None:
    """Print validation statistics."""
    unique_businesses_with_reviews = len(set(r["business_id"] for r in filtered_reviews))
    avg_reviews_per_business = (
        len(filtered_reviews) / max(unique_businesses_with_reviews, 1)
        if unique_businesses_with_reviews > 0
        else 0
    )

    print("\n" + "=" * 60)
    print("FILTERING STATISTICS")
    print("=" * 60)
    print(f"Total restaurants collected: {len(restaurant_ids):,}")
    print(f"Total filtered reviews: {len(filtered_reviews):,}")
    print(f"Unique businesses with reviews: {unique_businesses_with_reviews:,}")
    if unique_businesses_with_reviews > 0:
        print(f"Coverage: {unique_businesses_with_reviews / len(restaurant_ids) * 100:.1f}% of restaurants")
        print(f"Average reviews per business: {avg_reviews_per_business:.2f}")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter Kaggle reviews to only those matching collected restaurants"
    )
    parser.add_argument(
        "--restaurants",
        type=str,
        default="data/daily_exports/day1_restaurants.json",
        help="Path to restaurants JSON file",
    )
    parser.add_argument(
        "--reviews",
        type=str,
        default="data/kaggle/yelp_academic_dataset_review.json",
        help="Path to Kaggle reviews JSON file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/day1_filtered_reviews.json",
        help="Output path for filtered reviews",
    )
    args = parser.parse_args()

    restaurants_file = PROJECT_ROOT / args.restaurants
    reviews_file = PROJECT_ROOT / args.reviews
    output_file = PROJECT_ROOT / args.output

    print(f"Filtering Kaggle reviews to match collected restaurants")
    print(f"=" * 60)

    # Load restaurants
    restaurant_ids = load_restaurant_ids(restaurants_file)

    # Stream filter reviews
    filtered_reviews = filter_reviews_streaming(reviews_file, restaurant_ids)

    # Save output
    save_filtered_reviews(filtered_reviews, output_file)

    # Print statistics
    print_statistics(restaurant_ids, filtered_reviews)

    # Show example
    if filtered_reviews:
        print("\nExample filtered review:")
        print(json.dumps(filtered_reviews[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
