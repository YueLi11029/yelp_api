"""
Clean Yelp reviews dataset for NLP and ML tasks.

Input:
  - Kaggle reviews: data/kaggle/yelp_academic_dataset_review.json (JSONL)

Output:
  - Clean dataset: data/processed/reviews_clean.json

Processing:
  - Stream read JSONL file (line by line)
  - Clean text (lowercase, remove newlines, collapse spaces)
  - Keep only: review_id, stars, text
  - Optional sampling (max_reviews)
  - Print statistics
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"


def clean_text(text: str | None) -> str:
    """
    Clean review text:
    - Convert to lowercase
    - Remove newline characters
    - Remove extra spaces
    """
    if not isinstance(text, str):
        return ""

    # Lowercase
    text = text.lower()

    # Remove newlines and carriage returns
    text = text.replace("\n", " ")
    text = text.replace("\r", " ")
    text = text.replace("\t", " ")

    # Collapse multiple spaces into single space
    text = re.sub(r"\s+", " ", text)

    # Trim
    return text.strip()


def stream_read_reviews(
    reviews_file: Path | str,
    max_reviews: int | None = None,
) -> list[dict]:
    """
    Stream read Kaggle reviews and return clean subset.

    Reads line-by-line to avoid loading entire file into memory.
    """
    if isinstance(reviews_file, str):
        reviews_file = Path(reviews_file)

    if not reviews_file.exists():
        raise FileNotFoundError(f"Reviews file not found: {reviews_file}")

    print(f"Streaming reviews from {reviews_file}...")
    if max_reviews:
        print(f"(Sampling max {max_reviews:,} reviews)")
    else:
        print("(All reviews will be processed)")

    reviews = []
    total_read = 0
    skipped = 0
    text_lengths = []

    try:
        with open(reviews_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line in ("[]", "[", "]"):
                    continue

                total_read += 1

                # Progress indicator
                if total_read % 100000 == 0:
                    print(f"  Processed {total_read:,} reviews... Saved {len(reviews):,}")

                # Check if we've hit the sampling limit
                if max_reviews and len(reviews) >= max_reviews:
                    print(f"  Reached max_reviews limit ({max_reviews:,})")
                    break

                try:
                    review = json.loads(line)
                except json.JSONDecodeError:
                    skipped += 1
                    continue

                if not isinstance(review, dict):
                    skipped += 1
                    continue

                # Extract fields
                review_id = review.get("review_id")
                stars = review.get("stars", review.get("rating", 5))
                text = review.get("text", "")

                if not review_id:
                    skipped += 1
                    continue

                # Clean text
                clean = clean_text(text)

                if clean:
                    cleaned_review = {
                        "review_id": review_id,
                        "rating": int(stars) if isinstance(stars, (int, float)) else 5,
                        "text": clean,
                    }
                    reviews.append(cleaned_review)
                    text_lengths.append(len(clean))

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)

    print(f"\nProcessing complete:")
    print(f"  Total reviews read: {total_read:,}")
    print(f"  Reviews saved: {len(reviews):,}")
    print(f"  Skipped: {skipped}")

    return reviews, text_lengths


def save_reviews(reviews: list[dict], output_file: Path | str) -> None:
    """Save cleaned reviews to JSON file."""
    if isinstance(output_file, str):
        output_file = Path(output_file)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nSaving {len(reviews):,} reviews to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(reviews, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved successfully")


def print_statistics(reviews: list[dict], text_lengths: list[int]) -> None:
    """Print dataset statistics."""
    if not reviews:
        print("No reviews to analyze")
        return

    # Rating distribution
    rating_counts = {}
    for review in reviews:
        rating = review.get("rating", 5)
        rating_counts[rating] = rating_counts.get(rating, 0) + 1

    # Text statistics
    avg_text_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    min_text_length = min(text_lengths) if text_lengths else 0
    max_text_length = max(text_lengths) if text_lengths else 0

    print("\n" + "=" * 60)
    print("CLEAN DATASET STATISTICS")
    print("=" * 60)
    print(f"Total reviews: {len(reviews):,}")
    print(f"\nRating distribution:")
    for rating in sorted(rating_counts.keys()):
        count = rating_counts[rating]
        pct = count / len(reviews) * 100
        print(f"  {rating} stars: {count:,} ({pct:.1f}%)")
    print(f"\nText length statistics:")
    print(f"  Average: {avg_text_length:.0f} characters")
    print(f"  Min: {min_text_length} characters")
    print(f"  Max: {max_text_length} characters")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean Yelp reviews dataset for NLP and ML tasks"
    )
    parser.add_argument(
        "--reviews",
        type=str,
        default="data/kaggle/yelp_academic_dataset_review.json",
        help="Path to Kaggle reviews JSONL file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/reviews_clean.json",
        help="Output path for cleaned reviews",
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=100000,
        help="Maximum reviews to sample (None for all)",
    )
    args = parser.parse_args()

    reviews_file = PROJECT_ROOT / args.reviews
    output_file = PROJECT_ROOT / args.output

    print(f"Preparing clean review dataset for NLP/ML tasks")
    print(f"=" * 60)

    # Stream read and clean
    reviews, text_lengths = stream_read_reviews(reviews_file, args.max_reviews)

    if not reviews:
        print("No reviews found!")
        sys.exit(1)

    # Save output
    save_reviews(reviews, output_file)

    # Print statistics
    print_statistics(reviews, text_lengths)

    # Show example
    print("\nExample review:")
    print(json.dumps(reviews[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
