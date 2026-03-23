# NYC Restaurant Team Collector (Yelp Fusion)

Production-ready borough workflow for collecting NYC restaurant metadata, reviews, and image URLs from Yelp Fusion API.

## Project Structure

```text
nyc-restaurant-app/
├── src/
│   ├── collector.py
│   ├── database.py
│   ├── exporter.py
│   └── merge.py
├── data/
│   ├── borough_exports/
│   └── master/
├── config/
│   └── settings.py
├── requirements.txt
├── README.md
└── .gitignore
```

## Setup

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Set Yelp API keys as environment variables (no hardcoded secrets):

```bash
export YELP_API_KEY_1="your_key_1"
export YELP_API_KEY_2="your_key_2"
export YELP_API_KEY_3="your_key_3"
export YELP_API_KEY_4="your_key_4"
```

The collector auto-detects all `YELP_API_KEY_<number>` keys and rotates on HTTP 429 or local limit exhaustion.

## Run Borough Collector

Each teammate runs their own borough locally:

```bash
python src/collector.py --borough Manhattan
python src/collector.py --borough Brooklyn
python src/collector.py --borough Queens
python src/collector.py --borough Bronx
python src/collector.py --borough Staten_Island
```

Optional week override:

```bash
python src/collector.py --borough Manhattan --week 14
```

### What collector does

- Step 1: Calls `/v3/businesses/search` with pagination (`limit=50`, offsets `0..950`) to collect restaurant metadata.
- Step 2: Calls `/v3/businesses/{id}` for detail enrichment and photo URLs.
- Step 3: Calls `/v3/businesses/{id}/reviews` to collect review text (up to 3 per business from Yelp).
- Uses SQLite UPSERT/INSERT OR IGNORE to prevent duplicate restaurants, reviews, and images.
- Batch commits every 50 DB write operations.
- Rotates API keys when HTTP 429 occurs or key request usage exceeds 480.
- Applies per-run safety limits:
  - `MAX_DETAIL_CALLS_PER_RUN = 200`
  - `MAX_REVIEW_CALLS_PER_RUN = 200`

Per borough run, exports three CSV files to `data/borough_exports/`:

- `<borough>_restaurants_week<week>.csv`
- `<borough>_reviews_week<week>.csv`
- `<borough>_images_week<week>.csv`

## Run Weekly Merge

Merge weekly borough CSVs into three master files:

```bash
python src/merge.py --week 14
```

If `--week` is omitted, current ISO week is used.

Output:

- `data/master/master_restaurants_week<week>.csv`
- `data/master/master_reviews_week<week>.csv`
- `data/master/master_images_week<week>.csv`

Merge rules:

- Restaurants: de-duplicate by `id` (keep row with highest `review_count`).
- Reviews: de-duplicate by `review_id`.
- Images: remove duplicate `image_url` values.

## Team GitHub Workflow

Commit only standardized CSV exports from `data/borough_exports/`.

Do not commit local SQLite databases (`*.db`).

Recommended weekly flow per teammate:

1. Pull latest changes.
2. Run collector for assigned borough.
3. Commit updated borough CSV.
4. Push and open PR.

Project maintainer weekly flow:

1. Pull merged borough CSVs.
2. Run merge script.
3. Commit weekly master CSV files in `data/master/` if needed.

## Cron Example (Weekly)

Run every Monday at 7:00 AM:

```cron
0 7 * * 1 cd /Users/liyue/Documents/yelp_api && /usr/bin/python3 src/collector.py --borough Manhattan
```

Repeat with each teammate's borough on their own machine.

## Important Safety Notes

- Yelp API keys must come from environment variables only.
- Never hardcode API keys.
- Never commit `.db` files.
- System stops safely when all available keys are exhausted.

## Example Output

Running:

```bash
python src/collector.py --borough Manhattan --week 14
```

Generates:

- `data/borough_exports/Manhattan_restaurants_week14.csv`
- `data/borough_exports/Manhattan_reviews_week14.csv`
- `data/borough_exports/Manhattan_images_week14.csv`
