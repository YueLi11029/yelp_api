# NYC Restaurant Team Collector (Yelp Fusion)

Production-ready team workflow for collecting NYC restaurant data by borough using the Yelp Fusion API only.

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
# Optional additional keys:
export YELP_API_KEY_3="your_key_3"
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

- Uses Yelp Fusion search endpoint only.
- Paginates with `limit=50`, offsets `0..950`.
- Stops when no results are returned.
- Stores data in `data/local_<borough>.db`.
- Upserts by `id`:
  - Insert if new.
  - Update `rating`, `review_count`, `is_closed`, `last_updated` if changed.
- Exports full borough DB to:
  - `data/borough_exports/<borough>_week<week>.csv`
- Logs request count, active key index, key rotations, completion stats.

## Run Weekly Merge

Merge weekly borough CSVs into one master file:

```bash
python src/merge.py --week 14
```

If `--week` is omitted, current ISO week is used.

Output:

- `data/master/master_week<week>.csv`

Merge rules:

- Concatenate all borough weekly CSVs.
- De-duplicate by `id`.
- If duplicates exist, keep record with highest `review_count`.

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
3. Commit `data/master/master_week<week>.csv` if needed.

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
