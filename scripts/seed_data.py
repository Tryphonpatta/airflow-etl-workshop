"""Manually generate raw sales CSVs for a date range (handy for backfills).

Run from your host (needs Faker):
    pip install Faker
    python scripts/seed_data.py --start 2026-06-01 --end 2026-06-07

Or skip this entirely and just let the DAG generate data day by day.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Make `include` importable when run from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from include.generator import generate_for_date  # noqa: E402


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--rows", type=int, default=400)
    p.add_argument("--out", default="data/raw")
    args = p.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    for d in _daterange(start, end):
        path = generate_for_date(d.isoformat(), args.out, rows=args.rows)
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
