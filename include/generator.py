"""Synthetic sales data generator.

Produces one CSV of raw order lines for a given date. We deliberately inject
some *messy* data (duplicates, blank regions, negative quantities, the odd
null price) so the Transform step in the ETL actually has work to do.

Used by:
  - the DAG's `extract` task (one file per logical date)
  - scripts/seed_data.py (to backfill a whole date range by hand)
"""
from __future__ import annotations

import csv
import os
import random
from datetime import datetime

from faker import Faker

REGIONS = ["North", "South", "East", "West", "Central"]
CATEGORIES = {
    "Electronics": ["Headphones", "Keyboard", "Monitor", "Webcam", "Charger"],
    "Home": ["Lamp", "Mug", "Cushion", "Blanket", "Vase"],
    "Office": ["Notebook", "Pen Set", "Stapler", "Desk Pad", "Folder"],
    "Sports": ["Yoga Mat", "Water Bottle", "Dumbbell", "Jump Rope", "Gloves"],
}

# Fixed columns we write to the raw CSV.
FIELDNAMES = [
    "order_id", "order_date", "customer_id", "customer_name",
    "region", "category", "product", "quantity", "unit_price", "discount",
]


def generate_for_date(ds: str, out_dir: str, rows: int = 400, seed: int | None = None) -> str:
    """Write ``sales_<ds>.csv`` into ``out_dir`` and return its full path.

    ``ds`` is an ISO date string (YYYY-MM-DD), matching Airflow's logical date.
    Seeding off the date keeps each day reproducible across re-runs/backfills.
    """
    if seed is None:
        seed = int(datetime.strptime(ds, "%Y-%m-%d").strftime("%Y%m%d"))
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"sales_{ds}.csv")

    records = []
    for _ in range(rows):
        category = rng.choice(list(CATEGORIES))
        product = rng.choice(CATEGORIES[category])
        records.append({
            "order_id": fake.uuid4(),
            "order_date": ds,
            "customer_id": rng.randint(1000, 1200),
            "customer_name": fake.name(),
            # ~5% of rows have a blank region -> Transform must handle it
            "region": "" if rng.random() < 0.05 else rng.choice(REGIONS),
            "category": category,
            "product": product,
            # occasional bad quantity (0 or negative) -> filtered in Transform
            "quantity": rng.choice([rng.randint(1, 8)] * 18 + [0, -1]),
            # ~3% null price -> Transform drops or imputes
            "unit_price": "" if rng.random() < 0.03 else round(rng.uniform(5, 500), 2),
            "discount": rng.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]),
        })

    # Sprinkle in ~2% exact duplicate rows -> Transform de-dupes them.
    dupes = [dict(r) for r in rng.sample(records, k=max(1, rows // 50))]
    records.extend(dupes)
    rng.shuffle(records)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)

    return path


if __name__ == "__main__":
    # Quick manual smoke test:  python include/generator.py
    out = generate_for_date(datetime.today().strftime("%Y-%m-%d"), "data/raw")
    print(f"Wrote {out}")
