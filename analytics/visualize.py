"""Standalone analytics — run from your HOST against the warehouse.

The DAG already writes charts to data/reports/, but this script lets you
explore the warehouse interactively without Airflow. The warehouse Postgres is
published on localhost:5433 by docker-compose.

    pip install pandas matplotlib psycopg2-binary tabulate
    python analytics/visualize.py
"""
from __future__ import annotations

import os

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Override with env var if you changed the compose ports/creds.
DB_URL = os.environ.get(
    "WAREHOUSE_URL", "postgresql://warehouse:warehouse@localhost:5433/sales"
)
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "reports")


def main() -> None:
    df = pd.read_sql("SELECT * FROM agg_daily_sales ORDER BY order_date", DB_URL)
    if df.empty:
        print("Warehouse is empty. Trigger the `sales_etl` DAG first.")
        return

    # Headline numbers in the terminal.
    print("\n=== Top products by revenue ===")
    fact = pd.read_sql(
        "SELECT product, SUM(revenue) AS revenue, SUM(quantity) AS units "
        "FROM fact_sales GROUP BY product ORDER BY revenue DESC LIMIT 10",
        DB_URL,
    )
    print(fact.to_markdown(index=False))

    print(f"\nTotal revenue: {df['revenue'].sum():,.2f}")
    print(f"Days loaded:   {df['order_date'].nunique()}")

    # A combined chart you can tweak in the workshop.
    os.makedirs(OUT_DIR, exist_ok=True)
    daily = df.groupby("order_date", as_index=False)["revenue"].sum()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(daily["order_date"], daily["revenue"], marker="o")
    ax.set_title("Daily Revenue (from analytics/visualize.py)")
    fig.autofmt_xdate()
    out = os.path.abspath(os.path.join(OUT_DIR, "adhoc_revenue_trend.png"))
    fig.tight_layout()
    fig.savefig(out)
    print(f"\nSaved chart -> {out}")


if __name__ == "__main__":
    main()
