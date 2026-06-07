"""Reporting/visualization step (matplotlib).

Reads the aggregate table from the warehouse and writes PNG charts into
data/reports/. Runs as the final DAG task *and* can be run standalone
(see analytics/visualize.py) once the warehouse has data.
"""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")  # headless: render to file, no display needed
import matplotlib.pyplot as plt  # noqa: E402

from include.warehouse import read_aggregate,read_fact  # noqa: E402

REPORT_DIR = "/opt/airflow/data/reports"


def build_charts(report_dir: str = REPORT_DIR) -> list[str]:
    """Generate the standard chart set and return the written file paths."""
    os.makedirs(report_dir, exist_ok=True)
    df = read_aggregate()
    if df.empty:
        raise ValueError("agg_daily_sales is empty — run the ETL first.")

    written: list[str] = []

    # 1. Revenue trend over time.
    daily = df.groupby("order_date", as_index=False)["revenue"].sum()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(daily["order_date"], daily["revenue"], marker="o")
    ax.set_title("Daily Revenue")
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue")
    fig.autofmt_xdate()
    p = os.path.join(report_dir, "revenue_trend.png")
    fig.tight_layout(); fig.savefig(p); plt.close(fig)
    written.append(p)

    # 2. Revenue by region.
    by_region = df.groupby("region", as_index=False)["revenue"].sum().sort_values("revenue")
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(by_region["region"], by_region["revenue"])
    ax.set_title("Revenue by Region")
    ax.set_xlabel("Revenue")
    p = os.path.join(report_dir, "revenue_by_region.png")
    fig.tight_layout(); fig.savefig(p); plt.close(fig)
    written.append(p)

    # 3. Revenue by category.
    by_cat = df.groupby("category", as_index=False)["revenue"].sum().sort_values("revenue")
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(by_cat["category"], by_cat["revenue"])
    ax.set_title("Revenue by Category")
    ax.set_ylabel("Revenue")
    p = os.path.join(report_dir, "revenue_by_category.png")
    fig.tight_layout(); fig.savefig(p); plt.close(fig)
    written.append(p)

    df_fact = read_fact()

    # 4. Top 10 products by revenue.
    by_prod = df_fact.groupby("product", as_index=False)["quantity"].sum().sort_values("quantity", ascending=False).head(10)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(by_prod["product"].astype(str), by_prod["quantity"])
    ax.set_title("Top 10 Products by Quantity")
    ax.set_xlabel("Product")
    ax.set_ylabel("Quantity")
    p = os.path.join(report_dir, "top_products.png")
    fig.tight_layout(); fig.savefig(p); plt.close(fig)
    written.append(p)

    return written


if __name__ == "__main__":
    for f in build_charts():
        print(f"Wrote {f}")
