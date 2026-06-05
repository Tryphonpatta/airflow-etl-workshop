"""Transform step: clean the raw CSV and compute derived columns.

This is the heart of the workshop — everything the generator dirtied up gets
cleaned here. Returns a tidy DataFrame ready to load into the warehouse.
"""
from __future__ import annotations

import pandas as pd


def transform(raw_csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(raw_csv_path)

    # 1. Drop exact duplicate order lines.
    df = df.drop_duplicates()

    # 2. Fix missing regions -> "Unknown" so they still appear in reports.
    df["region"] = df["region"].fillna("Unknown").replace("", "Unknown")

    # 3. Coerce numerics; rows with an unparseable/blank price become NaN.
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0)

    # 4. Drop rows that can't be valid sales: no price, or quantity <= 0.
    df = df[df["unit_price"].notna()]
    df = df[df["quantity"] > 0]

    # 5. Derived metric: net revenue after discount.
    df["revenue"] = (df["quantity"] * df["unit_price"] * (1 - df["discount"])).round(2)

    # 6. Normalise the date column to a real date type.
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

    # Stable column order for the warehouse table.
    df = df[[
        "order_id", "order_date", "customer_id", "customer_name",
        "region", "category", "product", "quantity", "unit_price",
        "discount", "revenue",
    ]].reset_index(drop=True)

    return df
