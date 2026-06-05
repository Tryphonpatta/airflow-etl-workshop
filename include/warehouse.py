"""Warehouse helpers: schema creation, idempotent loads, and aggregation.

All functions take an Airflow connection id (default: ``warehouse_postgres``,
auto-registered from the AIRFLOW_CONN_WAREHOUSE_POSTGRES env var) and use the
PostgresHook so the DAG never hard-codes credentials.
"""
from __future__ import annotations

import os

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "warehouse_postgres"
SCHEMA_SQL = os.path.join(os.path.dirname(__file__), "sql", "schema.sql")


def _engine(conn_id: str = CONN_ID):
    return PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()


def create_schema(conn_id: str = CONN_ID) -> None:
    """Run the idempotent CREATE TABLE statements."""
    with open(SCHEMA_SQL) as f:
        ddl = f.read()
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(ddl)


def load_fact(df: pd.DataFrame, ds: str, conn_id: str = CONN_ID) -> int:
    """Idempotently load one day's cleaned rows into fact_sales.

    Deletes any existing rows for ``ds`` first so re-running the DAG for a date
    never double-counts (the classic backfill-safe load pattern).
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run("DELETE FROM fact_sales WHERE order_date = %s", parameters=(ds,))
    df.to_sql("fact_sales", _engine(conn_id), if_exists="append", index=False)
    return len(df)


def build_daily_aggregate(ds: str, conn_id: str = CONN_ID) -> None:
    """Refresh agg_daily_sales for one date from fact_sales."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(
        """
        DELETE FROM agg_daily_sales WHERE order_date = %(ds)s;

        INSERT INTO agg_daily_sales (order_date, region, category, orders, units, revenue)
        SELECT order_date,
               region,
               category,
               COUNT(*)        AS orders,
               SUM(quantity)   AS units,
               SUM(revenue)    AS revenue
        FROM fact_sales
        WHERE order_date = %(ds)s
        GROUP BY order_date, region, category;
        """,
        parameters={"ds": ds},
    )


def read_aggregate(conn_id: str = CONN_ID) -> pd.DataFrame:
    """Return the full daily aggregate table (used by the analytics layer)."""
    return pd.read_sql("SELECT * FROM agg_daily_sales ORDER BY order_date", _engine(conn_id))
