"""Sales ETL pipeline — the workshop's main DAG.

Flow (one run per day):

    generate_raw_data        EXTRACT  -> writes data/raw/sales_<ds>.csv
            │
    transform_data           TRANSFORM -> cleans + computes revenue (data/processed/)
            │
    create_warehouse_schema  (idempotent DDL)
            │
    load_to_warehouse        LOAD -> fact_sales (delete+insert for the date)
            │
    build_daily_aggregate    aggregate -> agg_daily_sales
            │
    data_quality_check       guard: fail loudly if a day loaded 0 rows
            │
    build_report             VISUALIZE -> PNG charts in data/reports/

Written with the TaskFlow API so data passes between tasks via XCom and the
dependency graph reads top-to-bottom.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from include import generator, transform as transform_mod, warehouse, reporting

RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"


@dag(
    dag_id="sales_etl",
    description="Generate → clean → load → aggregate → visualize daily sales",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,                # set True (and unpause) to backfill history
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["workshop", "etl", "sales"],
)
def sales_etl():

    @task
    def generate_raw_data(ds: str | None = None) -> str:
        """EXTRACT: produce the raw sales CSV for this run's logical date."""
        return generator.generate_for_date(ds, RAW_DIR, rows=400)

    @task
    def transform_data(raw_path: str, ds: str | None = None) -> str:
        """TRANSFORM: clean the raw file, write a processed parquet, return its path."""
        import os
        df = transform_mod.transform(raw_path)
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        out = os.path.join(PROCESSED_DIR, f"sales_{ds}.parquet")
        df.to_parquet(out, index=False)
        return out

    @task
    def create_warehouse_schema() -> None:
        warehouse.create_schema()

    @task
    def load_to_warehouse(processed_path: str, ds: str | None = None) -> int:
        """LOAD: idempotently push the day's rows into fact_sales."""
        import pandas as pd
        df = pd.read_parquet(processed_path)
        return warehouse.load_fact(df, ds)

    @task
    def build_daily_aggregate(ds: str | None = None) -> None:
        warehouse.build_daily_aggregate(ds)

    @task
    def data_quality_check(loaded_rows: int) -> None:
        """Cheap guard rail — a real pipeline would assert much more."""
        if loaded_rows == 0:
            raise ValueError("0 rows loaded for this date — something is wrong upstream.")

    @task
    def build_report() -> list[str]:
        """VISUALIZE: regenerate the matplotlib charts from the warehouse."""
        return reporting.build_charts()

    # ---- wire up the dependency graph ----
    raw = generate_raw_data()
    processed = transform_data(raw)
    schema = create_warehouse_schema()

    loaded = load_to_warehouse(processed)
    schema >> loaded                       # tables must exist before we load

    agg = build_daily_aggregate()
    loaded >> agg

    qc = data_quality_check(loaded)

    report = build_report()
    [agg, qc] >> report            # report runs only after aggregate + QC pass


sales_etl()
