-- Warehouse schema. Idempotent: safe to run on every DAG run.

-- Order-level fact table (one row per cleaned sale line).
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id      TEXT,
    order_date    DATE        NOT NULL,
    customer_id   INTEGER,
    customer_name TEXT,
    region        TEXT,
    category      TEXT,
    product       TEXT,
    quantity      INTEGER,
    unit_price    NUMERIC(10, 2),
    discount      NUMERIC(4, 2),
    revenue       NUMERIC(12, 2),
    loaded_at     TIMESTAMP   DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales (order_date);

-- Pre-aggregated daily rollup the report/analytics layer reads from.
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    order_date DATE,
    region     TEXT,
    category   TEXT,
    orders     INTEGER,
    units      INTEGER,
    revenue    NUMERIC(14, 2),
    PRIMARY KEY (order_date, region, category)
);
