# 🛠️ Sales ETL Workshop — Apache Airflow

A self-contained, hands-on workshop for practicing a real ETL pipeline:

> **Generate synthetic sales data → Extract → Transform → Load into Postgres → Aggregate → Visualize with matplotlib**, all orchestrated by **Apache Airflow** running in Docker.

```
 Faker generator        Airflow DAG (sales_etl)                      Postgres            matplotlib
┌────────────────┐   ┌───────────────────────────────────────┐   ┌──────────────┐   ┌──────────────┐
│ sales_<ds>.csv │──▶│ extract → transform → load → aggregate │──▶│ fact_sales    │──▶│ PNG charts   │
│ (data/raw/)    │   │              → quality check → report  │   │ agg_daily_... │   │ (data/reports)│
└────────────────┘   └───────────────────────────────────────┘   └──────────────┘   └──────────────┘
```

---

## 1. Prerequisites

- **Docker Desktop** running (you already have Docker 27.x ✅)
- ~4 GB free RAM for the containers
- A browser for the Airflow UI

---

## 2. Project layout

```
etl/
├── docker-compose.yaml      # Airflow (LocalExecutor) + 2 Postgres + init
├── Dockerfile               # Airflow image + Faker/matplotlib/pandas
├── requirements.txt
├── .env                     # UID, UI login, warehouse connection string
│
├── dags/
│   └── sales_etl.py         # ⭐ the pipeline (TaskFlow API)
├── include/                 # importable helper modules (mounted into Airflow)
│   ├── generator.py         #   EXTRACT  – synthetic, intentionally-messy data
│   ├── transform.py         #   TRANSFORM – clean + compute revenue
│   ├── warehouse.py         #   LOAD + AGGREGATE (PostgresHook)
│   ├── reporting.py         #   VISUALIZE – matplotlib charts
│   └── sql/schema.sql       #   warehouse DDL (idempotent)
├── scripts/seed_data.py     # manually pre-generate a date range (optional)
├── analytics/visualize.py   # host-side ad-hoc analysis vs localhost:5433
└── data/{raw,processed,reports}/
```

---

## 3. Start the stack

```bash
cd etl
docker compose up -d --build      # first run builds the image (a few minutes)
```

Watch it come up:

```bash
docker compose ps
```

When `airflow-webserver` is **healthy**, open **http://localhost:8080**
and log in with **`airflow` / `airflow`**.

> Stop everything later with `docker compose down`.
> Wipe all data and start clean with `docker compose down -v`.

---

## 4. Run the pipeline

1. In the UI, find the **`sales_etl`** DAG and toggle it **on** (it starts paused).
2. Click the **▶ Trigger** button to run it once.
3. Open the **Graph** view and watch the tasks go green in order:
   `generate_raw_data → transform_data → load_to_warehouse → build_daily_aggregate → data_quality_check → build_report`
4. Click any task → **Logs** to see what it did.

**Where the output lands:**
- Raw CSV → `data/raw/sales_<date>.csv`
- Cleaned parquet → `data/processed/sales_<date>.parquet`
- Charts → `data/reports/*.png`  ← open these!

---

## 5. Inspect the warehouse

The analytics Postgres is published on **`localhost:5433`** (user/pass/db = `warehouse`/`warehouse`/`sales`).

```bash
# Quick peek with psql inside the container:
docker compose exec warehouse-postgres \
  psql -U warehouse -d sales -c "SELECT region, SUM(revenue) FROM fact_sales GROUP BY region;"
```

Or run the host-side analysis script (prints top products + saves a chart):

```bash
pip install pandas matplotlib psycopg2-binary tabulate
python analytics/visualize.py
```

---

## 6. Backfill multiple days (see Airflow's real power)

The DAG is **idempotent** (load = delete-then-insert per date), so backfilling is safe.

**Option A — let Airflow backfill:** edit `dags/sales_etl.py`, set `catchup=True`,
move `start_date` back a week, save, then unpause. Airflow will queue one run per missed day.

**Option B — CLI backfill:**
```bash
docker compose exec airflow-scheduler \
  airflow dags backfill sales_etl -s 2026-06-01 -e 2026-06-07
```

Re-run `python analytics/visualize.py` and you'll see a real multi-day trend line.

---

## 7. 🎓 Workshop exercises (progressive)

Practice by extending the pipeline. Suggested order:

1. **Tune the data** — bump `rows=400` in `generate_raw_data`, or add a new
   category in `include/generator.py`. Re-run and watch it flow through.
2. **Add a transform rule** — in `include/transform.py`, flag orders over
   $1,000 as `is_high_value`. Add the column to `schema.sql` and the load.
3. **New aggregate** — add a `agg_monthly_sales` table + a task that fills it.
4. **Stronger data quality** — make `data_quality_check` fail if revenue drops
   >50% vs the previous day (query `agg_daily_sales`). Watch the DAG turn red.
5. **New chart** — add a "top 10 products" bar chart to `include/reporting.py`.
6. **Sensors / scheduling** — change `schedule` to `"0 6 * * *"` (6am daily) and
   read about `@daily` vs cron in the Airflow docs.
7. **Parametrize** — expose `rows` as a DAG `param` and trigger with config.
8. **Swap the viz** — replace matplotlib with a Streamlit dashboard reading
   from `localhost:5433` (stretch goal).

---

## 8. Concepts this workshop teaches

| Concept | Where you see it |
|---|---|
| DAGs & the TaskFlow API | `dags/sales_etl.py` |
| Task dependencies / graph | the `>>` wiring at the bottom of the DAG |
| XCom (passing data between tasks) | file paths returned from each `@task` |
| Connections & Hooks | `warehouse.py` via `AIRFLOW_CONN_WAREHOUSE_POSTGRES` |
| Idempotent / backfill-safe loads | `load_fact` delete-then-insert |
| Scheduling, catchup & backfill | `schedule`, `catchup`, section 6 |
| Retries & data-quality gates | `default_args`, `data_quality_check` |
| Separation of orchestration vs logic | thin DAG, real work in `include/` |

---

## 9. Troubleshooting

- **Webserver won't start / unhealthy:** give it 1–2 min after `--build`; check
  `docker compose logs airflow-webserver`.
- **`include` import errors in a task:** the DAG relies on `PYTHONPATH` set in
  compose — make sure you didn't remove it.
- **Permission errors on `logs/`/`data/`:** on Linux set `AIRFLOW_UID` in `.env`
  to `$(id -u)` and `docker compose up -d` again.
- **Port 8080 or 5433 in use:** change the host side of the `ports:` mapping.
- **Reset everything:** `docker compose down -v` then `docker compose up -d --build`.

Happy orchestrating! 🚀
