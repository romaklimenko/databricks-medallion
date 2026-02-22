# Medallion Architecture Comparison

A demonstration project comparing **seven different approaches** to implementing the Databricks Medallion Architecture (Bronze → Silver → Gold) on identical sample data. All approaches produce the same final gold-layer Kimball dimensional model, making it easy to compare developer experience, code volume, and maintainability.

## Architecture

```
CSV Files ──→ Bronze (raw + metadata) ──→ Silver (cleansed, typed) ──→ Gold (Kimball star schema)
```

### Gold Layer — Dimensional Model

```
dim_customer (SCD2)  ←──  fact_order_line  ──→  dim_product (SCD1)
                               │
                               ▼
                           dim_date
```

## Approaches

| # | Approach | Schema | SCD2 Method | Status |
|---|----------|--------|-------------|--------|
| 1 | Python Notebooks | `approach_notebooks` | Manual MERGE with DataFrames | Done |
| 2 | SQL with COPY INTO | `approach_sql` | MERGE INTO | Done |
| 3 | Materialized Views + Streaming Tables | `approach_mv_st` | Scheduled MERGE workaround | Done |
| 4 | Declarative Pipelines (SQL) | `approach_dpl_sql` | APPLY CHANGES INTO | Done |
| 5 | Declarative Pipelines (Python) | `approach_dpl_python` | dlt.apply_changes() | Done |
| 6 | Delta Live Tables | `approach_dlt` | APPLY CHANGES | Done |
| 7 | dbt-core | `approach_dbt` | dbt snapshots | Done |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI with bundle support (`databricks bundle` commands)
- [uv](https://docs.astral.sh/uv/) for Python package management
- Python 3.10+

## Quick Start

> **Note:** Before deploying, create the `medallion` catalog in the Databricks UI (Catalog Explorer → Create Catalog). The setup workflow creates schemas inside it but cannot create the catalog itself.

```bash
# Clone and install
git clone <repo-url>
cd databricks-medallion
uv sync

# Deploy the bundle
databricks bundle deploy

# Run setup (creates schemas + uploads data)
databricks bundle run setup

# Run any approach
databricks bundle run approach_notebooks
databricks bundle run approach_sql
# ... etc
```

## Project Structure

```
databricks-medallion/
├── databricks.yml              # Databricks Asset Bundle config
├── pyproject.toml              # Python project (uv-managed)
├── data/
│   ├── batch_1/                # Initial load (5 customers, 5 products, 7 orders, 10 lines)
│   └── batch_2/                # Incremental (SCD2 triggers + new records)
├── setup/                      # Schema creation + data upload
├── src/
│   ├── notebooks/              # Approach 1: Python notebooks
│   ├── sql/                    # Approach 2: SQL with COPY INTO
│   ├── mv_st/                  # Approach 3: Materialized Views + Streaming Tables
│   ├── dpl_sql/                # Approach 4: Declarative Pipelines (SQL)
│   ├── dpl_python/             # Approach 5: Declarative Pipelines (Python)
│   ├── dlt/                    # Approach 6: Delta Live Tables
│   ├── dbt_project/            # Approach 7: dbt-core
│   └── validate.sql            # Cross-approach validation
└── README.md
```

## Expected Gold Layer Output

After processing both batches, every approach produces:

| Table | Rows | Notes |
|-------|------|-------|
| `gold_dim_customer` | 8 | 5 current + 1 new + 2 historical (SCD2) |
| `gold_dim_product` | 5 | Static reference data |
| `gold_dim_date` | 91 | Full months Jan–Mar 2024 |
| `gold_fact_order_line` | 11 | 10 from batch 1 + 1 from batch 2 |

## Setup

Schemas and the landing volume are created automatically by `databricks bundle deploy` (defined as bundle resources). The setup workflow uploads CSV data to the volume:

```bash
databricks bundle deploy   # creates schemas + volume
databricks bundle run setup # uploads CSV files
```

The setup job runs a single task:
- **upload_data** — copies `data/batch_1/` and `data/batch_2/` CSVs into `/Volumes/{catalog}/landing/raw_files/`

After setup, the volume contains:

```
raw_files/
├── batch_1/
│   ├── customers.csv    (5 rows)
│   ├── products.csv     (5 rows)
│   ├── orders.csv       (7 rows)
│   └── order_lines.csv  (10 rows)
└── batch_2/
    ├── customers.csv    (3 rows — 2 updates + 1 new)
    ├── orders.csv       (2 rows — 1 correction + 1 new)
    └── order_lines.csv  (1 row)
```

## Approach Details

### Approach 1: Python Notebooks

Three PySpark notebooks in [src/notebooks/](src/notebooks/) implementing the full medallion pipeline:

- **bronze.py** — Reads CSVs from the landing volume using glob patterns (`batch_*/*.csv`). All columns kept as strings. Adds `_source_file`, `_ingested_at`, and `_batch_id` metadata.
- **silver.py** — Deduplicates (latest batch wins via `row_number` window), casts types, standardizes text (country → upper, email → lower, segment → initcap), and computes `line_amount`.
- **gold.py** — Builds the Kimball dimensional model:
  - `dim_product` — SCD1, latest version from silver
  - `dim_date` — Generated from order date range, padded to full months (Jan–Mar 2024 = 91 days)
  - `dim_customer` — SCD2 implemented manually by comparing batch_1 vs batch_2 customer snapshots from bronze, producing historical + current rows with `valid_from`/`valid_to`/`is_current`
  - `fact_order_line` — Joins silver order lines/orders to dimension tables; customer FK uses SCD2 date-range lookup

Run: `databricks bundle run approach_notebooks`

### Approach 2: SQL with COPY INTO

Three SQL notebooks in [src/sql/](src/sql/) implementing the full medallion pipeline using pure SQL:

- **bronze.sql** — Uses `COPY INTO` to ingest CSVs from the landing volume into Delta tables. Adds `_source_file`, `_ingested_at`, and `_batch_id` metadata via `_metadata.file_path`.
- **silver.sql** — `CREATE OR REPLACE TABLE AS SELECT` with CTE-based deduplication (latest batch wins via `ROW_NUMBER`), type casting, text standardization, and `line_amount` derivation.
- **gold.sql** — Builds the Kimball dimensional model entirely in SQL:
  - `dim_product` — SCD1, simple SELECT from silver
  - `dim_date` — Generated via `SEQUENCE()` + `LATERAL VIEW EXPLODE`, padded to full months
  - `dim_customer` — SCD2 built by comparing batch_1 vs batch_2 with CTEs (`LEFT ANTI JOIN` for unchanged/new, `INNER JOIN` with change detection for historical/current)
  - `fact_order_line` — Joins silver tables to dimensions; customer FK uses SCD2 date-range lookup

Run: `databricks bundle run approach_sql`

### Approach 3: Materialized Views + Streaming Tables

Two SQL notebooks in [src/mv_st/](src/mv_st/) using standalone streaming tables and materialized views — the simplest declarative approach:

- **setup.sql** — Creates the full pipeline declaratively:
  - **Bronze**: 4 streaming tables using `CREATE OR REFRESH STREAMING TABLE` with `STREAM read_files()` (Auto Loader under the hood)
  - **Silver**: 4 materialized views with `CREATE OR REPLACE MATERIALIZED VIEW` — dedup, type casting, standardization, `line_amount` derivation
  - **Gold**: `dim_product` and `dim_date` as materialized views
- **scd2_merge.sql** — SCD2 workaround (MVs cannot natively implement SCD2):
  - `dim_customer` built as a regular table using the same batch-comparison CTE approach
  - `fact_order_line` as a materialized view joining silver MVs to dimension tables

**Limitation**: Materialized views don't support SCD2 natively. The `dim_customer` dimension requires a scheduled MERGE notebook as a workaround.

Run: `databricks bundle run approach_mv_st`

### Approach 4: Declarative Pipelines (SQL)

A single SQL file in [src/dpl_sql/](src/dpl_sql/) defining a complete Declarative Pipeline (formerly DLT):

- **Bronze**: 4 streaming tables using `CREATE OR REFRESH STREAMING TABLE` with `STREAM read_files()` — same ingestion pattern as Approach 3 but managed by the pipeline runtime
- **Silver**: 4 materialized views with **data quality expectations** (`CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW`) — dedup, type casting, standardization, `line_amount` derivation
- **Gold**:
  - `dim_customer` — SCD2 via `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` with `TRACK HISTORY ON` limited to email, address, city, country, segment. An internal streaming table (`_scd2_dim_customer`) holds the raw SCD2 data; a MV wraps it to add `customer_sk`
  - `dim_product` — SCD1, materialized view from silver
  - `dim_date` — Generated from order date range, padded to full months
  - `fact_order_line` — Joins silver tables to dimensions with SCD2 range join

**Key difference from other approaches**: APPLY CHANGES produces `__START_AT`/`__END_AT` columns instead of `valid_from`/`valid_to`. `__END_AT` is `NULL` for current records (not `9999-12-31`). The fact table join uses `order_date >= __START_AT AND (__END_AT IS NULL OR order_date < __END_AT)`.

Run: `databricks bundle run approach_dpl_sql`

### Approach 5: Declarative Pipelines (Python)

A single Python file in [src/dpl_python/](src/dpl_python/) defining a complete Declarative Pipeline using the Python DLT API — same logical flow as Approach 4, different syntax:

- **Bronze**: 4 streaming tables using `@dlt.table` with `spark.readStream.format("cloudFiles")` (Auto Loader)
- **Silver**: 4 materialized views with `@dlt.expect_or_drop()` data quality expectations — dedup via PySpark `Window` + `row_number()`, type casting, standardization, `line_amount` derivation
- **Gold**:
  - `dim_customer` — SCD2 via `dlt.apply_changes()` with `stored_as_scd_type=2` and `track_history_column_list` limited to email, address, city, country, segment. A temporary streaming view feeds cleaned data; a MV wraps the SCD2 target to add `customer_sk`
  - `dim_product` — SCD1, materialized view from silver
  - `dim_date` — Generated from order date range, padded to full months
  - `fact_order_line` — PySpark DataFrame joins with SCD2 range join

**Same SCD2 column behavior as Approach 4**: `__START_AT`/`__END_AT` instead of `valid_from`/`valid_to`.

Run: `databricks bundle run approach_dpl_python`

### Approach 6: Delta Live Tables (Classic DLT Syntax)

A single SQL file in [src/dlt/](src/dlt/) using the **classic DLT SQL syntax** — the same pipeline runtime as Approaches 4/5 but with the original pre-rebrand keywords:

| Modern (Approach 4) | Classic DLT (Approach 6) |
|---|---|
| `CREATE OR REFRESH STREAMING TABLE` | `CREATE STREAMING LIVE TABLE` |
| `CREATE OR REFRESH MATERIALIZED VIEW` | `CREATE LIVE TABLE` |

- **Bronze**: 4 streaming live tables from `STREAM read_files()`
- **Silver**: 4 live tables with expectations — same dedup/transform logic as all other approaches
- **Gold**:
  - `dim_customer` — SCD2 via `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`, sourced from `STREAM(LIVE.bronze_customers)` with inline transforms (contrast with Approach 4 which reads directly from the volume)
  - `dim_product`, `dim_date`, `fact_order_line` — live tables

**Same SCD2 column behavior as Approaches 4/5**: `__START_AT`/`__END_AT` instead of `valid_from`/`valid_to`.

Run: `databricks bundle run approach_dlt`

### Approach 7: dbt-core

A full dbt project in [src/dbt_project/](src/dbt_project/) using the `dbt-databricks` adapter:

- **Bronze** (`models/bronze/`): 4 models using `read_files()` to ingest CSVs from the landing volume — same raw ingestion as other approaches
- **Silver** (`models/silver/`): 4 models with dedup, type casting, standardization. `silver_customers` accepts a `snapshot_batch` variable to control which batches are included (enables the two-phase snapshot workflow)
- **Gold** (`models/gold/`):
  - `dim_customer` — SCD2 via **dbt snapshot** (`strategy='check'` on email, address, city, country, segment). The `gold_dim_customer` model reads from `snap_dim_customer` and maps `dbt_valid_from`/`dbt_valid_to` to `valid_from`/`valid_to`/`is_current`
  - `dim_product` — SCD1 from silver
  - `dim_date` — Generated from order date range, padded to full months
  - `fact_order_line` — Joins silver tables to gold dimensions with SCD2 date-range lookup
- **Tests** (`schema.yml`): `not_null`, `unique`, `accepted_values`, `relationships`

**Workflow** (6 sequential dbt commands in one task):
1. `dbt run --select bronze silver` with `snapshot_batch: batch_1` (silver_customers sees only batch 1)
2. `dbt snapshot` (captures initial customer state)
3. `dbt run --select silver_customers` with `snapshot_batch: all` (silver_customers now includes batch 2)
4. `dbt snapshot` (detects changes → creates SCD2 history rows)
5. `dbt run --select gold` (builds dimensional model from snapshot + silver)
6. `dbt test` (validates all constraints)

**Key difference**: `valid_from`/`valid_to` are derived from `_batch_id` (batch_1 → 2024-01-01, batch_2 → 2024-03-01) using `LEAD()` window function, since dbt snapshot timestamps reflect execution time, not business dates. Row counts are identical.

Run: `databricks bundle run approach_dbt`

## Validation

A SQL notebook at [src/validate.sql](src/validate.sql) compares gold-layer output across all 7 approaches:

1. **Row count validation** — verifies each gold table has the expected number of rows (dim_customer=8, dim_product=5, dim_date=91, fact_order_line=11) across all approaches
2. **Content comparison: dim_product** — hashes business attributes (excluding surrogate keys) and verifies all approaches produce identical rows
3. **Content comparison: dim_customer (SCD2)** — normalizes column names (`__START_AT` → `valid_from`, `__END_AT` → `valid_to` with NULL → 9999-12-31) for DPL/DLT approaches, then compares hashes per customer+valid_from
4. **Content comparison: dim_date** — verifies all 91 date rows match across approaches (only shows mismatches)
5. **Content comparison: fact_order_line** — compares measures and degenerate dimensions (skips surrogate keys which vary by approach)
6. **Overall summary** — single pass/fail per table with list of any failing approaches

Run: `databricks bundle run validate`

## Configuration

Bundle variables (set in `databricks.yml` or override at deploy time):

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog_name` | `medallion` | Unity Catalog catalog (must be created via UI first) |
| `environment` | `dev` | Deployment environment |
