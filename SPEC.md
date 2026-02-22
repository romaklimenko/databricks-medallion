# Medallion Architecture Comparison Project

## Purpose

A demonstration project comparing **seven different approaches** to implementing the Databricks Medallion Architecture (Bronze → Silver → Gold) on identical sample data. All approaches produce the same final gold-layer Kimball dimensional model, making it easy to compare developer experience, code volume, and maintainability.

## Deployment

The entire project is deployed as a **Databricks Asset Bundle**. Each approach lives in its own schema within a single Unity Catalog catalog. The bundle handles deployment of notebooks, pipeline definitions, dbt profiles, and any supporting infrastructure.

---

## Catalog & Schema Layout

Use a single catalog (configurable via bundle variables, default: `medallion_comparison`).

| Schema | Approach |
|---|---|
| `landing` | Raw CSV files uploaded to a Unity Catalog volume |
| `approach_notebooks` | Python notebooks |
| `approach_sql` | SQL with views and COPY INTO |
| `approach_dbt` | dbt-core |
| `approach_dpl_sql` | Declarative Pipelines (SQL) |
| `approach_dpl_python` | Declarative Pipelines (Python) |
| `approach_mv_st` | Materialized Views + Streaming Tables (standalone) |
| `approach_dlt` | Delta Live Tables with APPLY CHANGES |

### Landing Volume

Create a Unity Catalog managed volume at:

```
{catalog}.landing.raw_files
```

Upload all CSV files from the `data/` folder into this volume. The CSVs are organized in two batches to support incremental/SCD2 testing:

```
raw_files/
  batch_1/
    customers.csv
    products.csv
    orders.csv
    order_lines.csv
  batch_2/
    customers.csv        ← contains updates + new customer (SCD2 trigger)
    orders.csv           ← status correction + new order
    order_lines.csv      ← lines for new order
```

---

## Sample Data Overview

### Entity Relationship

```
dim_customer (SCD2)  ←──  fact_order_line  ──→  dim_product
                              │
                              ▼
                          dim_date
```

### Grain

- **fact_order_line**: one row per order line (order_id + line_id)
- **dim_customer**: SCD2 — track address, email, and segment changes
- **dim_product**: SCD1 (overwrite) — simple, no history tracking needed
- **dim_date**: auto-generated from the date range in orders

### Row Counts

| Dataset | Batch 1 | Batch 2 | Notes |
|---|---|---|---|
| customers | 5 | 3 | 1 new, 2 changed (SCD2 triggers) |
| products | 5 | — | Static reference data |
| orders | 7 | 2 | 1 status correction, 1 new order |
| order_lines | 10 | 1 | Line for new order |

### SCD2 Scenarios (Customers)

After processing batch 2, `dim_customer` should contain:

| customer_id | What changed | Expected rows |
|---|---|---|
| 2 (Bob) | Email changed, segment upgraded Standard→Premium | 2 (old closed, new active) |
| 5 (Eva) | Moved from Copenhagen to Aarhus | 2 (old closed, new active) |
| 6 (Frank) | Brand new customer | 1 (active) |
| 1, 3, 4 | No change | 1 each (active) |

**Total dim_customer rows after both batches: 8** (6 original/active + 2 historical)

### Order Correction Scenario

- Order 1005: status changes from `shipped` → `completed` (simple overwrite in silver)
- Order 1008: new order from customer 6 (Frank, the new customer)

---

## Layer Definitions

### Bronze Layer

**Goal**: Ingest raw data with minimal transformation. Add metadata columns.

Tables per approach schema:
- `bronze_customers`
- `bronze_products`
- `bronze_orders`
- `bronze_order_lines`

Required metadata columns:
- `_source_file` (STRING) — source filename
- `_ingested_at` (TIMESTAMP) — ingestion timestamp
- `_batch_id` (STRING) — "batch_1" or "batch_2"

All source columns ingested as-is (preserve original types from CSV). Use schema inference or explicit schema — either is fine, but be consistent within each approach.

### Silver Layer

**Goal**: Cleansed, typed, deduplicated, conformed data. Business rules applied.

Tables:
- `silver_customers`
- `silver_products`
- `silver_orders`
- `silver_order_lines`

Transformations:
1. **Type casting**: proper DATE, DECIMAL, INTEGER types
2. **Deduplication**: if same record appears in multiple batches, latest batch wins
3. **Data quality checks**: reject rows where primary keys are null (log to a `_silver_quarantine` table or simply filter out)
4. **Standardization**:
   - `country` → uppercase
   - `email` → lowercase
   - `status` → lowercase
   - `segment` → capitalize first letter
5. **Derived columns**:
   - `order_lines.line_amount` = `quantity * unit_price * (1 - discount_pct / 100)`

### Gold Layer — Kimball Dimensional Model

Tables:
- `gold_dim_customer` — SCD2
- `gold_dim_product` — SCD1
- `gold_dim_date` — generated
- `gold_fact_order_line` — transactional fact

#### gold_dim_customer (SCD2)

| Column | Type | Notes |
|---|---|---|
| customer_sk | BIGINT | Surrogate key (auto-increment or generated) |
| customer_id | INT | Business/natural key |
| customer_name | STRING | |
| email | STRING | |
| address | STRING | |
| city | STRING | |
| country | STRING | |
| segment | STRING | |
| valid_from | DATE | |
| valid_to | DATE | 9999-12-31 for active records |
| is_current | BOOLEAN | |

#### gold_dim_product (SCD1)

| Column | Type | Notes |
|---|---|---|
| product_sk | BIGINT | Surrogate key |
| product_id | INT | Business key |
| product_name | STRING | |
| category | STRING | |
| subcategory | STRING | |
| unit_price | DECIMAL(10,2) | Current price from product master |

#### gold_dim_date

| Column | Type | Notes |
|---|---|---|
| date_key | INT | YYYYMMDD format |
| full_date | DATE | |
| year | INT | |
| quarter | INT | |
| month | INT | |
| month_name | STRING | |
| day_of_week | INT | |
| day_name | STRING | |
| is_weekend | BOOLEAN | |

Generate from min(order_date) to max(order_date) found in silver_orders, padded to full months.

#### gold_fact_order_line

| Column | Type | Notes |
|---|---|---|
| order_line_sk | BIGINT | Surrogate key |
| order_id | INT | Degenerate dimension |
| line_id | INT | Degenerate dimension |
| customer_sk | BIGINT | FK → dim_customer (match on business key + valid_from/valid_to range) |
| product_sk | BIGINT | FK → dim_product |
| order_date_key | INT | FK → dim_date |
| quantity | INT | |
| unit_price | DECIMAL(10,2) | Transaction price (from order line, not product master) |
| discount_pct | DECIMAL(5,2) | |
| line_amount | DECIMAL(12,2) | |
| order_status | STRING | Degenerate dimension |

---

## Approach-Specific Implementation Notes

### 1. Python Notebooks (`approach_notebooks`)

- One notebook per layer: `bronze.py`, `silver.py`, `gold.py`
- Use PySpark DataFrame API throughout
- Bronze: read CSVs with `spark.read.csv()`, add metadata, write as Delta
- Silver: read from bronze, transform, write as Delta
- Gold: read from silver, build dimensions and fact, write as Delta
- SCD2: implement manually with DataFrame operations (merge/join logic)
- Orchestrate via bundle workflow (task per notebook, sequential dependencies)

### 2. SQL with Views and COPY INTO (`approach_sql`)

- Bronze: use `COPY INTO` to ingest CSVs into Delta tables
- Silver: create views or tables using `CREATE OR REPLACE TABLE AS SELECT`
- Gold: SQL-based SCD2 using `MERGE INTO`
- One SQL file per layer, executed via notebook or SQL warehouse
- Orchestrate via bundle workflow

### 3. dbt-core (`approach_dbt`)

- Standard dbt project structure with `models/bronze/`, `models/silver/`, `models/gold/`
- Bronze: dbt sources pointing at landing volume, `dbt seed` or external tables
- Silver: dbt models with tests (`not_null`, `unique`, `accepted_values`)
- Gold: dbt models using dbt snapshots for SCD2 on dim_customer
- Include `schema.yml` with documentation and tests
- Use `dbt-databricks` adapter
- Bundle deploys dbt project and runs via `dbt_task` in workflow

### 4. Declarative Pipelines — SQL (`approach_dpl_sql`)

- Single pipeline definition with SQL syntax
- Use `CREATE OR REFRESH LIVE TABLE` for bronze/silver
- Use `CREATE OR REFRESH LIVE TABLE` with expectations for data quality
- Gold SCD2: use `AUTO CDC INTO` (SQL syntax)
- Define expectations with `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW`

### 5. Declarative Pipelines — Python (`approach_dpl_python`)

- Single pipeline definition with Python/PySpark syntax
- Use `@dp.table` decorators (`from pyspark import pipelines as dp`)
- Use `dp.expect_or_drop()` for data quality
- Gold SCD2: use `dp.create_auto_cdc_flow()`
- Same logical flow as the SQL declarative pipeline, different syntax

### 6. Materialized Views + Streaming Tables (`approach_mv_st`)

- Bronze: `CREATE STREAMING TABLE` fed from the landing volume (Auto Loader under the hood)
- Silver: `CREATE MATERIALIZED VIEW` on top of bronze tables with transformations
- Gold dimensions: `CREATE MATERIALIZED VIEW`
- Gold SCD2: this approach doesn't natively support SCD2 — implement via a scheduled MERGE notebook or acknowledge the limitation and do SCD1 only for this approach
- Note in documentation that this is the simplest approach but has SCD2 limitations

### 7. Delta Live Tables with APPLY CHANGES (`approach_dlt`)

- Dedicated DLT pipeline
- Bronze: streaming live tables from landing volume
- Silver: live tables with expectations
- Gold SCD2: `APPLY CHANGES` with `STORED AS SCD TYPE 2` on dim_customer
- This approach specifically highlights the built-in SCD2 capability
- Use either SQL or Python (pick one — SQL recommended for contrast with approach 4/5)

---

## Bundle Structure

```
medallion-comparison/
├── databricks.yml                    # Bundle configuration
├── data/
│   ├── batch_1/
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   ├── orders.csv
│   │   └── order_lines.csv
│   └── batch_2/
│       ├── customers.csv
│       ├── orders.csv
│       └── order_lines.csv
├── src/
│   ├── notebooks/                    # Approach 1: Python notebooks
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   ├── sql/                          # Approach 2: SQL
│   │   ├── bronze.sql
│   │   ├── silver.sql
│   │   └── gold.sql
│   ├── dbt_project/                  # Approach 3: dbt
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   └── models/
│   │       ├── bronze/
│   │       ├── silver/
│   │       └── gold/
│   ├── dpl_sql/                      # Approach 4: Declarative Pipelines SQL
│   │   └── pipeline.sql
│   ├── dpl_python/                   # Approach 5: Declarative Pipelines Python
│   │   └── pipeline.py
│   ├── mv_st/                        # Approach 6: Materialized Views
│   │   ├── setup.sql
│   │   └── scd2_merge.sql            # Workaround notebook for SCD2
│   └── dlt/                          # Approach 7: DLT with APPLY CHANGES
│       └── pipeline.sql
├── setup/
│   ├── create_schemas.sql            # Create all schemas
│   └── upload_data.py                # Upload CSVs to landing volume
└── README.md
```

---

## Bundle Configuration Notes

Use bundle variables for:
- `catalog_name` (default: `medallion_comparison`)
- `environment` (dev/staging/prod)

Define one workflow per approach (7 workflows total), plus a `setup` workflow that runs first to create schemas and upload data.

For declarative pipelines and DLT approaches, define pipeline resources in the bundle config pointing to the appropriate source files.

---

## Validation

After all approaches have run both batches, validate that each gold layer produces identical results:

```sql
-- Run for each approach schema to verify consistency
SELECT 'approach_notebooks' as approach, COUNT(*) as fact_rows FROM approach_notebooks.gold_fact_order_line
UNION ALL
SELECT 'approach_sql', COUNT(*) FROM approach_sql.gold_fact_order_line
-- ... etc for all approaches

-- Expected: 11 rows in fact (10 from batch 1 + 1 from batch 2)
-- Expected: 8 rows in dim_customer (5 original + 2 historical + 1 new)
-- Expected: 5 rows in dim_product
```

Include a `validate.sql` notebook in the bundle that runs these cross-approach comparisons.

---

## Implementation Priority

Implement in this order (simplest to most complex):
1. Setup (schemas + data upload)
2. Python notebooks (most familiar, good baseline)
3. SQL with COPY INTO (straightforward SQL)
4. Materialized Views + Streaming Tables (simplest declarative)
5. Declarative Pipelines — SQL
6. Declarative Pipelines — Python
7. Delta Live Tables with APPLY CHANGES
8. dbt-core (requires adapter setup)
9. Validation notebook
