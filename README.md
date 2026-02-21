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
| 1 | Python Notebooks | `approach_notebooks` | Manual MERGE with DataFrames | Planned |
| 2 | SQL with COPY INTO | `approach_sql` | MERGE INTO | Planned |
| 3 | Materialized Views + Streaming Tables | `approach_mv_st` | Scheduled MERGE workaround | Planned |
| 4 | Declarative Pipelines (SQL) | `approach_dpl_sql` | APPLY CHANGES INTO | Planned |
| 5 | Declarative Pipelines (Python) | `approach_dpl_python` | dlt.apply_changes() | Planned |
| 6 | Delta Live Tables | `approach_dlt` | APPLY CHANGES | Planned |
| 7 | dbt-core | `approach_dbt` | dbt snapshots | Planned |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI with bundle support (`databricks bundle` commands)
- [uv](https://docs.astral.sh/uv/) for Python package management
- Python 3.10+

## Quick Start

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
│   └── dbt_project/            # Approach 7: dbt-core
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

## Configuration

Bundle variables (set in `databricks.yml` or override at deploy time):

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog_name` | `medallion_comparison` | Unity Catalog catalog |
| `environment` | `dev` | Deployment environment |
