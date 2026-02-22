# Medallion Architecture Comparison — Implementation Plan

## Context

This is a greenfield Databricks project with only `SPEC.md`, `CLAUDE.md`, sample CSVs, and `.gitignore` in the repo. We need to build the full project: 7 approaches to the Medallion Architecture (Bronze→Silver→Gold), all producing identical gold-layer output, deployed as a Databricks Asset Bundle.

User preferences: `uv` for package management, README/docs updated after each task, implement one task at a time with review + commit between each.

### Key Decisions
- **SCD2 dates**: fixed batch convention — batch_1 → `valid_from = 2024-01-01`, batch_2 → `valid_from = 2024-03-01`
- **DLT column names**: keep native `__START_AT`/`__END_AT`, document difference vs `valid_from`/`valid_to`

---

## Task 1: Project Scaffolding + CLAUDE.md Update ✅

**Files to create:** `pyproject.toml`, `README.md`, `databricks.yml` (skeleton)
**Files to modify:** `.gitignore` (populate), `CLAUDE.md` (add user preferences)

- `pyproject.toml` — uv-managed, only optional dep is `dbt-databricks`
- `.gitignore` — Python, Databricks, dbt, uv patterns
- `databricks.yml` — bundle skeleton with variables (`catalog_name`, `environment`), targets (dev/prod), no jobs yet
- `README.md` — project overview, prerequisites, stub sections for each approach
- `CLAUDE.md` — append user preferences (uv, docs updates, incremental workflow)

---

## Task 2: Setup Workflow (Schemas + Data Upload) ✅

**Files to create:** `setup/create_schemas.sql`, `setup/upload_data.py`
**Files to modify:** `databricks.yml` (add setup job), `README.md`

- SQL notebook: creates catalog, 8 schemas, landing volume (parameterized via `catalog_name` widget)
- Python notebook: uploads CSVs from bundle workspace path to `/Volumes/{catalog}/landing/raw_files/`
- Bundle job: `setup` with two sequential tasks (create_schemas → upload_data)

---

## Task 3: Approach 1 — Python Notebooks ✅

**Files to create:** `src/notebooks/bronze.py`, `src/notebooks/silver.py`, `src/notebooks/gold.py`
**Files to modify:** `databricks.yml`, `README.md`

- **Bronze**: read CSVs from volume with `spark.read.csv()`, add `_source_file`, `_ingested_at`, `_batch_id`, write Delta
- **Silver**: dedup (latest batch wins via window function), type cast, standardize (country upper, email lower, etc.), add `line_amount`
- **Gold**: dim_product (SCD1), dim_date (generated from order date range), dim_customer (SCD2 via manual MERGE with DataFrame ops), fact_order_line (joins + FK lookups)
- Bundle job: `approach_notebooks` with 3 sequential tasks

SCD2 strategy: compare batch_1 vs batch_2 customer snapshots from bronze, build history rows with `valid_from`/`valid_to`/`is_current`.

---

## Task 4: Approach 2 — SQL with COPY INTO ✅

**Files to create:** `src/sql/bronze.sql`, `src/sql/silver.sql`, `src/sql/gold.sql`
**Files to modify:** `databricks.yml`, `README.md`

- **Bronze**: `COPY INTO` with `_metadata.file_path` for source tracking, `regexp_extract` for batch_id
- **Silver**: `CREATE OR REPLACE TABLE AS SELECT` with dedup + transforms
- **Gold**: SCD2 via `MERGE INTO` with SCD2 pattern (close old row + insert new)
- Bundle job: `approach_sql` with 3 sequential tasks

---

## Task 5: Approach 3 — Materialized Views + Streaming Tables ✅

**Files to create:** `src/mv_st/setup.sql`, `src/mv_st/scd2_merge.sql`
**Files to modify:** `databricks.yml`, `README.md`

- **Bronze**: `CREATE OR REFRESH STREAMING TABLE` from `read_files()`
- **Silver/Gold**: `CREATE OR REPLACE MATERIALIZED VIEW`
- **SCD2**: separate MERGE notebook (MV limitation documented)
- Bundle job: refresh streaming tables/MVs, then run SCD2 merge

---

## Task 6: Approach 4 — Declarative Pipelines (SQL) ✅

**Files to create:** `src/dpl_sql/pipeline.sql`
**Files to modify:** `databricks.yml`, `README.md`

- Single SQL file defining full pipeline
- Bronze: streaming tables, Silver: materialized views with expectations (`CONSTRAINT ... EXPECT`)
- Gold SCD2: `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`
- Bundle: pipeline resource targeting `approach_dpl_sql` schema

Note: DLT/Declarative Pipelines generate `__START_AT`/`__END_AT` columns instead of `valid_from`/`valid_to` — document this difference.

---

## Task 7: Approach 5 — Declarative Pipelines (Python) ✅

**Files to create:** `src/dpl_python/pipeline.py`
**Files to modify:** `databricks.yml`, `README.md`

- Same logical flow as Task 6, Python syntax
- `@dlt.table` decorators, `dlt.expect_or_drop()`, `dlt.apply_changes()` with `stored_as_scd_type=2`
- Bundle: pipeline resource targeting `approach_dpl_python` schema

---

## Task 8: Approach 6 — Delta Live Tables with APPLY CHANGES ✅

**Files to create:** `src/dlt/pipeline.sql`
**Files to modify:** `databricks.yml`, `README.md`

- SQL-based DLT pipeline using classic DLT syntax (`CREATE STREAMING LIVE TABLE`, `CREATE LIVE TABLE`)
- SCD2 via `APPLY CHANGES` sourced from `STREAM(LIVE.bronze_customers)` (contrast with Approach 4 which reads from volume)
- Bundle: pipeline resource targeting `approach_dlt` schema

---

## Task 9: Approach 7 — dbt-core ✅

**Files to create:** 20 files under `src/dbt_project/`
**Files to modify:** `databricks.yml`, `README.md`

- `dbt_project.yml`, `profiles.yml`
- `models/bronze/` — read from volume via `read_files()`, schema.yml with tests
- `models/silver/` — dedup + transforms with `{{ ref() }}`, `snapshot_batch` variable for SCD2 workflow
- `models/gold/` — dim/fact tables reading from snapshot + silver
- `snapshots/snap_dim_customer.sql` — SCD2 via dbt snapshot (`strategy='check'`)
- `gold_dim_customer.sql` maps dbt snapshot columns (`dbt_valid_from` → `valid_from`, etc.)
- Bundle: single dbt_task with 6 sequential commands (batch_1 run → snapshot → all run → snapshot → gold → test)

---

## Task 10: Validation Notebook

**Files to create:** `src/validate.sql`
**Files to modify:** `databricks.yml`, `README.md`

- Row count validation for all gold tables across all 7 approaches
- Content comparison via business-key hashing (skip surrogate keys)
- Account for column naming differences (DLT `__START_AT` vs `valid_from`)
- Bundle: validation job or standalone notebook

---

## Task 11: Final README + Cleanup

**Files to modify:** `README.md`, `.gitignore`, `databricks.yml` (if needed)

- Architecture diagram (ASCII), approach comparison table, deployment guide
- Verify all notebooks have consistent headers, table naming follows conventions
- Ensure bundle YAML is valid

---

## Verification

After each task:
1. Review generated files for correctness
2. Verify `databricks.yml` syntax (can run `databricks bundle validate` if CLI available)
3. Check README is updated
4. Commit

End-to-end (after all tasks):
1. `databricks bundle deploy` deploys everything
2. Run setup workflow → creates schemas + uploads data
3. Run each approach workflow independently
4. Run validation notebook → all gold layers match expected counts
