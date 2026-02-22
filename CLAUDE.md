# CLAUDE.md — Instructions for Claude Code

## Project Overview

This is a Databricks Medallion Architecture comparison project. Read `SPEC.md` thoroughly before starting — it contains the complete specification for all seven approaches, the data model, SCD2 requirements, and expected output counts.

## Key Constraints

- **Databricks Runtime**: Target serverless or DBR 15.4+. Use Unity Catalog throughout.
- **No external dependencies** except dbt-databricks for the dbt approach.
- **All approaches must produce identical gold-layer output** — same row counts, same values.
- **CSV data is in `data/`** — do not modify these files. They are the source of truth.
- **Bundle variables**: catalog name and environment must be configurable via `databricks.yml.tmpl` variables.
- **`databricks.yml` is generated, not committed** — edit `databricks.yml.tmpl` instead. Run `python scripts/generate_databricks_yml.py` to regenerate after changes.

## Implementation Order

Follow the order in SPEC.md. Start with the setup workflow, then implement approaches one at a time. After each approach, mentally verify it would produce the expected output counts from the spec.

## Coding Standards

- Python: use PySpark DataFrame API, not pandas. Type hints where practical.
- SQL: use ANSI SQL where possible. Databricks-specific syntax only where required (COPY INTO, MERGE, DLT/pipeline DDL).
- dbt: follow dbt best practices — staging models, ref() everywhere, schema.yml with tests.
- All notebooks should start with a comment block explaining which approach they belong to and what layer they implement.
- Use consistent table naming across all approaches: `bronze_*`, `silver_*`, `gold_dim_*`, `gold_fact_*`.

## SCD2 Implementation

This is the most critical piece to get right. Each approach handles it differently:

| Approach | SCD2 Method |
|---|---|
| Notebooks | Manual MERGE with DataFrame API |
| SQL | MERGE INTO with SCD2 logic |
| dbt | dbt snapshots |
| Declarative Pipelines SQL | AUTO CDC INTO ... STORED AS SCD TYPE 2 |
| Declarative Pipelines Python | create_auto_cdc_flow() with stored_as_scd_type=2 |
| MV + Streaming Tables | Acknowledge limitation, use scheduled MERGE workaround |
| DLT | APPLY CHANGES with SCD TYPE 2 |

The SCD2 tracking columns for dim_customer are: `valid_from`, `valid_to` (9999-12-31 for current), `is_current` (boolean).

## What "Done" Looks Like

1. `databricks.yml.tmpl` + `scripts/generate_databricks_yml.py` produce `databricks.yml` which deploys all schemas, volumes, workflows, and pipelines
2. Running the setup workflow creates schemas, volume, and uploads CSVs
3. Each approach workflow can run independently after setup
4. All seven gold layers contain identical data:
   - `gold_dim_customer`: 8 rows (5 current + 1 new + 2 historical)
   - `gold_dim_product`: 5 rows
   - `gold_dim_date`: covers Jan–Mar 2024 (full months)
   - `gold_fact_order_line`: 11 rows
5. `validate.sql` confirms cross-approach consistency

## Workflow Preferences

- **Package manager**: use `uv` (not pip/poetry)
- **Documentation**: update `README.md` after every feature/approach is implemented
- **Implementation cadence**: one task at a time — review and commit before moving on
- **Keep it simple**: follow best practices but avoid over-engineering

## Bundle Tagging Convention

Every job and pipeline in `databricks.yml.tmpl` (and resource YAML files) must have these tags:
- `project: medallion` — shared across all resources
- `approach: <name>` — identifies the specific approach (e.g., `setup`, `notebooks`, `sql`, `dbt`, `dpl_sql`, `dpl_python`, `mv_st`, `dlt`, `validate`)

## Key Design Decisions

- **Catalog name**: `medallion` (default). The catalog must be created manually via the Databricks UI before running the setup workflow — do NOT use `CREATE CATALOG` in notebooks (the workspace requires a managed storage location).
- **SCD2 dates**: fixed batch convention — batch_1 → `valid_from = 2024-01-01`, batch_2 → `valid_from = 2024-03-01`
- **DLT column names**: keep native `__START_AT`/`__END_AT` for DLT/Declarative Pipeline approaches, document difference vs `valid_from`/`valid_to`
