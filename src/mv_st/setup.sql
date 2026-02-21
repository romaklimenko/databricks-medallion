-- Databricks notebook source
-- Approach 3: Materialized Views + Streaming Tables — Setup
-- Creates bronze streaming tables from landing volume via read_files(),
-- silver materialized views with dedup/transforms, and gold dimension MVs.
-- Note: SCD2 for dim_customer is handled in scd2_merge.sql (MV limitation).

-- Parameters passed via job base_parameters (no CREATE WIDGET on SQL warehouses):
--   catalog_name, schema, landing_schema, volume_name

-- COMMAND ----------

-- =============================================
-- Bronze Layer: Streaming Tables from landing volume
-- =============================================

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${schema}.bronze_customers
AS SELECT
  customer_id,
  customer_name,
  email,
  address,
  city,
  country,
  segment,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM STREAM read_files(
  '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/customers.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${schema}.bronze_products
AS SELECT
  product_id,
  product_name,
  category,
  subcategory,
  unit_price,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM STREAM read_files(
  '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/products.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${schema}.bronze_orders
AS SELECT
  order_id,
  customer_id,
  order_date,
  status,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM STREAM read_files(
  '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/orders.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${schema}.bronze_order_lines
AS SELECT
  order_id,
  line_id,
  product_id,
  quantity,
  unit_price,
  discount_pct,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM STREAM read_files(
  '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/order_lines.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
);

-- COMMAND ----------

-- =============================================
-- Silver Layer: Materialized Views with dedup + transforms
-- =============================================

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.silver_customers AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _batch_id DESC) AS _rn
  FROM ${catalog_name}.${schema}.bronze_customers
)
SELECT
  CAST(customer_id AS INT) AS customer_id,
  TRIM(customer_name) AS customer_name,
  LOWER(TRIM(email)) AS email,
  TRIM(address) AS address,
  TRIM(city) AS city,
  UPPER(TRIM(country)) AS country,
  INITCAP(TRIM(segment)) AS segment
FROM ranked
WHERE _rn = 1
  AND customer_id IS NOT NULL;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.silver_products AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _batch_id DESC) AS _rn
  FROM ${catalog_name}.${schema}.bronze_products
)
SELECT
  CAST(product_id AS INT) AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(subcategory) AS subcategory,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price
FROM ranked
WHERE _rn = 1
  AND product_id IS NOT NULL;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.silver_orders AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _batch_id DESC) AS _rn
  FROM ${catalog_name}.${schema}.bronze_orders
)
SELECT
  CAST(order_id AS INT) AS order_id,
  CAST(customer_id AS INT) AS customer_id,
  CAST(order_date AS DATE) AS order_date,
  LOWER(TRIM(status)) AS status
FROM ranked
WHERE _rn = 1
  AND order_id IS NOT NULL;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.silver_order_lines AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id, line_id ORDER BY _batch_id DESC) AS _rn
  FROM ${catalog_name}.${schema}.bronze_order_lines
)
SELECT
  CAST(order_id AS INT) AS order_id,
  CAST(line_id AS INT) AS line_id,
  CAST(product_id AS INT) AS product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
  CAST(discount_pct AS DECIMAL(5,2)) AS discount_pct,
  CAST(
    ROUND(CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) * (1 - CAST(discount_pct AS DECIMAL(5,2)) / 100), 2)
    AS DECIMAL(12,2)
  ) AS line_amount
FROM ranked
WHERE _rn = 1
  AND order_id IS NOT NULL
  AND line_id IS NOT NULL;

-- COMMAND ----------

-- =============================================
-- Gold Layer: Dimension MVs
-- =============================================

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.gold_dim_product AS
SELECT
  ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
  product_id,
  product_name,
  category,
  subcategory,
  unit_price
FROM ${catalog_name}.${schema}.silver_products;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.gold_dim_date AS
WITH date_range AS (
  SELECT
    DATE_TRUNC('MONTH', MIN(order_date)) AS min_date,
    LAST_DAY(MAX(order_date)) AS max_date
  FROM ${catalog_name}.${schema}.silver_orders
)
SELECT
  CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
  full_date,
  YEAR(full_date) AS year,
  QUARTER(full_date) AS quarter,
  MONTH(full_date) AS month,
  DATE_FORMAT(full_date, 'MMMM') AS month_name,
  DAYOFWEEK(full_date) AS day_of_week,
  DATE_FORMAT(full_date, 'EEEE') AS day_name,
  CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range
LATERAL VIEW EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) t AS full_date;
