-- Databricks notebook source
-- Approach 4: Declarative Pipelines (SQL)
-- Full medallion pipeline defined as a single Declarative Pipeline (formerly DLT).
-- Bronze: streaming tables from landing volume via read_files()
-- Silver: materialized views with expectations, dedup, and transforms
-- Gold: dim_customer via AUTO CDC INTO (SCD TYPE 2), other dims + fact as MVs
--
-- Note: AUTO CDC produces __START_AT/__END_AT columns instead of
-- valid_from/valid_to. __END_AT is NULL for current records (not 9999-12-31).
--
-- Pipeline configuration keys (set in bundle YAML):
--   catalog_name, landing_schema, volume_name

-- COMMAND ----------

-- =============================================
-- Bronze Layer: Streaming Tables from landing volume
-- =============================================

CREATE OR REFRESH STREAMING TABLE bronze_customers
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

CREATE OR REFRESH STREAMING TABLE bronze_products
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

CREATE OR REFRESH STREAMING TABLE bronze_orders
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

CREATE OR REFRESH STREAMING TABLE bronze_order_lines
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
-- Silver Layer: Materialized Views with expectations
-- =============================================

CREATE OR REFRESH MATERIALIZED VIEW silver_customers (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _batch_id DESC) AS _rn
  FROM LIVE.bronze_customers
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
WHERE _rn = 1;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_products (
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _batch_id DESC) AS _rn
  FROM LIVE.bronze_products
)
SELECT
  CAST(product_id AS INT) AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(subcategory) AS subcategory,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price
FROM ranked
WHERE _rn = 1;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _batch_id DESC) AS _rn
  FROM LIVE.bronze_orders
)
SELECT
  CAST(order_id AS INT) AS order_id,
  CAST(customer_id AS INT) AS customer_id,
  CAST(order_date AS DATE) AS order_date,
  LOWER(TRIM(status)) AS status
FROM ranked
WHERE _rn = 1;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_order_lines (
  CONSTRAINT valid_order_line_id EXPECT (order_id IS NOT NULL AND line_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id, line_id ORDER BY _batch_id DESC) AS _rn
  FROM LIVE.bronze_order_lines
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
WHERE _rn = 1;

-- COMMAND ----------

-- =============================================
-- Gold Layer
-- =============================================

-- ----- SCD2: dim_customer via AUTO CDC -----
-- The AUTO CDC target is an internal streaming table. A downstream MV
-- wraps it to add customer_sk and present a clean interface.
-- TRACK HISTORY ON limits SCD2 triggers to the same columns as other approaches.

CREATE OR REFRESH STREAMING TABLE _scd2_dim_customer;

-- COMMAND ----------

CREATE FLOW scd2_dim_customer
AS AUTO CDC INTO
  LIVE._scd2_dim_customer
FROM (
  SELECT
    CAST(customer_id AS INT) AS customer_id,
    TRIM(customer_name) AS customer_name,
    LOWER(TRIM(email)) AS email,
    TRIM(address) AS address,
    TRIM(city) AS city,
    UPPER(TRIM(country)) AS country,
    INITCAP(TRIM(segment)) AS segment,
    CASE
      WHEN regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) = 'batch_1'
        THEN DATE '2024-01-01'
      ELSE DATE '2024-03-01'
    END AS _batch_date
  FROM STREAM read_files(
    '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/customers.csv',
    format => 'csv',
    header => 'true',
    inferSchema => 'false'
  )
  WHERE customer_id IS NOT NULL
)
KEYS (customer_id)
SEQUENCE BY _batch_date
COLUMNS * EXCEPT (_batch_date)
STORED AS SCD TYPE 2
TRACK HISTORY ON email, address, city, country, segment;

-- COMMAND ----------

-- Wrap the SCD2 target with a surrogate key.
-- __START_AT / __END_AT are the native DPL SCD2 columns:
--   __START_AT = when this version became active (DATE)
--   __END_AT   = when superseded (DATE), NULL for current records

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_customer
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY customer_id, __START_AT) AS customer_sk,
  customer_id,
  customer_name,
  email,
  address,
  city,
  country,
  segment,
  __START_AT,
  __END_AT
FROM LIVE._scd2_dim_customer;

-- COMMAND ----------

-- ----- dim_product (SCD1) -----

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_product
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
  product_id,
  product_name,
  category,
  subcategory,
  unit_price
FROM LIVE.silver_products;

-- COMMAND ----------

-- ----- dim_date (generated from order date range, padded to full months) -----

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_date
AS
WITH date_range AS (
  SELECT
    DATE_TRUNC('MONTH', MIN(order_date)) AS min_date,
    LAST_DAY(MAX(order_date)) AS max_date
  FROM LIVE.silver_orders
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

-- COMMAND ----------

-- ----- fact_order_line -----
-- SCD2 join uses __START_AT / __END_AT (NULL = current).
-- Note: __END_AT equals the __START_AT of the next version, so we use < (not <=).

CREATE OR REFRESH MATERIALIZED VIEW gold_fact_order_line
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY ol.order_id, ol.line_id) AS order_line_sk,
  ol.order_id,
  ol.line_id,
  dc.customer_sk,
  dp.product_sk,
  CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
  ol.quantity,
  ol.unit_price,
  ol.discount_pct,
  ol.line_amount,
  o.status AS order_status
FROM LIVE.silver_order_lines ol
INNER JOIN LIVE.silver_orders o ON ol.order_id = o.order_id
LEFT JOIN LIVE.gold_dim_customer dc
  ON o.customer_id = dc.customer_id
  AND o.order_date >= dc.__START_AT
  AND (dc.__END_AT IS NULL OR o.order_date < dc.__END_AT)
LEFT JOIN LIVE.gold_dim_product dp
  ON ol.product_id = dp.product_id;
