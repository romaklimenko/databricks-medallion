-- Databricks notebook source
-- Approach 2: SQL with COPY INTO — Gold Layer
-- Builds Kimball dimensional model: dim_product (SCD1), dim_date (generated),
-- dim_customer (SCD2 via MERGE INTO), fact_order_line.

-- COMMAND ----------

CREATE WIDGET TEXT catalog_name DEFAULT 'medallion';
CREATE WIDGET TEXT schema DEFAULT 'approach_sql';

-- COMMAND ----------

-- =============================================
-- gold_dim_product (SCD1 — latest version only)
-- =============================================

CREATE OR REPLACE TABLE ${catalog_name}.${schema}.gold_dim_product AS
SELECT
  ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
  product_id,
  product_name,
  category,
  subcategory,
  unit_price
FROM ${catalog_name}.${schema}.silver_products;

SELECT 'gold_dim_product' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.gold_dim_product;

-- COMMAND ----------

-- =============================================
-- gold_dim_date (generated from order date range, padded to full months)
-- =============================================

CREATE OR REPLACE TABLE ${catalog_name}.${schema}.gold_dim_date AS
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

SELECT 'gold_dim_date' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.gold_dim_date;

-- COMMAND ----------

-- =============================================
-- gold_dim_customer (SCD2 — built from bronze batches)
-- =============================================
-- Strategy: compare batch_1 vs batch_2 customer snapshots from bronze,
-- produce historical + current rows with valid_from/valid_to/is_current.
-- Batch dates: batch_1 → 2024-01-01, batch_2 → 2024-03-01.

CREATE OR REPLACE TABLE ${catalog_name}.${schema}.gold_dim_customer AS
WITH cleaned AS (
  SELECT
    CAST(customer_id AS INT) AS customer_id,
    TRIM(customer_name) AS customer_name,
    LOWER(TRIM(email)) AS email,
    TRIM(address) AS address,
    TRIM(city) AS city,
    UPPER(TRIM(country)) AS country,
    INITCAP(TRIM(segment)) AS segment,
    _batch_id,
    CASE WHEN _batch_id = 'batch_1' THEN DATE '2024-01-01' ELSE DATE '2024-03-01' END AS valid_from
  FROM ${catalog_name}.${schema}.bronze_customers
  WHERE customer_id IS NOT NULL
),
b1 AS (SELECT * FROM cleaned WHERE _batch_id = 'batch_1'),
b2 AS (SELECT * FROM cleaned WHERE _batch_id = 'batch_2'),

-- Customers only in batch_1 (no update in batch_2) → current
unchanged AS (
  SELECT
    b1.customer_id, b1.customer_name, b1.email, b1.address,
    b1.city, b1.country, b1.segment,
    b1.valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
  FROM b1
  LEFT ANTI JOIN b2 ON b1.customer_id = b2.customer_id
),

-- Customers in both batches with changes → historical (closed) + current (new)
changed_old AS (
  SELECT
    b1.customer_id, b1.customer_name, b1.email, b1.address,
    b1.city, b1.country, b1.segment,
    b1.valid_from,
    DATE_SUB(b2.valid_from, 1) AS valid_to,
    FALSE AS is_current
  FROM b1
  INNER JOIN b2 ON b1.customer_id = b2.customer_id
  WHERE b1.email != b2.email
     OR b1.address != b2.address
     OR b1.city != b2.city
     OR b1.country != b2.country
     OR b1.segment != b2.segment
),
changed_new AS (
  SELECT
    b2.customer_id, b2.customer_name, b2.email, b2.address,
    b2.city, b2.country, b2.segment,
    b2.valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
  FROM b1
  INNER JOIN b2 ON b1.customer_id = b2.customer_id
  WHERE b1.email != b2.email
     OR b1.address != b2.address
     OR b1.city != b2.city
     OR b1.country != b2.country
     OR b1.segment != b2.segment
),

-- Customers in both batches with no changes → current (keep batch_1 version)
unchanged_both AS (
  SELECT
    b1.customer_id, b1.customer_name, b1.email, b1.address,
    b1.city, b1.country, b1.segment,
    b1.valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
  FROM b1
  INNER JOIN b2 ON b1.customer_id = b2.customer_id
  WHERE NOT (
    b1.email != b2.email
    OR b1.address != b2.address
    OR b1.city != b2.city
    OR b1.country != b2.country
    OR b1.segment != b2.segment
  )
),

-- New customers (only in batch_2)
new_cust AS (
  SELECT
    b2.customer_id, b2.customer_name, b2.email, b2.address,
    b2.city, b2.country, b2.segment,
    b2.valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
  FROM b2
  LEFT ANTI JOIN b1 ON b2.customer_id = b1.customer_id
),

-- Union all groups
all_customers AS (
  SELECT * FROM unchanged
  UNION ALL SELECT * FROM changed_old
  UNION ALL SELECT * FROM changed_new
  UNION ALL SELECT * FROM unchanged_both
  UNION ALL SELECT * FROM new_cust
)

SELECT
  ROW_NUMBER() OVER (ORDER BY customer_id, valid_from) AS customer_sk,
  customer_id, customer_name, email, address, city, country, segment,
  valid_from, valid_to, is_current
FROM all_customers;

SELECT 'gold_dim_customer' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.gold_dim_customer;

-- COMMAND ----------

-- =============================================
-- gold_fact_order_line
-- =============================================

CREATE OR REPLACE TABLE ${catalog_name}.${schema}.gold_fact_order_line AS
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
FROM ${catalog_name}.${schema}.silver_order_lines ol
INNER JOIN ${catalog_name}.${schema}.silver_orders o ON ol.order_id = o.order_id
LEFT JOIN ${catalog_name}.${schema}.gold_dim_customer dc
  ON o.customer_id = dc.customer_id
  AND o.order_date >= dc.valid_from
  AND o.order_date <= dc.valid_to
LEFT JOIN ${catalog_name}.${schema}.gold_dim_product dp
  ON ol.product_id = dp.product_id;

SELECT 'gold_fact_order_line' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.gold_fact_order_line;

-- COMMAND ----------

-- Verify all gold tables
SELECT 'gold_dim_customer' AS table_name, COUNT(*) AS row_count FROM ${catalog_name}.${schema}.gold_dim_customer
UNION ALL
SELECT 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema}.gold_dim_product
UNION ALL
SELECT 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema}.gold_dim_date
UNION ALL
SELECT 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema}.gold_fact_order_line;
