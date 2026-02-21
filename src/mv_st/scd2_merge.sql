-- Databricks notebook source
-- Approach 3: Materialized Views + Streaming Tables — SCD2 Merge
-- Materialized views cannot natively implement SCD2. This notebook is a
-- scheduled workaround that rebuilds gold_dim_customer from bronze data
-- and then creates the fact table.

-- Parameters passed via job base_parameters (no CREATE WIDGET on SQL warehouses):
--   catalog_name, schema

-- COMMAND ----------

-- =============================================
-- gold_dim_customer (SCD2 — rebuilt from bronze batches)
-- =============================================
-- Since MVs don't support SCD2, we build dim_customer as a regular table.
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

-- Customers only in batch_1 (no update in batch_2)
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

-- Changed customers — historical (closed) row
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

-- Changed customers — current (new) row
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

-- Customers in both batches with no changes
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
-- gold_fact_order_line (MV — references dim_customer table + other MVs)
-- =============================================

CREATE OR REPLACE MATERIALIZED VIEW ${catalog_name}.${schema}.gold_fact_order_line AS
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

-- COMMAND ----------

-- Verify all gold tables
SELECT 'gold_dim_customer' AS table_name, COUNT(*) AS row_count FROM ${catalog_name}.${schema}.gold_dim_customer
UNION ALL
SELECT 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema}.gold_dim_product
UNION ALL
SELECT 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema}.gold_dim_date
UNION ALL
SELECT 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema}.gold_fact_order_line;
