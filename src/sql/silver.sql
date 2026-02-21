-- Databricks notebook source
-- Approach 2: SQL with COPY INTO — Silver Layer
-- Deduplicates (latest batch wins), casts types, standardizes text,
-- and computes derived columns.

-- COMMAND ----------

CREATE WIDGET TEXT catalog_name DEFAULT 'medallion';
CREATE WIDGET TEXT schema DEFAULT 'approach_sql';

-- COMMAND ----------

-- silver_customers
CREATE OR REPLACE TABLE ${catalog_name}.${schema}.silver_customers AS
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

SELECT 'silver_customers' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.silver_customers;

-- COMMAND ----------

-- silver_products
CREATE OR REPLACE TABLE ${catalog_name}.${schema}.silver_products AS
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

SELECT 'silver_products' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.silver_products;

-- COMMAND ----------

-- silver_orders
CREATE OR REPLACE TABLE ${catalog_name}.${schema}.silver_orders AS
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

SELECT 'silver_orders' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.silver_orders;

-- COMMAND ----------

-- silver_order_lines
CREATE OR REPLACE TABLE ${catalog_name}.${schema}.silver_order_lines AS
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

SELECT 'silver_order_lines' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.silver_order_lines;
