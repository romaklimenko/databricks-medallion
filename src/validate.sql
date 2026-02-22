-- Databricks notebook source
-- Validation: Cross-approach comparison of gold-layer output
-- Verifies that all 7 approaches produce identical gold-layer results.
-- Uses widget substitution ($var syntax) for dynamic table references.

-- COMMAND ----------

CREATE WIDGET TEXT catalog_name DEFAULT 'medallion';
CREATE WIDGET TEXT schema_notebooks DEFAULT 'approach_notebooks';
CREATE WIDGET TEXT schema_sql DEFAULT 'approach_sql';
CREATE WIDGET TEXT schema_mv_st DEFAULT 'approach_mv_st';
CREATE WIDGET TEXT schema_dpl_sql DEFAULT 'approach_dpl_sql';
CREATE WIDGET TEXT schema_dpl_python DEFAULT 'approach_dpl_python';
CREATE WIDGET TEXT schema_dlt DEFAULT 'approach_dlt';
CREATE WIDGET TEXT schema_dbt DEFAULT 'approach_dbt';

-- COMMAND ----------

-- ============================================================
-- 1. ROW COUNT VALIDATION
-- Expected: dim_customer=8, dim_product=5, dim_date=91, fact_order_line=11
-- ============================================================

SELECT * FROM (
  SELECT '1_notebooks' AS approach, 'gold_dim_customer' AS table_name, COUNT(*) AS row_count FROM ${catalog_name}.${schema_notebooks}.gold_dim_customer
  UNION ALL SELECT '1_notebooks', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_dim_product
  UNION ALL SELECT '1_notebooks', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_dim_date
  UNION ALL SELECT '1_notebooks', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_fact_order_line

  UNION ALL SELECT '2_sql', 'gold_dim_customer', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_customer
  UNION ALL SELECT '2_sql', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_product
  UNION ALL SELECT '2_sql', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_date
  UNION ALL SELECT '2_sql', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_fact_order_line

  UNION ALL SELECT '3_mv_st', 'gold_dim_customer', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_customer
  UNION ALL SELECT '3_mv_st', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_product
  UNION ALL SELECT '3_mv_st', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_date
  UNION ALL SELECT '3_mv_st', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_fact_order_line

  UNION ALL SELECT '4_dpl_sql', 'gold_dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_customer
  UNION ALL SELECT '4_dpl_sql', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_product
  UNION ALL SELECT '4_dpl_sql', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_date
  UNION ALL SELECT '4_dpl_sql', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_fact_order_line

  UNION ALL SELECT '5_dpl_python', 'gold_dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_customer
  UNION ALL SELECT '5_dpl_python', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_product
  UNION ALL SELECT '5_dpl_python', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_date
  UNION ALL SELECT '5_dpl_python', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_fact_order_line

  UNION ALL SELECT '6_dlt', 'gold_dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_customer
  UNION ALL SELECT '6_dlt', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_product
  UNION ALL SELECT '6_dlt', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_date
  UNION ALL SELECT '6_dlt', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_fact_order_line

  UNION ALL SELECT '7_dbt', 'gold_dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_customer
  UNION ALL SELECT '7_dbt', 'gold_dim_product', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_product
  UNION ALL SELECT '7_dbt', 'gold_dim_date', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_date
  UNION ALL SELECT '7_dbt', 'gold_fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_fact_order_line
) counts
ORDER BY table_name, approach

-- COMMAND ----------

-- ============================================================
-- 2. ROW COUNT SUMMARY — expected vs actual with PASS/FAIL
-- ============================================================

WITH counts AS (
  SELECT '1_notebooks' AS approach, 'dim_customer' AS tbl, COUNT(*) AS cnt FROM ${catalog_name}.${schema_notebooks}.gold_dim_customer
  UNION ALL SELECT '1_notebooks', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_dim_product
  UNION ALL SELECT '1_notebooks', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_dim_date
  UNION ALL SELECT '1_notebooks', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_fact_order_line

  UNION ALL SELECT '2_sql', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_customer
  UNION ALL SELECT '2_sql', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_product
  UNION ALL SELECT '2_sql', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_date
  UNION ALL SELECT '2_sql', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_fact_order_line

  UNION ALL SELECT '3_mv_st', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_customer
  UNION ALL SELECT '3_mv_st', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_product
  UNION ALL SELECT '3_mv_st', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_date
  UNION ALL SELECT '3_mv_st', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_fact_order_line

  UNION ALL SELECT '4_dpl_sql', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_customer
  UNION ALL SELECT '4_dpl_sql', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_product
  UNION ALL SELECT '4_dpl_sql', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_date
  UNION ALL SELECT '4_dpl_sql', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_fact_order_line

  UNION ALL SELECT '5_dpl_python', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_customer
  UNION ALL SELECT '5_dpl_python', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_product
  UNION ALL SELECT '5_dpl_python', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_date
  UNION ALL SELECT '5_dpl_python', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_fact_order_line

  UNION ALL SELECT '6_dlt', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_customer
  UNION ALL SELECT '6_dlt', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_product
  UNION ALL SELECT '6_dlt', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_date
  UNION ALL SELECT '6_dlt', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_fact_order_line

  UNION ALL SELECT '7_dbt', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_customer
  UNION ALL SELECT '7_dbt', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_product
  UNION ALL SELECT '7_dbt', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_date
  UNION ALL SELECT '7_dbt', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_fact_order_line
),
expected AS (
  SELECT 'dim_customer' AS tbl, 8 AS expected_count
  UNION ALL SELECT 'dim_product', 5
  UNION ALL SELECT 'dim_date', 91
  UNION ALL SELECT 'fact_order_line', 11
)
SELECT
  c.tbl AS table_name,
  e.expected_count,
  c.approach,
  c.cnt AS actual_count,
  CASE WHEN c.cnt = e.expected_count THEN 'PASS' ELSE 'FAIL' END AS status
FROM counts c
JOIN expected e ON c.tbl = e.tbl
ORDER BY c.tbl, c.approach

-- COMMAND ----------

-- ============================================================
-- 3. CONTENT COMPARISON: gold_dim_product
-- Compare on business key + attributes (skip surrogate product_sk).
-- All 7 approaches should produce identical rows.
-- ============================================================

WITH all_products AS (
  SELECT '1_notebooks' AS approach, product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_notebooks}.gold_dim_product
  UNION ALL SELECT '2_sql', product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_sql}.gold_dim_product
  UNION ALL SELECT '3_mv_st', product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_mv_st}.gold_dim_product
  UNION ALL SELECT '4_dpl_sql', product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_product
  UNION ALL SELECT '5_dpl_python', product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_dpl_python}.gold_dim_product
  UNION ALL SELECT '6_dlt', product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_dlt}.gold_dim_product
  UNION ALL SELECT '7_dbt', product_id, product_name, category, subcategory, unit_price FROM ${catalog_name}.${schema_dbt}.gold_dim_product
),
hashed AS (
  SELECT approach, product_id, md5(concat_ws('|', product_name, category, subcategory, CAST(unit_price AS STRING))) AS row_hash
  FROM all_products
)
SELECT product_id, COUNT(DISTINCT row_hash) AS distinct_hashes, COUNT(DISTINCT approach) AS approach_count,
  CASE WHEN COUNT(DISTINCT row_hash) = 1 THEN 'PASS' ELSE 'FAIL' END AS status
FROM hashed
GROUP BY product_id
ORDER BY product_id

-- COMMAND ----------

-- ============================================================
-- 4. CONTENT COMPARISON: gold_dim_customer (SCD2)
-- Normalizes column names: __START_AT→valid_from, __END_AT→valid_to
-- for DLT/DPL approaches. Skips surrogate customer_sk and is_current.
-- ============================================================

WITH all_customers AS (
  -- Approaches with valid_from / valid_to
  SELECT '1_notebooks' AS approach, customer_id, customer_name, email, address, city, country, segment,
    valid_from, valid_to
  FROM ${catalog_name}.${schema_notebooks}.gold_dim_customer
  UNION ALL
  SELECT '2_sql', customer_id, customer_name, email, address, city, country, segment,
    valid_from, valid_to
  FROM ${catalog_name}.${schema_sql}.gold_dim_customer
  UNION ALL
  SELECT '3_mv_st', customer_id, customer_name, email, address, city, country, segment,
    valid_from, valid_to
  FROM ${catalog_name}.${schema_mv_st}.gold_dim_customer
  UNION ALL
  SELECT '7_dbt', customer_id, customer_name, email, address, city, country, segment,
    valid_from, valid_to
  FROM ${catalog_name}.${schema_dbt}.gold_dim_customer
  UNION ALL
  -- Approaches with __START_AT / __END_AT (DPL + DLT)
  -- Normalize: __END_AT is the start of the next version, so subtract 1 day.
  -- NULL __END_AT (current record) → 9999-12-31
  SELECT '4_dpl_sql', customer_id, customer_name, email, address, city, country, segment,
    CAST(__START_AT AS DATE) AS valid_from,
    COALESCE(CAST(__END_AT AS DATE) - INTERVAL 1 DAY, DATE '9999-12-31') AS valid_to
  FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_customer
  UNION ALL
  SELECT '5_dpl_python', customer_id, customer_name, email, address, city, country, segment,
    CAST(__START_AT AS DATE) AS valid_from,
    COALESCE(CAST(__END_AT AS DATE) - INTERVAL 1 DAY, DATE '9999-12-31') AS valid_to
  FROM ${catalog_name}.${schema_dpl_python}.gold_dim_customer
  UNION ALL
  SELECT '6_dlt', customer_id, customer_name, email, address, city, country, segment,
    CAST(__START_AT AS DATE) AS valid_from,
    COALESCE(CAST(__END_AT AS DATE) - INTERVAL 1 DAY, DATE '9999-12-31') AS valid_to
  FROM ${catalog_name}.${schema_dlt}.gold_dim_customer
),
hashed AS (
  SELECT approach, customer_id, valid_from,
    md5(concat_ws('|', customer_name, email, address, city, country, segment, CAST(valid_from AS STRING), CAST(valid_to AS STRING))) AS row_hash
  FROM all_customers
)
SELECT customer_id, valid_from, COUNT(DISTINCT row_hash) AS distinct_hashes, COUNT(DISTINCT approach) AS approach_count,
  CASE WHEN COUNT(DISTINCT row_hash) = 1 THEN 'PASS' ELSE 'FAIL' END AS status
FROM hashed
GROUP BY customer_id, valid_from
ORDER BY customer_id, valid_from

-- COMMAND ----------

-- ============================================================
-- 5. CONTENT COMPARISON: gold_dim_date
-- Compare on date_key + all attributes. Only shows mismatches.
-- ============================================================

WITH all_dates AS (
  SELECT '1_notebooks' AS approach, date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_notebooks}.gold_dim_date
  UNION ALL SELECT '2_sql', date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_sql}.gold_dim_date
  UNION ALL SELECT '3_mv_st', date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_mv_st}.gold_dim_date
  UNION ALL SELECT '4_dpl_sql', date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_date
  UNION ALL SELECT '5_dpl_python', date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_dpl_python}.gold_dim_date
  UNION ALL SELECT '6_dlt', date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_dlt}.gold_dim_date
  UNION ALL SELECT '7_dbt', date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend FROM ${catalog_name}.${schema_dbt}.gold_dim_date
),
hashed AS (
  SELECT approach, date_key,
    md5(concat_ws('|', CAST(full_date AS STRING), CAST(year AS STRING), CAST(quarter AS STRING), CAST(month AS STRING), month_name, CAST(day_of_week AS STRING), day_name, CAST(is_weekend AS STRING))) AS row_hash
  FROM all_dates
)
SELECT date_key, COUNT(DISTINCT row_hash) AS distinct_hashes, COUNT(DISTINCT approach) AS approach_count,
  CASE WHEN COUNT(DISTINCT row_hash) = 1 THEN 'PASS' ELSE 'FAIL' END AS status
FROM hashed
GROUP BY date_key
HAVING COUNT(DISTINCT row_hash) > 1
ORDER BY date_key

-- COMMAND ----------

-- ============================================================
-- 6. CONTENT COMPARISON: gold_fact_order_line
-- Compare on business keys (order_id, line_id) + measures.
-- Skip surrogate keys (order_line_sk, customer_sk, product_sk).
-- ============================================================

WITH all_facts AS (
  SELECT '1_notebooks' AS approach, order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_notebooks}.gold_fact_order_line
  UNION ALL SELECT '2_sql', order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_sql}.gold_fact_order_line
  UNION ALL SELECT '3_mv_st', order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_mv_st}.gold_fact_order_line
  UNION ALL SELECT '4_dpl_sql', order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_dpl_sql}.gold_fact_order_line
  UNION ALL SELECT '5_dpl_python', order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_dpl_python}.gold_fact_order_line
  UNION ALL SELECT '6_dlt', order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_dlt}.gold_fact_order_line
  UNION ALL SELECT '7_dbt', order_id, line_id, order_date_key, quantity, unit_price, discount_pct, line_amount, order_status FROM ${catalog_name}.${schema_dbt}.gold_fact_order_line
),
hashed AS (
  SELECT approach, order_id, line_id,
    md5(concat_ws('|', CAST(order_date_key AS STRING), CAST(quantity AS STRING), CAST(unit_price AS STRING), CAST(discount_pct AS STRING), CAST(line_amount AS STRING), order_status)) AS row_hash
  FROM all_facts
)
SELECT order_id, line_id, COUNT(DISTINCT row_hash) AS distinct_hashes, COUNT(DISTINCT approach) AS approach_count,
  CASE WHEN COUNT(DISTINCT row_hash) = 1 THEN 'PASS' ELSE 'FAIL' END AS status
FROM hashed
GROUP BY order_id, line_id
ORDER BY order_id, line_id

-- COMMAND ----------

-- ============================================================
-- 7. OVERALL SUMMARY
-- Single pass/fail for each table across all approaches.
-- ============================================================

WITH count_check AS (
  SELECT 'dim_customer' AS tbl, 8 AS expected
  UNION ALL SELECT 'dim_product', 5
  UNION ALL SELECT 'dim_date', 91
  UNION ALL SELECT 'fact_order_line', 11
),
actual_counts AS (
  SELECT '1_notebooks' AS approach, 'dim_customer' AS tbl, COUNT(*) AS cnt FROM ${catalog_name}.${schema_notebooks}.gold_dim_customer
  UNION ALL SELECT '2_sql', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_customer
  UNION ALL SELECT '3_mv_st', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_customer
  UNION ALL SELECT '4_dpl_sql', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_customer
  UNION ALL SELECT '5_dpl_python', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_customer
  UNION ALL SELECT '6_dlt', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_customer
  UNION ALL SELECT '7_dbt', 'dim_customer', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_customer

  UNION ALL SELECT '1_notebooks', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_dim_product
  UNION ALL SELECT '2_sql', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_product
  UNION ALL SELECT '3_mv_st', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_product
  UNION ALL SELECT '4_dpl_sql', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_product
  UNION ALL SELECT '5_dpl_python', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_product
  UNION ALL SELECT '6_dlt', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_product
  UNION ALL SELECT '7_dbt', 'dim_product', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_product

  UNION ALL SELECT '1_notebooks', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_dim_date
  UNION ALL SELECT '2_sql', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_dim_date
  UNION ALL SELECT '3_mv_st', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_dim_date
  UNION ALL SELECT '4_dpl_sql', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_dim_date
  UNION ALL SELECT '5_dpl_python', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_dim_date
  UNION ALL SELECT '6_dlt', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_dim_date
  UNION ALL SELECT '7_dbt', 'dim_date', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_dim_date

  UNION ALL SELECT '1_notebooks', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_notebooks}.gold_fact_order_line
  UNION ALL SELECT '2_sql', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_sql}.gold_fact_order_line
  UNION ALL SELECT '3_mv_st', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_mv_st}.gold_fact_order_line
  UNION ALL SELECT '4_dpl_sql', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dpl_sql}.gold_fact_order_line
  UNION ALL SELECT '5_dpl_python', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dpl_python}.gold_fact_order_line
  UNION ALL SELECT '6_dlt', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dlt}.gold_fact_order_line
  UNION ALL SELECT '7_dbt', 'fact_order_line', COUNT(*) FROM ${catalog_name}.${schema_dbt}.gold_fact_order_line
)
SELECT
  cc.tbl AS table_name,
  cc.expected AS expected_rows,
  COUNT(CASE WHEN ac.cnt = cc.expected THEN 1 END) AS approaches_passing,
  COUNT(*) AS total_approaches,
  CASE WHEN COUNT(CASE WHEN ac.cnt = cc.expected THEN 1 END) = COUNT(*) THEN 'ALL PASS' ELSE 'SOME FAIL' END AS overall_status,
  CONCAT_WS(', ', COLLECT_LIST(CASE WHEN ac.cnt != cc.expected THEN ac.approach || '=' || ac.cnt END)) AS failing_approaches
FROM count_check cc
JOIN actual_counts ac ON cc.tbl = ac.tbl
GROUP BY cc.tbl, cc.expected
ORDER BY cc.tbl
