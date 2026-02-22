-- Approach 7: dbt — Gold dim_date (generated from order date range)

{{ config(materialized='table') }}

WITH date_range AS (
  SELECT
    DATE_TRUNC('MONTH', MIN(order_date)) AS min_date,
    LAST_DAY(MAX(order_date)) AS max_date
  FROM {{ ref('silver_orders') }}
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
LATERAL VIEW EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) t AS full_date
