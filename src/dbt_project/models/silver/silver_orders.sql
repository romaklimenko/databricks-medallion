-- Approach 7: dbt — Silver orders

{{ config(materialized='table') }}

WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _batch_id DESC) AS _rn
  FROM {{ ref('bronze_orders') }}
)
SELECT
  CAST(order_id AS INT) AS order_id,
  CAST(customer_id AS INT) AS customer_id,
  CAST(order_date AS DATE) AS order_date,
  LOWER(TRIM(status)) AS status
FROM ranked
WHERE _rn = 1
  AND order_id IS NOT NULL
