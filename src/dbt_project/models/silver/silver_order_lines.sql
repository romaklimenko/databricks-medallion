-- Approach 7: dbt — Silver order lines

{{ config(materialized='table') }}

WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id, line_id ORDER BY _batch_id DESC) AS _rn
  FROM {{ ref('bronze_order_lines') }}
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
  AND line_id IS NOT NULL
