-- Approach 7: dbt — Silver products

{{ config(materialized='table') }}

WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _batch_id DESC) AS _rn
  FROM {{ ref('bronze_products') }}
)
SELECT
  CAST(product_id AS INT) AS product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(subcategory) AS subcategory,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price
FROM ranked
WHERE _rn = 1
  AND product_id IS NOT NULL
