-- Approach 7: dbt — Gold dim_product (SCD1)

{{ config(materialized='table') }}

SELECT
  ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
  product_id,
  product_name,
  category,
  subcategory,
  unit_price
FROM {{ ref('silver_products') }}
