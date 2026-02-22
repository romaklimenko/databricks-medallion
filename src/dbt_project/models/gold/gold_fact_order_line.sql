-- Approach 7: dbt — Gold fact_order_line

{{ config(materialized='table') }}

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
FROM {{ ref('silver_order_lines') }} ol
INNER JOIN {{ ref('silver_orders') }} o ON ol.order_id = o.order_id
LEFT JOIN {{ ref('gold_dim_customer') }} dc
  ON o.customer_id = dc.customer_id
  AND o.order_date >= dc.valid_from
  AND o.order_date <= dc.valid_to
LEFT JOIN {{ ref('gold_dim_product') }} dp
  ON ol.product_id = dp.product_id
