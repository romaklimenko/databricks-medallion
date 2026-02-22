-- Approach 7: dbt — Gold dim_customer (SCD2)
-- Reads from the dbt snapshot and derives business dates from _batch_id.
-- dbt snapshot timestamps reflect execution time, not business dates, so we
-- map _batch_id → business valid_from and use LEAD() for valid_to.

{{ config(materialized='table') }}

WITH snapshot_with_dates AS (
  SELECT
    customer_id,
    customer_name,
    email,
    address,
    city,
    country,
    segment,
    CASE _batch_id
      WHEN 'batch_1' THEN DATE '2024-01-01'
      WHEN 'batch_2' THEN DATE '2024-03-01'
    END AS valid_from,
    dbt_valid_to
  FROM {{ ref('snap_dim_customer') }}
)
SELECT
  ROW_NUMBER() OVER (ORDER BY customer_id, valid_from) AS customer_sk,
  customer_id,
  customer_name,
  email,
  address,
  city,
  country,
  segment,
  valid_from,
  COALESCE(
    LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) - INTERVAL 1 DAY,
    DATE '9999-12-31'
  ) AS valid_to,
  dbt_valid_to IS NULL AS is_current
FROM snapshot_with_dates
