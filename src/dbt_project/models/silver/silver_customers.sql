-- Approach 7: dbt — Silver customers
-- Dedup + type cast + standardize.
-- The snapshot_batch variable controls which batches are included:
--   'batch_1' = only batch_1 (for initial snapshot capture)
--   'all'     = all batches, latest wins (for change detection on second snapshot)

{{ config(materialized='table') }}

WITH src AS (
  SELECT *
  FROM {{ ref('bronze_customers') }}
  {% if var('snapshot_batch', 'all') == 'batch_1' %}
  WHERE _batch_id = 'batch_1'
  {% endif %}
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _batch_id DESC) AS _rn
  FROM src
)
SELECT
  CAST(customer_id AS INT) AS customer_id,
  TRIM(customer_name) AS customer_name,
  LOWER(TRIM(email)) AS email,
  TRIM(address) AS address,
  TRIM(city) AS city,
  UPPER(TRIM(country)) AS country,
  INITCAP(TRIM(segment)) AS segment,
  _batch_id
FROM ranked
WHERE _rn = 1
  AND customer_id IS NOT NULL
