-- Approach 7: dbt — Bronze orders

{{ config(materialized='table') }}

SELECT
  order_id,
  customer_id,
  order_date,
  status,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM read_files(
  '/Volumes/{{ var("catalog_name") }}/{{ var("landing_schema") }}/{{ var("volume_name") }}/batch_*/orders.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
)
