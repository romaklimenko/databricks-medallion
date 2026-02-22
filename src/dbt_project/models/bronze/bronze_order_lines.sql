-- Approach 7: dbt — Bronze order lines

{{ config(materialized='table') }}

SELECT
  order_id,
  line_id,
  product_id,
  quantity,
  unit_price,
  discount_pct,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM read_files(
  '/Volumes/{{ var("catalog_name") }}/{{ var("landing_schema") }}/{{ var("volume_name") }}/batch_*/order_lines.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
)
