-- Approach 7: dbt — Bronze products

{{ config(materialized='table') }}

SELECT
  product_id,
  product_name,
  category,
  subcategory,
  unit_price,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM read_files(
  '/Volumes/{{ var("catalog_name") }}/{{ var("landing_schema") }}/{{ var("volume_name") }}/batch_*/products.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
)
