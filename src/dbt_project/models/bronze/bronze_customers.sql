-- Approach 7: dbt — Bronze customers
-- Read raw CSVs from landing volume with metadata columns.

{{ config(materialized='table') }}

SELECT
  customer_id,
  customer_name,
  email,
  address,
  city,
  country,
  segment,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at,
  regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
FROM read_files(
  '/Volumes/{{ var("catalog_name") }}/{{ var("landing_schema") }}/{{ var("volume_name") }}/batch_*/customers.csv',
  format => 'csv',
  header => 'true',
  inferSchema => 'false'
)
