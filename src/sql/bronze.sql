-- Databricks notebook source
-- Approach 2: SQL with COPY INTO — Bronze Layer
-- Ingests raw CSVs from the landing volume into Delta tables using COPY INTO.
-- Adds metadata columns: _source_file, _ingested_at, _batch_id.

-- COMMAND ----------

CREATE WIDGET TEXT catalog_name DEFAULT 'medallion';
CREATE WIDGET TEXT schema DEFAULT 'approach_sql';
CREATE WIDGET TEXT landing_schema DEFAULT 'landing';
CREATE WIDGET TEXT volume_name DEFAULT 'raw_files';

-- COMMAND ----------

-- bronze_customers
CREATE TABLE IF NOT EXISTS ${catalog_name}.${schema}.bronze_customers (
  customer_id STRING,
  customer_name STRING,
  email STRING,
  address STRING,
  city STRING,
  country STRING,
  segment STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _batch_id STRING
);

TRUNCATE TABLE ${catalog_name}.${schema}.bronze_customers;

COPY INTO ${catalog_name}.${schema}.bronze_customers
FROM (
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
  FROM '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/customers.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true');

SELECT 'bronze_customers' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.bronze_customers;

-- COMMAND ----------

-- bronze_products
CREATE TABLE IF NOT EXISTS ${catalog_name}.${schema}.bronze_products (
  product_id STRING,
  product_name STRING,
  category STRING,
  subcategory STRING,
  unit_price STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _batch_id STRING
);

TRUNCATE TABLE ${catalog_name}.${schema}.bronze_products;

COPY INTO ${catalog_name}.${schema}.bronze_products
FROM (
  SELECT
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at,
    regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
  FROM '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/products.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true');

SELECT 'bronze_products' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.bronze_products;

-- COMMAND ----------

-- bronze_orders
CREATE TABLE IF NOT EXISTS ${catalog_name}.${schema}.bronze_orders (
  order_id STRING,
  customer_id STRING,
  order_date STRING,
  status STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _batch_id STRING
);

TRUNCATE TABLE ${catalog_name}.${schema}.bronze_orders;

COPY INTO ${catalog_name}.${schema}.bronze_orders
FROM (
  SELECT
    order_id,
    customer_id,
    order_date,
    status,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at,
    regexp_extract(_metadata.file_path, '(batch_\\d+)', 1) AS _batch_id
  FROM '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/orders.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true');

SELECT 'bronze_orders' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.bronze_orders;

-- COMMAND ----------

-- bronze_order_lines
CREATE TABLE IF NOT EXISTS ${catalog_name}.${schema}.bronze_order_lines (
  order_id STRING,
  line_id STRING,
  product_id STRING,
  quantity STRING,
  unit_price STRING,
  discount_pct STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  _batch_id STRING
);

TRUNCATE TABLE ${catalog_name}.${schema}.bronze_order_lines;

COPY INTO ${catalog_name}.${schema}.bronze_order_lines
FROM (
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
  FROM '/Volumes/${catalog_name}/${landing_schema}/${volume_name}/batch_*/order_lines.csv'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true');

SELECT 'bronze_order_lines' AS table_name, COUNT(*) AS row_count
FROM ${catalog_name}.${schema}.bronze_order_lines;
