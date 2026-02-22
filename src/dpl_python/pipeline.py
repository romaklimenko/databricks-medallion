# Databricks notebook source
# Approach 5: Declarative Pipelines (Python)
# Full medallion pipeline defined as a single Declarative Pipeline using the Python API.
# Bronze: streaming tables via Auto Loader (@dp.table with readStream)
# Silver: materialized views with expectations (@dp.expect_or_drop)
# Gold: dim_customer via create_auto_cdc_flow() (SCD TYPE 2), other dims + fact as MVs
#
# Note: AUTO CDC produces __START_AT/__END_AT columns instead of
# valid_from/valid_to. __END_AT is NULL for current records (not 9999-12-31).
#
# Pipeline configuration keys (set in bundle YAML):
#   catalog_name, landing_schema, volume_name

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog_name = spark.conf.get("catalog_name")
landing_schema = spark.conf.get("landing_schema")
volume_name = spark.conf.get("volume_name")
volume_path = f"/Volumes/{catalog_name}/{landing_schema}/{volume_name}"

# COMMAND ----------

# =============================================
# Bronze Layer: Streaming Tables from landing volume
# =============================================


@dp.table(name="bronze_customers", comment="Raw customers from landing volume")
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{volume_path}/batch_*/customers.csv")
        .select(
            "customer_id", "customer_name", "email", "address", "city", "country", "segment",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1).alias("_batch_id"),
        )
    )


# COMMAND ----------


@dp.table(name="bronze_products", comment="Raw products from landing volume")
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{volume_path}/batch_*/products.csv")
        .select(
            "product_id", "product_name", "category", "subcategory", "unit_price",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1).alias("_batch_id"),
        )
    )


# COMMAND ----------


@dp.table(name="bronze_orders", comment="Raw orders from landing volume")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{volume_path}/batch_*/orders.csv")
        .select(
            "order_id", "customer_id", "order_date", "status",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1).alias("_batch_id"),
        )
    )


# COMMAND ----------


@dp.table(name="bronze_order_lines", comment="Raw order lines from landing volume")
def bronze_order_lines():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{volume_path}/batch_*/order_lines.csv")
        .select(
            "order_id", "line_id", "product_id", "quantity", "unit_price", "discount_pct",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1).alias("_batch_id"),
        )
    )


# COMMAND ----------

# =============================================
# Silver Layer: Materialized Views with expectations
# =============================================


@dp.table(name="silver_customers")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_customers():
    w = Window.partitionBy("customer_id").orderBy(F.desc("_batch_id"))
    return (
        dp.read("bronze_customers")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("address")).alias("address"),
            F.trim(F.col("city")).alias("city"),
            F.upper(F.trim(F.col("country"))).alias("country"),
            F.initcap(F.trim(F.col("segment"))).alias("segment"),
        )
    )


# COMMAND ----------


@dp.table(name="silver_products")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_products():
    w = Window.partitionBy("product_id").orderBy(F.desc("_batch_id"))
    return (
        dp.read("bronze_products")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("product_id").cast("int").alias("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("category")).alias("category"),
            F.trim(F.col("subcategory")).alias("subcategory"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        )
    )


# COMMAND ----------


@dp.table(name="silver_orders")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
def silver_orders():
    w = Window.partitionBy("order_id").orderBy(F.desc("_batch_id"))
    return (
        dp.read("bronze_orders")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.col("order_date").cast("date").alias("order_date"),
            F.lower(F.trim(F.col("status"))).alias("status"),
        )
    )


# COMMAND ----------


@dp.table(name="silver_order_lines")
@dp.expect_or_drop("valid_order_line_id", "order_id IS NOT NULL AND line_id IS NOT NULL")
def silver_order_lines():
    w = Window.partitionBy("order_id", "line_id").orderBy(F.desc("_batch_id"))
    return (
        dp.read("bronze_order_lines")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("line_id").cast("int").alias("line_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            F.col("discount_pct").cast("decimal(5,2)").alias("discount_pct"),
            F.round(
                F.col("quantity").cast("int")
                * F.col("unit_price").cast("decimal(10,2)")
                * (1 - F.col("discount_pct").cast("decimal(5,2)") / 100),
                2,
            ).cast("decimal(12,2)").alias("line_amount"),
        )
    )


# COMMAND ----------

# =============================================
# Gold Layer
# =============================================

# ----- SCD2: dim_customer via create_auto_cdc_flow() -----
# A temporary streaming view provides the cleaned/typed source with _batch_date.
# The AUTO CDC target is an internal streaming table. A downstream MV
# wraps it to add customer_sk.
# track_history_column_list limits SCD2 triggers to the same columns as other approaches.


@dp.view(name="_scd2_customers_input")
def _scd2_customers_input():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{volume_path}/batch_*/customers.csv")
        .select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim(F.col("customer_name")).alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("address")).alias("address"),
            F.trim(F.col("city")).alias("city"),
            F.upper(F.trim(F.col("country"))).alias("country"),
            F.initcap(F.trim(F.col("segment"))).alias("segment"),
            F.when(
                F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1) == "batch_1",
                F.lit("2024-01-01").cast("date"),
            ).otherwise(F.lit("2024-03-01").cast("date")).alias("_batch_date"),
        )
        .filter(F.col("customer_id").isNotNull())
    )


dp.create_streaming_table(name="_scd2_dim_customer")

dp.create_auto_cdc_flow(
    target="_scd2_dim_customer",
    source="_scd2_customers_input",
    keys=["customer_id"],
    sequence_by=F.col("_batch_date"),
    except_column_list=["_batch_date"],
    stored_as_scd_type=2,
    track_history_column_list=["email", "address", "city", "country", "segment"],
)

# COMMAND ----------

# Wrap the SCD2 target with a surrogate key.
# __START_AT / __END_AT are the native DPL SCD2 columns:
#   __START_AT = when this version became active (DATE)
#   __END_AT   = when superseded (DATE), None for current records


@dp.table(name="gold_dim_customer")
def gold_dim_customer():
    w = Window.orderBy("customer_id", "__START_AT")
    return (
        dp.read("_scd2_dim_customer")
        .select(
            F.row_number().over(w).alias("customer_sk"),
            "customer_id", "customer_name", "email",
            "address", "city", "country", "segment",
            "__START_AT", "__END_AT",
        )
    )


# COMMAND ----------

# ----- dim_product (SCD1) -----


@dp.table(name="gold_dim_product")
def gold_dim_product():
    w = Window.orderBy("product_id")
    return (
        dp.read("silver_products")
        .select(
            F.row_number().over(w).alias("product_sk"),
            "product_id", "product_name", "category", "subcategory", "unit_price",
        )
    )


# COMMAND ----------

# ----- dim_date (generated from order date range, padded to full months) -----


@dp.table(name="gold_dim_date")
def gold_dim_date():
    orders = dp.read("silver_orders")
    date_range = orders.agg(
        F.date_trunc("month", F.min("order_date")).alias("min_date"),
        F.last_day(F.max("order_date")).alias("max_date"),
    )
    return (
        date_range
        .select(
            F.explode(F.sequence(F.col("min_date"), F.col("max_date"), F.expr("INTERVAL 1 DAY"))).alias("full_date")
        )
        .select(
            F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
            F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        )
    )


# COMMAND ----------

# ----- fact_order_line -----
# SCD2 join uses __START_AT / __END_AT (NULL = current).
# Note: __END_AT equals the __START_AT of the next version, so we use < (not <=).


@dp.table(name="gold_fact_order_line")
def gold_fact_order_line():
    ol = dp.read("silver_order_lines")
    o = dp.read("silver_orders")
    dc = dp.read("gold_dim_customer")
    dim_prod = dp.read("gold_dim_product")

    # Join order lines with orders (order_id is the join key)
    ol_with_order = ol.join(o, "order_id")

    # Join with dim_customer (SCD2 range join)
    with_cust = ol_with_order.join(
        dc,
        (ol_with_order["customer_id"] == dc["customer_id"])
        & (ol_with_order["order_date"] >= dc["__START_AT"])
        & (dc["__END_AT"].isNull() | (ol_with_order["order_date"] < dc["__END_AT"])),
        "left",
    ).select(ol_with_order["*"], dc["customer_sk"])

    # Join with dim_product
    with_prod = with_cust.join(
        dim_prod,
        with_cust["product_id"] == dim_prod["product_id"],
        "left",
    ).select(with_cust["*"], dim_prod["product_sk"])

    w = Window.orderBy("order_id", "line_id")
    return with_prod.select(
        F.row_number().over(w).alias("order_line_sk"),
        "order_id",
        "line_id",
        "customer_sk",
        "product_sk",
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        "quantity",
        with_prod["unit_price"],
        "discount_pct",
        "line_amount",
        F.col("status").alias("order_status"),
    )
