# Databricks notebook source
# Approach 1: Python Notebooks — Silver Layer
# Reads from bronze tables, applies deduplication (latest batch wins),
# type casting, standardization, and derived columns.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "medallion")
dbutils.widgets.text("schema", "approach_notebooks")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# --- silver_customers ---

bronze_cust = spark.table(f"{catalog}.{schema}.bronze_customers")

w = Window.partitionBy("customer_id").orderBy(F.desc("_batch_id"))
silver_cust = (
    bronze_cust.withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_source_file", "_ingested_at", "_batch_id")
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("address", F.trim(F.col("address")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("segment", F.initcap(F.trim(F.col("segment"))))
    .filter(F.col("customer_id").isNotNull())
)

silver_cust.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_customers")

# COMMAND ----------

# --- silver_products ---

bronze_prod = spark.table(f"{catalog}.{schema}.bronze_products")

w = Window.partitionBy("product_id").orderBy(F.desc("_batch_id"))
silver_prod = (
    bronze_prod.withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_source_file", "_ingested_at", "_batch_id")
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("subcategory", F.trim(F.col("subcategory")))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    .filter(F.col("product_id").isNotNull())
)

silver_prod.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_products")

# COMMAND ----------

# --- silver_orders ---

bronze_ord = spark.table(f"{catalog}.{schema}.bronze_orders")

w = Window.partitionBy("order_id").orderBy(F.desc("_batch_id"))
silver_ord = (
    bronze_ord.withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_source_file", "_ingested_at", "_batch_id")
    .withColumn("order_id", F.col("order_id").cast("int"))
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("order_date", F.col("order_date").cast("date"))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .filter(F.col("order_id").isNotNull())
)

silver_ord.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_orders")

# COMMAND ----------

# --- silver_order_lines ---

bronze_ol = spark.table(f"{catalog}.{schema}.bronze_order_lines")

w = Window.partitionBy("order_id", "line_id").orderBy(F.desc("_batch_id"))
silver_ol = (
    bronze_ol.withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_source_file", "_ingested_at", "_batch_id")
    .withColumn("order_id", F.col("order_id").cast("int"))
    .withColumn("line_id", F.col("line_id").cast("int"))
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    .withColumn("discount_pct", F.col("discount_pct").cast("decimal(5,2)"))
    .withColumn(
        "line_amount",
        F.round(
            F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100),
            2,
        ).cast("decimal(12,2)"),
    )
    .filter(F.col("order_id").isNotNull() & F.col("line_id").isNotNull())
)

silver_ol.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_order_lines")

# COMMAND ----------

# Verify row counts
for t in ["silver_customers", "silver_products", "silver_orders", "silver_order_lines"]:
    print(f"{t}: {spark.table(f'{catalog}.{schema}.{t}').count()} rows")
