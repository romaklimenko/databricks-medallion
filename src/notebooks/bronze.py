# Databricks notebook source
# Approach 1: Python Notebooks — Bronze Layer
# Reads raw CSVs from the landing volume and writes Delta tables
# with metadata columns (_source_file, _ingested_at, _batch_id).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "medallion")
dbutils.widgets.text("schema", "approach_notebooks")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("volume_name", "raw_files")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema")
landing_schema = dbutils.widgets.get("landing_schema")
volume_name = dbutils.widgets.get("volume_name")

# COMMAND ----------

from pyspark.sql import functions as F

volume_path = f"/Volumes/{catalog}/{landing_schema}/{volume_name}"
entities = ["customers", "products", "orders", "order_lines"]

# COMMAND ----------

for entity in entities:
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{volume_path}/batch_*/{entity}.csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_batch_id", F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1)
        )
    )

    table = f"{catalog}.{schema}.bronze_{entity}"
    df.write.mode("overwrite").saveAsTable(table)
    print(f"{table}: {spark.table(table).count()} rows")
