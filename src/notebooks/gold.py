# Databricks notebook source
# Approach 1: Python Notebooks — Gold Layer
# Builds Kimball dimensional model: dim_product (SCD1), dim_date (generated),
# dim_customer (SCD2 via manual DataFrame operations), fact_order_line.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "medallion")
dbutils.widgets.text("schema", "approach_notebooks")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# =============================================
# gold_dim_product (SCD1 — latest version only)
# =============================================

silver_prod = spark.table(f"{catalog}.{schema}.silver_products")

dim_product = silver_prod.withColumn(
    "product_sk", F.row_number().over(Window.orderBy("product_id"))
).select("product_sk", "product_id", "product_name", "category", "subcategory", "unit_price")

dim_product.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_dim_product")
print(f"gold_dim_product: {spark.table(f'{catalog}.{schema}.gold_dim_product').count()} rows")

# COMMAND ----------

# =============================================
# gold_dim_date (generated from order date range, padded to full months)
# =============================================

import calendar

silver_orders = spark.table(f"{catalog}.{schema}.silver_orders")

date_range = silver_orders.agg(
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date"),
).collect()[0]

min_date = date_range["min_date"].replace(day=1)
max_raw = date_range["max_date"]
max_date = max_raw.replace(day=calendar.monthrange(max_raw.year, max_raw.month)[1])

dim_date = (
    spark.sql(
        f"SELECT explode(sequence(DATE '{min_date}', DATE '{max_date}', INTERVAL 1 DAY)) AS full_date"
    )
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
)

dim_date.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_dim_date")
print(f"gold_dim_date: {spark.table(f'{catalog}.{schema}.gold_dim_date').count()} rows")

# COMMAND ----------

# =============================================
# gold_dim_customer (SCD2 — manual implementation)
# =============================================
# Build history from bronze_customers using fixed batch dates:
#   batch_1 → valid_from = 2024-01-01
#   batch_2 → valid_from = 2024-03-01
# Compare batch versions to detect changes and produce historical + current rows.

bronze_cust = spark.table(f"{catalog}.{schema}.bronze_customers")

# Apply silver-level transforms to bronze data (need per-batch versions for SCD2)
cleaned = (
    bronze_cust.withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("address", F.trim(F.col("address")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("segment", F.initcap(F.trim(F.col("segment"))))
    .filter(F.col("customer_id").isNotNull())
    .withColumn(
        "valid_from",
        F.when(F.col("_batch_id") == "batch_1", F.lit("2024-01-01").cast("date")).otherwise(
            F.lit("2024-03-01").cast("date")
        ),
    )
)

scd_cols = ["customer_name", "email", "address", "city", "country", "segment"]
select_cols = ["customer_id"] + scd_cols + ["valid_from"]

b1 = cleaned.filter(F.col("_batch_id") == "batch_1").select(*select_cols)
b2 = cleaned.filter(F.col("_batch_id") == "batch_2").select(*select_cols)

# COMMAND ----------

# Customers only in batch_1 (unchanged)
unchanged = b1.join(b2, "customer_id", "left_anti").withColumn(
    "valid_to", F.lit("9999-12-31").cast("date")
).withColumn("is_current", F.lit(True))

# Customers in both batches — detect changes
b1a = b1.alias("old")
b2a = b2.alias("new")
joined = b1a.join(b2a, F.col("old.customer_id") == F.col("new.customer_id"))

change_cond = (
    (F.col("old.email") != F.col("new.email"))
    | (F.col("old.address") != F.col("new.address"))
    | (F.col("old.city") != F.col("new.city"))
    | (F.col("old.country") != F.col("new.country"))
    | (F.col("old.segment") != F.col("new.segment"))
)

changed = joined.filter(change_cond)

# Historical rows (old version, closed the day before batch_2)
historical = changed.select(
    F.col("old.customer_id"),
    *[F.col(f"old.{c}").alias(c) for c in scd_cols],
    F.col("old.valid_from"),
    F.date_sub(F.col("new.valid_from"), 1).alias("valid_to"),
    F.lit(False).alias("is_current"),
)

# Current rows (new version from batch_2)
current_updated = changed.select(
    F.col("new.customer_id"),
    *[F.col(f"new.{c}").alias(c) for c in scd_cols],
    F.col("new.valid_from"),
    F.lit("9999-12-31").cast("date").alias("valid_to"),
    F.lit(True).alias("is_current"),
)

# Customers in both batches but no value change
unchanged_both = joined.filter(~change_cond).select(
    F.col("old.customer_id"),
    *[F.col(f"old.{c}").alias(c) for c in scd_cols],
    F.col("old.valid_from"),
    F.lit("9999-12-31").cast("date").alias("valid_to"),
    F.lit(True).alias("is_current"),
)

# New customers (only in batch_2)
new_cust = b2.join(b1, "customer_id", "left_anti").withColumn(
    "valid_to", F.lit("9999-12-31").cast("date")
).withColumn("is_current", F.lit(True))

# COMMAND ----------

# Union all groups and assign sequential surrogate keys
dim_customer = (
    unchanged
    .unionByName(historical)
    .unionByName(current_updated)
    .unionByName(unchanged_both)
    .unionByName(new_cust)
)

dim_customer = dim_customer.withColumn(
    "customer_sk", F.row_number().over(Window.orderBy("customer_id", "valid_from"))
).select(
    "customer_sk", "customer_id", "customer_name", "email",
    "address", "city", "country", "segment",
    "valid_from", "valid_to", "is_current",
)

dim_customer.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_dim_customer")
print(f"gold_dim_customer: {spark.table(f'{catalog}.{schema}.gold_dim_customer').count()} rows")

# COMMAND ----------

# =============================================
# gold_fact_order_line
# =============================================

silver_ol = spark.table(f"{catalog}.{schema}.silver_order_lines")
silver_ord = spark.table(f"{catalog}.{schema}.silver_orders")
dim_cust = spark.table(f"{catalog}.{schema}.gold_dim_customer")
dim_prod = spark.table(f"{catalog}.{schema}.gold_dim_product")

# Join order lines with orders
ol_with_order = silver_ol.join(silver_ord, "order_id").select(
    silver_ol["order_id"],
    silver_ol["line_id"],
    silver_ord["customer_id"],
    silver_ol["product_id"],
    silver_ord["order_date"],
    silver_ol["quantity"],
    silver_ol["unit_price"],
    silver_ol["discount_pct"],
    silver_ol["line_amount"],
    silver_ord["status"].alias("order_status"),
)

# FK to dim_customer (SCD2 range lookup: order_date between valid_from and valid_to)
with_cust = ol_with_order.join(
    dim_cust,
    (ol_with_order["customer_id"] == dim_cust["customer_id"])
    & (ol_with_order["order_date"] >= dim_cust["valid_from"])
    & (ol_with_order["order_date"] <= dim_cust["valid_to"]),
    "left",
).select(ol_with_order["*"], dim_cust["customer_sk"])

# FK to dim_product
with_prod = with_cust.join(
    dim_prod,
    with_cust["product_id"] == dim_prod["product_id"],
    "left",
).select(with_cust["*"], dim_prod["product_sk"])

# Add date key and surrogate key
fact = with_prod.withColumn(
    "order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int")
).withColumn(
    "order_line_sk", F.row_number().over(Window.orderBy("order_id", "line_id"))
)

fact.select(
    "order_line_sk", "order_id", "line_id",
    "customer_sk", "product_sk", "order_date_key",
    "quantity", "unit_price", "discount_pct", "line_amount",
    "order_status",
).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_fact_order_line")

print(f"gold_fact_order_line: {spark.table(f'{catalog}.{schema}.gold_fact_order_line').count()} rows")

# COMMAND ----------

# Verify all gold tables
print("\n=== Gold Layer Summary ===")
for t in ["gold_dim_customer", "gold_dim_product", "gold_dim_date", "gold_fact_order_line"]:
    print(f"  {t}: {spark.table(f'{catalog}.{schema}.{t}').count()} rows")
