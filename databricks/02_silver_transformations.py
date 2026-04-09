# Databricks notebook source
# pyright: reportMissingImports=false, reportUndefinedVariable=false

from pyspark.sql.functions import col, lit, to_date, when, datediff

catalog = "hive_metastore"
bronze_schema = "indicium_bronze"
silver_schema = "indicium_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

orders = spark.table(f"{catalog}.{bronze_schema}.orders")
order_details = spark.table(f"{catalog}.{bronze_schema}.order_details")

# Tipagem e padronizacao de pedidos
orders_clean = (
    orders.select(
        col("order_id").cast("int").alias("order_id"),
        col("customer_id"),
        col("employee_id").cast("int").alias("employee_id"),
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
        to_date(col("required_date"), "yyyy-MM-dd").alias("required_date"),
        to_date(col("shipped_date"), "yyyy-MM-dd").alias("shipped_date"),
        col("ship_via").cast("int").alias("ship_via"),
        col("freight").cast("double").alias("freight"),
        col("ship_name"),
        col("ship_address"),
        col("ship_city"),
        col("ship_region"),
        col("ship_postal_code"),
        col("ship_country"),
    )
    .where(col("order_id").isNotNull())
)

order_details_clean = (
    order_details.select(
        col("order_id").cast("int").alias("order_id"),
        col("product_id").cast("int").alias("product_id"),
        col("unit_price").cast("double").alias("unit_price"),
        col("quantity").cast("int").alias("quantity"),
        col("discount").cast("double").alias("discount"),
    )
    .where(col("order_id").isNotNull())
    .where(col("product_id").isNotNull())
    .where(col("quantity") > lit(0))
    .where(col("unit_price") >= lit(0))
    .where((col("discount") >= lit(0)) & (col("discount") <= lit(1)))
)

orders_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{silver_schema}.orders_clean"
)

order_details_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{silver_schema}.order_details_clean"
)

# Fato linha de pedido em Silver
sales_line = (
    order_details_clean.alias("d")
    .join(orders_clean.alias("o"), on="order_id", how="inner")
    .select(
        col("o.order_id"),
        col("d.product_id"),
        col("o.customer_id"),
        col("o.employee_id"),
        col("o.ship_via"),
        col("o.order_date"),
        col("o.required_date"),
        col("o.shipped_date"),
        col("o.ship_country"),
        col("o.ship_region"),
        col("o.ship_city"),
        col("d.unit_price"),
        col("d.quantity"),
        col("d.discount"),
        (col("d.unit_price") * col("d.quantity")).alias("gross_sales"),
        (col("d.unit_price") * col("d.quantity") * col("d.discount")).alias("discount_value"),
        (col("d.unit_price") * col("d.quantity") * (lit(1) - col("d.discount"))).alias("net_sales"),
        when(col("o.shipped_date").isNull(), lit(None))
        .when(col("o.shipped_date") <= col("o.required_date"), lit(True))
        .otherwise(lit(False))
        .alias("shipped_on_time_flag"),
        datediff(col("o.shipped_date"), col("o.required_date")).alias("shipping_delay_days"),
    )
)

sales_line.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{silver_schema}.sales_line"
)

print("Silver criada com sucesso.")
print(f"orders_clean: {spark.table(f'{catalog}.{silver_schema}.orders_clean').count()}")
print(f"order_details_clean: {spark.table(f'{catalog}.{silver_schema}.order_details_clean').count()}")
print(f"sales_line: {spark.table(f'{catalog}.{silver_schema}.sales_line').count()}")
