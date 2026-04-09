# Databricks notebook source
# pyright: reportMissingImports=false, reportUndefinedVariable=false

from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    month,
    year,
    quarter,
    dayofweek,
    when,
    explode,
    sequence,
    lit,
    min as spark_min,
    max as spark_max,
)

catalog = "workspace"
silver_schema = "indicium_silver"
bronze_schema = "indicium_bronze"
gold_schema = "indicium_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

sales_line = spark.table(f"{catalog}.{silver_schema}.sales_line")
products = spark.table(f"{catalog}.{bronze_schema}.products")
categories = spark.table(f"{catalog}.{bronze_schema}.categories")
customers = spark.table(f"{catalog}.{bronze_schema}.customers")
employees = spark.table(f"{catalog}.{bronze_schema}.employees")
shippers = spark.table(f"{catalog}.{bronze_schema}.shippers")
territories = spark.table(f"{catalog}.{bronze_schema}.territories")
region = spark.table(f"{catalog}.{bronze_schema}.region")
employee_territories = spark.table(f"{catalog}.{bronze_schema}.employee_territories")

# Dimensoes principais

dim_product = (
    products.select(
        col("product_id").cast("int").alias("product_id"),
        col("product_name"),
        col("supplier_id").cast("int").alias("supplier_id"),
        col("category_id").cast("int").alias("category_id"),
        col("quantity_per_unit"),
        col("unit_price").cast("double").alias("list_price"),
        col("units_in_stock").cast("int").alias("units_in_stock"),
        col("units_on_order").cast("int").alias("units_on_order"),
        col("reorder_level").cast("int").alias("reorder_level"),
        col("discontinued").cast("int").alias("discontinued"),
    )
    .where(col("product_id").isNotNull())
)

dim_category = categories.select(
    col("category_id").cast("int").alias("category_id"),
    col("category_name"),
    col("description").alias("category_description"),
)

dim_customer = customers.select(
    col("customer_id"),
    col("company_name"),
    col("contact_name"),
    col("contact_title"),
    col("city"),
    col("region"),
    col("country"),
)

dim_employee = employees.select(
    col("employee_id").cast("int").alias("employee_id"),
    col("first_name"),
    col("last_name"),
    col("title"),
    col("city"),
    col("region"),
    col("country"),
)

dim_shipper = shippers.select(
    col("shipper_id").cast("int").alias("shipper_id"),
    col("company_name").alias("shipper_name"),
    col("phone").alias("shipper_phone"),
)

dim_territory = territories.select(
    col("territory_id"),
    col("territory_description"),
    col("region_id").cast("int").alias("region_id"),
)

dim_region = region.select(
    col("region_id").cast("int").alias("region_id"),
    col("region_description"),
)

# Dimensao de data
min_max = sales_line.select(
    spark_min(col("order_date")).alias("min_date"),
    spark_max(col("order_date")).alias("max_date"),
).collect()[0]

if min_max["min_date"] is None or min_max["max_date"] is None:
    raise ValueError("Nao foi possivel criar dim_date: order_date vazio.")

dim_date = (
    spark.createDataFrame([(min_max["min_date"], min_max["max_date"])], ["min_date", "max_date"])
    .select(explode(sequence(col("min_date"), col("max_date"))).alias("date"))
    .select(
        col("date"),
        date_format(col("date"), "yyyyMMdd").cast("int").alias("date_key"),
        year(col("date")).alias("year"),
        quarter(col("date")).alias("quarter"),
        month(col("date")).alias("month"),
        date_format(col("date"), "MMMM").alias("month_name"),
        dayofmonth(col("date")).alias("day"),
        dayofweek(col("date")).alias("day_of_week"),
        when(dayofweek(col("date")).isin([1, 7]), lit(True)).otherwise(lit(False)).alias("is_weekend"),
    )
)

# Relacao funcionario-territorio (ponte simples para analise)
bridge_employee_territory = employee_territories.select(
    col("employee_id").cast("int").alias("employee_id"),
    col("territory_id"),
)

# Fato principal
fact_sales = (
    sales_line.select(
        col("order_id"),
        col("product_id"),
        col("customer_id"),
        col("employee_id"),
        col("ship_via").alias("shipper_id"),
        col("order_date"),
        date_format(col("order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        col("required_date"),
        col("shipped_date"),
        col("ship_country"),
        col("ship_region"),
        col("ship_city"),
        col("unit_price"),
        col("quantity"),
        col("discount"),
        col("gross_sales"),
        col("discount_value"),
        col("net_sales"),
        col("shipped_on_time_flag"),
        col("shipping_delay_days"),
    )
)

# Persistencia Gold
for name, df in [
    ("dim_product", dim_product),
    ("dim_category", dim_category),
    ("dim_customer", dim_customer),
    ("dim_employee", dim_employee),
    ("dim_shipper", dim_shipper),
    ("dim_territory", dim_territory),
    ("dim_region", dim_region),
    ("dim_date", dim_date),
    ("bridge_employee_territory", bridge_employee_territory),
    ("fact_sales", fact_sales),
]:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{gold_schema}.{name}")
    )
    print(f"Tabela Gold criada: {catalog}.{gold_schema}.{name} ({df.count()} linhas)")

print("Camada Gold concluida.")
