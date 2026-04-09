# Databricks notebook source
# pyright: reportMissingImports=false, reportUndefinedVariable=false

from pyspark.sql.functions import current_timestamp, input_file_name

# Setup de destino
catalog = "workspace"
bronze_schema = "indicium_bronze"

# Ajuste aqui conforme o local dos arquivos no DBFS.
# Exemplo recomendado (Unity Catalog Volume): /Volumes/workspace/indicium_bronze/raw_data
# Exemplo legado (DBFS FileStore): dbfs:/FileStore/indicium
base_path = "/Volumes/workspace/indicium_bronze/raw_data"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

# Lista de tabelas de origem (nome do arquivo sem .csv)
source_tables = [
    "categories",
    "customers",
    "customer_customer_demo",
    "customer_demographics",
    "employees",
    "employee_territories",
    "orders",
    "order_details",
    "products",
    "region",
    "shippers",
    "suppliers",
    "territories",
    "us_states",
]

for table_name in source_tables:
    file_path = f"{base_path}/{table_name}.csv"

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "false")
        .load(file_path)
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    target = f"{catalog}.{bronze_schema}.{table_name}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    print(f"Bronze carregada: {target} ({df.count()} linhas)")

# Validacao rapida
for table_name in source_tables:
    target = f"{catalog}.{bronze_schema}.{table_name}"
    row_count = spark.table(target).count()
    print(f"{target}: {row_count}")
