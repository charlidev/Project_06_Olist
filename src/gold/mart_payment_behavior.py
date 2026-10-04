import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, current_timestamp

spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Construyendo Data Mart de Pagos en {env_catalog}.gold...")

df_orders = spark.table(f"{env_catalog}.silver.orders")
df_payments = spark.table(f"{env_catalog}.silver.order_payments")

df_joined = df_payments.join(df_orders, "order_id", "inner")

df_mart = df_joined.groupBy(
    col("order_purchase_timestamp").cast("date").alias("sale_date"),
    col("payment_type")
).agg(
    count("order_id").alias("total_transactions"),
    _sum("payment_value").alias("total_volume"),
    avg("payment_installments").alias("avg_installments")
)

df_gold = df_mart.withColumn("_gold_timestamp", current_timestamp())

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.gold.mart_payment_behavior")

print("Capa Gold: mart_payment_behavior creado exitosamente.")