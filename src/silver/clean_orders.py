import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Procesando tabla orders para la capa Silver en {env_catalog}...")

# 1. Leer tabla cruda de Bronze
df_raw = spark.table(f"{env_catalog}.bronze.raw_orders")

# 2. Reglas de Limpieza y Transformación (Casteo de fechas y eliminación de duplicados)
df_clean = df_raw.withColumn(
    "order_purchase_timestamp", col("order_purchase_timestamp").cast("timestamp")
).withColumn(
    "order_approved_at", col("order_approved_at").cast("timestamp")
).withColumn(
    "order_delivered_carrier_date", col("order_delivered_carrier_date").cast("timestamp")
).withColumn(
    "order_delivered_customer_date", col("order_delivered_customer_date").cast("timestamp")
).withColumn(
    "order_estimated_delivery_date", col("order_estimated_delivery_date").cast("timestamp")
).dropDuplicates(["order_id"])

# 3. Inyectar metadato técnico de la capa
df_silver = df_clean.withColumn("_silver_timestamp", current_timestamp())

# 4. Escribir en Silver (Sobrescritura estructurada)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.silver.orders")

print("Capa Silver: Tabla orders procesada exitosamente y fechas castadas a TIMESTAMP.")