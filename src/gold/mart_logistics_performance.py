import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, when, datediff, current_timestamp

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Construyendo Data Mart de Logística en {env_catalog}.gold...")

# 1. Leer tablas de Silver
df_orders = spark.table(f"{env_catalog}.silver.orders")
df_customers = spark.table(f"{env_catalog}.silver.customers")

# 2. Unir y calcular métricas a nivel pedido (días de entrega y bandera de retraso)
df_logistics = df_orders.filter(col("order_status") == "delivered") \
    .join(df_customers, "customer_id", "inner") \
    .withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
    .withColumn("is_delayed", when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0))

# 3. Agregar métricas por fecha y estado del cliente
df_mart = df_logistics.groupBy(
    col("order_purchase_timestamp").cast("date").alias("sale_date"),
    col("customer_state")
).agg(
    count("order_id").alias("total_orders"),
    avg("delivery_days").alias("avg_delivery_days"),
    _sum("is_delayed").alias("total_delayed_orders")
)

# 4. Inyectar linaje y guardar en Gold
df_gold = df_mart.withColumn("_gold_timestamp", current_timestamp())

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.gold.mart_logistics_performance")

print("Capa Gold: mart_logistics_performance creado exitosamente.")