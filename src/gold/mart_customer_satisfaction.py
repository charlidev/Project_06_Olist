import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, when, current_timestamp

spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Construyendo Data Mart de Satisfacción en {env_catalog}.gold...")

# Leer tablas de Silver
df_orders = spark.table(f"{env_catalog}.silver.orders")
df_reviews = spark.table(f"{env_catalog}.silver.order_reviews")
df_customers = spark.table(f"{env_catalog}.silver.customers")

# Unir reseñas con la orden y el cliente
df_joined = df_reviews.join(df_orders, "order_id", "inner") \
    .join(df_customers, "customer_id", "inner")

# Agregar métricas de calidad
df_mart = df_joined.groupBy(
    col("order_purchase_timestamp").cast("date").alias("sale_date"),
    col("customer_state")
).agg(
    count("review_id").alias("total_reviews"),
    avg("review_score").alias("avg_review_score"),
    _sum(when(col("review_score") <= 2, 1).otherwise(0)).alias("negative_reviews")
)

# Guardar en Gold
df_gold = df_mart.withColumn("_gold_timestamp", current_timestamp())

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.gold.mart_customer_satisfaction")

print("Capa Gold: mart_customer_satisfaction creado exitosamente.")