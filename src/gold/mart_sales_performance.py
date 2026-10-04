import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, current_timestamp

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Construyendo Data Mart de Ventas en {env_catalog}.gold...")

# 1. Leer tablas de Silver
df_orders = spark.table(f"{env_catalog}.silver.orders")
df_items = spark.table(f"{env_catalog}.silver.order_items")
df_products = spark.table(f"{env_catalog}.silver.products")
df_customers = spark.table(f"{env_catalog}.silver.customers")

# 2. Unir tablas (Filtro por órdenes completadas)
df_joined = df_orders.filter(col("order_status") == "delivered") \
    .join(df_items, "order_id", "inner") \
    .join(df_products, "product_id", "left") \
    .join(df_customers, "customer_id", "left")

# 3. Agregar métricas de negocio
df_mart = df_joined.groupBy(
    col("order_purchase_timestamp").cast("date").alias("sale_date"),
    col("product_category_name"),
    col("customer_state")
).agg(
    count("order_item_id").alias("total_items_sold"),
    sum("price").alias("total_revenue"),
    sum("freight_value").alias("total_freight")
)

# 4. Inyectar linaje y guardar en Gold
df_gold = df_mart.withColumn("_gold_timestamp", current_timestamp())

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.gold.mart_sales_performance")

print("Capa Gold: mart_sales_performance creado exitosamente.")