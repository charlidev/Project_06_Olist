from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, current_timestamp
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

try:
    env_catalog = dbutils.widgets.get("env_catalog")
except:
    env_catalog = "olist_dev"

print(f"Construyendo Data Mart de Vendedores en {env_catalog}.gold...")

df_orders = spark.table(f"{env_catalog}.silver.orders")
df_items = spark.table(f"{env_catalog}.silver.order_items")
df_sellers = spark.table(f"{env_catalog}.silver.sellers")

# Filtro exclusivo de órdenes completadas
df_joined = df_sellers.join(df_items, "seller_id", "inner") \
    .join(df_orders, "order_id", "inner") \
    .filter(col("order_status") == "delivered")

df_mart = df_joined.groupBy(
    col("order_purchase_timestamp").cast("date").alias("sale_date"),
    col("seller_id"),
    col("seller_state")
).agg(
    count("order_item_id").alias("items_dispatched"),
    _sum("price").alias("total_revenue"),
    _sum("freight_value").alias("total_freight_managed")
)

df_gold = df_mart.withColumn("_gold_timestamp", current_timestamp())

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.gold.mart_seller_performance")

print("Capa Gold: mart_seller_performance creado exitosamente.")