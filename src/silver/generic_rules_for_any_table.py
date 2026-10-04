import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Procesando tablas restantes para la capa Silver en {env_catalog}...")

# Diccionario de configuración por tabla
tables_config = {
    "order_items": {
        "timestamps": ["shipping_limit_date"], 
        "doubles": ["price", "freight_value"], 
        "pk": ["order_id", "order_item_id"]
    },
    "order_payments": {
        "timestamps": [], 
        "doubles": ["payment_value"], 
        "pk": ["order_id", "payment_sequential"]
    },
    "order_reviews": {
        "timestamps": ["review_creation_date", "review_answer_timestamp"], 
        "doubles": [], 
        "pk": ["review_id"]
    },
    "customers": {
        "timestamps": [], "doubles": [], "pk": ["customer_id"]
    },
    "geolocation": {
        "timestamps": [], "doubles": ["geolocation_lat", "geolocation_lng"], 
        "pk": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
    },
    "sellers": {
        "timestamps": [], "doubles": [], "pk": ["seller_id"]
    },
    "category_translation": {
        "timestamps": [], "doubles": [], "pk": ["product_category_name"]
    }
}

for table, config in tables_config.items():
    print(f"Limpiando {table}...")
    df = spark.table(f"{env_catalog}.bronze.raw_{table}")

    # Uso de try_cast para tolerar strings desplazados y devolver NULL
    for c in config["timestamps"]:
        df = df.withColumn(c, expr(f"try_cast({c} AS TIMESTAMP)"))

    for c in config["doubles"]:
        df = df.withColumn(c, expr(f"try_cast({c} AS DOUBLE)"))

    # Deduplicar por Llave Primaria e inyectar metadato
    df_silver = df.dropDuplicates(config["pk"]) \
                  .withColumn("_silver_timestamp", current_timestamp())

    # Guardar en Silver
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{env_catalog}.silver.{table}")

print("Capa Silver completada: Todas las tablas procesadas exitosamente.")