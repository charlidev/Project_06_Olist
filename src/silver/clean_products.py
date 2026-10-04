import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Obtener parámetros vía línea de comandos (Reemplazo de dbutils)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

print(f"Procesando tabla products para la capa Silver en {env_catalog}...")

# 1. Leer de Bronze
df_raw = spark.table(f"{env_catalog}.bronze.raw_products")

# 2. Reglas de Limpieza y Transformación
df_clean = df_raw.withColumn(
    "product_category_name", 
    when(col("product_category_name").isNull(), "Unknown").otherwise(col("product_category_name"))
).withColumn(
    "product_weight_g", col("product_weight_g").cast("double")
).withColumn(
    "product_length_cm", col("product_length_cm").cast("double")
).withColumn(
    "product_height_cm", col("product_height_cm").cast("double")
).withColumn(
    "product_width_cm", col("product_width_cm").cast("double")
).dropDuplicates(["product_id"])

# 3. Inyectar metadato técnico de la capa
df_silver = df_clean.withColumn("_silver_timestamp", current_timestamp())

# 4. Escribir en Silver (Sobrescritura estructurada para tablas de dimensión)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{env_catalog}.silver.products")

print("Capa Silver: Tabla products procesada exitosamente.")