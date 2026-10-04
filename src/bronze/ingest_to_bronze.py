import json
import uuid
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# 1. Obtener parámetros vía línea de comandos (Compatible con Workflows)
parser = argparse.ArgumentParser()
parser.add_argument("--env_catalog", default="olist_dev")
parser.add_argument("--repo_path", default="/Workspace/Users/dani149810@gmail.com/Project_06_Olist")
args, unknown = parser.parse_known_args()

env_catalog = args.env_catalog
repo_path = args.repo_path

# 2. Rutas dinámicas basadas en el entorno
config_path = f"{repo_path}/config/ingestion_metadata.json"

# 3. Checkpoint aislado por ambiente
checkpoint_base = f"abfss://bronze@adlsolist.dfs.core.windows.net/checkpoints/{env_catalog}/auto_loader/"

# 4. Crear la tabla de auditoría si no existe en el catálogo actual
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {env_catalog}.bronze.ingestion_audit_log (
        log_id STRING,
        table_name STRING,
        load_start_time TIMESTAMP,
        load_end_time TIMESTAMP,
        status STRING,
        records_inserted INT,
        error_message STRING
    )
""")

def log_audit(table_name, status, error_msg=""):
    """Inserta un registro en la tabla de control"""
    log_id = str(uuid.uuid4())
    # Escapar comillas simples en el mensaje de error para evitar que rompa el SQL
    error_msg_clean = error_msg.replace("'", "''") 
    spark.sql(f"""
        INSERT INTO {env_catalog}.bronze.ingestion_audit_log 
        (log_id, table_name, load_start_time, load_end_time, status, records_inserted, error_message)
        VALUES ('{log_id}', '{table_name}', current_timestamp(), current_timestamp(), '{status}', 0, '{error_msg_clean}')
    """)

# Leer metadatos
try:
    with open(config_path, "r") as f:
        metadata = json.load(f)
except Exception as e:
    print(f"Error leyendo config: {e}")
    raise e

landing_base_path = metadata["landing_base_path"]

print(f"Iniciando ingesta incremental a capa Bronze en el catálogo: {env_catalog}")
print(f"Leyendo configuración desde: {config_path}")

# Iterar sobre cada dataset del JSON
for dataset in metadata["datasets"]:
    table_name = dataset["table_name"]
    
    # Extraer el nombre de la carpeta a partir del file_pattern
    folder_name = dataset["file_pattern"].replace("*.csv", "")
    source_path = f"{landing_base_path}{folder_name}/" 
    checkpoint_path = f"{checkpoint_base}{table_name}"
    
    print(f"Procesando: {table_name} desde {source_path}")
    
    try:
        # Configurar Auto Loader apuntando a la subcarpeta
        df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", dataset["format"]) \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema") \
            .option("header", dataset["has_header"]) \
            .load(source_path)
            
        # Extraer metadatos técnicos compatibles con Unity Catalog
        df_enriched = df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", col("_metadata.file_path"))
            
        # Escritura incremental a Delta
        query = df_enriched.writeStream.format("delta") \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true") \
            .trigger(availableNow=True) \
            .table(f"{env_catalog}.bronze.raw_{table_name}")
            
        query.awaitTermination()
        
        # Auditoría Exitosa
        log_audit(table_name, "SUCCESS")
        
    except Exception as e:
        print(f"Error procesando {table_name}: {str(e)}")
        log_audit(table_name, "FAILED", str(e)[:200])

print("Ingesta a Bronze finalizada.")