import json
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.dbutils import DBUtils

# Inicializar Spark y DBUtils
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Obtener parámetros (ej. dev, test, prod) - Por defecto 'olist_dev'
dbutils.widgets.text("env_catalog", "olist_dev")
env_catalog = dbutils.widgets.get("env_catalog")

# Rutas base
# Nota: En Databricks Repos, la ruta local se lee con open() nativo de Python
config_path = "/Workspace/Repos/tu_usuario/Project_06_Olist/config/ingestion_metadata.json"
checkpoint_base = f"abfss://bronze@adlsolist.dfs.core.windows.net/checkpoints/auto_loader/"

def log_audit(table_name, status, error_msg=""):
    """Inserta un registro en la tabla de control"""
    log_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO {env_catalog}.bronze.ingestion_audit_log 
        (log_id, table_name, load_start_time, load_end_time, status, records_inserted, error_message)
        VALUES ('{log_id}', '{table_name}', current_timestamp(), current_timestamp(), '{status}', 0, '{error_msg}')
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

# Iterar sobre cada dataset del JSON
for dataset in metadata["datasets"]:
    table_name = dataset["table_name"]
    file_pattern = dataset["file_pattern"]
    source_path = f"{landing_base_path}{table_name}/" # Asumiendo que acomodas los archivos en subcarpetas
    checkpoint_path = f"{checkpoint_base}{table_name}"
    
    print(f"Procesando: {table_name}")
    
    try:
        # 3.1 & 3.2 Configurar Auto Loader
        df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", dataset["format"]) \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema") \
            .option("header", dataset["has_header"]) \
            .load(landing_base_path) # Auto Loader buscará los archivos según su patrón interno
            
        # 3.5 Metadata técnica
        df_enriched = df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", input_file_name())
            
        # 3.3, 3.4 & 3.6 Escritura incremental a Delta (Raw preservation)
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