CREATE TABLE IF NOT EXISTS dev.bronze.ingestion_audit_log (
    log_id STRING COMMENT 'UUID generado en tiempo de ejecucion',
    table_name STRING,
    load_start_time TIMESTAMP,
    load_end_time TIMESTAMP,
    status STRING COMMENT 'SUCCESS o FAILED',
    records_inserted BIGINT,
    error_message STRING
)
USING DELTA
COMMENT 'Tabla de control para auditoria de ingesta desde Landing hacia Bronze';