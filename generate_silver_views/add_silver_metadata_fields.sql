-- ============================================================================
-- Script para agregar campos de Silver Views a metadata_consolidated_tables
-- ============================================================================
-- Este script agrega los campos necesarios para almacenar los resultados
-- del análisis de layouts de vistas Silver.
--
-- Ejecutar en: pph-central.management.metadata_consolidated_tables
-- ============================================================================

-- Verificar que la tabla existe
-- Si no existe, primero crearla con los campos base
-- (Este script asume que la tabla ya existe con los campos base:
--  table_name, partition_fields, cluster_fields, update_strategy, etc.)

-- ============================================================================
-- AGREGAR CAMPOS PARA SILVER VIEWS
-- ============================================================================

-- 1. Agregar campo silver_layout_definition (ARRAY<STRUCT>)
--    Estructura del STRUCT:
--    - field_name: Nombre del campo
--    - target_type: Tipo de dato objetivo
--    - field_order: Orden en el layout (para UNION ALL)
--    - has_type_conflict: Si tuvo conflicto de tipos
--    - is_partial: Si solo está en algunas compañías
--    - alias_name: Alias si es campo aplanado
--    - is_repeated: Si es ARRAY/REPEATED

ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS silver_layout_definition ARRAY<STRUCT<
  field_name STRING,
  target_type STRING,
  field_order INT64,
  has_type_conflict BOOL,
  is_partial BOOL,
  alias_name STRING,
  is_repeated BOOL
>>;

-- 2. Agregar campo silver_view_ddl (SQL DDL completo)
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS silver_view_ddl STRING;

-- 3. Agregar campo silver_analysis_timestamp (Cuándo se analizó)
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS silver_analysis_timestamp TIMESTAMP;

-- 4. Agregar campo silver_status (Estado del análisis)
--    Valores posibles: 'completed', 'pending', 'error', NULL
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS silver_status STRING;

-- 5. Agregar campo silver_use_bronze (Tipo de fuente de datos)
--    BOOL: TRUE = tablas manuales en bronze, FALSE = tablas de Fivetran, NULL = no especificado
--    Este campo indica de dónde provienen los datos para generar la vista Silver
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS silver_use_bronze BOOL;

ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS active BOOL;



UPDATE `pph-central.management.metadata_consolidated_tables`
   SET silver_use_bronze = FALSE
WHERE table_name IS NOT NULL;

SELECT * FROM `pph-central.management.metadata_consolidated_tables`
WHERE table_name IN ("business_unit","campaign","job_type","jobs_timesheet","purchase_order","technician") --,"return","vendor","export_job-canceled-logs","job-cancel-reasons")
 order by table_name

UPDATE `pph-central.management.metadata_consolidated_tables`
   SET silver_use_bronze = TRUE   
 WHERE table_name IN ("business_unit","campaign","job_type","jobs_timesheet","purchase_order","technician") --,"return","vendor","export_job-canceled-logs","job-cancel-reasons")

	



-- ============================================================================
-- VERIFICACIÓN
-- ============================================================================
-- Ejecutar esta consulta para verificar que los campos se agregaron correctamente:

-- SELECT
--   column_name,
--   data_type,
--   is_nullable
-- FROM `pph-central.management.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'metadata_consolidated_tables'
--   AND column_name LIKE 'silver_%'
-- ORDER BY ordinal_position;

-- ============================================================================
-- NOTAS
-- ============================================================================
-- - Los campos son NULL por defecto hasta que se ejecute el análisis
-- - silver_layout_definition puede ser NULL si no se ha analizado la tabla
-- - silver_view_ddl contiene el SQL DDL completo con placeholders <PROJECT_ID>
-- - silver_status puede ser: 'completed', 'pending', 'error', o NULL
-- ============================================================================

