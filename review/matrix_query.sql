-- Query para visualizar companies_consolidated en formato matriz
-- Filas: Compañías, Columnas: Tablas, Valores: Estados con iconos

WITH company_names AS (
  -- Obtener nombres de compañías para hacer el JOIN
  SELECT DISTINCT 
    c.company_id,
    c.company_name
  FROM `platform-partners-des.settings.companies` c
  WHERE c.company_fivetran_status = TRUE 
    AND c.company_bigquery_status = TRUE
),

available_tables AS (
  -- Obtener dinámicamente todas las tablas de la tabla companies_consolidated
  SELECT DISTINCT table_name
  FROM `platform-partners-des.settings.companies_consolidated`
  ORDER BY table_name
),

all_combinations AS (
  -- Generar todas las combinaciones compañía-tabla posibles (dinámicamente)
  SELECT 
    cn.company_id,
    cn.company_name,
    at.table_name
  FROM company_names cn
  CROSS JOIN available_tables at
),

consolidation_data AS (
  -- Obtener datos de consolidación con nombres de compañía y iconos
  SELECT 
    ac.company_id,
    ac.company_name,
    ac.table_name,
    COALESCE(cc.consolidated_status, 0) AS consolidated_status,
    CASE 
      WHEN COALESCE(cc.consolidated_status, 0) = 0 THEN '⚠️'  -- Tabla no existe
      WHEN COALESCE(cc.consolidated_status, 0) = 1 THEN '✅'  -- Éxito
      WHEN COALESCE(cc.consolidated_status, 0) = 2 THEN '❌'  -- Error
      ELSE '❓'  -- Estado desconocido
    END AS status_icon,
    cc.error_message,
    cc.notes,
    cc.updated_at
  FROM all_combinations ac
  LEFT JOIN `platform-partners-des.settings.companies_consolidated` cc
    ON ac.company_id = cc.company_id 
    AND ac.table_name = cc.table_name
),

-- Generar dinámicamente la lista de tablas para PIVOT
pivot_tables AS (
  SELECT STRING_AGG(DISTINCT CONCAT("'", table_name, "'"), ', ') AS table_list
  FROM available_tables
)

-- Query principal con PIVOT dinámico
SELECT 
  company_name,
  -- Las columnas se generarán dinámicamente basadas en las tablas disponibles
  call, campaign, customer, job, technician, appointment, estimate, invoice, location, lead
FROM (
  SELECT 
    company_name,
    table_name,
    status_icon
  FROM consolidation_data
)
PIVOT (
  MAX(status_icon) FOR table_name IN (
    'call', 'campaign', 'customer', 'job', 'technician',
    'appointment', 'estimate', 'invoice', 'location', 'lead'
  )
)
ORDER BY company_name;

-- Query alternativo con información detallada de errores
/*
SELECT 
  company_name,
  call,
  call_error,
  campaign,
  campaign_error,
  customer,
  customer_error,
  job,
  job_error,
  technician,
  technician_error
FROM (
  SELECT 
    company_name,
    table_name,
    consolidated_status,
    CASE 
      WHEN consolidated_status = 2 THEN error_message 
      ELSE NULL 
    END AS error_message
  FROM consolidation_data
)
PIVOT (
  MAX(consolidated_status) FOR table_name IN (
    'call', 'campaign', 'customer', 'job', 'technician'
  )
)
PIVOT (
  MAX(error_message) FOR table_name IN (
    'call', 'campaign', 'customer', 'job', 'technician'
  )
)
ORDER BY company_name;
*/

-- Query de resumen por tabla
/*
SELECT 
  table_name,
  COUNT(*) as total_companies,
  COUNTIF(consolidated_status = 1) as success_count,
  COUNTIF(consolidated_status = 2) as error_count,
  COUNTIF(consolidated_status = 0) as missing_count,
  ROUND(COUNTIF(consolidated_status = 1) * 100.0 / COUNT(*), 1) as completion_rate,
  CASE 
    WHEN COUNTIF(consolidated_status = 2) = 0 
     AND COUNTIF(consolidated_status = 0) = 0 
     AND COUNT(*) > 0 
    THEN 'COMPLETED' 
    ELSE 'PENDING' 
  END as status
FROM consolidation_data
GROUP BY table_name
ORDER BY completion_rate DESC, table_name;
*/
