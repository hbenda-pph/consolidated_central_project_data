-- Query simple para matriz de consolidación con iconos
-- Copiar y pegar en BigQuery Console

WITH company_names AS (
  SELECT DISTINCT 
    c.company_id,
    c.company_name
  FROM `platform-partners-des.settings.companies` c
  WHERE c.company_fivetran_status = TRUE 
    AND c.company_bigquery_status = TRUE
),

consolidation_data AS (
  SELECT 
    cn.company_name,
    cc.table_name,
    CASE 
      WHEN COALESCE(cc.consolidated_status, 0) = 0 THEN '⚠️'
      WHEN COALESCE(cc.consolidated_status, 0) = 1 THEN '✅'
      WHEN COALESCE(cc.consolidated_status, 0) = 2 THEN '❌'
      ELSE '❓'
    END AS status_icon
  FROM company_names cn
  LEFT JOIN `platform-partners-des.settings.companies_consolidated` cc
    ON cn.company_id = cc.company_id
)

-- Formato largo (fácil de exportar a Excel y pivotar)
SELECT 
  company_name,
  table_name,
  status_icon
FROM consolidation_data
ORDER BY company_name, table_name;
