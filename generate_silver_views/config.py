# -*- coding: utf-8 -*-
"""
Configuration File for Consolidated Central Project Data

Configuración centralizada para los procesos de consolidación de datos.
"""

# =============================================================================
# CONFIGURACIÓN DE PROYECTOS
# =============================================================================

# Proyecto fuente donde están los datos de las compañías
PROJECT_SOURCE = "platform-partners-des"

# Proyecto central donde se crearán las tablas consolidadas
PROJECT_CENTRAL = "pph-central"

# Dataset y tabla de configuración de compañías
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# =============================================================================
# CONFIGURACIÓN DE DATASETS
# =============================================================================

# Nombre del dataset Bronze (donde están los datos raw)
BRONZE_DATASET_PREFIX = "servicetitan_"

# Nombre del dataset Silver (donde se crearán las vistas normalizadas)
SILVER_DATASET = "silver"

# =============================================================================
# CONFIGURACIÓN DE TABLAS (DINÁMICO)
# =============================================================================

# ✅ Las tablas se obtienen DINÁMICAMENTE usando get_tables_dynamically()
# NO hay lista hardcoded - el sistema se adapta automáticamente a:
# - Nuevas tablas de ServiceTitan
# - Cambios en Fivetran
# - Diferencias entre compañías

# =============================================================================
# CONFIGURACIÓN DE VISTAS
# =============================================================================

# Prefijo para vistas Silver normalizadas
SILVER_VIEW_PREFIX = "vw_"

# Prefijo para vistas consolidadas centrales
CONSOLIDATED_VIEW_PREFIX = "vw_consolidated_"

# =============================================================================
# CONFIGURACIÓN DE SALIDA
# =============================================================================

# Directorio base para archivos SQL generados
OUTPUT_BASE_DIR = "output"

# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def get_tables_dynamically():
    """
    Obtiene lista de tablas dinámicamente desde las compañías
    
    RECOMENDADO: Usar esta función en lugar de TABLES_TO_PROCESS
    
    Returns:
        list: Lista de nombres de tablas únicas encontradas
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_SOURCE)
    
    # Obtener compañías activas
    companies_query = f"""
        SELECT DISTINCT company_project_id
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_fivetran_status = TRUE
          AND company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
    """
    
    companies_result = client.query(companies_query).result()
    companies = [row.company_project_id for row in companies_result]
    
    # Recopilar tablas de todas las compañías
    all_tables = set()
    
    for company_project_id in companies:
        try:
            dataset_name = f"{BRONZE_DATASET_PREFIX}{company_project_id.replace('-', '_')}"
            
            tables_query = f"""
                SELECT table_name
                FROM `{company_project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
                WHERE table_type = 'BASE TABLE'
                  AND table_name NOT LIKE '_fivetran%'
            """
            
            tables_result = client.query(tables_query).result()
            company_tables = [row.table_name for row in tables_result]
            all_tables.update(company_tables)
            
        except Exception:
            continue
    
    return sorted(list(all_tables))

def validate_config():
    """
    Valida que la configuración sea correcta
    """
    errors = []
    
    # Validar proyectos
    if not PROJECT_SOURCE:
        errors.append("PROJECT_SOURCE no puede estar vacío")
    
    if not PROJECT_CENTRAL:
        errors.append("PROJECT_CENTRAL no puede estar vacío")
    
    # Validar datasets
    if not DATASET_NAME:
        errors.append("DATASET_NAME no puede estar vacío")
    
    if not TABLE_NAME:
        errors.append("TABLE_NAME no puede estar vacío")
    
    if errors:
        raise ValueError(f"Errores de configuración: {', '.join(errors)}")
    
    return True

# Validar configuración al importar
try:
    validate_config()
except ValueError as e:
    print(f"❌ Error en configuración: {str(e)}")
    raise
