# -*- coding: utf-8 -*-
"""
Configuration File for Consolidated Central Project Data

Centraliza toda la configuración necesaria para los scripts de generación
de vistas Silver y consolidadas.
"""

# =============================================================================
# CONFIGURACIÓN DE PROYECTOS
# =============================================================================

# Proyecto fuente donde están los datos de las compañías
PROJECT_SOURCE = "platform-partners-qua"

# Proyecto central donde se crearán las vistas consolidadas
CENTRAL_PROJECT = "pph-central"

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
# CONFIGURACIÓN DE TABLAS
# =============================================================================

# Lista de tablas a procesar (basada en el análisis de 42 tablas únicas)
TABLES_TO_PROCESS = [
    'appointment', 'appointment_assignment', 'booking', 'business_unit', 'call', 
    'campaign', 'campaign_category', 'campaign_cost', 'campaign_phone_number',
    'customer', 'customer_contact', 'employee', 'estimate', 'estimate_item',
    'inventory_bill', 'inventory_bill_item', 'invoice', 'invoice_item', 'job',
    'job_hold_reason', 'job_split', 'job_type', 'job_type_business_unit_id',
    'job_type_skill', 'lead', 'location', 'location_contact', 'membership',
    'non_job_appointment', 'payment', 'payment_applied_to', 'project',
    'project_status', 'project_sub_status', 'technician', 'zone', 'zone_city',
    'zone_service_day', 'zone_technician', 'zone_zip', 'zone_business_unit',
    'estimate_external_link'
]

# =============================================================================
# CONFIGURACIÓN DE VISTAS
# =============================================================================

# Prefijo para vistas Silver normalizadas
SILVER_VIEW_PREFIX = "vw_"

# Prefijo para vistas consolidadas centrales
CONSOLIDATED_VIEW_PREFIX = "vw_consolidated_"

# =============================================================================
# CONFIGURACIÓN DE VALORES POR DEFECTO
# =============================================================================

# Mapeo de tipos de campos a valores por defecto
DEFAULT_VALUES = {
    'text_fields': {
        'keywords': ['name', 'description', 'note', 'comment', 'reason', 'type', 'title', 'summary'],
        'default': "COALESCE(NULL, '')"
    },
    'numeric_fields': {
        'keywords': ['id', 'count', 'qty', 'amount', 'cost', 'price', 'duration', 'number', 'total'],
        'default': "COALESCE(NULL, 0)"
    },
    'date_fields': {
        'keywords': ['date', 'time', 'created', 'modified', 'updated', 'start', 'end'],
        'default': "COALESCE(NULL, NULL)"
    },
    'boolean_fields': {
        'keywords': ['active', 'enabled', 'visible', 'is_', 'has_', 'do_not_'],
        'default': "COALESCE(NULL, FALSE)"
    },
    'unknown_fields': {
        'default': "COALESCE(NULL, NULL)"
    }
}

# =============================================================================
# CONFIGURACIÓN DE METADATA
# =============================================================================

# Campos de metadata que se agregan a cada vista Silver
METADATA_FIELDS = [
    'source_project',
    'silver_processed_at', 
    'company_name'
]

# =============================================================================
# CONFIGURACIÓN DE SALIDA
# =============================================================================

# Directorio base para archivos de salida
OUTPUT_BASE_DIR = "generated_views"

# Formato de timestamp para nombres de archivo
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# =============================================================================
# CONFIGURACIÓN DE LOGGING
# =============================================================================

# Nivel de logging
LOG_LEVEL = "INFO"

# Formato de logs
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# =============================================================================
# CONFIGURACIÓN DE VALIDACIÓN
# =============================================================================

# Porcentaje mínimo de compañías que deben tener un campo para considerarlo "común"
MIN_COMPANIES_FOR_COMMON_FIELD = 0.8  # 80%

# Número máximo de compañías para procesar en modo de prueba
MAX_COMPANIES_FOR_TEST = 5

# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def get_dataset_name(project_id):
    """
    Genera el nombre del dataset para un proyecto específico
    """
    return f"{BRONZE_DATASET_PREFIX}{project_id.replace('-', '_')}"

def get_silver_view_name(table_name):
    """
    Genera el nombre de la vista Silver para una tabla específica
    """
    return f"{SILVER_VIEW_PREFIX}{table_name}"

def get_consolidated_view_name(table_name):
    """
    Genera el nombre de la vista consolidada para una tabla específica
    """
    return f"{CONSOLIDATED_VIEW_PREFIX}{table_name}"

def get_default_value_for_field(field_name):
    """
    Determina el valor por defecto para un campo basándose en su nombre
    """
    field_lower = field_name.lower()
    
    # Verificar cada categoría
    for category, config in DEFAULT_VALUES.items():
        if category == 'unknown_fields':
            continue
            
        if any(keyword in field_lower for keyword in config['keywords']):
            return f"{config['default']} as {field_name}"
    
    # Si no coincide con ninguna categoría, usar unknown_fields
    return f"{DEFAULT_VALUES['unknown_fields']['default']} as {field_name}"

def validate_config():
    """
    Valida que la configuración sea correcta
    """
    errors = []
    
    # Validar proyectos
    if not PROJECT_SOURCE:
        errors.append("PROJECT_SOURCE no puede estar vacío")
    
    if not CENTRAL_PROJECT:
        errors.append("CENTRAL_PROJECT no puede estar vacío")
    
    # Validar datasets
    if not SILVER_DATASET:
        errors.append("SILVER_DATASET no puede estar vacío")
    
    # Validar tablas
    if not TABLES_TO_PROCESS:
        errors.append("TABLES_TO_PROCESS no puede estar vacío")
    
    if errors:
        raise ValueError(f"Errores de configuración: {', '.join(errors)}")
    
    return True

# Validar configuración al importar
if __name__ != "__main__":
    validate_config()
