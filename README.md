# Sistema de Consolidación de Datos BigQuery

## Descripción
Sistema automatizado para consolidar datos de múltiples proyectos BigQuery (compañías) en un proyecto central, siguiendo la arquitectura Bronze-Silver-Gold.

## Arquitectura
- **Bronze**: Datos raw extraídos de ServiceTitan
- **Silver**: Datos normalizados por compañía
- **Silver**: Datos consolidados de todas las compañías
- **Bronze**: Tablas consolidadas optimizadas con particionado y clusterizado

## Scripts Principales

### 1. `generate_silver_views.py`
Genera vistas Silver por compañía con normalización de campos y tipos de datos.

**Uso:**
```bash
python generate_silver_views.py
```

**Características:**
- Analiza diferencias de esquemas entre compañías
- Normaliza tipos de datos (JSON → STRING, etc.)
- Maneja campos faltantes con COALESCE
- Actualiza estado de consolidación
- Salta tablas ya 100% consolidadas

### 2. `consolidated_tables_create.py`
Crea tablas consolidadas en bronze con particionado y clusterizado.

**Uso:**
```bash
python consolidated_tables_create.py
```

**Características:**
- Usa metadatos para configuración de particionado/clusterizado
- Crea tablas optimizadas para performance
- Solo procesa tablas 100% consolidadas

### 3. `consolidated_metadata_manager.py`
Maneja metadatos de configuración para tablas consolidadas.

**Funciones:**
- Análisis automático de campos de particionado
- Configuración de clusterizado
- Gestión de estrategias de actualización

### 4. `consolidated_metadata_update.py`
Actualiza configuración de metadatos de forma interactiva.

**Uso:**
```bash
python consolidated_metadata_update.py
```

## Tablas de Control

### `companies_consolidated`
Rastrea el estado de consolidación por compañía y tabla:
- `company_id`: ID de la compañía
- `table_name`: Nombre de la tabla
- `consolidated_status`: 0=No existe, 1=Éxito, 2=Error
- `created_at`, `updated_at`: Timestamps
- `error_message`: Mensaje de error si aplica

### `metadata_consolidated_tables`
Configuración de metadatos para tablas consolidadas:
- `table_name`: Nombre de la tabla
- `partition_fields`: Array de campos para particionado
- `cluster_fields`: Array de campos para clusterizado (máx. 4)
- `update_strategy`: incremental o full_refresh

## Flujo de Trabajo

1. **Generar vistas Silver** por compañía
2. **Inicializar metadatos**: `python consolidated_metadata_initialize.py`
3. **Configurar metadatos** (opcional): `python consolidated_metadata_update.py`
4. **Crear tablas consolidadas**: `python consolidated_tables_create.py`
5. **Crear vistas consolidadas** en silver

## Configuración

### Campos de Particionado (orden de prioridad)
1. `created_on`
2. `updated_on`
3. `date_created`
4. `modified_on`
5. `timestamp`

### Clusterizado por Defecto
- `company_id` (principal)
- Campos adicionales configurables por tabla

## Notas Importantes

- Las vistas Silver excluyen campos `_fivetran*`
- Se manejan automáticamente diferencias de tipos de datos
- El sistema salta tablas ya procesadas exitosamente
- Se requiere autenticación BigQuery configurada
- Los metadatos se almacenan en el dataset `management`