# Consolidated Central Project Data

Este repositorio contiene scripts para generar automáticamente vistas Silver normalizadas y consolidadas para todas las tablas de ServiceTitan en la plataforma multitenant de BigQuery.

## 🎯 Objetivo

Implementar la **Estrategia Híbrida** de consolidación de datos:
- **Bronze** → **Silver** (por compañía, normalizado) → **Central-Silver** (consolidado)

### 🔧 Características Clave
- **Normalización de Campos**: Identifica campos comunes vs parciales
- **Normalización de Tipos**: Detecta y resuelve conflictos de tipos de datos
- **CAST Inteligente**: Convierte automáticamente tipos incompatibles
- **Valores por Defecto**: Rellena campos faltantes con valores apropiados

## 📁 Archivos del Repositorio

### Scripts Principales
- `generate_silver_views.py` - Genera vistas Silver para todas las tablas (con normalización de tipos)
- `test_single_table_analysis.py` - Script de prueba para una tabla específica
- `generate_central_consolidated_views.py` - Genera vistas consolidadas centrales
- `analyze_data_types.py` - Análisis detallado de conflictos de tipos de datos

### Configuración
- `config.py` - Configuración centralizada de proyectos y parámetros
- `requirements.txt` - Dependencias de Python necesarias

## 🚀 Uso en Cloud Shell

### 1. Configuración Automática (Recomendado)
```bash
# Clonar el repositorio
git clone <tu-repo-url> consolidated_central_project_data
cd consolidated_central_project_data

# Configurar entorno automáticamente
python cloud_shell_setup.py

# Ejecutar proceso completo
python cloud_shell_runner.py all
```

### 2. Configuración Manual
```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar autenticación
gcloud auth login
gcloud config set project <tu-proyecto-central>
```

### 3. Configurar Parámetros
Edita `config.py` con tus valores específicos:
```python
PROJECT_SOURCE = "platform-partners-qua"  # Proyecto fuente
CENTRAL_PROJECT = "platform-partners-des"  # Proyecto central
DATASET_NAME = "settings"
TABLE_NAME = "companies"
```

### 4. Comandos de Ejecución

#### Comandos Principales
```bash
# Análisis de prueba (recomendado primero)
python cloud_shell_runner.py test

# Generar vistas Silver únicamente
python cloud_shell_runner.py silver

# Generar vistas consolidadas únicamente
python cloud_shell_runner.py consolidated

# Proceso completo
python cloud_shell_runner.py all

# Validar vistas existentes
python cloud_shell_runner.py validate

# Monitorear estado del sistema
python cloud_shell_runner.py monitor
```

#### Gestión de Rollback
```bash
# Ver sesiones disponibles
python cloud_shell_runner.py sessions

# Rollback de vistas Silver (dry run)
python rollback_manager.py silver

# Rollback de vistas Silver (real)
python rollback_manager.py silver --execute

# Rollback completo (dry run)
python rollback_manager.py all

# Rollback completo (real)
python rollback_manager.py all --execute
```

### 5. Ejecución Paso a Paso (Alternativa)

#### Paso 1: Prueba con una tabla
```bash
python test_single_table_analysis.py
```

#### Paso 2: Generar todas las vistas Silver
```bash
python generate_silver_views.py
```

#### Paso 3: Generar vistas consolidadas
```bash
python generate_central_consolidated_views.py
```

## 📊 Resultados Esperados

### Vistas Silver (por compañía)
- Ubicación: `{project_id}.silver.vw_normalized_{table_name}`
- Campos: Comunes + parciales con COALESCE
- Metadata: `source_project`, `silver_processed_at`, `company_name`

### Vistas Consolidadas (central)
- Ubicación: `{central_project}.silver.vw_consolidated_{table_name}`
- Función: UNION ALL de todas las vistas Silver
- Performance: Optimizada con particionado y clustering

## 🛡️ Gestión de Errores y Rollback

### Sistema de Gestión Robusta
El sistema incluye una **capa de gestión robusta** que permite:

#### ✅ Logging Detallado
- **Sesiones**: Cada ejecución crea una sesión única con timestamp
- **Logs**: Registro detallado de todas las operaciones
- **Operaciones**: JSON con historial de todas las acciones ejecutadas

#### ✅ Rollback Automático
- **Detección de Errores**: Rollback automático si algo falla
- **Scripts de Rollback**: Generados automáticamente para cada sesión
- **Dry Run**: Posibilidad de simular rollback antes de ejecutar

#### ✅ Monitoreo y Validación
- **Validación de Vistas**: Verifica que las vistas existen y son accesibles
- **Pruebas de Consulta**: Ejecuta consultas de prueba en las vistas
- **Reportes de Estado**: Genera reportes detallados del estado del sistema

### Archivos de Gestión
- `execution_manager.py` - Gestión de ejecución con logging
- `rollback_manager.py` - Sistema de rollback automático
- `monitoring_manager.py` - Monitoreo y validación de vistas
- `cloud_shell_runner.py` - Script maestro con gestión completa
- `cloud_shell_setup.py` - Configuración automática del entorno

### Directorio de Sesiones
Cada ejecución crea un directorio en `execution_sessions/` con:
- `execution.log` - Log detallado de la ejecución
- `operations.json` - Historial de operaciones
- `rollback.sql` - Script de rollback generado
- `session_summary.json` - Resumen de la sesión

## 🔧 Personalización

### Campos por Defecto
El script determina automáticamente valores por defecto basándose en el nombre del campo:
- **Texto**: `COALESCE(NULL, '')`
- **Números**: `COALESCE(NULL, 0)`
- **Fechas**: `COALESCE(NULL, NULL)`
- **Booleanos**: `COALESCE(NULL, FALSE)`

### Normalización de Tipos
El script detecta automáticamente conflictos de tipos de datos y los resuelve:
- **INT64 → STRING**: `CAST(field AS STRING)`
- **STRING → INT64**: `SAFE_CAST(field AS INT64)`
- **INT64 → FLOAT64**: `CAST(field AS FLOAT64)`
- **Conversiones complejas**: `COALESCE(SAFE_CAST(field AS target_type), default_value)`

### Tablas Procesadas
Se procesan automáticamente todas las tablas identificadas en el análisis:
- appointment, call, campaign, customer, job, invoice, etc.
- Total: ~42 tablas únicas

## 📈 Ventajas de la Implementación

1. **✅ Automatización Completa**: Basado en análisis de esquemas existente
2. **✅ Campos Preservados**: Usa COALESCE con valores inteligentes
3. **✅ Calidad de Datos**: Incluye metadata y validaciones
4. **✅ Escalabilidad**: Fácil agregar nuevas compañías
5. **✅ Performance**: Vistas optimizadas con UNION ALL

## 🛠️ Troubleshooting

### Error de Permisos
```bash
# Verificar permisos
gcloud projects get-iam-policy <tu-proyecto>
```

### Error de Dataset
```bash
# Verificar que existe el dataset silver
bq ls <proyecto>:silver
```

### Error de Tabla
```bash
# Verificar que existe la tabla en bronze
bq ls <proyecto>:bronze
```

## 📝 Logs y Debugging

Los scripts generan logs detallados y archivos de salida organizados por timestamp:
- `silver_views_YYYYMMDD_HHMMSS/` - Vistas Silver
- `central_consolidated_views_YYYYMMDD_HHMMSS/` - Vistas consolidadas

## 🔄 Próximos Pasos

1. **Fase 1**: Ejecutar pruebas con tabla `call`
2. **Fase 2**: Generar todas las vistas Silver
3. **Fase 3**: Crear vistas consolidadas centrales
4. **Fase 4**: Implementar en producción con CI/CD

## 📞 Soporte

Para dudas o problemas, revisar:
1. Logs de ejecución
2. Archivos de configuración
3. Permisos de BigQuery
4. Existencia de datasets y tablas
