# Consolidated Central Project Data

Scripts para generar vistas Silver normalizadas y consolidadas para todas las tablas de ServiceTitan.

## 🎯 Objetivo

**Bronze** → **Silver** (por compañía, normalizado) → **Central-Silver** (consolidado)

### 🔧 Características
- Normalización de campos y tipos de datos
- CAST automático para tipos incompatibles
- Filtro de campos `_fivetran`
- Seguimiento de estados de consolidación

## 📁 Archivos Principales

- `generate_silver_views.py` - Genera vistas Silver con seguimiento de estados
- `generate_central_consolidated_views.py` - Genera vistas consolidadas centrales
- `analyze_data_types.py` - Análisis de conflictos de tipos de datos
- `consolidation_status_manager.py` - Gestión de estados de consolidación
- `config.py` - Configuración centralizada
- `requirements.txt` - Dependencias de Python

## 🚀 Uso en Cloud Shell

### 1. Configuración
```bash
# Clonar repositorio
git clone <tu-repo-url> consolidated_central_project_data
cd consolidated_central_project_data

# Instalar dependencias
pip install -r requirements.txt

# Configurar autenticación
gcloud auth login
gcloud config set project <tu-proyecto-central>
```

### 2. Configurar Parámetros
Edita `config.py` con tus valores:
```python
PROJECT_SOURCE = "platform-partners-qua"
CENTRAL_PROJECT = "platform-partners-des"
DATASET_NAME = "settings"
TABLE_NAME = "companies"
```

### 3. Comandos Principales
```bash
# Análisis de tipos de datos
python analyze_data_types.py

# Generar vistas Silver
python generate_silver_views.py

# Generar vistas consolidadas
python generate_central_consolidated_views.py

# Gestión de estados
python consolidation_status_manager.py summary
```

### 4. Gestión de Estados
```bash
# Ver resumen de estados
python consolidation_status_manager.py summary

# Ver compañías pendientes
python consolidation_status_manager.py pending

# Ver compañías completadas
python consolidation_status_manager.py completed

# Ver compañías con errores
python consolidation_status_manager.py errors

# Resetear todos los estados
python consolidation_status_manager.py reset
```

## 📊 Estados de Consolidación

### Estados Disponibles
- **0 - PENDING**: Por consolidar (estado inicial)
- **1 - COMPLETED**: Consolidación exitosa
- **2 - ERROR**: Error en el proceso

### Seguimiento Automático
- **Generación Silver**: Actualiza estado automáticamente
- **Solo procesa PENDING**: Evita reprocesar compañías
- **Control de errores**: Identifica compañías con problemas

## 📋 Resultados

### Vistas Silver (por compañía)
- Ubicación: `{project_id}.silver.vw_normalized_{table_name}`
- Normalización de campos y tipos de datos
- Filtro automático de campos `_fivetran`

### Vistas Consolidadas (central)
- Ubicación: `{central_project}.central-silver.vw_consolidated_{table_name}`
- UNION ALL de todas las vistas Silver
- Solo incluye compañías con estado COMPLETED