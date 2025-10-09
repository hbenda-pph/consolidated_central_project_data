# 🏗️ Consolidación de Datos - Platform Partners

Sistema completo para consolidar datos de ServiceTitan de múltiples compañías en un proyecto central.

## 📋 Estructura del Proyecto

```
consolidated_central_project_data/
├── config.py                           # ✅ Configuración compartida (dinámico)
├── README.md                           # ✅ Este archivo (documentación general)
├── .gitignore                          # ✅ Git ignore
│
├── generate_silver_views/              # 📁 PASO 2: Vistas Silver Normalizadas
│   ├── generate_silver_views.py        # ✅ Script funcional ÚNICO
│   ├── main.py                         # ✅ Entry point para Cloud Run Job
│   ├── generate_silver_views_notebook.ipynb  # Notebook de pruebas
│   ├── consolidation_tracking_manager.py     # Tracking por tabla×compañía
│   ├── config.py                       # Configuración (100% dinámico)
│   ├── build_deploy.sh                 # Deploy del Job
│   ├── Dockerfile                      # Imagen Docker
│   ├── requirements.txt                # Dependencias Python
│   └── .dockerignore                   # Docker ignore
│
├── generate_consolidated_tables/       # 📁 PASO 3: Tablas Consolidadas
│   ├── consolidated_tables_job.py      # Cloud Run Job
│   ├── consolidated_tables_notebook.ipynb  # Notebook de pruebas
│   ├── consolidated_tables_create.py   # Script auxiliar
│   ├── execute_consolidated_tables.py  # Script auxiliar
│   ├── build_deploy.sh                 # Deploy del Job
│   ├── Dockerfile                      # Imagen Docker
│   ├── requirements.txt                # Dependencias Python
│   ├── .dockerignore                   # Docker ignore
│   └── README.md                       # Documentación detallada
│
└── review/                             # 📁 Archivos obsoletos (para borrar)
    ├── generate_silver_views_old.py    # Versión antigua
    ├── consolidation_status_manager.py # Manager obsoleto
    ├── test_*.py                       # Scripts de prueba
    ├── debug_*.py                      # Scripts de debug
    └── [otros 20+ archivos obsoletos]
```

---

## 🎯 Flujo de Datos

```
┌─────────────────────────────────────────────────────────────┐
│ PROYECTOS DE COMPAÑÍAS (30+)                                │
│ ├─ shape-mhs-1                                              │
│ ├─ shape-chc-2                                              │
│ └─ ... (28 más)                                             │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ PASO 1: Bronze por Compañía (Fivetran)                      │
│ {company}.servicetitan_{company}.{table}                     │
│ - Datos RAW de ServiceTitan                                 │
│ - Incluye campos _fivetran                                  │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ PASO 2: Silver por Compañía (Vistas Normalizadas)           │
│ {company}.silver.vw_{table}                                  │
│ - Omite campos _fivetran                                    │
│ - Normaliza tipos de datos (conflictos → STRING)            │
│ - Layout consistente entre compañías                        │
│ - Campos en orden alfabético                                │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ PASO 3: Bronze Central (Tablas Consolidadas)                │
│ pph-central.bronze.consolidated_{table}                      │
│ - UNION ALL de todas las compañías                          │
│ - PARTITION BY DATE_TRUNC(field, MONTH)                     │
│ - CLUSTER BY company_id, [otros]                            │
│ - Scheduled refresh diario (2 AM)                           │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ PASO 4: Silver Central (Vistas Consolidadas) - PENDIENTE    │
│ pph-central.silver.vw_{table}                                │
│ - Capa de acceso para usuarios                              │
│ - Permisos controlados                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Ejecución

### Paso 2: Generar Vistas Silver

#### Opción A: Cloud Run Job (Recomendado para producción)
```bash
cd generate_silver_views
./build_deploy.sh
gcloud run jobs execute generate-silver-views-job --region=us-east1
```

#### Opción B: Ejecución Manual (Para pruebas)
```bash
cd generate_silver_views
python generate_silver_views.py --force a
```

El Job/Script:
- Obtiene tablas dinámicamente (sin hardcoding)
- Analiza esquemas de 30 compañías
- Normaliza tipos de datos (conflictos → STRING)
- Crea vistas Silver en cada compañía
- Orden alfabético de campos para UNION ALL

### Paso 3: Crear Tablas Consolidadas

```bash
cd generate_consolidated_tables
./build_deploy_consolidated.sh
```

Esto crea el Cloud Run Job `create-consolidated-tables-job` que:
- Lee metadatos de `pph-central.management.metadata_consolidated_tables`
- Crea 42 tablas consolidadas con partition/cluster
- Configura scheduled queries para refresh diario

---

## 📊 Proyectos y Datasets

### Proyectos de Compañías
- **Bronze:** `{company}.servicetitan_{company}.*`
- **Silver:** `{company}.silver.vw_*`

### Proyecto Central (pph-central)
- **Bronze:** `pph-central.bronze.consolidated_*`
- **Silver:** `pph-central.silver.vw_*` (pendiente)
- **Management:** `pph-central.management.metadata_consolidated_tables`

### Proyecto de Gestión (platform-partners-des)
- **Settings:** `platform-partners-des.settings.companies`
- **Tracking:** `platform-partners-des.management.companies_consolidated`

---

## 🔧 Configuración

### Service Account
```
data-analytics@platform-partners-des.iam.gserviceaccount.com
```

**Permisos requeridos:**
- `bigquery.dataViewer` en todos los proyectos de compañías
- `bigquery.dataEditor` en pph-central
- `bigquery.jobUser` en todos los proyectos

### Cloud Run Jobs
- **Región:** us-east1
- **Proyecto:** platform-partners-des

---

## 📝 Archivos Compartidos

### `config.py`
Configuración centralizada:
- `PROJECT_SOURCE = "platform-partners-des"`
- `TABLES_TO_PROCESS = [...]` (42 tablas)
- Paths y constantes

### `consolidation_status_manager.py`
Gestión de estados de consolidación por compañía

### `consolidation_tracking_manager.py`
Tracking detallado por compañía y tabla

---

## 🔍 Troubleshooting

### Job se cae por timeout
**Solución:** Usar filtro temporal en `generate_silver_views_job.py`:
```python
START_FROM_LETTER = 'm'  # Reiniciar desde donde falló
```

### Tabla no se puede reemplazar
**Error:** "Cannot replace a table with a different..."
**Causa:** Cambio de particionamiento (día → mes)
**Solución:** Eliminar tabla manualmente cuando no haya dependencias

### Scheduled Query no se crea
**Error:** "API has not been used"
**Solución:** 
```bash
gcloud services enable bigquerydatatransfer.googleapis.com --project=pph-central
```

---

## 📅 Mantenimiento

### Re-ejecutar Paso 2 (actualizar vistas Silver)
```bash
cd generate_silver_views
gcloud run jobs execute generate-silver-views-job --region=us-east1
```

### Re-ejecutar Paso 3 (actualizar tablas consolidadas)
```bash
cd generate_consolidated_tables
gcloud run jobs execute create-consolidated-tables-job --region=us-east1
```

### Ver logs
```bash
gcloud run jobs logs JOB_NAME --region=us-east1
```

---

## 📂 Carpeta `review/`

Contiene archivos obsoletos, scripts de prueba y debugging que no son necesarios para la operación normal pero se mantienen por referencia histórica.

**Puedes eliminar esta carpeta si no necesitas el historial.**

---

**Última actualización:** 2025-10-09  
**Versión:** 2.0 (Reorganizada)
