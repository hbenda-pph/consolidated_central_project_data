# 🔧 Generate Silver Views - Paso 2

Genera vistas Silver normalizadas para cada compañía con manejo automático de diferencias de esquemas.

## 📋 Archivos

- **`generate_silver_views.py`** - Script funcional único (puede ejecutarse directamente o ser llamado)
- **`main.py`** - Entry point para Cloud Run Job (sin interacción)
- **`config.py`** - Configuración (obtención dinámica de tablas)
- **`consolidation_tracking_manager.py`** - Tracking por tabla×compañía
- **`build_deploy.sh`** - Script de deploy del Cloud Run Job
- **`Dockerfile`** - Imagen Docker para el Job
- **`requirements.txt`** - Dependencias Python
- **`generate_silver_views_notebook.ipynb`** - Notebook para pruebas en BigQuery

---

## 🚀 Uso

### Opción 1: Cloud Run Job (Producción)

```bash
# 1. Deploy del Job
./build_deploy.sh

# 2. Ejecutar
gcloud run jobs execute generate-silver-views-job --region=us-east1 --project=platform-partners-des
```

### Opción 2: Ejecución Manual (Desarrollo/Pruebas)

```bash
# Modo normal (con confirmación)
python generate_silver_views.py

# Modo forzado (sin confirmación)
python generate_silver_views.py --force

# Desde letra específica (reiniciar después de timeout)
python generate_silver_views.py --force m
```

### Opción 3: BigQuery Notebook (Testing)

Abre `generate_silver_views_notebook.ipynb` en BigQuery y ejecuta las celdas.

---

## ⚙️ Funcionalidades

### 1. Detección Dinámica de Tablas
```python
# ✅ NO hardcoded - consulta INFORMATION_SCHEMA
all_tables = get_tables_dynamically()
```

**Beneficios:**
- Se adapta automáticamente a nuevas tablas de ServiceTitan
- Detecta cambios en Fivetran
- Sin mantenimiento manual

### 2. Normalización de Tipos de Datos

**Regla simple:** Cualquier conflicto de tipos → `STRING`

**Ejemplo:**
- 29 compañías: `created_at` es `TIMESTAMP`
- 1 compañía: `created_at` es `INT64`
- **Resultado:** TODAS usan `STRING`

### 3. Layout Consistente

- Omite campos `_fivetran*`
- Campos faltantes: `CAST(NULL AS tipo)`
- **Orden alfabético** (crítico para UNION ALL)

### 4. Tracking Granular

Tabla `companies_consolidated` registra:
- Estado por tabla × compañía
- 0 = No existe
- 1 = Éxito
- 2 = Error

---

## 🔧 Configuración

### Filtro de Reinicio (para timeouts)

Edita `main.py` línea 20:
```python
start_from_letter='m'  # Cambiar según necesites
```

O en ejecución manual:
```bash
python generate_silver_views.py --force m
```

### Proyectos

- **Source:** `platform-partners-des` (compañías y tracking)
- **Companies:** 30+ proyectos individuales

---

## 📊 Resultado

**Vistas creadas:**
```
{company_project_id}.silver.vw_{table_name}
```

**Ejemplo:**
```
shape-mhs-1.silver.vw_appointment
shape-chc-2.silver.vw_appointment
...
```

**Características:**
- ✅ Tipos normalizados (STRING para conflictos)
- ✅ Campos en orden alfabético
- ✅ Layout consistente entre compañías
- ✅ Sin campos `_fivetran`

---

## 🐛 Troubleshooting

### Job se cae por timeout

**Solución:** Reiniciar desde donde quedó

1. Ver en logs la última tabla procesada (ej: `location`)
2. Editar `main.py`: `start_from_letter='m'`
3. Re-deploy y ejecutar

### Error de tipos en UNION ALL

**Causa:** Vistas creadas antes de las correcciones

**Solución:** Re-ejecutar con `--force` para recrear todas

### Import errors

**Causa:** `config.py` o `consolidation_tracking_manager.py` no encontrados

**Solución:** Verificar que ambos archivos estén en el mismo directorio

---

**Última actualización:** 2025-10-09  
**Versión:** 3.0 (Arquitectura simplificada)

