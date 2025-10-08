# 🚀 Cloud Run Job: Crear Tablas Consolidadas

## 📋 Descripción

Este Cloud Run Job crea **tablas consolidadas optimizadas** en `pph-central.bronze` a partir de las vistas Silver de cada compañía.

## 🎯 Características

- ✅ **Particionamiento por MES** (`DATE_TRUNC(created_on, MONTH)`) - Evita límite de 4000 particiones
- ✅ **Clusterizado** según metadatos de `management.metadata_consolidated_tables`
- ✅ **No interactivo** - Procesa todas las tablas automáticamente
- ✅ **Timeout de 1 hora** - Suficiente para procesar todas las tablas
- ✅ **8GB RAM + 4 CPUs** - Recursos suficientes para queries grandes

## 📊 Flujo del Job

```
1. Cargar metadatos (partition_fields, cluster_fields)
   ↓
2. Obtener tablas con vistas Silver disponibles
   ↓
3. Para cada tabla:
   - Obtener compañías que tienen la vista
   - Construir UNION ALL de todas las vistas
   - Crear tabla con PARTITION BY y CLUSTER BY
   ↓
4. Resumen final (éxitos, errores, saltadas)
```

## 🔧 Configuración

### Proyecto y Datasets

- **Proyecto Central:** `pph-central`
- **Dataset Bronze:** `bronze` (tablas consolidadas)
- **Proyecto Source:** `platform-partners-pro` (metadatos y companies)
- **Dataset Silver:** `silver` (vistas por compañía)

### Service Account

```
data-analytics@platform-partners-pro.iam.gserviceaccount.com
```

**Permisos requeridos:**
- ✅ `bigquery.dataViewer` en proyectos de compañías
- ✅ `bigquery.dataEditor` en `pph-central` (para crear tablas)
- ✅ `bigquery.jobUser` en todos los proyectos

### Recursos del Job

- **Memoria:** 8 GB
- **CPU:** 4 cores
- **Timeout:** 3600 segundos (1 hora)
- **Max Retries:** 3

## 🚀 Despliegue

### Paso 1: Build y Deploy

```bash
cd bq_management/consolidated_central_project_data
chmod +x build_deploy_consolidated.sh
./build_deploy_consolidated.sh
```

Este script:
1. Crea imagen Docker con `consolidated_tables_job.py`
2. Sube la imagen a `gcr.io/pph-central/create-consolidated-tables-job`
3. Crea/actualiza el Cloud Run Job

### Paso 2: Ejecutar el Job

```bash
gcloud run jobs execute create-consolidated-tables-job \
  --region=us-east1 \
  --project=pph-central
```

### Paso 3: Monitorear Logs

```bash
# Ver logs en tiempo real
gcloud run jobs logs create-consolidated-tables-job \
  --region=us-east1 \
  --project=pph-central

# Ver logs en Cloud Console
# https://console.cloud.google.com/run/jobs/details/us-east1/create-consolidated-tables-job
```

## 📊 Estructura de Tablas Consolidadas

### Naming Convention

```
pph-central.bronze.consolidated_{table_name}
```

Ejemplos:
- `consolidated_appointment`
- `consolidated_customer`
- `consolidated_invoice`

### Schema

Todas las tablas consolidadas tienen estos campos adicionales:

```sql
company_project_id STRING   -- Proyecto de la compañía (ej: "shape-mhs-1")
company_id INT64            -- ID de la compañía (ej: 1)
... [campos originales de la tabla] ...
```

### Particionamiento

```sql
PARTITION BY DATE_TRUNC(created_on, MONTH)
```

**¿Por qué MES y no DÍA?**
- Límite de BigQuery: 4000 particiones máximo
- Por día: 4000 días = ~11 años
- Por mes: 4000 meses = ~333 años ✅

### Clusterizado

Según metadatos en `management.metadata_consolidated_tables`:

```sql
CLUSTER BY company_id, [otros campos según tabla]
```

**Beneficios:**
- ✅ Queries más rápidos al filtrar por compañía
- ✅ Menor costo de queries
- ✅ Mejor organización de datos

## 🔍 Validación

### Verificar que las tablas se crearon

```sql
SELECT 
  table_name,
  row_count,
  size_bytes,
  ROUND(size_bytes / POW(1024, 3), 2) as size_gb,
  creation_time
FROM `pph-central.bronze.__TABLES__`
WHERE table_id LIKE 'consolidated_%'
ORDER BY creation_time DESC;
```

### Ver distribución de datos por compañía

```sql
SELECT 
  company_project_id,
  company_id,
  COUNT(*) as total_records
FROM `pph-central.bronze.consolidated_appointment`
GROUP BY company_project_id, company_id
ORDER BY total_records DESC;
```

### Verificar particionamiento

```sql
SELECT 
  partition_id,
  total_rows,
  ROUND(total_logical_bytes / POW(1024, 3), 2) as size_gb
FROM `pph-central.bronze.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'consolidated_appointment'
ORDER BY partition_id DESC
LIMIT 10;
```

## ⚠️ Troubleshooting

### Error: Too many partitions

**Problema:** Más de 4000 días de datos

**Solución:** El Job ya usa `DATE_TRUNC(created_on, MONTH)`. Si aún falla:
1. Cambiar a `DATE_TRUNC(created_on, YEAR)`
2. O filtrar datos históricos

### Error: Timeout

**Problema:** El Job no termina en 1 hora

**Solución:**
1. Aumentar `--task-timeout` a 7200 (2 horas)
2. O procesar tablas en lotes

### Error: Out of Memory

**Problema:** Query muy grande para 8GB

**Solución:**
1. Aumentar `--memory` a 16Gi
2. Reducir número de compañías por lote

### Error: Permission Denied

**Problema:** Service Account sin permisos

**Solución:**
```bash
# Para cada proyecto de compañía
gcloud projects add-iam-policy-binding COMPANY_PROJECT_ID \
  --member="serviceAccount:data-analytics@platform-partners-pro.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

## 📝 Logs de Ejemplo

### Éxito

```
🚀 CLOUD RUN JOB - CREAR TABLAS CONSOLIDADAS
================================================================================
✅ Metadatos cargados: 24 tablas
✅ Tablas disponibles: 24

[1/24] appointment
  🔄 Creando tabla: consolidated_appointment
     📊 Compañías: 30
     ⚙️  Particionado: created_on (por MES)
     🔗 Clusterizado: ['company_id']
  ✅ Tabla creada exitosamente

...

================================================================================
🎯 RESUMEN FINAL
================================================================================
✅ Tablas creadas exitosamente: 24
❌ Tablas con errores: 0
⏭️  Tablas saltadas: 0
📊 Total procesadas: 24
```

### Con Errores

```
[5/24] customer
  🔄 Creando tabla: consolidated_customer
     📊 Compañías: 28
  ❌ Error: Column 'email' type mismatch...

================================================================================
🎯 RESUMEN FINAL
================================================================================
✅ Tablas creadas exitosamente: 23
❌ Tablas con errores: 1
⏭️  Tablas saltadas: 0
📊 Total procesadas: 24
⚠️  Algunas tablas tuvieron errores. Revisa los logs arriba.
```

## 🔄 Re-ejecución

El Job usa `CREATE OR REPLACE TABLE`, por lo que:
- ✅ Es seguro re-ejecutar
- ✅ Sobrescribe tablas existentes
- ✅ No duplica datos
- ⚠️ Destruye datos antiguos (si cambiaste la lógica)

## 📅 Mantenimiento

### Actualizar el Job

```bash
# 1. Modificar consolidated_tables_job.py
# 2. Re-deployar
./build_deploy_consolidated.sh
```

### Eliminar el Job

```bash
gcloud run jobs delete create-consolidated-tables-job \
  --region=us-east1 \
  --project=pph-central
```

### Ver historial de ejecuciones

```bash
gcloud run jobs executions list \
  --job=create-consolidated-tables-job \
  --region=us-east1 \
  --project=pph-central
```

## 🎯 Próximos Pasos

Después de crear las tablas consolidadas en Bronze:

1. **Paso 4:** Crear vistas consolidadas en `pph-central.silver`
2. **Paso 5:** Configurar permisos para usuarios finales
3. **Paso 6:** Crear dashboards en Looker/Tableau

---

**Fecha de creación:** 2025-10-08  
**Versión:** 1.0  
**Autor:** Data Engineering Team

