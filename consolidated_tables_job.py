#!/usr/bin/env python3
"""
Cloud Run Job: Crear Tablas Consolidadas en pph-central.bronze
Versión NO INTERACTIVA - Procesa todas las tablas disponibles
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import sys

# Configuración
PROJECT_SOURCE = 'platform-partners-des'
PROJECT_CENTRAL = 'pph-central'
DATASET_BRONZE = 'bronze'
DATASET_SILVER = 'silver'
DATASET_MANAGEMENT = 'management'

# Cliente BigQuery
client = bigquery.Client(project=PROJECT_CENTRAL)

def get_metadata_dict():
    """Obtiene metadatos de particionamiento y clusterizado"""
    query = f"""
        SELECT 
            table_name,
            partition_fields,
            cluster_fields
        FROM `{PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables`
        WHERE is_active = TRUE
        ORDER BY table_name
    """
    
    print(f"📋 Cargando metadatos desde: {PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables")
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = pd.DataFrame([dict(row) for row in results])
        
        metadata_dict = {}
        for _, row in df.iterrows():
            metadata_dict[row['table_name']] = {
                'partition_fields': row['partition_fields'],
                'cluster_fields': row['cluster_fields']
            }
        
        print(f"✅ Metadatos cargados: {len(metadata_dict)} tablas")
        return metadata_dict
    except Exception as e:
        print(f"⚠️  Error cargando metadatos: {str(e)}")
        print("   Usando configuración por defecto para todas las tablas")
        return {}

def get_available_tables():
    """Obtiene lista de tablas que tienen vistas Silver disponibles"""
    # Obtener lista de compañías activas
    companies_query = f"""
        SELECT DISTINCT company_project_id
        FROM `{PROJECT_SOURCE}.settings.companies`
        WHERE company_fivetran_status = TRUE
          AND company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
    """
    
    try:
        # Obtener compañías
        query_job = client.query(companies_query)
        results = query_job.result()
        companies = [row.company_project_id for row in results]
        
        if not companies:
            print("❌ No se encontraron compañías activas")
            return []
        
        print(f"📊 Analizando {len(companies)} compañías...")
        
        # Recopilar todas las tablas únicas de todas las compañías
        all_tables = set()
        
        for company_project_id in companies:
            try:
                # Buscar vistas Silver en esta compañía
                tables_query = f"""
                    SELECT DISTINCT REPLACE(table_name, 'vw_', '') as table_name
                    FROM `{company_project_id}.silver.INFORMATION_SCHEMA.TABLES`
                    WHERE table_name LIKE 'vw_%'
                """
                
                query_job = client.query(tables_query)
                results = query_job.result()
                company_tables = [row.table_name for row in results]
                all_tables.update(company_tables)
                
            except Exception as e:
                # Saltar compañías con errores (ej: sin dataset silver)
                continue
        
        tables = sorted(list(all_tables))
        print(f"✅ Tablas disponibles: {len(tables)}")
        return tables
        
    except Exception as e:
        print(f"❌ Error obteniendo tablas: {str(e)}")
        return []

def get_companies_for_table(table_name):
    """Obtiene compañías que tienen una vista Silver específica"""
    query = f"""
        SELECT 
            company_id,
            company_name,
            company_project_id
        FROM `{PROJECT_SOURCE}.settings.companies`
        WHERE company_fivetran_status = TRUE
          AND company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        all_companies = pd.DataFrame([dict(row) for row in results])
        
        # Filtrar compañías que tienen la vista
        companies_with_view = []
        
        for _, company in all_companies.iterrows():
            try:
                # Verificar si la vista existe en esta compañía
                check_query = f"""
                    SELECT 1
                    FROM `{company['company_project_id']}.silver.INFORMATION_SCHEMA.TABLES`
                    WHERE table_name = 'vw_{table_name}'
                    LIMIT 1
                """
                
                check_job = client.query(check_query)
                check_results = list(check_job.result())
                
                if check_results:
                    companies_with_view.append(company)
                    
            except Exception:
                # Saltar compañías sin la vista
                continue
        
        df = pd.DataFrame(companies_with_view)
        return df
        
    except Exception as e:
        print(f"  ⚠️  Error obteniendo compañías: {str(e)}")
        return pd.DataFrame()

def detect_partition_field(table_name, sample_company_project_id):
    """Detecta un campo de fecha apropiado para particionar"""
    # Lista de campos comunes de fecha (en orden de preferencia)
    date_fields = ['created_on', 'created_at', 'date', 'timestamp', 'modified_on', 'updated_on']
    
    try:
        # Obtener schema de la vista
        schema_query = f"""
            SELECT column_name, data_type
            FROM `{sample_company_project_id}.silver.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'vw_{table_name}'
              AND data_type IN ('TIMESTAMP', 'DATETIME', 'DATE')
            ORDER BY ordinal_position
        """
        
        query_job = client.query(schema_query)
        results = query_job.result()
        date_columns = [row.column_name for row in results]
        
        # Buscar el primer campo de fecha común
        for field in date_fields:
            if field in date_columns:
                return field
        
        # Si no encontró ninguno común, usar el primero disponible
        if date_columns:
            return date_columns[0]
        
        # Sin campos de fecha
        return None
        
    except Exception as e:
        print(f"    ⚠️  No se pudo detectar campo de fecha: {str(e)}")
        return None

def create_consolidated_table(table_name, companies_df, metadata_dict):
    """Crea una tabla consolidada para una tabla específica"""
    
    if companies_df.empty:
        print(f"  ❌ No hay compañías disponibles")
        return False
    
    # Obtener metadatos
    if table_name in metadata_dict:
        metadata = metadata_dict[table_name]
        partition_field = metadata['partition_fields'][0] if metadata['partition_fields'] else None
        cluster_fields = metadata['cluster_fields'] if metadata['cluster_fields'] else ['company_id']
    else:
        print(f"  ⚠️  Tabla '{table_name}' NO está en metadatos")
        partition_field = None
        cluster_fields = ['company_id']
    
    # Si no hay partition_field, intentar detectarlo
    if not partition_field:
        print(f"  🔍 Detectando campo de particionamiento automáticamente...")
        partition_field = detect_partition_field(table_name, companies_df.iloc[0]['company_project_id'])
        
        if not partition_field:
            print(f"  ❌ ERROR: No se encontró campo de fecha para particionar")
            print(f"     Solución: Agregar tabla '{table_name}' a metadata_consolidated_tables")
            print(f"     con un partition_field apropiado (created_on, created_at, etc.)")
            return False
    
    # Construir UNION ALL
    # 🚧 TEMPORAL: Las vistas Silver actualmente YA incluyen company_project_id y company_id (INCORRECTO)
    # Por ahora, usamos EXCEPT para evitar duplicados
    # 
    # 🔮 FUTURO: Cuando se corrija el Paso 2 (generate_silver_views) para NO incluir
    # estos campos metadata en las vistas individuales, descomentar esta versión:
    #
    # union_part = f"""
    #     SELECT 
    #       '{company['company_project_id']}' AS company_project_id,
    #       {company['company_id']} AS company_id,
    #       *
    #     FROM `{company['company_project_id']}.{DATASET_SILVER}.vw_{table_name}`"""
    #
    # RAZÓN: Los campos company_project_id y company_id son METADATA de consolidación,
    # NO deberían estar en vistas individuales de cada compañía (solo en la consolidada)
    
    union_parts = []
    for _, company in companies_df.iterrows():
        union_part = f"""
        SELECT 
          '{company['company_project_id']}' AS company_project_id,
          {company['company_id']} AS company_id,
          * EXCEPT(company_project_id, company_id)
        FROM `{company['company_project_id']}.{DATASET_SILVER}.vw_{table_name}`"""
        union_parts.append(union_part)
    
    # Configurar clusterizado
    cluster_sql = f"CLUSTER BY {', '.join(cluster_fields)}" if cluster_fields else ""
    
    # SQL completo - SIEMPRE con particionamiento por MES
    create_sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_CENTRAL}.{DATASET_BRONZE}.consolidated_{table_name}`
    PARTITION BY DATE_TRUNC({partition_field}, MONTH)
    {cluster_sql}
    AS
    {' UNION ALL '.join(union_parts)}
    """
    
    print(f"  🔄 Creando tabla: consolidated_{table_name}")
    print(f"     📊 Compañías: {len(companies_df)}")
    print(f"     ⚙️  Particionado: {partition_field} (por MES)")
    print(f"     🔗 Clusterizado: {cluster_fields}")
    
    try:
        query_job = client.query(create_sql)
        query_job.result()
        print(f"  ✅ Tabla creada exitosamente")
        return True
    except Exception as e:
        error_msg = str(e)
        # Truncar mensaje de error si es muy largo
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "..."
        print(f"  ❌ Error: {error_msg}")
        return False

def main():
    """Función principal del Job"""
    print("=" * 80)
    print("🚀 CLOUD RUN JOB - CREAR TABLAS CONSOLIDADAS")
    print(f"   Proyecto Central: {PROJECT_CENTRAL}")
    print(f"   Dataset Bronze: {DATASET_BRONZE}")
    print(f"   Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Cargar metadatos
    print("\n📋 PASO 1: Cargar metadatos de particionamiento/clusterizado")
    metadata_dict = get_metadata_dict()
    
    # 2. Obtener tablas disponibles
    print("\n📊 PASO 2: Obtener tablas con vistas Silver disponibles")
    available_tables = get_available_tables()
    
    if not available_tables:
        print("❌ No se encontraron tablas disponibles")
        sys.exit(1)
    
    print(f"\n📋 Tablas a procesar: {len(available_tables)}")
    
    # 3. Procesar cada tabla
    print("\n🔄 PASO 3: Crear tablas consolidadas")
    print("=" * 80)
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    for i, table_name in enumerate(available_tables, 1):
        print(f"\n[{i}/{len(available_tables)}] {table_name}")
        
        # Obtener compañías para esta tabla
        companies_df = get_companies_for_table(table_name)
        
        if companies_df.empty:
            print(f"  ⚠️  Sin compañías - SALTAR")
            skipped_count += 1
            continue
        
        # Crear tabla consolidada
        success = create_consolidated_table(table_name, companies_df, metadata_dict)
        
        if success:
            success_count += 1
        else:
            error_count += 1
    
    # 4. Resumen final
    print("\n" + "=" * 80)
    print("🎯 RESUMEN FINAL")
    print("=" * 80)
    print(f"✅ Tablas creadas exitosamente: {success_count}")
    print(f"❌ Tablas con errores: {error_count}")
    print(f"⏭️  Tablas saltadas: {skipped_count}")
    print(f"📊 Total procesadas: {success_count + error_count + skipped_count}")
    print(f"⏱️  Fecha fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    if error_count > 0:
        print("\n⚠️  Algunas tablas tuvieron errores. Revisa los logs arriba.")
        sys.exit(1)
    else:
        print("\n✅ ¡Todas las tablas se crearon exitosamente!")
        sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        sys.exit(1)

