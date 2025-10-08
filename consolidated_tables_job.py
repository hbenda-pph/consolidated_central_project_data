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
PROJECT_CENTRAL = 'platform-partners-qua'
PROJECT_SOURCE = 'pph-central'
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
        FROM `{PROJECT_SOURCE}.{DATASET_MANAGEMENT}.metadata_consolidated_tables`
        WHERE is_active = TRUE
        ORDER BY table_name
    """
    
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
    query = f"""
        SELECT DISTINCT
            REPLACE(table_name, 'vw_', '') as table_name,
            COUNT(DISTINCT table_schema) as company_count
        FROM `{PROJECT_SOURCE}.region-us.INFORMATION_SCHEMA.TABLES`
        WHERE table_schema LIKE 'silver%'
          AND table_name LIKE 'vw_%'
        GROUP BY table_name
        HAVING company_count >= 1
        ORDER BY table_name
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = pd.DataFrame([dict(row) for row in results])
        
        tables = df['table_name'].tolist()
        print(f"✅ Tablas disponibles: {len(tables)}")
        return tables
    except Exception as e:
        print(f"❌ Error obteniendo tablas: {str(e)}")
        return []

def get_companies_for_table(table_name):
    """Obtiene compañías que tienen una vista Silver específica"""
    query = f"""
        SELECT DISTINCT
            c.company_id,
            c.company_name,
            c.company_project_id
        FROM `{PROJECT_SOURCE}.settings.companies` c
        WHERE c.company_fivetran_status = TRUE
          AND c.company_bigquery_status = TRUE
          AND c.company_project_id IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM `{PROJECT_SOURCE}.region-us.INFORMATION_SCHEMA.TABLES` t
              WHERE t.table_schema = CONCAT('silver_', REPLACE(c.company_project_id, '-', '_'))
                AND t.table_name = CONCAT('vw_', '{table_name}')
          )
        ORDER BY c.company_id
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = pd.DataFrame([dict(row) for row in results])
        return df
    except Exception as e:
        print(f"  ⚠️  Error obteniendo compañías: {str(e)}")
        return pd.DataFrame()

def create_consolidated_table(table_name, companies_df, metadata_dict):
    """Crea una tabla consolidada para una tabla específica"""
    
    if companies_df.empty:
        print(f"  ❌ No hay compañías disponibles")
        return False
    
    # Obtener metadatos
    if table_name in metadata_dict:
        metadata = metadata_dict[table_name]
        partition_field = metadata['partition_fields'][0] if metadata['partition_fields'] else 'created_on'
        cluster_fields = metadata['cluster_fields'] if metadata['cluster_fields'] else ['company_id']
    else:
        partition_field = 'created_on'
        cluster_fields = ['company_id']
    
    # Construir UNION ALL
    union_parts = []
    for _, company in companies_df.iterrows():
        union_part = f"""
        SELECT 
          '{company['company_project_id']}' AS company_project_id,
          {company['company_id']} AS company_id,
          *
        FROM `{company['company_project_id']}.{DATASET_SILVER}.vw_{table_name}`"""
        union_parts.append(union_part)
    
    # Configurar clusterizado
    cluster_sql = f"CLUSTER BY {', '.join(cluster_fields)}" if cluster_fields else ""
    
    # SQL completo - PARTICIONADO POR MES para evitar límite de 4000 particiones
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

