#!/usr/bin/env python3
"""
Cloud Run Job: Crear Tablas Consolidadas en pph-central.bronze
Versión NO INTERACTIVA - Procesa todas las tablas disponibles
"""

from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1
import pandas as pd
from datetime import datetime
import sys
import json

# Configuración
PROJECT_CENTRAL = 'pph-central'            # Proyecto central único
DATASET_SETTINGS = 'settings'              # Configuración de compañías
DATASET_BRONZE = 'bronze'                  # Tablas consolidadas destino
DATASET_SILVER = 'silver'                  # Vistas Silver (en proyectos shape-*)
DATASET_MANAGEMENT = 'management'          # Metadatos y gobierno

# Clientes BigQuery
client = bigquery.Client(project=PROJECT_CENTRAL)
transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient()

def get_metadata_dict():
    """Obtiene metadatos de particionamiento y clusterizado"""
    query = f"""
        SELECT 
            table_name,
            partition_fields,
            cluster_fields
        FROM `{PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables`
        ORDER BY table_name
    """
    
    print(f"📋 Cargando metadatos desde: {PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables")
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = pd.DataFrame([dict(row) for row in results])
        
        print(f"🔍 DEBUG: Filas obtenidas de metadatos: {len(df)}")
        
        if len(df) > 0:
            print(f"📋 Primeras 5 tablas en metadatos:")
            for i, row in df.head(5).iterrows():
                print(f"   - {row['table_name']}: partition={row['partition_fields']}, cluster={row['cluster_fields']}")
        
        metadata_dict = {}
        for _, row in df.iterrows():
            metadata_dict[row['table_name']] = {
                'partition_fields': row['partition_fields'],
                'cluster_fields': row['cluster_fields']
            }
        
        print(f"✅ Metadatos cargados: {len(metadata_dict)} tablas")
        print(f"📋 Tablas en diccionario: {list(metadata_dict.keys())[:10]}")
        return metadata_dict
    except Exception as e:
        print(f"⚠️  Error cargando metadatos: {str(e)}")
        print("   Usando configuración por defecto para todas las tablas")
        return {}

def get_available_tables():
    """
    Obtiene lista de tablas desde METADATOS (no desde vistas Silver)
    Los metadatos son la GUÍA del proceso
    """
    query = f"""
        SELECT table_name
        FROM `{PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables`
        ORDER BY table_name
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        tables = [row.table_name for row in results]
        
        print(f"✅ Tablas desde metadatos: {len(tables)}")
        return tables
        
    except Exception as e:
        print(f"❌ Error obteniendo tablas desde metadatos: {str(e)}")
        return []

def get_companies_for_table(table_name):
    """Obtiene compañías que tienen una vista Silver específica"""
    query = f"""
        SELECT 
            company_id,
            company_name,
            company_project_id
        FROM `{PROJECT_CENTRAL}.{DATASET_SETTINGS}.companies`
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

def verify_field_exists(table_name, field_name, company_project_id):
    """Verifica si un campo existe en una vista Silver"""
    try:
        query = f"""
            SELECT 1
            FROM `{company_project_id}.silver.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'vw_{table_name}'
              AND column_name = '{field_name}'
            LIMIT 1
        """
        
        result = client.query(query).result()
        return len(list(result)) > 0
    except Exception:
        return False

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
        return False, None, []
    
    # Obtener metadatos
    if table_name in metadata_dict:
        metadata = metadata_dict[table_name]
        print(f"  🔍 DEBUG: Metadatos encontrados para '{table_name}'")
        print(f"     partition_fields: {metadata['partition_fields']} (tipo: {type(metadata['partition_fields'])})")
        print(f"     cluster_fields: {metadata['cluster_fields']} (tipo: {type(metadata['cluster_fields'])})")
        
        # Manejar arrays de BigQuery
        partition_fields_list = list(metadata['partition_fields']) if metadata['partition_fields'] else []
        cluster_fields_list = list(metadata['cluster_fields']) if metadata['cluster_fields'] else []
        
        # PARTICIONAMIENTO: Intentar campos en orden (tipo COALESCE)
        # Solo se usa 1 campo, pero se prueban varios por si el primero no existe
        partition_field = None
        if partition_fields_list:
            for field in partition_fields_list:
                # Verificar si el campo existe en la vista
                if verify_field_exists(table_name, field, companies_df.iloc[0]['company_project_id']):
                    partition_field = field
                    print(f"  ✅ Campo de particionamiento seleccionado: {field}")
                    break
                else:
                    print(f"  ⚠️  Campo '{field}' no existe en la vista, probando siguiente...")
        
        # CLUSTERIZADO: Usar todos los campos (hasta 4)
        cluster_fields = cluster_fields_list[:4] if cluster_fields_list else ['company_id']
    else:
        print(f"  ⚠️  Tabla '{table_name}' NO está en metadatos")
        partition_field = None
        cluster_fields = ['company_id']
    
    # Si no hay partition_field, intentar detectarlo automáticamente
    if not partition_field:
        print(f"  🔍 Detectando campo de particionamiento automáticamente...")
        partition_field = detect_partition_field(table_name, companies_df.iloc[0]['company_project_id'])
        
        if not partition_field:
            print(f"  ❌ ERROR: No se encontró campo de fecha para particionar")
            print(f"     Solución: Agregar tabla '{table_name}' a metadata_consolidated_tables")
            print(f"     con un partition_field apropiado (created_on, created_at, etc.)")
            return False, None, []
    
    # Construir UNION ALL con metadata de compañía
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
        return True, partition_field, cluster_fields
    except Exception as e:
        error_msg = str(e)
        
        # Detectar tipo de error específico
        if "Cannot replace a table with a different" in error_msg:
            print(f"  ⚠️  TABLA NO ACTUALIZADA: La tabla existente tiene configuración incompatible")
            print(f"     Razón: Cambio de esquema de particionamiento (día→mes) o estructura")
            print(f"     Acción: La tabla antigua se mantiene intacta (seguro)")
            print(f"     Para actualizar: Eliminar manualmente cuando no haya dependencias")
        elif "Unrecognized name" in error_msg:
            campo_error = error_msg.split("Unrecognized name: ")[1].split(" ")[0] if "Unrecognized name: " in error_msg else "desconocido"
            print(f"  ❌ ERROR: Campo '{campo_error}' no existe en las vistas Silver")
            print(f"     Solución: Verificar partition_fields en metadatos para '{table_name}'")
        elif "Too many partitions" in error_msg:
            print(f"  ❌ ERROR: Demasiadas particiones (límite: 4000)")
            print(f"     Solución: Cambiar particionamiento a YEAR o filtrar datos históricos")
        else:
            # Error genérico - mostrar primeras 300 caracteres
            if len(error_msg) > 300:
                error_msg = error_msg[:300] + "..."
            print(f"  ❌ ERROR: {error_msg}")
        
        return False, None, []

def create_or_update_scheduled_query(table_name, companies_df, partition_field, cluster_fields):
    """
    Crea o actualiza un Scheduled Query para refresh diario de la tabla consolidada
    
    NOTA: Esta función crea la configuración del scheduled query.
    Para que funcione completamente, se requiere:
    1. Habilitar BigQuery Data Transfer API
    2. Permisos del Service Account en todos los proyectos de compañías
    """
    display_name = f"sq_consolidated_{table_name}"
    
    # Construir query de refresh con MERGE (atómico y seguro)
    # Clave compuesta: company_project_id + id (único en tabla consolidada)
    # Sin filtro temporal - procesa TODAS las vistas para capturar cualquier actualización
    
    union_parts = []
    for _, company in companies_df.iterrows():
        union_part = f"""
        SELECT 
          '{company['company_project_id']}' AS company_project_id,
          {company['company_id']} AS company_id,
          *
        FROM `{company['company_project_id']}.{DATASET_SILVER}.vw_{table_name}`"""
        union_parts.append(union_part)
    
    refresh_sql = f"""
/*
 * Refresh completo para {table_name}
 * Recrea la tabla completa desde las vistas Silver
 * Mantiene particionamiento y clusterizado originales
 * Generado automáticamente
 */
CREATE OR REPLACE TABLE `{PROJECT_CENTRAL}.{DATASET_BRONZE}.consolidated_{table_name}`
PARTITION BY DATE_TRUNC({partition_field}, MONTH)
CLUSTER BY ({', '.join(cluster_fields)})
AS
{' UNION ALL '.join(union_parts)};
"""
    
    try:
        parent = f"projects/{PROJECT_CENTRAL}/locations/us"
        
        # Configuración del scheduled query
        transfer_config = bigquery_datatransfer_v1.TransferConfig(
            display_name=display_name,
            data_source_id="scheduled_query",
            schedule="every 6 hours",  # Corre cada 6 horas (aligned con Fivetran)
            disabled=True,  # Crear DESHABILITADO para sincronización perfecta
            params={
                "query": refresh_sql
                # No se especifica write_disposition porque CREATE OR REPLACE maneja el reemplazo
            }
        )
        
        # Buscar si ya existe
        list_request = bigquery_datatransfer_v1.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"]
        )
        
        existing_config = None
        for config in transfer_client.list_transfer_configs(request=list_request):
            if config.display_name == display_name:
                existing_config = config
                break
        
        if existing_config:
            # Actualizar existente
            transfer_config.name = existing_config.name
            update_mask = {"paths": ["schedule", "params"]}
            transfer_client.update_transfer_config(
                transfer_config=transfer_config,
                update_mask=update_mask
            )
            print(f"  🔄 Scheduled Query actualizado")
        else:
            # Crear nuevo
            transfer_client.create_transfer_config(
                parent=parent,
                transfer_config=transfer_config
            )
            print(f"  ✅ Scheduled Query creado")
        
        return True
        
    except Exception as e:
        error_str = str(e)
        if "API has not been used" in error_str or "not been enabled" in error_str:
            print(f"  ⚠️  BigQuery Data Transfer API no habilitada")
            print(f"     Habilitar: gcloud services enable bigquerydatatransfer.googleapis.com")
        else:
            print(f"  ⚠️  Error creando Scheduled Query: {error_str[:250]}")
        
        print(f"     Tabla creada OK - Scheduled Query se puede crear manualmente después")
        return False

def create_all_consolidated_tables(create_schedules=True, start_from_letter='a', specific_table=None):
    """
    Función principal para crear tablas consolidadas
    
    Args:
        create_schedules (bool): Si True, crea scheduled queries para refresh automático
        start_from_letter (str): Letra inicial para filtrar tablas (útil para reiniciar)
        specific_table (str): Si se proporciona, genera solo esta tabla
        
    Returns:
        dict: Estadísticas de ejecución
    """
    print("=" * 80)
    print("🚀 CREAR TABLAS CONSOLIDADAS")
    print(f"   Proyecto Central: {PROJECT_CENTRAL}")
    print(f"   Dataset Bronze: {DATASET_BRONZE}")
    print(f"   Scheduled Queries: {'SÍ' if create_schedules else 'NO'}")
    print(f"   Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. Cargar metadatos
    print("\n📋 PASO 1: Cargar metadatos de particionamiento/clusterizado")
    metadata_dict = get_metadata_dict()
    
    # 2. Obtener tablas disponibles
    print("\n📊 PASO 2: Obtener tablas con vistas Silver disponibles")
    all_tables = get_available_tables()
    
    if not all_tables:
        print("❌ No se encontraron tablas disponibles")
        sys.exit(1)
    
    # Filtrar tablas según los parámetros
    if specific_table:
        # Procesar solo una tabla específica
        if specific_table in all_tables:
            available_tables = [specific_table]
            print(f"🎯 TABLA ESPECÍFICA: Procesando solo '{specific_table}'")
        else:
            print(f"❌ ERROR: La tabla '{specific_table}' no existe")
            sys.exit(1)
    else:
        # Aplicar filtro de letra inicial
        available_tables = [t for t in all_tables if t >= start_from_letter]
        
        if start_from_letter != 'a':
            print(f"🔍 FILTRO ACTIVO: Procesando tablas desde '{start_from_letter}'")
        
        print(f"📋 Tablas a procesar: {len(available_tables)} de {len(all_tables)} totales")
    
    # 3. Procesar cada tabla
    print("\n🔄 PASO 3: Crear tablas consolidadas")
    print("=" * 80)
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    incompatible_count = 0
    error_tables = []
    incompatible_tables = []
    
    for i, table_name in enumerate(available_tables, 1):
        print(f"\n[{i}/{len(available_tables)}] {table_name}")
        
        # Obtener compañías para esta tabla
        companies_df = get_companies_for_table(table_name)
        
        if companies_df.empty:
            print(f"  ⚠️  Sin compañías - SALTAR")
            skipped_count += 1
            continue
        
        # Crear tabla consolidada (retorna éxito, partition_field, cluster_fields)
        table_created, partition_field, cluster_fields = create_consolidated_table(table_name, companies_df, metadata_dict)
        
        if table_created:
            success_count += 1
            
            # Crear scheduled query solo si hay partition_field y está habilitado
            if create_schedules and partition_field:
                print(f"  📅 Configurando refresh automático...")
                create_or_update_scheduled_query(table_name, companies_df, partition_field, cluster_fields)
            elif not partition_field:
                print(f"  ⚠️  Sin partition_field - No se crea scheduled query")
            elif not create_schedules:
                print(f"  ⏭️  Scheduled queries deshabilitados")
        else:
            error_count += 1
            error_tables.append(table_name)
    
    # 4. Resumen final
    print("\n" + "=" * 80)
    print("🎯 RESUMEN FINAL")
    print("=" * 80)
    print(f"✅ Tablas creadas/actualizadas: {success_count}")
    print(f"❌ Tablas con errores: {error_count}")
    print(f"⏭️  Tablas saltadas (sin compañías): {skipped_count}")
    print(f"📊 Total procesadas: {success_count + error_count + skipped_count}")
    print(f"⏱️  Fecha fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    if error_tables:
        print(f"\n⚠️  TABLAS CON ERRORES ({len(error_tables)}):")
        for table in error_tables:
            print(f"   - {table}")
        print(f"\n💡 NOTA: Revisa los logs arriba para detalles de cada error")
        print(f"   Las tablas con 'configuración incompatible' mantienen su versión anterior")
    
    # Instrucciones para Scheduled Queries
    if create_schedules and success_count > 0:
        print(f"\n" + "="*80)
        print(f"🔔 IMPORTANTE: SCHEDULED QUERIES CREADOS DESHABILITADOS")
        print(f"="*80)
        print(f"📋 Total de Scheduled Queries creados: {success_count}")
        print(f"⏸️  Estado: PAUSADOS (para sincronización perfecta)")
        print(f"\n✅ SIGUIENTE PASO - Ejecuta este comando cuando quieras activarlos:")
        print(f"   python generate_consolidated_tables/enable_all_schedules.py")
        print(f"\n💡 Esto habilitará TODOS los schedules a la vez de forma sincronizada")
        print(f"   Todos empezarán cada 6 horas desde el momento de activación")
        print(f"="*80)
    
    # Retornar estadísticas
    return {
        'success_count': success_count,
        'error_count': error_count,
        'skipped_count': skipped_count,
        'error_tables': error_tables,
        'total_tables': len(available_tables)
    }

if __name__ == "__main__":
    import sys
    import argparse
    
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Crea tablas consolidadas en pph-central.bronze')
    parser.add_argument('--no-schedules', action='store_true', help='No crear Scheduled Queries para refresh automático')
    parser.add_argument('--start-letter', '-s', default='a', help='Letra inicial para filtrar tablas (default: a)')
    parser.add_argument('--table', '-t', help='Procesar solo una tabla específica')
    parser.add_argument('--yes', '-y', action='store_true', help='Responder "sí" a todas las confirmaciones')
    
    args = parser.parse_args()
    
    try:
        stats = create_all_consolidated_tables(
            create_schedules=not args.no_schedules,
            start_from_letter=args.start_letter,
            specific_table=args.table
        )
        
        if stats['error_count'] > 0:
            print(f"\n⚠️  Completado con {stats['error_count']} error(es)")
            sys.exit(1)
        else:
            print("\n✅ ¡Todas las tablas creadas exitosamente!")
            sys.exit(0)
            
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        sys.exit(1)

