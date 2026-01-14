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
    """Obtiene metadatos de particionamiento, clusterizado y layout de Silver"""
    query = f"""
        SELECT 
            table_name,
            partition_fields,
            cluster_fields,
            silver_layout_definition
        FROM `{PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables`
        ORDER BY table_name
    """
    
    print(f"📋 Cargando metadatos desde: {PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables")
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        # Convertir resultados a lista de dicts
        rows_list = []
        for row in results:
            rows_list.append({
                'table_name': row.table_name,
                'partition_fields': row.partition_fields,
                'cluster_fields': row.cluster_fields,
                'silver_layout_definition': row.silver_layout_definition
            })
        
        df = pd.DataFrame(rows_list)
        
        print(f"🔍 DEBUG: Filas obtenidas de metadatos: {len(df)}")
        
        if len(df) > 0:
            print(f"📋 Primeras 5 tablas en metadatos:")
            for i, row in df.head(5).iterrows():
                print(f"   - {row['table_name']}: partition={row['partition_fields']}, cluster={row['cluster_fields']}")
        
        metadata_dict = {}
        for _, row in df.iterrows():
            # Convertir ARRAY<STRUCT> a lista de diccionarios si existe
            layout_list = []
            if row['silver_layout_definition']:
                for field_struct in row['silver_layout_definition']:
                    layout_list.append({
                        'field_name': field_struct.get('field_name'),
                        'target_type': field_struct.get('target_type'),
                        'field_order': field_struct.get('field_order')
                    })
            
            metadata_dict[row['table_name']] = {
                'partition_fields': row['partition_fields'],
                'cluster_fields': row['cluster_fields'],
                'silver_layout_definition': layout_list
            }
        
        print(f"✅ Metadatos cargados: {len(metadata_dict)} tablas")
        print(f"📋 Tablas en diccionario: {list(metadata_dict.keys())[:10]}")
        return metadata_dict
    except Exception as e:
        print(f"⚠️  Error cargando metadatos: {str(e)}")
        import traceback
        traceback.print_exc()
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
        WHERE table_name IS NOT NULL
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
        import traceback
        traceback.print_exc()
        return []

def get_companies_for_table(table_name):
    """
    Obtiene compañías que tienen una vista Silver específica exitosamente generada.
    Usa companies_consolidated como fuente de verdad (consolidated_status = 1)
    """
    query = f"""
        SELECT 
            c.company_id,
            c.company_name,
            c.company_project_id
        FROM `{PROJECT_CENTRAL}.{DATASET_SETTINGS}.companies_consolidated` cc
        JOIN `{PROJECT_CENTRAL}.{DATASET_SETTINGS}.companies` c
            ON cc.company_id = c.company_id
        WHERE cc.table_name = '{table_name}'
          AND cc.consolidated_status = 1  -- Solo vistas Silver exitosas
          AND c.company_fivetran_status = TRUE
          AND c.company_bigquery_status = TRUE
          AND c.company_project_id IS NOT NULL
        ORDER BY c.company_id
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        # Convertir resultados a lista de dicts
        companies_list = []
        for row in results:
            companies_list.append({
                'company_id': row.company_id,
                'company_name': row.company_name,
                'company_project_id': row.company_project_id
            })
        
        df = pd.DataFrame(companies_list)
        print(f"  📊 Compañías con vista Silver exitosa: {len(df)}")
        return df
        
    except Exception as e:
        print(f"  ⚠️  Error obteniendo compañías: {str(e)}")
        import traceback
        traceback.print_exc()
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

def validate_silver_view_schemas(table_name, companies_df):
    """
    Valida que todas las vistas Silver tengan el mismo esquema (mismos campos en mismo orden)
    
    Args:
        table_name: Nombre de la tabla
        companies_df: DataFrame con compañías a validar
        
    Returns:
        tuple: (is_valid, reference_schema, error_messages)
    """
    if companies_df.empty:
        return True, [], []
    
    # Obtener esquema de la primera compañía como referencia
    reference_project_id = companies_df.iloc[0]['company_project_id']
    reference_company_name = companies_df.iloc[0]['company_name']
    
    try:
        ref_query = f"""
            SELECT 
                column_name,
                data_type,
                ordinal_position
            FROM `{reference_project_id}.silver.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'vw_{table_name}'
            ORDER BY ordinal_position
        """
        
        ref_result = client.query(ref_query).result()
        reference_schema = [(row.column_name, row.data_type, row.ordinal_position) for row in ref_result]
        
        if not reference_schema:
            return False, [], [f"Vista de referencia no encontrada: {reference_company_name}"]
        
    except Exception as e:
        return False, [], [f"Error obteniendo esquema de referencia: {str(e)}"]
    
    # Comparar con todas las demás compañías
    error_messages = []
    
    for _, company in companies_df.iterrows():
        project_id = company['company_project_id']
        company_name = company['company_name']
        
        if project_id == reference_project_id:
            continue  # Saltar la referencia
        
        try:
            comp_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    ordinal_position
                FROM `{project_id}.silver.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = 'vw_{table_name}'
                ORDER BY ordinal_position
            """
            
            comp_result = client.query(comp_query).result()
            company_schema = [(row.column_name, row.data_type, row.ordinal_position) for row in comp_result]
            
            # Comparar esquemas
            if len(company_schema) != len(reference_schema):
                error_messages.append(
                    f"{company_name}: Diferente número de campos "
                    f"({len(company_schema)} vs {len(reference_schema)})"
                )
                continue
            
            # Comparar campo por campo (por posición)
            for i, (ref_field, comp_field) in enumerate(zip(reference_schema, company_schema)):
                ref_name, ref_type, ref_pos = ref_field
                comp_name, comp_type, comp_pos = comp_field
                
                if ref_name != comp_name:
                    error_messages.append(
                        f"{company_name}: Campo en posición {i+1} diferente "
                        f"('{comp_name}' vs '{ref_name}')"
                    )
                elif ref_type != comp_type:
                    error_messages.append(
                        f"{company_name}: Campo '{ref_name}' tiene tipo diferente "
                        f"({comp_type} vs {ref_type})"
                    )
        
        except Exception as e:
            error_messages.append(f"{company_name}: Error validando esquema - {str(e)}")
    
    is_valid = len(error_messages) == 0
    return is_valid, reference_schema, error_messages

def extract_date_fields_from_layout(silver_layout_definition):
    """
    Extrae campos de fecha del silver_layout_definition
    
    Args:
        silver_layout_definition: Lista de diccionarios con campos del layout
        
    Returns:
        list: Lista de nombres de campos de tipo fecha, ordenados por field_order
    """
    if not silver_layout_definition:
        return []
    
    date_types = ['DATE', 'TIMESTAMP', 'DATETIME']
    date_fields = []
    
    for field_info in silver_layout_definition:
        field_name = field_info.get('field_name')
        target_type = field_info.get('target_type', '').upper()
        field_order = field_info.get('field_order', 999)
        
        if target_type in date_types:
            date_fields.append({
                'field_name': field_name,
                'field_order': field_order,
                'target_type': target_type
            })
    
    # Ordenar por field_order (prioridad) y luego por tipo (DATE > TIMESTAMP > DATETIME)
    type_priority = {'DATE': 1, 'TIMESTAMP': 2, 'DATETIME': 3}
    date_fields.sort(key=lambda x: (x['field_order'], type_priority.get(x['target_type'], 99)))
    
    return [f['field_name'] for f in date_fields]

def detect_partition_field(table_name, sample_company_project_id, silver_layout_definition=None):
    """
    Detecta un campo de fecha apropiado para particionar
    
    Prioridad:
    1. Campos de fecha del silver_layout_definition (si está disponible)
    2. Lista hardcodeada de campos comunes
    3. Cualquier campo de fecha disponible en la vista
    4. None (la tabla se creará sin partición como último recurso)
    
    Args:
        table_name: Nombre de la tabla
        sample_company_project_id: Proyecto de ejemplo para verificar existencia
        silver_layout_definition: Layout de Silver (lista de dicts) o None
        
    Returns:
        str: Nombre del campo de partición o None
    """
    # PRIORIDAD 1: Extraer campos de fecha del layout
    if silver_layout_definition:
        layout_date_fields = extract_date_fields_from_layout(silver_layout_definition)
        if layout_date_fields:
            # Verificar que el campo existe en la vista
            for field_name in layout_date_fields:
                if verify_field_exists(table_name, field_name, sample_company_project_id):
                    print(f"    ✅ Campo de partición desde layout: {field_name}")
                    return field_name
                else:
                    print(f"    ⚠️  Campo '{field_name}' del layout no existe en vista, probando siguiente...")
    
    # PRIORIDAD 2: Lista hardcodeada de campos comunes (último recurso antes de fallar)
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
        
        # Buscar el primer campo de fecha común (hardcodeado)
        for field in date_fields:
            if field in date_columns:
                print(f"    ✅ Campo de partición desde lista común: {field}")
                return field
        
        # PRIORIDAD 3: Si no encontró ninguno común, usar el primero disponible
        if date_columns:
            print(f"    ✅ Campo de partición (primero disponible): {date_columns[0]}")
            return date_columns[0]
        
        # Sin campos de fecha - retornar None (la tabla se creará sin partición)
        print(f"    ⚠️  No se encontraron campos de fecha para particionar")
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
    silver_layout_definition = None
    if table_name in metadata_dict:
        metadata = metadata_dict[table_name]
        print(f"  🔍 DEBUG: Metadatos encontrados para '{table_name}'")
        print(f"     partition_fields: {metadata['partition_fields']} (tipo: {type(metadata['partition_fields'])})")
        print(f"     cluster_fields: {metadata['cluster_fields']} (tipo: {type(metadata['cluster_fields'])})")
        
        # Obtener layout de Silver si está disponible
        silver_layout_definition = metadata.get('silver_layout_definition', None)
        if silver_layout_definition:
            print(f"     silver_layout_definition: {len(silver_layout_definition)} campos disponibles")
        
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
        
        # CLUSTERIZADO: Filtrar campos con punto (no soportados) y usar hasta 4
        valid_cluster_fields = []
        for field in cluster_fields_list:
            if '.' in field:
                print(f"  ⚠️  Campo '{field}' ignorado para clustering (contiene '.')")
                continue
            valid_cluster_fields.append(field)
        
        cluster_fields = valid_cluster_fields[:4] if valid_cluster_fields else ['company_id']
    else:
        print(f"  ⚠️  Tabla '{table_name}' NO está en metadatos")
        partition_field = None
        cluster_fields = ['company_id']
    
    # Si no hay partition_field, intentar detectarlo automáticamente
    if not partition_field:
        print(f"  🔍 Detectando campo de particionamiento automáticamente...")
        partition_field = detect_partition_field(
            table_name, 
            companies_df.iloc[0]['company_project_id'],
            silver_layout_definition
        )
        
        if not partition_field:
            print(f"  ⚠️  ADVERTENCIA: No se encontró campo de fecha para particionar")
            print(f"     La tabla se creará SIN particionamiento (no recomendado para tablas grandes)")
            print(f"     Solución: Agregar tabla '{table_name}' a metadata_consolidated_tables")
            print(f"     con un partition_field apropiado (created_on, created_at, etc.)")
            # NO retornar False - continuar para crear la tabla sin partición
    
    # Validar que todas las vistas tienen el mismo esquema
    print(f"  🔍 Validando esquemas de vistas Silver...")
    is_valid, reference_schema, schema_errors = validate_silver_view_schemas(table_name, companies_df)
    
    if not is_valid:
        print(f"  ⚠️  ADVERTENCIA: Diferencias en esquemas detectadas:")
        for error in schema_errors[:5]:  # Mostrar máximo 5 errores
            print(f"     - {error}")
        if len(schema_errors) > 5:
            print(f"     ... y {len(schema_errors) - 5} más")
        print(f"  💡 Las vistas Silver deben tener el mismo esquema para UNION ALL")
        print(f"  💡 Ejecuta 'generate_silver_views.py' para regenerar vistas con layouts consistentes")
        # Continuar de todas formas - puede que funcione si las diferencias son menores
    
    # Construir UNION ALL con metadata de compañía
    # NOTA: Usamos SELECT * porque las vistas Silver ya tienen campos en orden consistente
    # (ordenados por field_order del layout en metadata_consolidated_tables)
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
    
    # SQL completo - con particionamiento por MES si hay partition_field
    # Si no hay partition_field, crear sin partición (último recurso)
    # Lógica genérica para todas las tablas (las vistas Silver ya tienen campos aplanados)
    if partition_field:
        partition_sql = f"PARTITION BY DATE_TRUNC({partition_field}, MONTH)"
    else:
        partition_sql = ""
        print(f"  ⚠️  Creando tabla SIN particionamiento (no hay campo de fecha disponible)")
    
    create_sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_CENTRAL}.{DATASET_BRONZE}.consolidated_{table_name}`
    {partition_sql}
    {cluster_sql}
    AS
    {' UNION ALL '.join(union_parts)}
    """
    
    print(f"  🔄 Creando tabla: consolidated_{table_name}")
    print(f"     📊 Compañías: {len(companies_df)}")
    if partition_field:
        print(f"     ⚙️  Particionado: {partition_field} (por MES)")
    else:
        print(f"     ⚙️  Particionado: NINGUNO (tabla sin partición)")
    print(f"     🔗 Clusterizado: {cluster_fields}")
    # Mostrar SQL generado para revisión
    print(f"\n  📝 SQL GENERADO:")
    print(f"  {'='*80}")
    print(create_sql)
    print(f"  {'='*80}\n")
    
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
    
    
    # Lógica genérica para todas las tablas (las vistas Silver ya tienen campos aplanados)
    if partition_field:
        partition_sql = f"PARTITION BY DATE_TRUNC({partition_field}, MONTH)"
    else:
        partition_sql = ""
    
    cluster_sql = f"CLUSTER BY ({', '.join(cluster_fields)})" if cluster_fields else ""
    
    refresh_sql = f"""
/*
 * Refresh completo para {table_name}
 * Recrea la tabla completa desde las vistas Silver
 * Mantiene particionamiento y clusterizado originales
 * Generado automáticamente
 */
CREATE OR REPLACE TABLE `{PROJECT_CENTRAL}.{DATASET_BRONZE}.consolidated_{table_name}`
{partition_sql}
{cluster_sql}
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