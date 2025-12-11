# -*- coding: utf-8 -*-
"""
Analysis Script for Silver Views Layout
Este script analiza todas las compañías para determinar el layout unificado
de las vistas Silver y persiste los resultados en metadata_consolidated_tables.

Solo realiza ANÁLISIS, no genera vistas.
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
import time
from datetime import datetime
import warnings
from collections import defaultdict, Counter
import os
import sys
warnings.filterwarnings('ignore')

# Importar configuración
from config import *
from consolidation_tracking_manager import ConsolidationTrackingManager

# Configuración adicional
PROJECT_CENTRAL = "pph-central"
DATASET_MANAGEMENT = "management"
METADATA_TABLE = f"{PROJECT_CENTRAL}.{DATASET_MANAGEMENT}.metadata_consolidated_tables"

# Variable global para modo debug
DEBUG_MODE = False

# Crear cliente BigQuery con reconexión automática
def create_bigquery_client(project_id=PROJECT_SOURCE):
    """Crea cliente BigQuery con manejo de reconexión"""
    try:
        return bigquery.Client(project=project_id)
    except Exception as e:
        print(f"⚠️  Error creando cliente BigQuery: {str(e)}")
        print("🔄 Reintentando conexión...")
        time.sleep(5)
        return bigquery.Client(project=project_id)

try:
    client = create_bigquery_client(PROJECT_SOURCE)
    client_central = create_bigquery_client(PROJECT_CENTRAL)
    print(f"✅ Clientes BigQuery creados exitosamente")
except Exception as e:
    print(f"❌ Error al crear clientes BigQuery: {str(e)}")
    raise

def get_companies_info():
    """
    Obtiene información de las compañías desde la tabla companies
    """
    query = f"""
        SELECT
            company_id,
            company_name,
            company_new_name,
            company_project_id,
            company_bigquery_status,
            company_consolidated_status
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_fivetran_status = TRUE
          AND company_bigquery_status = TRUE
        ORDER BY company_id
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
        companies_df = pd.DataFrame([dict(row) for row in results])
        print(f"✅ Información de compañías obtenida: {len(companies_df)} registros")
        return companies_df
    except Exception as e:
        print(f"❌ Error al obtener información de compañías: {str(e)}")
        raise

def get_manual_table_name(table_name):
    """
    Obtiene el nombre de la tabla manual agregando 's' al final
    Por ejemplo: 'campaign' -> 'campaigns'
    """
    return f"{table_name}s"

def get_table_fields_with_types(project_id, table_name, use_bronze=False):
    """
    Obtiene información de campos con tipos de datos de una tabla específica
    
    Args:
        project_id: ID del proyecto
        table_name: Nombre de la tabla
        use_bronze: Si True, busca en dataset bronze, si False, en servicetitan_*
    """
    if use_bronze:
        dataset_name = "bronze"
        source_table = get_manual_table_name(table_name)
    else:
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        source_table = table_name
        
    try:
        # Obtener campos básicos
        query = f"""
            SELECT
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{source_table}'
            ORDER BY ordinal_position
        """

        query_job = client.query(query)
        results = query_job.result()
        fields_df = pd.DataFrame([dict(row) for row in results])
        
        # Obtener información de campos REPEATED desde COLUMN_FIELD_PATHS
        repeated_query = f"""
            SELECT DISTINCT
                field_path as column_name
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
            WHERE table_name = '{source_table}'
              AND data_type LIKE 'ARRAY%'
              AND field_path NOT LIKE '%.%'  -- Solo campos de nivel superior
        """
        
        repeated_job = client.query(repeated_query)
        repeated_results = repeated_job.result()
        repeated_fields = set([row.column_name for row in repeated_results])
        
        if DEBUG_MODE:
            print(f"  🔍 Campos REPEATED encontrados: {repeated_fields if repeated_fields else 'Ninguno'}")
        
        # Agregar columna is_repeated
        fields_df['is_repeated'] = fields_df['column_name'].apply(lambda x: x in repeated_fields)
        
        # Aplanar campos STRUCT (pero NO si son REPEATED)
        flattened_fields = []
        for _, row in fields_df.iterrows():
            row_dict = row.to_dict()
            data_type = row_dict['data_type']
            is_repeated = row_dict.get('is_repeated', False)
            
            # Si es REPEATED (ARRAY de cualquier cosa), convertir a JSON STRING
            if is_repeated:
                row_dict['alias_name'] = row_dict['column_name']
                row_dict['is_repeated_record'] = True
                flattened_fields.append(row_dict)
            # Si es STRUCT simple (no repeated), agregar los campos aplanados
            elif data_type.startswith('STRUCT<'):
                # Extraer los subcampos del STRUCT
                struct_fields = data_type.replace('STRUCT<', '').replace('>', '').split(', ')
                for struct_field in struct_fields:
                    name, type_info = struct_field.split(' ')
                    flattened_fields.append({
                        'column_name': f"{row_dict['column_name']}.{name}",
                        'alias_name': f"{row_dict['column_name']}_{name}",
                        'data_type': type_info,
                        'is_nullable': row_dict['is_nullable'],
                        'ordinal_position': row_dict['ordinal_position'],
                        'is_repeated_record': False
                    })
            else:
                # Campos normales
                row_dict['alias_name'] = row_dict['column_name']
                row_dict['is_repeated_record'] = False
                flattened_fields.append(row_dict)
        
        fields_df = pd.DataFrame(flattened_fields)
        return fields_df
    except Exception as e:
        print(f"⚠️  Error al obtener campos de {project_id}.{dataset_name}.{table_name}: {str(e)}")
        return pd.DataFrame()

def analyze_table_fields_across_companies(table_name, use_bronze=False, companies_df=None):
    """
    Analiza los campos de una tabla específica en todas las compañías
    
    Args:
        table_name: Nombre de la tabla a analizar
        use_bronze: Si True, busca en dataset bronze, si False, en servicetitan_*
        companies_df: DataFrame de compañías a procesar (si None, obtiene todas)
    """
    print(f"\n🔍 ANALIZANDO TABLA: {table_name}")
    print("=" * 80)
    
    if companies_df is None:
        companies_df = get_companies_info()
    table_analysis_results = []
    all_table_fields = set()
    
    for idx, row in companies_df.iterrows():
        company_id = row['company_id']
        company_name = row['company_name']
        project_id = row['company_project_id']
        
        if project_id is None:
            continue
        
        # Obtener campos de la tabla
        fields_df = get_table_fields_with_types(project_id, table_name, use_bronze)
        
        if fields_df.empty:
            source_type = "manual (bronze)" if use_bronze else "original"
            source_name = get_manual_table_name(table_name) if use_bronze else table_name
            print(f"  ⚠️  {company_name}: Tabla {source_type} '{source_name}' no encontrada")
            continue
            
        # Filtrar campos de control del ETL (deben quedarse solo en Bronze)
        filtered_fields_df = fields_df[~fields_df['column_name'].str.startswith('_')]
        fields_list = filtered_fields_df['column_name'].tolist()
        field_count = len(fields_list)
        
        # Guardar información
        table_analysis_results.append({
            'company_id': company_id,
            'company_name': company_name,
            'project_id': project_id,
            'field_count': field_count,
            'fields': fields_list,
            'fields_df': filtered_fields_df
        })
        
        all_table_fields.update(fields_list)
    
    if not table_analysis_results:
        print(f"  ❌ No se encontraron datos para la tabla '{table_name}'")
        return None
    
    # Analizar campos comunes y únicos
    field_frequency = Counter()
    for result in table_analysis_results:
        field_frequency.update(result['fields'])
    
    # Determinar campos comunes (presentes en todas las compañías)
    total_companies = len(table_analysis_results)
    common_fields = []
    partial_fields = []
    
    for field, count in field_frequency.items():
        if count == total_companies:
            common_fields.append(field)
        else:
            partial_fields.append(field)
    
    # Analizar tipos de datos
    print(f"\n🔍 ANALIZANDO TIPOS DE DATOS...")
    field_consensus, type_conflicts = analyze_data_types_for_table(table_analysis_results)
    
    print(f"\n📊 ANÁLISIS DE CAMPOS PARA '{table_name}':")
    print(f"  Total de compañías con la tabla: {total_companies}")
    print(f"  Total de campos únicos: {len(all_table_fields)}")
    print(f"  Campos comunes: {len(common_fields)}")
    print(f"  Campos parciales: {len(partial_fields)}")
    print(f"  Campos sin conflicto de tipo: {len(field_consensus)}")
    print(f"  Campos con conflicto de tipo: {len(type_conflicts)}")
    
    if partial_fields and DEBUG_MODE:
        print(f"\n⚠️  CAMPOS PARCIALES:")
        for field in partial_fields:
            count = field_frequency[field]
            print(f"    - {field}: {count}/{total_companies} compañías")
    
    if type_conflicts and DEBUG_MODE:
        print(f"\n⚠️  CONFLICTOS DE TIPO:")
        for field_name, conflict in type_conflicts.items():
            print(f"    - {field_name}: {', '.join(conflict['types'])} → {conflict['consensus_type']}")
    
    return {
        'table_name': table_name,
        'total_companies': total_companies,
        'all_fields': sorted(all_table_fields),
        'common_fields': sorted(common_fields),
        'partial_fields': sorted(partial_fields),
        'field_consensus': field_consensus,
        'type_conflicts': type_conflicts,
        'company_results': table_analysis_results,
        'field_frequency': field_frequency
    }

def analyze_data_types_for_table(table_analysis_results):
    """
    Analiza las diferencias de tipos de datos para una tabla
    """
    field_type_analysis = defaultdict(list)
    
    # Recopilar información de tipos por campo
    for result in table_analysis_results:
        company_name = result['company_name']
        project_id = result['project_id']
        fields_df = result['fields_df']
        
        for _, field in fields_df.iterrows():
            field_name = field['column_name']
            data_type = field['data_type']
            
            field_type_analysis[field_name].append({
                'company_name': company_name,
                'project_id': project_id,
                'data_type': data_type,
                'is_nullable': field['is_nullable']
            })
    
    # Analizar diferencias de tipos
    type_conflicts = {}
    field_consensus = {}
    
    for field_name, type_info_list in field_type_analysis.items():
        unique_types = list(set([info['data_type'] for info in type_info_list]))
        
        if len(unique_types) > 1:
            # Hay conflicto de tipos
            type_conflicts[field_name] = {
                'types': unique_types,
                'companies': type_info_list,
                'consensus_type': determine_consensus_type(unique_types, type_info_list)
            }
        else:
            # No hay conflicto
            field_consensus[field_name] = {
                'type': unique_types[0],
                'companies': type_info_list
            }
    
    return field_consensus, type_conflicts

def determine_consensus_type(types, type_info_list):
    """
    Determina el tipo consenso: SI HAY DIFERENCIAS → STRING
    """
    unique_types = set(types)
    
    if len(unique_types) == 1:
        return list(unique_types)[0]
    
    # SI HAY CUALQUIER DIFERENCIA → STRING
    return 'STRING'

def build_layout_definition_array(table_analysis):
    """
    Construye el ARRAY<STRUCT> para silver_layout_definition
    
    Returns:
        list: Lista de diccionarios con estructura del STRUCT
    """
    layout_fields = []
    
    # Obtener todos los campos analizados (comunes y parciales)
    all_analyzed_fields = set(table_analysis['field_consensus'].keys()) | set(table_analysis['type_conflicts'].keys())
    
    # Obtener información de aliases desde la primera compañía (todas deberían tener la misma estructura)
    first_company_fields = {}
    if table_analysis['company_results']:
        first_company_result = table_analysis['company_results'][0]
        for _, row in first_company_result['fields_df'].iterrows():
            field_name = row['column_name']
            first_company_fields[field_name] = {
                'alias_name': row.get('alias_name', field_name),
                'is_repeated': row.get('is_repeated_record', False)
            }
    
    # Ordenar campos alfabéticamente (igual que en el script original)
    sorted_field_names = sorted(all_analyzed_fields)
    
    # Construir STRUCT para cada campo
    for order, field_name in enumerate(sorted_field_names, start=1):
        # Determinar tipo y conflictos
        if field_name in table_analysis['type_conflicts']:
            target_type = table_analysis['type_conflicts'][field_name]['consensus_type']
            has_conflict = True
        else:
            target_type = table_analysis['field_consensus'][field_name]['type']
            has_conflict = False
        
        # Determinar si es parcial
        is_partial = field_name in table_analysis['partial_fields']
        
        # Obtener alias e información de repeated
        field_info = first_company_fields.get(field_name, {})
        alias_name = field_info.get('alias_name', field_name)
        is_repeated = field_info.get('is_repeated', False)
        
        layout_fields.append({
            'field_name': field_name,
            'target_type': target_type,
            'field_order': order,
            'has_type_conflict': has_conflict,
            'is_partial': is_partial,
            'alias_name': alias_name,
            'is_repeated': is_repeated
        })
    
    return layout_fields

def generate_cast_for_field(field_name, source_type, target_type):
    """Genera la expresión CAST apropiada para un campo"""
    if source_type == target_type:
        return field_name
    
    if target_type == 'STRING':
        if source_type == 'JSON':
            return f"COALESCE(TO_JSON_STRING({field_name}), '')"
        elif source_type in ['STRUCT', 'ARRAY', 'RECORD']:
            return f"COALESCE(TO_JSON_STRING({field_name}), '')"
        else:
            return f"CAST({field_name} AS STRING)"
    
    if source_type == 'STRING':
        return f"CAST({field_name} AS STRING)"
    
    return f"SAFE_CAST({field_name} AS {target_type})"

def get_default_value_for_type_with_cast(data_type):
    """Obtiene el valor por defecto para un tipo de datos con CAST explícito"""
    defaults = {
        'STRING': "CAST(NULL AS STRING)",
        'INT64': "CAST(NULL AS INT64)",
        'FLOAT64': "CAST(NULL AS FLOAT64)",
        'BOOL': "CAST(NULL AS BOOL)",
        'DATE': "CAST(NULL AS DATE)",
        'DATETIME': "CAST(NULL AS DATETIME)",
        'TIMESTAMP': "CAST(NULL AS TIMESTAMP)",
        'JSON': "CAST(NULL AS JSON)",
        'BYTES': "CAST(NULL AS BYTES)"
    }
    return defaults.get(data_type, 'NULL')

def generate_sample_view_ddl(table_analysis, use_bronze=False):
    """
    Genera un DDL de ejemplo para una compañía (la primera disponible)
    Esto servirá como template/referencia
    
    Returns:
        str: SQL DDL completo
    """
    if not table_analysis['company_results']:
        return None
    
    # Usar la primera compañía como ejemplo
    sample_company = table_analysis['company_results'][0]
    table_name = table_analysis['table_name']
    project_id = sample_company['project_id']
    
    # Obtener campos de esta compañía
    company_fields_df = sample_company['fields_df']
    company_fields = {}
    company_aliases = {}
    company_repeated_records = {}
    
    for _, row in company_fields_df.iterrows():
        field_name = row['column_name']
        company_fields[field_name] = row['data_type']
        company_aliases[field_name] = row.get('alias_name', field_name)
        if row.get('is_repeated_record', False):
            company_repeated_records[field_name] = True
    
    company_field_names = set(company_fields.keys())
    
    # Determinar dataset fuente
    if use_bronze:
        source_dataset = "bronze"
        source_table = get_manual_table_name(table_name)
    else:
        source_dataset = f"servicetitan_{project_id.replace('-', '_')}"
        source_table = table_name
    
    silver_fields = []
    processed_fields = set()
    
    # Construir campos según el layout definido
    layout_array = build_layout_definition_array(table_analysis)
    
    for field_info in layout_array:
        field_name = field_info['field_name']
        target_type = field_info['target_type']
        alias_name = field_info['alias_name']
        is_repeated = field_info['is_repeated']
        
        if field_name in company_field_names:
            # Campo existe en esta compañía
            source_type = company_fields.get(field_name)
            
            if is_repeated:
                cast_expression = f"TO_JSON_STRING({field_name})"
            else:
                if source_type == target_type:
                    cast_expression = field_name
                else:
                    cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            
            silver_fields.append(f"    {cast_expression} as {alias_name}")
        else:
            # Campo faltante - usar NULL tipado
            default_value = get_default_value_for_type_with_cast(target_type)
            silver_fields.append(f"    {default_value} as {field_name}")
    
    # Crear SQL template con placeholder PROJECT_ID
    # Este DDL servirá como template que el script de generación reemplazará
    # con el project_id real de cada compañía
    view_name = f"vw_{table_name}"
    fields_content = ',\n'.join(silver_fields)
    source_comment = "tabla manual en bronze" if use_bronze else "tabla Fivetran"
    
    # Determinar el formato del dataset para el template
    # Para tablas Fivetran: servicetitan_{PROJECT_ID} (con guiones convertidos a guiones bajos)
    # Para tablas bronze: siempre "bronze"
    if use_bronze:
        source_dataset_template = "bronze"
        source_table_template = get_manual_table_name(table_name)
    else:
        # El dataset de Fivetran tiene formato: servicetitan_{project_id}
        # donde project_id tiene guiones convertidos a guiones bajos
        # Usamos placeholder <PROJECT_ID> y el script de generación hará la conversión
        source_dataset_template = "servicetitan_<PROJECT_ID>"
        source_table_template = table_name
    
    sql = f"""-- Vista Silver Template - Tabla {table_name}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Fuente: {source_comment}
-- Template para todas las compañías. El script de generación reemplazará:
--   <PROJECT_ID> con el project_id real de cada compañía
--   En dataset Fivetran: también convertirá guiones a guiones bajos (ej: shape-mhs-1 -> shape_mhs_1)

CREATE OR REPLACE VIEW `<PROJECT_ID>.silver.{view_name}` AS (
SELECT
{fields_content}
FROM `<PROJECT_ID>.{source_dataset_template}.{source_table_template}`
);
"""
    
    return sql

def save_analysis_to_metadata(table_analysis, layout_array, view_ddl, use_bronze=False):
    """
    Guarda el análisis en la tabla metadata_consolidated_tables
    Usa parámetros de query para evitar problemas de escape
    
    Args:
        table_analysis: Resultado del análisis de la tabla
        layout_array: Array de STRUCT con la definición del layout
        view_ddl: SQL DDL de ejemplo
        use_bronze: Si True, la fuente es bronze, si False es fivetran
    """
    table_name = table_analysis['table_name']
    # silver_use_bronze es BOOL: True = bronze, False = fivetran
    
    # Construir el ARRAY<STRUCT> como lista de diccionarios para BigQuery
    # BigQuery Python client puede manejar esto directamente
    struct_list = []
    for field in layout_array:
        struct_list.append({
            'field_name': field['field_name'],
            'target_type': field['target_type'],
            'field_order': field['field_order'],
            'has_type_conflict': field['has_type_conflict'],
            'is_partial': field['is_partial'],
            'alias_name': field['alias_name'],
            'is_repeated': field['is_repeated']
        })
    
    # Convertir a formato JSON para insertar en BigQuery
    # Usar json.dumps para el DDL (más seguro que escape manual)
    import json
    
    # Construir SQL usando parámetros (más seguro)
    # Pero BigQuery no soporta parámetros para ARRAY<STRUCT>, así que usamos JSON
    
    # Método alternativo: construir el ARRAY<STRUCT> como string SQL pero con mejor escape
    struct_values = []
    for field in layout_array:
        # Usar JSON_STRING para escapar correctamente
        field_name_json = json.dumps(field['field_name'])
        target_type_json = json.dumps(field['target_type'])
        alias_name_json = json.dumps(field['alias_name'])
        
        struct_value = f"STRUCT({field_name_json} AS field_name, {target_type_json} AS target_type, {field['field_order']} AS field_order, {str(field['has_type_conflict']).upper()} AS has_type_conflict, {str(field['is_partial']).upper()} AS is_partial, {alias_name_json} AS alias_name, {str(field['is_repeated']).upper()} AS is_repeated)"
        struct_values.append(struct_value)
    
    layout_definition_sql = f"[{', '.join(struct_values)}]"
    
    # Para el DDL, usar JSON.dumps para escapar correctamente (maneja saltos de línea, comillas, etc.)
    # JSON.dumps usa comillas dobles, que BigQuery acepta
    view_ddl_escaped = json.dumps(view_ddl) if view_ddl else 'NULL'
    
    # MERGE usando JSON.dumps para escapar strings
    merge_query = f"""
    MERGE `{METADATA_TABLE}` T
    USING (
        SELECT
            {json.dumps(table_name)} as table_name,
            {layout_definition_sql} as silver_layout_definition,
            {view_ddl_escaped} as silver_view_ddl,
            CURRENT_TIMESTAMP() as silver_analysis_timestamp,
            'completed' as silver_status,
            {str(use_bronze).upper()} as silver_use_bronze,
            CURRENT_TIMESTAMP() as updated_at
    ) S
    ON T.table_name = S.table_name
    WHEN MATCHED THEN
        UPDATE SET
            silver_layout_definition = S.silver_layout_definition,
            silver_view_ddl = S.silver_view_ddl,
            silver_analysis_timestamp = S.silver_analysis_timestamp,
            silver_status = S.silver_status,
            silver_use_bronze = S.silver_use_bronze,
            updated_at = S.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
            table_name,
            silver_layout_definition,
            silver_view_ddl,
            silver_analysis_timestamp,
            silver_status,
            silver_use_bronze,
            created_at,
            updated_at
        )
        VALUES (
            S.table_name,
            S.silver_layout_definition,
            S.silver_view_ddl,
            S.silver_analysis_timestamp,
            S.silver_status,
            S.silver_use_bronze,
            S.silver_analysis_timestamp,
            S.updated_at
        )
    """
    
    try:
        query_job = client_central.query(merge_query)
        query_job.result()
        print(f"  ✅ Metadatos guardados para '{table_name}'")
        return True
    except Exception as e:
        print(f"  ❌ Error guardando metadatos para '{table_name}': {str(e)}")
        if DEBUG_MODE:
            print(f"  🔍 Query (primeros 2000 chars):")
            print(merge_query[:2000])
            print(f"\n  🔍 View DDL length: {len(view_ddl) if view_ddl else 0}")
        return False

def get_table_use_bronze_from_metadata(table_name):
    """
    Obtiene si debe usar bronze desde los metadatos
    
    Returns:
        bool: True si debe usar bronze, False si fivetran, None si no está en metadatos
    """
    try:
        query = f"""
        SELECT silver_use_bronze
        FROM `{METADATA_TABLE}`
        WHERE table_name = '{table_name}'
        LIMIT 1
        """
        query_job = client_central.query(query)
        results = query_job.result()
        row = next(results, None)
        if row and row.silver_use_bronze is not None:
            return bool(row.silver_use_bronze)
        return None
    except Exception as e:
        if DEBUG_MODE:
            print(f"  ⚠️  Error leyendo silver_use_bronze para {table_name}: {str(e)}")
        return None

def analyze_all_tables(use_bronze=None, start_from_letter='a', specific_table=None, debug=False):
    """
    Analiza todas las tablas y guarda los resultados en metadata_consolidated_tables
    
    Args:
        use_bronze: 
            - None: Lee desde metadatos (default)
            - True: Fuerza uso de bronze para todas las tablas
            - False: Fuerza uso de fivetran para todas las tablas
        start_from_letter: Letra inicial para filtrar tablas
        specific_table: Si se proporciona, analiza solo esta tabla
        debug: Si True, muestra mensajes detallados
    """
    global DEBUG_MODE
    DEBUG_MODE = debug
    
    # Determinar modo de operación
    if use_bronze is None:
        mode_text = "AUTOMÁTICO (desde metadatos)"
    elif use_bronze:
        mode_text = "FORZADO: BRONZE (tablas manuales)"
    else:
        mode_text = "FORZADO: FIVETRAN (tablas ServiceTitan)"
    
    print("🚀 ANÁLISIS DE LAYOUTS PARA VISTAS SILVER")
    print("=" * 80)
    print(f"Modo: {mode_text}")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Obtener compañías
    print("\n📋 Obteniendo compañías activas...")
    companies_df = get_companies_info()
    
    if companies_df.empty:
        print("❌ No hay compañías activas para procesar")
        return
    
    print(f"✅ Compañías encontradas: {len(companies_df)}")
    
    # Obtener tablas dinámicamente
    print("\n📋 Obteniendo lista de tablas...")
    # Si use_bronze está forzado, usar ese método
    # Si es None, obtener desde metadatos o usar fivetran como default
    if use_bronze is True:
        if companies_df.empty:
            print("❌ No hay compañías para obtener tablas")
            return
        project_id_for_tables = companies_df.iloc[0]['company_project_id']
        query = f"""
        SELECT table_name 
        FROM `{project_id_for_tables}.bronze.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '%s'
        ORDER BY table_name
        """
        try:
            query_job = client.query(query)
            results = query_job.result()
            all_tables_full = [row.table_name[:-1] for row in results]
            print(f"✅ Tablas manuales encontradas en bronze: {len(all_tables_full)}")
        except Exception as e:
            print(f"❌ Error obteniendo tablas de bronze: {str(e)}")
            return
    else:
        # Si use_bronze es None o False, obtener desde Fivetran (default)
        from config import get_tables_dynamically
        all_tables_full = get_tables_dynamically()
    
    if not all_tables_full:
        print("❌ No se encontraron tablas")
        return
    
    print(f"✅ Tablas encontradas: {len(all_tables_full)}")
    
    # Filtrar tablas
    if specific_table:
        if specific_table in all_tables_full:
            all_tables = [specific_table]
            print(f"🎯 TABLA ESPECÍFICA: Analizando solo '{specific_table}'")
        else:
            print(f"❌ ERROR: La tabla '{specific_table}' no existe")
            return
    else:
        all_tables = [t for t in all_tables_full if t >= start_from_letter]
        if start_from_letter != 'a':
            print(f"🔍 FILTRO ACTIVO: Analizando tablas desde '{start_from_letter}'")
        print(f"📋 Tablas a analizar: {len(all_tables)} de {len(all_tables_full)} totales")
    
    # Procesar cada tabla
    results_summary = {
        'success': 0,
        'errors': 0,
        'skipped': 0
    }
    
    for idx, table_name in enumerate(all_tables, 1):
        print(f"\n{'='*80}")
        print(f"[{idx}/{len(all_tables)}] Procesando: {table_name}")
        print(f"{'='*80}")
        
        try:
            # Determinar use_bronze para esta tabla específica
            table_use_bronze = use_bronze
            if table_use_bronze is None:
                # Leer desde metadatos
                metadata_use_bronze = get_table_use_bronze_from_metadata(table_name)
                if metadata_use_bronze is not None:
                    table_use_bronze = metadata_use_bronze
                else:
                    # Si no está en metadatos, usar fivetran (False) como default
                    table_use_bronze = False
                    if DEBUG_MODE:
                        print(f"  ℹ️  No hay silver_use_bronze en metadatos para '{table_name}', usando 'fivetran' (False) como default")
            
            source_type_text = "BRONZE" if table_use_bronze else "FIVETRAN"
            print(f"  📊 Fuente: {source_type_text}")
            
            # Analizar tabla
            table_analysis = analyze_table_fields_across_companies(table_name, table_use_bronze, companies_df)
            
            if table_analysis is None:
                print(f"  ⏭️  Saltando tabla '{table_name}' - no se encontraron datos")
                results_summary['skipped'] += 1
                continue
            
            # Construir layout definition
            layout_array = build_layout_definition_array(table_analysis)
            print(f"  ✅ Layout construido: {len(layout_array)} campos")
            
            # Generar DDL de ejemplo
            view_ddl = generate_sample_view_ddl(table_analysis, table_use_bronze)
            if view_ddl:
                print(f"  ✅ DDL de ejemplo generado")
            
            # Guardar en metadata (pasar table_use_bronze para guardar source_type)
            if save_analysis_to_metadata(table_analysis, layout_array, view_ddl, table_use_bronze):
                results_summary['success'] += 1
            else:
                results_summary['errors'] += 1
                
        except Exception as e:
            print(f"  ❌ Error procesando '{table_name}': {str(e)}")
            results_summary['errors'] += 1
            if DEBUG_MODE:
                import traceback
                traceback.print_exc()
    
    # Resumen final
    print(f"\n{'='*80}")
    print("📊 RESUMEN FINAL")
    print(f"{'='*80}")
    print(f"✅ Éxitos: {results_summary['success']}")
    print(f"❌ Errores: {results_summary['errors']}")
    print(f"⏭️  Omitidas: {results_summary['skipped']}")
    print(f"📋 Total procesadas: {len(all_tables)}")
    print(f"\n✅ Análisis completado")
    print(f"📊 Metadatos guardados en: {METADATA_TABLE}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Analiza layouts para vistas Silver')
    parser.add_argument('--bronze', '-b', action='store_true',
        help='Forzar uso de tablas manuales de bronze para todas las tablas')
    parser.add_argument('--fivetran', '-f', action='store_true',
        help='Forzar uso de tablas Fivetran para todas las tablas')
    parser.add_argument('--start-letter', '-s', default='a',
        help='Letra inicial para filtrar tablas (default: a)')
    parser.add_argument('--table', '-t',
        help='Analizar solo una tabla específica')
    parser.add_argument('--debug', '-d', action='store_true',
        help='Activar modo debug')
    
    args = parser.parse_args()
    
    # Determinar use_bronze: None (auto desde metadatos), True (bronze), False (fivetran)
    if args.bronze and args.fivetran:
        print("❌ ERROR: No puedes usar --bronze y --fivetran al mismo tiempo")
        sys.exit(1)
    
    use_bronze_param = None  # Default: leer desde metadatos
    if args.bronze:
        use_bronze_param = True
    elif args.fivetran:
        use_bronze_param = False
    
    analyze_all_tables(
        use_bronze=use_bronze_param,
        start_from_letter=args.start_letter,
        specific_table=args.table,
        debug=args.debug
    )

