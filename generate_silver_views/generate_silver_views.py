# -*- coding: utf-8 -*-
"""
Generate Silver Views for All Tables - CLOUD RUN JOB VERSION
Este script genera automáticamente las vistas Silver para todas las tablas
basándose en los layouts definidos en metadata_consolidated_tables.

VERSIÓN ACTUALIZADA:
- Usa metadata_consolidated_tables como FUENTE DE VERDAD
- Lee layouts predefinidos (más eficiente)
- Mantiene análisis dinámico como fallback opcional
- Sin prompts interactivos
- Modo forzado activado por defecto
- Procesa TODAS las tablas y compañías
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
import time
from datetime import datetime
import warnings
from collections import defaultdict, Counter
import os
warnings.filterwarnings('ignore')

# Importar configuración y tracking manager (mismo directorio)
from config import *
from consolidation_tracking_manager import ConsolidationTrackingManager

# Configuración de metadatos
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
        time.sleep(5)  # Esperar 5 segundos
        return bigquery.Client(project=project_id)

try:
    client = create_bigquery_client(PROJECT_SOURCE)
    client_central = create_bigquery_client(PROJECT_CENTRAL)
    print(f"✅ Clientes BigQuery creados exitosamente")
    print(f"   - Source: {PROJECT_SOURCE}")
    print(f"   - Central: {PROJECT_CENTRAL}")
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
    
    Las tablas manuales en bronze siempre terminan en 's' para
    diferenciarlas de las tablas originales.
    """
    return f"{table_name}s"

def get_table_metadata_from_metadata_table(table_name):
    """
    Obtiene el layout y configuración de una tabla desde metadata_consolidated_tables
    
    Args:
        table_name: Nombre de la tabla
        
    Returns:
        dict: {
            'layout_definition': lista de diccionarios con campos,
            'use_bronze': bool,
            'exists': bool
        } o None si no existe
    """
    try:
        query = f"""
        SELECT
            silver_layout_definition,
            silver_use_bronze,
            silver_status
        FROM `{METADATA_TABLE}`
        WHERE table_name = '{table_name}'
        LIMIT 1
        """
        
        query_job = client_central.query(query)
        results = query_job.result()
        row = next(results, None)
        
        if row is None:
            if DEBUG_MODE:
                print(f"  ⚠️  Tabla '{table_name}' no encontrada en metadatos")
            return None
        
        # Verificar que tiene layout definido
        if row.silver_layout_definition is None or len(row.silver_layout_definition) == 0:
            if DEBUG_MODE:
                print(f"  ⚠️  Tabla '{table_name}' no tiene layout definido en metadatos")
            return None
        
        # Convertir ARRAY<STRUCT> a lista de diccionarios
        layout_list = []
        for field_struct in row.silver_layout_definition:
            layout_list.append({
                'field_name': field_struct.get('field_name'),
                'target_type': field_struct.get('target_type'),
                'field_order': field_struct.get('field_order'),
                'has_type_conflict': field_struct.get('has_type_conflict', False),
                'is_partial': field_struct.get('is_partial', False),
                'alias_name': field_struct.get('alias_name'),
                'is_repeated': field_struct.get('is_repeated', False)
            })
        
        # Ordenar por field_order
        layout_list.sort(key=lambda x: x['field_order'])
        
        use_bronze = bool(row.silver_use_bronze) if row.silver_use_bronze is not None else False
        
        return {
            'layout_definition': layout_list,
            'use_bronze': use_bronze,
            'exists': True,
            'status': row.silver_status
        }
        
    except Exception as e:
        if DEBUG_MODE:
            print(f"  ⚠️  Error leyendo metadatos para '{table_name}': {str(e)}")
        return None

def get_tables_from_metadata():
    """
    Obtiene lista de tablas que tienen layouts definidos en metadata_consolidated_tables
    
    Returns:
        list: Lista de nombres de tablas
    """
    try:
        query = f"""
        SELECT table_name
        FROM `{METADATA_TABLE}`
        WHERE silver_layout_definition IS NOT NULL
          AND ARRAY_LENGTH(silver_layout_definition) > 0
          AND silver_status = 'completed'
        ORDER BY table_name
        """
        
        query_job = client_central.query(query)
        results = query_job.result()
        tables = [row.table_name for row in results]
        
        return tables
        
    except Exception as e:
        print(f"⚠️  Error obteniendo tablas desde metadatos: {str(e)}")
        return []

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
        # Usar el nombre mapeado para tablas manuales
        source_table = get_manual_table_name(table_name)
    else:
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        source_table = table_name
        
    try:
        # Primero obtener los campos básicos
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
        
        # Ahora obtener información de campos REPEATED desde COLUMN_FIELD_PATHS
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
            print(f"  🔍 Campos REPEATED encontrados en INFORMATION_SCHEMA: {repeated_fields if repeated_fields else 'Ninguno'}")
        
        # Agregar columna is_repeated al DataFrame
        fields_df['is_repeated'] = fields_df['column_name'].apply(lambda x: x in repeated_fields)
        
        # Aplanar campos STRUCT (pero NO si son REPEATED)
        flattened_fields = []
        for _, row in fields_df.iterrows():
            row_dict = row.to_dict()  # Convertir la fila a diccionario
            data_type = row_dict['data_type']
            is_repeated = row_dict.get('is_repeated', False)
            
            # Si es REPEATED (ARRAY de cualquier cosa), convertir a JSON STRING
            if is_repeated:
                if DEBUG_MODE:
                    print(f"  ⚠️ Campo REPEATED detectado: {row_dict['column_name']} (tipo: {data_type}) - Se convertirá a JSON STRING en la vista")
                # NO cambiar el data_type aquí, mantener el original para el análisis
                row_dict['alias_name'] = row_dict['column_name']
                row_dict['is_repeated_record'] = True  # Marcar para conversión especial en SQL
                flattened_fields.append(row_dict)
            # Si es STRUCT simple (no repeated), agregar los campos aplanados
            elif data_type.startswith('STRUCT<'):
                # Extraer los subcampos del STRUCT
                struct_fields = data_type.replace('STRUCT<', '').replace('>', '').split(', ')
                for struct_field in struct_fields:
                    name, type_info = struct_field.split(' ')
                    flattened_fields.append({
                        'column_name': f"{row_dict['column_name']}.{name}",  # Nombre original con punto
                        'alias_name': f"{row_dict['column_name']}_{name}",  # Nombre aplanado para el alias
                        'data_type': type_info,
                        'is_nullable': row_dict['is_nullable'],
                        'ordinal_position': row_dict['ordinal_position'],
                        'is_repeated_record': False  # Campos aplanados NO son repeated
                    })
            else:
                # Campos normales: asegurarnos que tengan el mismo nombre como alias
                row_dict['alias_name'] = row_dict['column_name']  # El alias es el mismo nombre
                row_dict['is_repeated_record'] = False  # Campos normales NO son repeated
                flattened_fields.append(row_dict)
        
        # Actualizar el DataFrame con TODOS los campos
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
    
    if partial_fields:
        print(f"\n⚠️  CAMPOS PARCIALES:")
        for field in partial_fields:
            count = field_frequency[field]
            print(f"    - {field}: {count}/{total_companies} compañías")
    
    if type_conflicts:
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

def generate_silver_view_sql_from_metadata(table_name, company_result, layout_definition, use_bronze=False):
    """
    Genera el SQL para crear una vista Silver usando layout de metadata_consolidated_tables
    
    Args:
        table_name: Nombre de la tabla
        company_result: Información de la compañía (dict con company_name, project_id, fields_df)
        layout_definition: Lista de diccionarios con definición de campos desde metadatos
        use_bronze: Si True, usa tabla de bronze en lugar de servicetitan_*
    
    Returns:
        str: SQL para crear la vista o None si hay error
    """
    company_name = company_result['company_name']
    project_id = company_result['project_id']
    
    # Obtener campos de esta compañía con sus tipos (si está disponible)
    company_fields = {}
    company_aliases = {}
    company_repeated_records = {}
    
    if 'fields_df' in company_result and company_result['fields_df'] is not None:
        company_fields_df = company_result['fields_df']
        for _, row in company_fields_df.iterrows():
            field_name = row['column_name']
            company_fields[field_name] = row['data_type']
            if 'alias_name' in row:
                company_aliases[field_name] = row['alias_name']
            if row.get('is_repeated_record', False):
                company_repeated_records[field_name] = True
    
    company_field_names = set(company_fields.keys())
    
    # Determinar dataset y nombre de tabla fuente
    if use_bronze:
        source_dataset = "bronze"
        source_table = get_manual_table_name(table_name)
    else:
        source_dataset = f"servicetitan_{project_id.replace('-', '_')}"
        source_table = table_name
    
    silver_fields = []
    
    # Procesar cada campo del layout definido en metadatos
    for field_info in layout_definition:
        field_name = field_info['field_name']
        target_type = field_info['target_type']
        alias_name = field_info.get('alias_name', field_name)
        is_repeated = field_info.get('is_repeated', False)
        
        # Verificar si el campo existe en esta compañía
        if field_name in company_field_names:
            # Campo existe - aplicar transformación según tipo
            source_type = company_fields.get(field_name)
            
            if is_repeated:
                # Campo REPEATED - convertir a JSON STRING
                cast_expression = f"TO_JSON_STRING({field_name})"
                if DEBUG_MODE:
                    print(f"    🔍 Campo {field_name}: REPEATED → TO_JSON_STRING")
            else:
                # Campo normal - aplicar cast si es necesario
                if source_type == target_type:
                    cast_expression = field_name
                else:
                    cast_expression = generate_cast_for_field(field_name, source_type, target_type)
                    if DEBUG_MODE:
                        print(f"    🔍 Campo {field_name}: {source_type} → {target_type}")
            
            silver_fields.append(f"    {cast_expression} as {alias_name}")
        else:
            # Campo faltante - usar NULL tipado para mantener layout consistente
            default_value = get_default_value_for_type_with_cast(target_type)
            silver_fields.append(f"    {default_value} as {field_name}")
            if DEBUG_MODE:
                print(f"    ⚠️  Campo {field_name} faltante - usando {default_value}")
    
    # Validar que hay campos para procesar
    if not silver_fields:
        print(f"  ⚠️  No hay campos para procesar en {company_name} - {table_name}")
        return None
    
    # CRÍTICO: Mantener el orden del layout (field_order) para consistencia en UNION ALL
    # BigQuery une por POSICIÓN, no por nombre
    # El layout_definition ya viene ordenado por field_order, mantener ese orden
    # NO reordenar alfabéticamente - esto rompería la compatibilidad con UNION ALL
    
    # Agregar comas entre campos (excepto el último)
    fields_with_commas = []
    for i, field in enumerate(silver_fields):
        if i < len(silver_fields) - 1:
            fields_with_commas.append(field + ",")
        else:
            fields_with_commas.append(field)
    
    # Crear el contenido de campos con saltos de línea
    fields_content = '\n'.join(fields_with_commas)
    
    # Agregar comentario específico para tablas manuales
    source_comment = "tabla manual en bronze" if use_bronze else "tabla Fivetran"
    
    sql = f"""-- Vista Silver para {company_name} - Tabla {table_name}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Fuente: {source_comment}
-- Layout desde metadata_consolidated_tables

CREATE OR REPLACE VIEW `{project_id}.silver.vw_{table_name}` AS (
SELECT
{fields_content}
FROM `{project_id}.{source_dataset}.{source_table}`
);
"""
    
    return sql

def generate_silver_view_sql(table_analysis, company_result, use_bronze=False):
    """
    Genera el SQL para crear una vista Silver para una compañía específica
    Incluye normalización de tipos de datos
    
    DEPRECATED: Esta función usa análisis dinámico. 
    Preferir generate_silver_view_sql_from_metadata() que usa layouts de metadatos.
    
    Args:
        table_analysis: Análisis de la tabla
        company_result: Información de la compañía
        use_bronze: Si True, usa tabla de bronze en lugar de servicetitan_*
    """
    table_name = table_analysis['table_name']
    company_name = company_result['company_name']
    project_id = company_result['project_id']
    
    # Obtener campos de esta compañía con sus tipos
    company_fields_df = company_result['fields_df']
    company_fields = {}
    company_aliases = {}
    company_repeated_records = {}  # Para campos ARRAY<STRUCT> que necesitan TO_JSON_STRING
    for _, row in company_fields_df.iterrows():
        field_name = row['column_name']
        company_fields[field_name] = row['data_type']
        # Si el campo tiene un alias (campos aplanados), guardarlo
        if 'alias_name' in row:
            company_aliases[field_name] = row['alias_name']
        # Si es un REPEATED RECORD, marcarlo
        if row.get('is_repeated_record', False):
            company_repeated_records[field_name] = True
    company_field_names = set(company_fields.keys())
    
    if DEBUG_MODE:
        print(f"  🔍 DEBUG company_repeated_records: {company_repeated_records}")
    
    # Determinar dataset y nombre de tabla fuente
    if use_bronze:
        source_dataset = "bronze"
        source_table = get_manual_table_name(table_name)
    else:
        source_dataset = f"servicetitan_{project_id.replace('-', '_')}"
        source_table = table_name
    
    silver_fields = []
    processed_fields = set()  # Para evitar duplicados
    
    # CRÍTICO: Primero procesar campos CON conflictos (tienen prioridad)
    # Si un campo tiene conflicto en CUALQUIER compañía, TODAS deben usar el consensus_type
    
    # 1. Procesar campos con conflictos de tipo PRIMERO
    for field_name, conflict_info in table_analysis['type_conflicts'].items():
        # SOLO incluir campos que existen en esta compañía y no se han procesado
        if field_name not in company_field_names or field_name in processed_fields:
            continue
            
        target_type = conflict_info['consensus_type']  # Siempre STRING si hay conflicto
        source_type = company_fields.get(field_name)
        
        # Si el campo no existe en esta compañía, saltarlo
        if source_type is None:
            continue
        
        # Si es REPEATED RECORD, usar TO_JSON_STRING directamente
        if field_name in company_repeated_records:
            cast_expression = f"TO_JSON_STRING({field_name})"
            if DEBUG_MODE:
                print(f"    🔍 [CONFLICTO] Campo {field_name}: REPEATED → TO_JSON_STRING")
        else:
            # SIEMPRE aplicar cast para asegurar consistencia de tipos
            cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            if DEBUG_MODE:
                print(f"    🔍 [CONFLICTO] Campo {field_name}: {source_type} → {target_type} = {cast_expression[:50]}")
        # Usar el alias si existe, si no usar el nombre original
        alias = company_aliases.get(field_name, field_name)
        silver_fields.append(f"    {cast_expression} as {alias}")
        processed_fields.add(field_name)
    
    # 2. Procesar campos SIN conflictos (solo los que NO fueron procesados arriba)
    for field_name, field_info in table_analysis['field_consensus'].items():
        # SOLO incluir campos que existen en esta compañía y NO se procesaron ya
        if field_name not in company_field_names or field_name in processed_fields:
            continue
            
        target_type = field_info['type']
        source_type = company_fields.get(field_name)
        
        # Si el campo no existe en esta compañía, saltarlo
        if source_type is None:
            continue
        
        # Si es REPEATED RECORD, usar TO_JSON_STRING directamente
        if field_name in company_repeated_records:
            cast_expression = f"TO_JSON_STRING({field_name})"
            if DEBUG_MODE:
                print(f"    🔍 [CONSENSO] Campo {field_name}: REPEATED → TO_JSON_STRING")
        else:
            ## SIEMPRE aplicar cast para asegurar consistencia de tipos
            #cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            # Solo aplicar cast si los tipos son diferentes
            if source_type == target_type:
                cast_expression = field_name
            else:
                cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            if DEBUG_MODE:
                print(f"    🔍 [CONSENSO] Campo {field_name}: {source_type} → {target_type} = {cast_expression[:50]}")
        # Usar el alias si existe, si no usar el nombre original
        alias = company_aliases.get(field_name, field_name)
        silver_fields.append(f"    {cast_expression} as {alias}")
        processed_fields.add(field_name)
    
    # 3. Procesar campos faltantes (con valores por defecto)
    # IMPORTANTE: Mantener layout consistente para UNION ALL
    all_analyzed_fields = set(table_analysis['field_consensus'].keys()) | set(table_analysis['type_conflicts'].keys())
    missing_fields = all_analyzed_fields - company_field_names
    
    for field_name in missing_fields:
        # Determinar tipo objetivo
        if field_name in table_analysis['field_consensus']:
            target_type = table_analysis['field_consensus'][field_name]['type']
        else:
            target_type = table_analysis['type_conflicts'][field_name]['consensus_type']
        
        # CRÍTICO: Usar CAST(NULL AS tipo) para mantener compatibilidad con UNION ALL
        default_value = get_default_value_for_type_with_cast(target_type)
        silver_fields.append(f"    {default_value} as {field_name}")
    
    # Crear SQL
    view_name = f"vw_{table_name}"
    
    # Validar que hay campos para procesar
    if not silver_fields:
        print(f"  ⚠️  No hay campos para procesar en {company_name} - {table_name}")
        return None
    
    # CRÍTICO: Ordenar campos alfabéticamente para consistencia en UNION ALL
    # BigQuery une por POSICIÓN, no por nombre
    # Extraer nombre del campo de cada expresión (después del "as")
    field_dict = {}
    for field_expr in silver_fields:
        # Extraer el nombre del campo (después de "as")
        field_name = field_expr.strip().split(' as ')[-1]
        field_dict[field_name] = field_expr
    
    # Ordenar alfabéticamente por nombre de campo
    sorted_field_names = sorted(field_dict.keys())
    sorted_silver_fields = [field_dict[name] for name in sorted_field_names]
    
    # Agregar comas entre campos (excepto el último)
    fields_with_commas = []
    for i, field in enumerate(sorted_silver_fields):
        if i < len(sorted_silver_fields) - 1:
            fields_with_commas.append(field + ",")
        else:
            fields_with_commas.append(field)
    
    # Crear el contenido de campos con saltos de línea
    fields_content = '\n'.join(fields_with_commas)
    
    # Agregar comentario específico para tablas manuales
    source_comment = "tabla manual en bronze" if use_bronze else "tabla Fivetran"
    
    sql = f"""-- Vista Silver para {company_name} - Tabla {table_name}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Fuente: {source_comment}
-- Incluye normalización de tipos de datos

CREATE OR REPLACE VIEW `{project_id}.silver.{view_name}` AS (
SELECT
{fields_content}
FROM `{project_id}.{source_dataset}.{source_table}`
);
"""
    
    return sql

# Funciones para análisis de tipos de datos
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
    # Si todos los tipos son iguales, usar ese tipo
    unique_types = set(types)
    
    if len(unique_types) == 1:
        return list(unique_types)[0]
    
    # SI HAY CUALQUIER DIFERENCIA → STRING
    return 'STRING'

def generate_cast_for_field(field_name, source_type, target_type):
    """
    Genera la expresión CAST apropiada para un campo
    REGLA SIMPLE: Cualquier tipo → STRING siempre es seguro
    """
    if source_type == target_type:
        return field_name
    
    # Si el target es STRING, SIEMPRE hacer CAST simple
    if target_type == 'STRING':
        # Casos especiales para tipos complejos
        if source_type == 'JSON':
            return f"COALESCE(TO_JSON_STRING({field_name}), '')"
        elif source_type in ['STRUCT', 'ARRAY', 'RECORD']:
            return f"COALESCE(TO_JSON_STRING({field_name}), '')"
        else:
            # Para todos los demás tipos, CAST simple a STRING
            return f"CAST({field_name} AS STRING)"
    
    # Si el target NO es STRING pero el source SÍ es STRING, mantener como STRING
    # (porque estamos en un escenario de conflicto y STRING es más seguro)
    if source_type == 'STRING':
        return f"CAST({field_name} AS STRING)"
    
    # Para otros casos (mismo tipo o conversiones entre no-STRING)
    return f"SAFE_CAST({field_name} AS {target_type})"

def get_default_value_for_type(data_type):
    """Obtiene el valor por defecto para un tipo de datos"""
    defaults = {
        'STRING': "''",
        'INT64': '0',
        'FLOAT64': '0.0',
        'BOOL': 'FALSE',
        'DATE': 'NULL',
        'DATETIME': 'NULL',
        'TIMESTAMP': 'NULL',
        'JSON': 'NULL',
        'BYTES': 'NULL'
    }
    return defaults.get(data_type, 'NULL')

def get_default_value_for_type_with_cast(data_type):
    """
    Obtiene el valor por defecto para un tipo de datos con CAST explícito
    CRÍTICO: Para campos faltantes, usar CAST(NULL AS tipo) para UNION ALL
    """
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

def generate_all_silver_views(force_mode=True, start_from_letter='a', specific_table=None, use_bronze=None, specific_company_id=None, debug=False, use_metadata=True):
    """
    Genera vistas Silver para todas las tablas o una específica
    
    Args:
        force_mode (bool): Si True, procesa todas sin confirmación
        start_from_letter (str): Letra inicial para filtrar tablas (útil para reiniciar)
        specific_table (str): Si se proporciona, genera solo esta tabla
        use_bronze (bool): Si True, fuerza uso de bronze; si False, fuerza Fivetran; si None, lee desde metadatos
        specific_company_id (int): Si se proporciona, procesa solo esta compañía
        debug (bool): Si True, muestra mensajes de debug detallados
        use_metadata (bool): Si True, usa metadata_consolidated_tables como fuente de verdad (default: True)
    
    Returns:
        tuple: (all_results, output_dir)
    """
    # Guardar flag de debug globalmente para acceso en otras funciones
    global DEBUG_MODE
    DEBUG_MODE = debug
    
    # Determinar modo de operación
    mode_text = "FORZADO" if force_mode else "NORMAL"
    metadata_text = "METADATOS (metadata_consolidated_tables)" if use_metadata else "ANÁLISIS DINÁMICO"
    print(f"🚀 GENERACIÓN DE VISTAS SILVER")
    print(f"   Modo: {mode_text}")
    print(f"   Fuente de layouts: {metadata_text}")
    print("=" * 80)
    
    # Inicializar gestor de tracking
    tracking_manager = ConsolidationTrackingManager()
    
    # Obtener compañías
    print("📋 Obteniendo compañías activas...")
    companies_df = get_companies_info()
    
    if companies_df.empty:
        print("❌ No hay compañías activas para procesar")
        return {}, {}
    
    # Filtrar por company_id si se especificó
    if specific_company_id is not None:
        companies_df = companies_df[companies_df['company_id'] == specific_company_id]
        if companies_df.empty:
            print(f"❌ ERROR: No se encontró la compañía con company_id={specific_company_id}")
            return {}, {}
        print(f"🎯 Compañía específica: company_id={specific_company_id} ({companies_df.iloc[0]['company_name']})")
    else:
        print(f"✅ Compañías encontradas: {len(companies_df)}")
    
    # Obtener tablas según el modo
    if use_metadata:
        print("📋 Obteniendo lista de tablas desde metadata_consolidated_tables...")
        all_tables_full = get_tables_from_metadata()
        
        if not all_tables_full:
            print("⚠️  No se encontraron tablas en metadatos. ¿Deseas usar análisis dinámico?")
            print("   Ejecuta con --no-metadata para usar análisis dinámico")
            return {}, {}
        
        print(f"✅ Tablas encontradas en metadatos: {len(all_tables_full)}")
    else:
        # Modo análisis dinámico (fallback)
        print("📋 Obteniendo lista de tablas dinámicamente desde INFORMATION_SCHEMA...")
        
        if use_bronze:
            # Obtener tablas desde bronze (terminan en 's')
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
                return {}, {}
        else:
            # Obtener tablas desde el proceso de extracción original
            from config import get_tables_dynamically
            all_tables_full = get_tables_dynamically()
        
        if not all_tables_full:
            print("❌ ERROR: No se encontraron tablas")
            return {}, {}
        
        print(f"✅ Tablas encontradas dinámicamente: {len(all_tables_full)}")
    
    # Filtrar tablas según los parámetros
    if specific_table:
        # Procesar solo una tabla específica
        if specific_table in all_tables_full:
            all_tables = [specific_table]
            print(f"🎯 TABLA ESPECÍFICA: Procesando solo '{specific_table}'")
        else:
            print(f"❌ ERROR: La tabla '{specific_table}' no existe")
            return {}, {}
    else:
        # Aplicar filtro de letra inicial
        all_tables = [t for t in all_tables_full if t >= start_from_letter]
        
        if start_from_letter != 'a':
            print(f"🔍 FILTRO ACTIVO: Procesando tablas desde '{start_from_letter}'")
        
        print(f"📋 Tablas a procesar: {len(all_tables)} de {len(all_tables_full)} totales")
    
    all_results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear directorio para archivos SQL
    output_dir = f"{OUTPUT_BASE_DIR}/silver_views_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"📁 Directorio de salida: {output_dir}")
    print("=" * 80)
    
    for table_name in all_tables:
        print(f"\n🔄 Procesando tabla: {table_name}")
        
        # Mostrar estado actual (informativo, no bloquea ejecución)
        completion_status = tracking_manager.get_table_completion_status(table_name)
        print(f"  📊 Estado actual: {completion_status['completion_rate']:.1f}% completada")
        print(f"     ✅ Éxitos: {completion_status['success_count']}")
        print(f"     ❌ Errores: {completion_status['error_count']}")
        print(f"     ⚠️  No existe: {completion_status['missing_count']}")
        
        # Obtener layout desde metadatos o análisis dinámico
        if use_metadata:
            # MODO METADATOS: Leer layout desde metadata_consolidated_tables
            metadata_info = get_table_metadata_from_metadata_table(table_name)
            
            if metadata_info is None:
                print(f"  ⚠️  Tabla '{table_name}' no tiene layout en metadatos")
                print(f"  💡 Ejecuta 'analysis_silver_views.py' para generar el layout")
                # Registrar estado 0 para todas las compañías
                for _, company in companies_df.iterrows():
                    tracking_manager.update_status(
                        company_id=company['company_id'],
                        table_name=table_name,
                        status=0,
                        notes="Tabla no tiene layout en metadatos"
                    )
                continue
            
            layout_definition = metadata_info['layout_definition']
            table_use_bronze = metadata_info['use_bronze']
            
            # Si use_bronze fue especificado explícitamente, usarlo; si no, usar el de metadatos
            if use_bronze is not None:
                table_use_bronze = use_bronze
            
            print(f"  📋 Layout obtenido desde metadatos: {len(layout_definition)} campos")
            print(f"  📦 Fuente: {'BRONZE' if table_use_bronze else 'FIVETRAN'}")
            
            # Obtener campos de cada compañía para aplicar casts correctos
            company_results = []
            for _, company in companies_df.iterrows():
                project_id = company['company_project_id']
                if project_id is None:
                    continue
                
                # Obtener campos de la tabla en esta compañía
                fields_df = get_table_fields_with_types(project_id, table_name, table_use_bronze)
                
                if not fields_df.empty:
                    # Filtrar campos de control del ETL
                    filtered_fields_df = fields_df[~fields_df['column_name'].str.startswith('_')]
                    company_results.append({
                        'company_id': company['company_id'],
                        'company_name': company['company_name'],
                        'project_id': project_id,
                        'fields_df': filtered_fields_df
                    })
            
            if not company_results:
                print(f"  ⏭️  Saltando tabla '{table_name}' - no se encontraron datos en ninguna compañía")
                for _, company in companies_df.iterrows():
                    tracking_manager.update_status(
                        company_id=company['company_id'],
                        table_name=table_name,
                        status=0,
                        notes="Tabla no existe en esta compañía"
                    )
                continue
            
            company_sql_files = []
            
            # Procesar cada compañía usando el layout de metadatos
            for company_result in company_results:
                company_name = company_result['company_name']
                project_id = company_result['project_id']
                
                # Generar SQL usando layout de metadatos
                sql_content = generate_silver_view_sql_from_metadata(
                    table_name, 
                    company_result, 
                    layout_definition, 
                    table_use_bronze
                )
                
                # Validar que se generó SQL válido
                if sql_content is None:
                    print(f"    ⚠️  No se pudo generar SQL para {company_name}")
                    tracking_manager.update_status(
                        company_id=company_result['company_id'],
                        table_name=table_name,
                        status=2,
                        notes="Error generando SQL - sin campos válidos"
                    )
                    continue
                
                # Ejecutar vista directamente en BigQuery con reconexión automática
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if attempt > 0:
                            print(f"    🔄 Reintento {attempt + 1}/{max_retries} para {company_name}")
                            time.sleep(2)
                        
                        print(f"    🔄 Creando vista: {project_id}.silver.vw_{table_name}")
                        if DEBUG_MODE:
                            print(f"\n{'='*80}")
                            print(f"SQL GENERADO:")
                            print(f"{'='*80}")
                            print(sql_content)
                            print(f"{'='*80}\n")
                        query_job = client.query(sql_content)
                        query_job.result()
                        print(f"    ✅ Vista creada: {company_name}")
                        company_sql_files.append(f"SUCCESS: {company_name}")
                        
                        tracking_manager.update_status(
                            company_id=company_result['company_id'],
                            table_name=table_name,
                            status=1,
                            notes=f"Vista creada exitosamente en {project_id}.silver (desde metadatos)"
                        )
                        break
                    
                    except Exception as e:
                        error_msg = str(e)
                        if attempt == max_retries - 1:
                            print(f"    ❌ Error final creando vista {company_name}: {error_msg}")
                            company_sql_files.append(f"ERROR: {company_name}")
                            
                            tracking_manager.update_status(
                                company_id=company_result['company_id'],
                                table_name=table_name,
                                status=2,
                                error_message=error_msg,
                                notes=f"Error al crear vista en {project_id}.silver"
                            )
                        else:
                            print(f"    ⚠️  Error en intento {attempt + 1}: {error_msg}")
                            continue
            
            # Guardar resumen para esta tabla
            all_results[table_name] = {
                'table_name': table_name,
                'total_companies': len(company_results),
                'layout_source': 'metadata_consolidated_tables',
                'field_count': len(layout_definition)
            }
        
        else:
            # MODO ANÁLISIS DINÁMICO (fallback)
            table_analysis = analyze_table_fields_across_companies(table_name, use_bronze, companies_df)
            
            if table_analysis is None:
                print(f"  ⏭️  Saltando tabla '{table_name}' - no se encontraron datos")
                for _, company in companies_df.iterrows():
                    tracking_manager.update_status(
                        company_id=company['company_id'],
                        table_name=table_name,
                        status=0,
                        notes="Tabla no existe en esta compañía"
                    )
                continue
            
            all_results[table_name] = table_analysis
            
            companies_with_table = {result['company_name'] for result in table_analysis['company_results']}
            
            for _, company in companies_df.iterrows():
                company_name = company['company_name']
                if company_name not in companies_with_table:
                    tracking_manager.update_status(
                        company_id=company['company_id'],
                        table_name=table_name,
                        status=0,
                        notes="Tabla no existe en esta compañía"
                    )
            
            company_sql_files = []
            
            for company_result in table_analysis['company_results']:
                company_name = company_result['company_name']
                project_id = company_result['project_id']
                
                sql_content = generate_silver_view_sql(table_analysis, company_result, use_bronze)
                
                if sql_content is None:
                    print(f"    ⚠️  No se pudo generar SQL para {company_name}")
                    tracking_manager.update_status(
                        company_id=company_result['company_id'],
                        table_name=table_name,
                        status=2,
                        notes="Error generando SQL - sin campos válidos"
                    )
                    continue
                
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if attempt > 0:
                            print(f"    🔄 Reintento {attempt + 1}/{max_retries} para {company_name}")
                            time.sleep(2)
                        
                        print(f"    🔄 Creando vista: {project_id}.silver.vw_{table_name}")
                        if DEBUG_MODE:
                            print(f"\n{'='*80}")
                            print(f"SQL GENERADO:")
                            print(f"{'='*80}")
                            print(sql_content)
                            print(f"{'='*80}\n")
                        query_job = client.query(sql_content)
                        query_job.result()
                        print(f"    ✅ Vista creada: {company_name}")
                        company_sql_files.append(f"SUCCESS: {company_name}")
                        
                        tracking_manager.update_status(
                            company_id=company_result['company_id'],
                            table_name=table_name,
                            status=1,
                            notes=f"Vista creada exitosamente en {project_id}.silver"
                        )
                        break
                    
                    except Exception as e:
                        error_msg = str(e)
                        if attempt == max_retries - 1:
                            print(f"    ❌ Error final creando vista {company_name}: {error_msg}")
                            company_sql_files.append(f"ERROR: {company_name}")
                            
                            tracking_manager.update_status(
                                company_id=company_result['company_id'],
                                table_name=table_name,
                                status=2,
                                error_message=error_msg,
                                notes=f"Error al crear vista en {project_id}.silver"
                            )
                        else:
                            print(f"    ⚠️  Error en intento {attempt + 1}: {error_msg}")
                            continue
        
        # Crear archivo consolidado para la tabla
        consolidated_filename = f"{output_dir}/consolidated_{table_name}_analysis.sql"
        with open(consolidated_filename, 'w', encoding='utf-8') as f:
            f.write(f"-- Análisis consolidado para tabla: {table_name}\n")
            f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            if use_metadata and table_name in all_results:
                result = all_results[table_name]
                f.write(f"-- Fuente: metadata_consolidated_tables\n")
                f.write(f"-- Total compañías: {result['total_companies']}\n")
                f.write(f"-- Total campos: {result['field_count']}\n\n")
            elif not use_metadata and table_name in all_results:
                table_analysis = all_results[table_name]
                f.write(f"-- Fuente: Análisis dinámico\n")
                f.write(f"-- Total compañías: {table_analysis['total_companies']}\n")
                f.write(f"-- Campos comunes: {len(table_analysis['common_fields'])}\n")
                f.write(f"-- Campos parciales: {len(table_analysis['partial_fields'])}\n\n")
            
            f.write(f"\n-- Archivos SQL generados:\n")
            for sql_file in company_sql_files:
                f.write(f"--   - {sql_file}\n")
        
        print(f"    📄 Archivo consolidado: {consolidated_filename}")
    
    # Generar resumen final
    summary_filename = f"{output_dir}/GENERATION_SUMMARY.md"
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Resumen de Generación de Vistas Silver\n\n")
        f.write(f"**Fecha de generación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Fuente de layouts:** {'metadata_consolidated_tables' if use_metadata else 'Análisis dinámico'}\n\n")
        
        f.write(f"## Tablas Procesadas\n\n")
        f.write(f"Total de tablas procesadas: {len(all_results)}\n\n")
        
        for table_name, result in all_results.items():
            f.write(f"### {table_name}\n")
            if use_metadata:
                f.write(f"- Compañías procesadas: {result['total_companies']}\n")
                f.write(f"- Total campos: {result['field_count']}\n")
                f.write(f"- Fuente: metadata_consolidated_tables\n\n")
            else:
                f.write(f"- Compañías con la tabla: {result['total_companies']}\n")
                f.write(f"- Campos comunes: {len(result.get('common_fields', []))}\n")
                f.write(f"- Campos parciales: {len(result.get('partial_fields', []))}\n\n")
    
    # Mostrar resumen final
    print(f"\n📊 RESUMEN FINAL:")
    print("=" * 80)
    
    for table_name in all_tables:
        completion_status = tracking_manager.get_table_completion_status(table_name)
        print(f"  🔄 {table_name}: {completion_status['completion_rate']:.1f}% completada")
    
    print(f"\n🎯 CLOUD RUN JOB COMPLETADO")
    print(f"📁 Directorio: {output_dir}")
    print(f"📊 Tablas procesadas: {len(all_results)}")
    print(f"📄 Resumen: {summary_filename}")
    print(f"📊 Tracking: Tabla companies_consolidated actualizada")
    
    return all_results, output_dir

if __name__ == "__main__":
    import sys
    import argparse
    
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Genera vistas Silver para ServiceTitan')
    parser.add_argument('--force', '-f', action='store_true', help='Modo forzado: recrea todas las vistas sin confirmación')
    parser.add_argument('--start-letter', '-s', default='a', help='Letra inicial para filtrar tablas (default: a)')
    parser.add_argument('--table', '-t', help='Procesar solo una tabla específica')
    parser.add_argument('--company-id', '-c', type=int, help='Procesar solo una compañía específica (por company_id)')
    parser.add_argument('--yes', '-y', action='store_true', help='Responder "sí" a todas las confirmaciones')
    parser.add_argument('--bronze', '-b', action='store_true',
        help='Forzar uso de tablas manuales de bronze. Si no se especifica, se lee desde metadatos')
    parser.add_argument('--fivetran', action='store_true',
        help='Forzar uso de tablas Fivetran. Si no se especifica, se lee desde metadatos')
    parser.add_argument('--no-metadata', action='store_true',
        help='Usar análisis dinámico en lugar de metadata_consolidated_tables (modo fallback)')
    parser.add_argument('--debug', '-d', action='store_true',
        help='Activar modo debug: muestra mensajes detallados de procesamiento y SQL generado')
    
    args = parser.parse_args()
    
    # Determinar use_bronze
    use_bronze = None
    if args.bronze:
        use_bronze = True
    elif args.fivetran:
        use_bronze = False
    
    if args.force and not args.yes:
        print("🔄 MODO FORZADO ACTIVADO")
        print("⚠️  ADVERTENCIA: Recreará todas las vistas Silver")
        confirm = input("¿Continuar? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Operación cancelada")
            sys.exit(0)
    
    # Ejecutar generación
    results, output_dir = generate_all_silver_views(
        force_mode=args.force,
        start_from_letter=args.start_letter,
        specific_table=args.table,
        use_bronze=use_bronze,
        specific_company_id=args.company_id,
        debug=args.debug,
        use_metadata=not args.no_metadata
    )
    
    print(f"\n✅ Proceso completado exitosamente!")
    print(f"📁 Archivos generados en: {output_dir}")

