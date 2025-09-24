# -*- coding: utf-8 -*-
"""
Generate Silver Views for All Tables

Este script genera automáticamente las vistas Silver para todas las tablas
basándose en el análisis de campos comunes y únicos entre compañías.

Paso 1: Crear vistas Silver por compañía con layout normalizado
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
from collections import defaultdict, Counter
import os
warnings.filterwarnings('ignore')

print("✅ Librerías importadas correctamente")

# Importar configuración centralizada
from config import *

print(f"🔧 Configuración:")
print(f"   Proyecto: {PROJECT_SOURCE}")
print(f"   Dataset: {DATASET_NAME}")
print(f"   Tabla: {TABLE_NAME}")

# Crear cliente de BigQuery
try:
    client = bigquery.Client(project=PROJECT_SOURCE)
    print(f"✅ Cliente BigQuery creado exitosamente para proyecto: {PROJECT_SOURCE}")
except Exception as e:
    print(f"❌ Error al crear cliente BigQuery: {str(e)}")
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
            company_bigquery_status
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status IS NOT NULL
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

def get_table_fields_with_types(project_id, table_name):
    """
    Obtiene información de campos con tipos de datos de una tabla específica
    """
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    try:
        query = f"""
            SELECT
                table_catalog,
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """

        query_job = client.query(query)
        results = query_job.result()
        fields_df = pd.DataFrame([dict(row) for row in results])
        return fields_df
    except Exception as e:
        print(f"⚠️  Error al obtener campos de {project_id}.{dataset_name}.{table_name}: {str(e)}")
        return pd.DataFrame()

def analyze_table_fields_across_companies(table_name):
    """
    Analiza los campos de una tabla específica en todas las compañías
    """
    print(f"\n🔍 ANALIZANDO TABLA: {table_name}")
    print("=" * 80)
    
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
        fields_df = get_table_fields_with_types(project_id, table_name)
        
        if fields_df.empty:
            print(f"  ⚠️  {company_name}: Tabla '{table_name}' no encontrada")
            continue
            
        # Filtrar campos _fivetran (campos del ETL que deben quedarse solo en Bronze)
        filtered_fields_df = fields_df[~fields_df['column_name'].str.startswith('_fivetran')]
        fields_list = filtered_fields_df['column_name'].tolist()
        field_count = len(fields_list)
        
        print(f"  ✅ {company_name}: {field_count} campos (filtrados _fivetran)")
        
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
    
    if common_fields:
        print(f"\n✅ CAMPOS COMUNES:")
        for field in common_fields:
            print(f"    - {field}")
    
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

def generate_silver_view_sql(table_analysis, company_result):
    """
    Genera el SQL para crear una vista Silver para una compañía específica
    Incluye normalización de tipos de datos
    """
    table_name = table_analysis['table_name']
    company_name = company_result['company_name']
    project_id = company_result['project_id']
    
    # Obtener campos de esta compañía con sus tipos
    company_fields_df = company_result['fields_df']
    company_fields = {row['column_name']: row['data_type'] for _, row in company_fields_df.iterrows()}
    company_field_names = set(company_fields.keys())
    
    silver_fields = []
    
    # 1. Procesar campos comunes (sin conflictos de tipo)
    for field_name, field_info in table_analysis['field_consensus'].items():
        target_type = field_info['type']
        source_type = company_fields.get(field_name, target_type)
        
        if source_type == target_type:
            silver_fields.append(f"    {field_name}")
        else:
            cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            silver_fields.append(f"    {cast_expression}")
    
    # 2. Procesar campos con conflictos de tipo
    for field_name, conflict_info in table_analysis['type_conflicts'].items():
        target_type = conflict_info['consensus_type']
        source_type = company_fields.get(field_name, target_type)
        
        if source_type == target_type:
            silver_fields.append(f"    {field_name}")
        else:
            cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            silver_fields.append(f"    {cast_expression}")
    
    # 3. Procesar campos faltantes (con valores por defecto)
    all_analyzed_fields = set(table_analysis['field_consensus'].keys()) | set(table_analysis['type_conflicts'].keys())
    missing_fields = all_analyzed_fields - company_field_names
    
    for field_name in missing_fields:
        # Determinar tipo objetivo
        if field_name in table_analysis['field_consensus']:
            target_type = table_analysis['field_consensus'][field_name]['type']
        else:
            target_type = table_analysis['type_conflicts'][field_name]['consensus_type']
        
        default_value = get_default_value_for_type(target_type)
        silver_fields.append(f"    {default_value} as {field_name}")
    
    # 4. Metadata fields
    for metadata_field in METADATA_FIELDS:
        if metadata_field == 'source_project':
            silver_fields.append(f"    '{project_id}' as {metadata_field}")
        elif metadata_field == 'silver_processed_at':
            silver_fields.append(f"    CURRENT_TIMESTAMP() as {metadata_field}")
        elif metadata_field == 'company_name':
            silver_fields.append(f"    '{company_name}' as {metadata_field}")
    
    # Crear SQL
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    view_name = f"vw_normalized_{table_name}"
    
    sql = f"""-- Vista Silver para {company_name} - Tabla {table_name}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Incluye normalización de tipos de datos

CREATE OR REPLACE VIEW `{project_id}.silver.{view_name}` AS (
SELECT
{chr(10).join(silver_fields)}
FROM `{project_id}.{dataset_name}.{table_name}`
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
    Determina el tipo de consenso para un campo con conflictos
    """
    type_priority = {
        'STRING': 1,      # Más flexible
        'INT64': 2,       
        'FLOAT64': 3,     
        'BOOL': 4,        
        'DATE': 5,        
        'DATETIME': 6,    
        'TIMESTAMP': 7,   
        'JSON': 8,        
        'BYTES': 9        
    }
    
    # Si hay STRING, usar STRING (más flexible)
    if 'STRING' in types:
        return 'STRING'
    
    # Si hay FLOAT64, usar FLOAT64 (puede contener enteros)
    if 'FLOAT64' in types:
        return 'FLOAT64'
    
    # Si hay INT64, usar INT64
    if 'INT64' in types:
        return 'INT64'
    
    # Para otros tipos, usar el de mayor prioridad
    min_priority = min([type_priority.get(t, 999) for t in types])
    for t in types:
        if type_priority.get(t, 999) == min_priority:
            return t
    
    return types[0]

def generate_cast_for_field(field_name, source_type, target_type):
    """
    Genera la expresión CAST apropiada para un campo
    """
    if source_type == target_type:
        return field_name
    
    # Mapeo de conversiones seguras
    safe_casts = {
        ('INT64', 'STRING'): f"CAST({field_name} AS STRING)",
        ('INT64', 'FLOAT64'): f"CAST({field_name} AS FLOAT64)",
        ('FLOAT64', 'STRING'): f"CAST({field_name} AS STRING)",
        ('STRING', 'INT64'): f"SAFE_CAST({field_name} AS INT64)",
        ('STRING', 'FLOAT64'): f"SAFE_CAST({field_name} AS FLOAT64)",
        ('STRING', 'BOOL'): f"SAFE_CAST({field_name} AS BOOL)",
        ('BOOL', 'STRING'): f"CAST({field_name} AS STRING)",
        ('DATE', 'STRING'): f"CAST({field_name} AS STRING)",
        ('DATETIME', 'STRING'): f"CAST({field_name} AS STRING)",
        ('TIMESTAMP', 'STRING'): f"CAST({field_name} AS STRING)"
    }
    
    cast_key = (source_type, target_type)
    if cast_key in safe_casts:
        return safe_casts[cast_key]
    
    # Para conversiones no seguras, usar SAFE_CAST con valor por defecto
    return f"COALESCE(SAFE_CAST({field_name} AS {target_type}), {get_default_value_for_type(target_type)})"

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

def generate_all_silver_views():
    """
    Genera vistas Silver para todas las tablas identificadas
    """
    # Usar configuración centralizada
    tables_to_process = TABLES_TO_PROCESS
    
    all_results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear directorio para archivos SQL
    output_dir = f"{OUTPUT_BASE_DIR}/silver_views_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"🚀 INICIANDO GENERACIÓN DE VISTAS SILVER")
    print(f"📁 Directorio de salida: {output_dir}")
    print(f"📋 Tablas a procesar: {len(tables_to_process)}")
    print("=" * 80)
    
    for table_name in tables_to_process:
        print(f"\n🔄 Procesando tabla: {table_name}")
        
        # Analizar campos de la tabla
        table_analysis = analyze_table_fields_across_companies(table_name)
        
        if table_analysis is None:
            print(f"  ⏭️  Saltando tabla '{table_name}' - no se encontraron datos")
            continue
        
        all_results[table_name] = table_analysis
        
        # Generar vistas Silver para cada compañía
        company_sql_files = []
        
        for company_result in table_analysis['company_results']:
            company_name = company_result['company_name']
            project_id = company_result['project_id']
            
            # Generar SQL
            sql_content = generate_silver_view_sql(table_analysis, company_result)
            
            # Guardar archivo SQL
            sql_filename = f"{output_dir}/silver_{table_name}_{project_id}.sql"
            with open(sql_filename, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            
            company_sql_files.append(sql_filename)
            print(f"    ✅ {company_name}: {sql_filename}")
        
        # Crear archivo consolidado para la tabla
        consolidated_filename = f"{output_dir}/consolidated_{table_name}_analysis.sql"
        with open(consolidated_filename, 'w', encoding='utf-8') as f:
            f.write(f"-- Análisis consolidado para tabla: {table_name}\n")
            f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"-- Resumen:\n")
            f.write(f"-- Total compañías: {table_analysis['total_companies']}\n")
            f.write(f"-- Campos comunes: {len(table_analysis['common_fields'])}\n")
            f.write(f"-- Campos parciales: {len(table_analysis['partial_fields'])}\n\n")
            
            f.write(f"-- Campos comunes:\n")
            for field in table_analysis['common_fields']:
                f.write(f"--   - {field}\n")
            
            f.write(f"\n-- Campos parciales:\n")
            for field in table_analysis['partial_fields']:
                count = table_analysis['field_frequency'][field]
                f.write(f"--   - {field}: {count}/{table_analysis['total_companies']} compañías\n")
            
            f.write(f"\n-- Archivos SQL generados:\n")
            for sql_file in company_sql_files:
                f.write(f"--   - {sql_file}\n")
        
        print(f"    📄 Archivo consolidado: {consolidated_filename}")
    
    # Generar resumen final
    summary_filename = f"{output_dir}/GENERATION_SUMMARY.md"
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Resumen de Generación de Vistas Silver\n\n")
        f.write(f"**Fecha de generación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"## Tablas Procesadas\n\n")
        f.write(f"Total de tablas procesadas: {len(all_results)}\n\n")
        
        for table_name, analysis in all_results.items():
            f.write(f"### {table_name}\n")
            f.write(f"- Compañías con la tabla: {analysis['total_companies']}\n")
            f.write(f"- Campos comunes: {len(analysis['common_fields'])}\n")
            f.write(f"- Campos parciales: {len(analysis['partial_fields'])}\n\n")
        
        f.write(f"## Archivos Generados\n\n")
        f.write(f"Para cada tabla se generaron:\n")
        f.write(f"- Un archivo SQL por compañía: `silver_{{table_name}}_{{project_id}}.sql`\n")
        f.write(f"- Un archivo de análisis consolidado: `consolidated_{{table_name}}_analysis.sql`\n\n")
        
        f.write(f"## Próximos Pasos\n\n")
        f.write(f"1. Revisar los archivos SQL generados\n")
        f.write(f"2. Ejecutar las vistas Silver en cada proyecto de compañía\n")
        f.write(f"3. Crear las vistas consolidadas en el proyecto central\n")
    
    print(f"\n🎯 GENERACIÓN COMPLETADA")
    print(f"📁 Directorio: {output_dir}")
    print(f"📊 Tablas procesadas: {len(all_results)}")
    print(f"📄 Resumen: {summary_filename}")
    
    return all_results, output_dir

if __name__ == "__main__":
    # Ejecutar generación
    results, output_dir = generate_all_silver_views()
    
    print(f"\n✅ Script completado exitosamente!")
    print(f"📁 Revisa los archivos en: {output_dir}")
