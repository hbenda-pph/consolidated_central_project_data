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
import time
from datetime import datetime
import warnings
from collections import defaultdict, Counter
import os
warnings.filterwarnings('ignore')

# print("✅ Librerías importadas correctamente")

# Importar configuración centralizada
from config import *
from consolidation_status_manager import ConsolidationStatusManager
from consolidation_tracking_manager import ConsolidationTrackingManager

# print(f"🔧 Configuración:")
# print(f"   Proyecto: {PROJECT_SOURCE}")
# print(f"   Dataset: {DATASET_NAME}")
# print(f"   Tabla: {TABLE_NAME}")

# Crear cliente BigQuery con reconexión automática
def create_bigquery_client():
    """Crea cliente BigQuery con manejo de reconexión"""
    try:
        return bigquery.Client(project=PROJECT_SOURCE)
    except Exception as e:
        print(f"⚠️  Error creando cliente BigQuery: {str(e)}")
        print("🔄 Reintentando conexión...")
        time.sleep(5)  # Esperar 5 segundos
        return bigquery.Client(project=PROJECT_SOURCE)

try:
    client = create_bigquery_client()
    # print(f"✅ Cliente BigQuery creado exitosamente para proyecto: {PROJECT_SOURCE}")
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
        # print(f"✅ Información de compañías obtenida: {len(companies_df)} registros")
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
        
        # print(f"  ✅ {company_name}: {field_count} campos (filtrados _fivetran)")
        
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
    
    # if common_fields:
    #     print(f"\n✅ CAMPOS COMUNES:")
    #     for field in common_fields:
    #         print(f"    - {field}")
    
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
    processed_fields = set()  # Para evitar duplicados
    
    # 1. Procesar campos comunes (sin conflictos de tipo)
    for field_name, field_info in table_analysis['field_consensus'].items():
        # SOLO incluir campos que existen en esta compañía y no se han procesado
        if field_name not in company_field_names or field_name in processed_fields:
            continue
            
        target_type = field_info['type']
        source_type = company_fields.get(field_name)
        
        # Si el campo no existe en esta compañía, saltarlo
        if source_type is None:
            continue
        
        # SIEMPRE aplicar cast para asegurar consistencia de tipos
        cast_expression = generate_cast_for_field(field_name, source_type, target_type)
        silver_fields.append(f"    {cast_expression} as {field_name}")
        processed_fields.add(field_name)
        
    # 2. Procesar campos con conflictos de tipo (solo los no procesados)
    for field_name, conflict_info in table_analysis['type_conflicts'].items():
        # SOLO incluir campos que existen en esta compañía y no se han procesado
        if field_name not in company_field_names or field_name in processed_fields:
            continue
            
        target_type = conflict_info['consensus_type']
        source_type = company_fields.get(field_name)
        
        # Si el campo no existe en esta compañía, saltarlo
        if source_type is None:
            continue
        
        # SIEMPRE aplicar cast para asegurar consistencia de tipos
        cast_expression = generate_cast_for_field(field_name, source_type, target_type)
        silver_fields.append(f"    {cast_expression} as {field_name}")
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
        
        default_value = get_default_value_for_type(target_type)
        silver_fields.append(f"    {default_value} as {field_name}")
    
    # 4. Metadata fields (eliminados - no necesarios en vistas por compañía)
    
    # Crear SQL
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    view_name = f"vw_{table_name}"
    
    # Validar que hay campos para procesar
    if not silver_fields:
        print(f"  ⚠️  No hay campos para procesar en {company_name} - {table_name}")
        return None
    
    # Agregar comas entre campos (excepto el último)
    fields_with_commas = []
    for i, field in enumerate(silver_fields):
        if i < len(silver_fields) - 1:
            fields_with_commas.append(field + ",")
        else:
            fields_with_commas.append(field)
    
    # Crear el contenido de campos con saltos de línea
    fields_content = '\n'.join(fields_with_commas)
    
    sql = f"""-- Vista Silver para {company_name} - Tabla {table_name}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Incluye normalización de tipos de datos

CREATE OR REPLACE VIEW `{project_id}.silver.{view_name}` AS (
SELECT
{fields_content}
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
        ('TIMESTAMP', 'STRING'): f"CAST({field_name} AS STRING)",
        # JSON a otros tipos - usar TO_JSON_STRING para convertir a STRING
        ('JSON', 'STRING'): f"COALESCE(TO_JSON_STRING({field_name}), '')",
        ('JSON', 'INT64'): f"COALESCE(SAFE_CAST(TO_JSON_STRING({field_name}) AS INT64), 0)",
        ('JSON', 'FLOAT64'): f"COALESCE(SAFE_CAST(TO_JSON_STRING({field_name}) AS FLOAT64), 0.0)"
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

def generate_all_silver_views(force_recreate=False):
    """
    Genera vistas Silver para todas las tablas identificadas con seguimiento de estados
    
    Args:
        force_recreate (bool): Si True, recrea todas las vistas sin importar el estado de consolidación
    """
    print("🚀 Iniciando generación de vistas Silver para todas las tablas")
    
    # Inicializar gestores
    status_manager = ConsolidationStatusManager()
    tracking_manager = ConsolidationTrackingManager()
    
    # Obtener compañías pendientes de consolidación
    pending_companies = status_manager.get_companies_for_consolidation()
    
    if pending_companies.empty:
        print("ℹ️  No hay compañías pendientes de consolidación")
        return {}, {}
    
    print(f"📋 Compañías a procesar: {len(pending_companies)}")
    
    # Usar configuración centralizada
    all_tables = TABLES_TO_PROCESS
    
    if force_recreate:
        print("🔄 MODO FORZADO: Recreando todas las vistas sin importar estado de consolidación")
        tables_to_process = all_tables
    else:
        print("📋 MODO NORMAL: Filtrando tablas ya consolidadas")
        tables_to_process = tracking_manager.get_tables_to_process(all_tables)
    
    all_results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear directorio para archivos SQL
    output_dir = f"{OUTPUT_BASE_DIR}/silver_views_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    if not tables_to_process:
        print("✅ Todas las tablas están 100% consolidadas. No hay nada que procesar.")
        return {}, {}
    
    print(f"🚀 INICIANDO GENERACIÓN DE VISTAS SILVER")
    print(f"📁 Directorio de salida: {output_dir}")
    print(f"📋 Tablas a procesar: {len(tables_to_process)}")
    print("=" * 80)
    
    for table_name in tables_to_process:
        print(f"\n🔄 Procesando tabla: {table_name}")
        
        # Verificar si la tabla ya está 100% consolidada (solo en modo normal)
        if not force_recreate:
            completion_status = tracking_manager.get_table_completion_status(table_name)
            
            if completion_status['is_fully_consolidated']:
                print(f"  ⏭️  Saltando tabla '{table_name}' - 100% consolidada ({completion_status['completion_rate']:.1f}%)")
                continue
        else:
            # En modo forzado, mostrar estado pero no saltar
            completion_status = tracking_manager.get_table_completion_status(table_name)
        
        print(f"  📊 Estado actual: {completion_status['completion_rate']:.1f}% completada")
        print(f"     ✅ Éxitos: {completion_status['success_count']}")
        print(f"     ❌ Errores: {completion_status['error_count']}")
        print(f"     ⚠️  No existe: {completion_status['missing_count']}")
        
        # Analizar campos de la tabla
        table_analysis = analyze_table_fields_across_companies(table_name)
        
        if table_analysis is None:
            print(f"  ⏭️  Saltando tabla '{table_name}' - no se encontraron datos")
            # Registrar estado 0 para todas las compañías (tabla no existe)
            for _, company in pending_companies.iterrows():
                tracking_manager.update_status(
                    company_id=company['company_id'],
                    table_name=table_name,
                    status=0,
                    notes="Tabla no existe en esta compañía"
                )
            continue
        
        all_results[table_name] = table_analysis
        
        # Obtener compañías que tienen la tabla
        companies_with_table = {result['company_name'] for result in table_analysis['company_results']}
        
        # Registrar estado 0 para compañías que no tienen la tabla
        for _, company in pending_companies.iterrows():
            company_name = company['company_name']
            if company_name not in companies_with_table:
                tracking_manager.update_status(
                    company_id=company['company_id'],
                    table_name=table_name,
                    status=0,
                    notes="Tabla no existe en esta compañía"
                )
        
        # Generar vistas Silver para cada compañía
        company_sql_files = []
        
        for company_result in table_analysis['company_results']:
            company_name = company_result['company_name']
            project_id = company_result['project_id']
            
            # Generar y ejecutar SQL directamente
            sql_content = generate_silver_view_sql(table_analysis, company_result)
            
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
                        # Recrear cliente en caso de error de autenticación
                        global client
                        client = create_bigquery_client()
                        time.sleep(2)  # Esperar antes del reintento
                    
                    print(f"    🔄 Creando vista: {project_id}.silver.vw_{table_name}")
                    query_job = client.query(sql_content)
                    query_job.result()  # Esperar a que termine
                    print(f"    ✅ Vista creada: {company_name}")
                    company_sql_files.append(f"SUCCESS: {company_name}")
                    
                    # Actualizar tracking
                    tracking_manager.update_status(
                        company_id=company_result['company_id'],
                        table_name=table_name,
                        status=1,
                        notes=f"Vista creada exitosamente en {project_id}.silver"
                    )
                    break  # Éxito, salir del loop de reintentos
                    
                except Exception as e:
                    error_msg = str(e)
                    if attempt == max_retries - 1:  # Último intento
                        print(f"    ❌ Error final creando vista {company_name}: {error_msg}")
                        company_sql_files.append(f"ERROR: {company_name}")
                        
                        # Actualizar tracking con error
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
    
    # Actualizar estados de compañías procesadas
    print(f"\n📊 Actualizando estados de consolidación...")
    for _, company in pending_companies.iterrows():
        company_id = company['company_id']
        company_name = company['company_name']
        
        # Verificar si la compañía fue procesada exitosamente
        company_success = True
        for table_name in tables_to_process:
            if table_name in all_results:
                # Verificar si esta compañía tiene datos para esta tabla
                company_has_table = any(
                    result['company_id'] == company_id 
                    for result in all_results[table_name]['company_results']
                )
                if not company_has_table:
                    # No es un error si la compañía no tiene cierta tabla
                    continue
        
        # Actualizar estado
        if company_success:
            status_manager.update_company_status(company_id, status_manager.STATUS['COMPLETED'])
            # print(f"  ✅ {company_name}: Estado actualizado a COMPLETED")
        else:
            status_manager.update_company_status(company_id, status_manager.STATUS['ERROR'])
            print(f"  ❌ {company_name}: Estado actualizado a ERROR")
    
    # Mostrar resumen de estados
    status_manager.print_consolidation_summary()
    
    # Mostrar reporte de consolidación
    tracking_manager.print_consolidation_report()
    
    # Mostrar resumen de tablas saltadas
    print(f"\n📋 RESUMEN DE TABLAS:")
    print("=" * 50)
    
    processed_count = 0
    skipped_count = 0
    
    for table_name in all_tables:
        completion_status = tracking_manager.get_table_completion_status(table_name)
        
        if not force_recreate and completion_status['is_fully_consolidated']:
            skipped_count += 1
            print(f"  ⏭️  {table_name}: SALTADA - 100% consolidada")
        else:
            processed_count += 1
            if force_recreate:
                print(f"  🔄 {table_name}: RECREADA - {completion_status['completion_rate']:.1f}% completada")
            else:
                print(f"  🔄 {table_name}: PROCESADA - {completion_status['completion_rate']:.1f}% completada")
    
    print(f"\n🎯 GENERACIÓN COMPLETADA")
    print(f"📁 Directorio: {output_dir}")
    print(f"📊 Tablas procesadas: {processed_count}")
    print(f"⏭️  Tablas saltadas: {skipped_count}")
    print(f"📄 Resumen: {summary_filename}")
    print(f"📊 Tracking: Tabla companies_consolidated actualizada")
    
    return all_results, output_dir

if __name__ == "__main__":
    import sys
    
    # Verificar si se solicita recreación forzada
    force_recreate = len(sys.argv) > 1 and sys.argv[1].lower() in ['--force', '-f', 'force']
    
    if force_recreate:
        print("🔄 MODO FORZADO ACTIVADO: Recreando todas las vistas Silver")
        print("⚠️  ADVERTENCIA: Esto puede tomar mucho tiempo")
        confirm = input("¿Continuar? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Operación cancelada")
            sys.exit(0)
    
    # Ejecutar generación
    results, output_dir = generate_all_silver_views(force_recreate=force_recreate)
    
    print(f"\n✅ Script completado exitosamente!")
    print(f"📁 Revisa los archivos en: {output_dir}")
