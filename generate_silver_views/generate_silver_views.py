# -*- coding: utf-8 -*-
"""
Generate Silver Views for All Tables - CLOUD RUN JOB VERSION
Este script genera automáticamente las vistas Silver para todas las tablas
basándose en el análisis de campos comunes y únicos entre compañías.

VERSIÓN PARA CLOUD RUN JOB:
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
            WHERE table_name = '{source_table}'
            ORDER BY ordinal_position
        """

        query_job = client.query(query)
        results = query_job.result()
        fields_df = pd.DataFrame([dict(row) for row in results])
        
        print(f"\nCAMPOS ORIGINALES para {project_id}.{dataset_name}.{source_table}:")
        for _, row in fields_df.iterrows():
            if row['data_type'].startswith('STRUCT<'):
                print(f"  {row['column_name']}: {row['data_type']} ⚠️ Campo a aplanar")
            else:
                print(f"  {row['column_name']}: {row['data_type']}")
        
        # Aplanar campos STRUCT
        flattened_fields = []
        for _, row in fields_df.iterrows():
            if row['data_type'].startswith('STRUCT<'):
                # Extraer los subcampos del STRUCT
                struct_fields = row['data_type'].replace('STRUCT<', '').replace('>', '').split(', ')
                for struct_field in struct_fields:
                    name, type_info = struct_field.split(' ')
                    flattened_fields.append({
                        'column_name': f"{row['column_name']}_{name}",
                        'data_type': type_info,
                        'is_nullable': row['is_nullable'],
                        'ordinal_position': row['ordinal_position']
                    })
            else:
                flattened_fields.append(row)
        
        # Actualizar el DataFrame con los campos aplanados
        fields_df = pd.DataFrame(flattened_fields)
        
        print("\nCAMPOS DESPUÉS DE APLANAR:")
        # Por ahora mostrar los mismos campos
        for _, row in fields_df.iterrows():
            print(f"  {row['column_name']}: {row['data_type']}")
        
        return fields_df
    except Exception as e:
        print(f"⚠️  Error al obtener campos de {project_id}.{dataset_name}.{table_name}: {str(e)}")
        return pd.DataFrame()

def analyze_table_fields_across_companies(table_name, use_bronze=False):
    """
    Analiza los campos de una tabla específica en todas las compañías
    
    Args:
        table_name: Nombre de la tabla a analizar
        use_bronze: Si True, busca en dataset bronze, si False, en servicetitan_*
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

def generate_silver_view_sql(table_analysis, company_result, use_bronze=False):
    """
    Genera el SQL para crear una vista Silver para una compañía específica
    Incluye normalización de tipos de datos
    
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
    company_fields = {row['column_name']: row['data_type'] for _, row in company_fields_df.iterrows()}
    company_field_names = set(company_fields.keys())
    
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
        
        # SIEMPRE aplicar cast para asegurar consistencia de tipos
        cast_expression = generate_cast_for_field(field_name, source_type, target_type)
        silver_fields.append(f"    {cast_expression} as {field_name}")
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

def generate_all_silver_views(force_mode=True, start_from_letter='a', specific_table=None, use_bronze=False):
    """
    Genera vistas Silver para todas las tablas o una específica
    
    Args:
        force_mode (bool): Si True, procesa todas sin confirmación
        start_from_letter (str): Letra inicial para filtrar tablas (útil para reiniciar)
        specific_table (str): Si se proporciona, genera solo esta tabla
        use_bronze (bool): Si True, usa tablas manuales de bronze en lugar de Fivetran
    
    Returns:
        tuple: (all_results, output_dir)
    """
    # Determinar modo de operación
    mode_text = "FORZADO" if force_mode else "NORMAL"
    source_text = "BRONZE (tablas manuales)" if use_bronze else "ORIGINAL (tablas ServiceTitan)"
    print(f"🚀 GENERACIÓN DE VISTAS SILVER")
    print(f"   Modo: {mode_text}")
    print(f"   Fuente: {source_text}")
    print("=" * 80)
    
    # Inicializar gestor de tracking
    tracking_manager = ConsolidationTrackingManager()
    
    # HARDCODED: Obtener TODAS las compañías activas (sin filtro de status)
    print("📋 Obteniendo TODAS las compañías activas...")
    companies_df = get_companies_info()
    
    if companies_df.empty:
        print("❌ No hay compañías activas para procesar")
        return {}, {}
    
    print(f"✅ Compañías encontradas: {len(companies_df)}")
    
    # Obtener tablas dinámicamente
    print("📋 Obteniendo lista de tablas dinámicamente desde INFORMATION_SCHEMA...")
    
    if use_bronze:
        # Obtener tablas desde bronze (terminan en 's')
        query = f"""
        SELECT table_name 
        FROM `{companies_df.iloc[0]['company_project_id']}.bronze.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '%s'  -- Solo tablas que terminan en 's'
        ORDER BY table_name
        """
        try:
            query_job = client.query(query)
            results = query_job.result()
            # Quitar la 's' final para normalizar los nombres
            all_tables_full = [row.table_name[:-1] for row in results]
            print(f"✅ Tablas manuales encontradas en bronze: {len(all_tables_full)}")
            print(f"   {', '.join(all_tables_full)}")
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
        
        # Analizar campos de la tabla
        table_analysis = analyze_table_fields_across_companies(table_name, use_bronze)
        
        if table_analysis is None:
            print(f"  ⏭️  Saltando tabla '{table_name}' - no se encontraron datos")
            # Registrar estado 0 para todas las compañías (tabla no existe)
            for _, company in companies_df.iterrows():
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
        for _, company in companies_df.iterrows():
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
            sql_content = generate_silver_view_sql(table_analysis, company_result, use_bronze)
            
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
                        # Esperar antes del reintento
                        time.sleep(2)
                    
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
        f.write(f"# Resumen de Generación de Vistas Silver - CLOUD RUN JOB\n\n")
        f.write(f"**Fecha de generación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"## Tablas Procesadas\n\n")
        f.write(f"Total de tablas procesadas: {len(all_results)}\n\n")
        
        for table_name, analysis in all_results.items():
            f.write(f"### {table_name}\n")
            f.write(f"- Compañías con la tabla: {analysis['total_companies']}\n")
            f.write(f"- Campos comunes: {len(analysis['common_fields'])}\n")
            f.write(f"- Campos parciales: {len(analysis['partial_fields'])}\n\n")
    
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
    parser.add_argument('--yes', '-y', action='store_true', help='Responder "sí" a todas las confirmaciones')
    parser.add_argument('--bronze', '-b', action='store_true',
        help='Usar tablas manuales de bronze en lugar de las originales. Se puede combinar con --force, --table y --start-letter')
    
    args = parser.parse_args()
    
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
        use_bronze=args.bronze
    )
    
    print(f"\n✅ Proceso completado exitosamente!")
    print(f"📁 Archivos generados en: {output_dir}")

