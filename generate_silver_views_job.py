"""
Script para generar vistas Silver por compañía (VERSIÓN JOB - NO INTERACTIVO)
Genera vistas normalizadas para cada compañía con manejo de diferencias de esquemas
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
import sys
from config import PROJECT_SOURCE, TABLES_TO_PROCESS, OUTPUT_BASE_DIR
from consolidation_status_manager import ConsolidationStatusManager
from consolidation_tracking_manager import ConsolidationTrackingManager

def create_bigquery_client():
    """Crea cliente BigQuery con manejo de reconexión"""
    try:
        return bigquery.Client(project=PROJECT_SOURCE)
    except Exception as e:
        import time
        time.sleep(5)  # Esperar 5 segundos
        return bigquery.Client(project=PROJECT_SOURCE)

def analyze_table_fields_across_companies(table_name):
    """
    Analiza campos de una tabla específica en todas las compañías
    """
    client = create_bigquery_client()
    
    try:
        # Obtener todas las compañías activas
        companies_query = f"""
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
        
        query_job = client.query(companies_query)
        results = query_job.result()
        companies_df = pd.DataFrame([dict(row) for row in results])
        
        if companies_df.empty:
            return None
        
        
        # Analizar campos en cada compañía
        table_analysis_results = []
        all_fields = set()
        field_types = {}
        
        for _, company in companies_df.iterrows():
            company_id = company['company_id']
            company_name = company['company_name']
            project_id = company['company_project_id']
            
            # Construir nombre del dataset
            dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
            
            try:
                # Obtener campos de la tabla
                fields_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable
                FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                  AND column_name NOT LIKE '_fivetran%'
                ORDER BY ordinal_position
                """
                
                query_job = client.query(fields_query)
                results = query_job.result()
                fields_df = pd.DataFrame([dict(row) for row in results])
                
                if not fields_df.empty:
                    company_fields = {row['column_name']: row['data_type'] for _, row in fields_df.iterrows()}
                    
                    table_analysis_results.append({
                        'company_id': company_id,
                        'company_name': company_name,
                        'project_id': project_id,
                        'fields_df': fields_df,
                        'company_fields': company_fields
                    })
                    
                    all_fields.update(company_fields.keys())
                    
                    # Recopilar tipos por campo
                    for field_name, field_type in company_fields.items():
                        if field_name not in field_types:
                            field_types[field_name] = []
                        field_types[field_name].append(field_type)
                    
            except Exception as e:
                continue
        
        if not table_analysis_results:
            return None
        
        # Analizar consenso de campos y tipos
        field_consensus = {}
        type_conflicts = {}
        
        for field_name in all_fields:
            field_occurrences = sum(1 for result in table_analysis_results 
                                  if field_name in result['company_fields'])
            
            # Si el campo existe en más del 80% de las compañías, considerarlo común
            if field_occurrences >= len(table_analysis_results) * 0.8:
                field_types_list = field_types[field_name]
                
                # Determinar tipo consenso
                consensus_type = determine_consensus_type(field_types_list)
                
                if len(set(field_types_list)) == 1:
                    # Todos los tipos son iguales
                    field_consensus[field_name] = {
                        'type': consensus_type,
                        'occurrences': field_occurrences
                    }
        else:
                    # Hay conflictos de tipo
                    type_conflicts[field_name] = {
                        'consensus_type': consensus_type,
                        'types_found': list(set(field_types_list)),
                        'occurrences': field_occurrences
                    }
        
        return {
            'table_name': table_name,
            'field_consensus': field_consensus,
            'type_conflicts': type_conflicts,
            'company_results': table_analysis_results,
            'field_frequency': {field: sum(1 for result in table_analysis_results 
                                         if field in result['company_fields']) 
                              for field in all_fields}
        }
        
    except Exception as e:
        return None

def determine_consensus_type(types_list):
    """
    Determina el tipo consenso: SI HAY DIFERENCIAS → STRING
    """
    # Si todos los tipos son iguales, usar ese tipo
    unique_types = set(types_list)
    
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
    
    # Crear SQL
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    view_name = f"vw_{table_name}"
    
    # Validar que hay campos para procesar
    if not silver_fields:
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

def generate_all_silver_views(force_recreate=True):
    """
    Genera vistas Silver para todas las tablas identificadas (VERSIÓN JOB - NO INTERACTIVO)
    """
    
    # Inicializar gestores
    status_manager = ConsolidationStatusManager()
    tracking_manager = ConsolidationTrackingManager()
    
    # En modo job, usar TODAS las compañías (force_recreate=True)
    if force_recreate:
        try:
            # Obtener todas las compañías activas (igual que el script manual)
            all_companies_query = f"""
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
            client = create_bigquery_client()
            query_job = client.query(all_companies_query)
            results = query_job.result()
            pending_companies = pd.DataFrame([dict(row) for row in results])
            print(f"COMPAÑÍAS ENCONTRADAS: {len(pending_companies)}")
            if len(pending_companies) == 0:
                print("ERROR: No se encontraron compañías activas")
                return {}, {}
        except Exception as e:
            print(f"ERROR obteniendo compañías: {str(e)}")
            return {}, {}
    else:
        # Obtener compañías activas (modo original que funcionaba)
        try:
            pending_companies = status_manager.get_companies_by_status(0)
            if pending_companies.empty:
                print("ℹ️  No hay compañías pendientes de consolidación")
                return {}, {}
        except Exception as e:
            print(f"❌ Error obteniendo compañías: {str(e)}")
            return {}, {}
    
    
    # Usar configuración centralizada - PROCESAR TODAS LAS TABLAS
    all_tables = TABLES_TO_PROCESS
    print(f"TABLAS A PROCESAR: {len(all_tables)}")
    
    all_results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear directorio para archivos SQL
    output_dir = f"{OUTPUT_BASE_DIR}/silver_views_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    processed_count = 0
    for table_name in all_tables:
        processed_count += 1
        print(f"PROCESANDO TABLA {processed_count}/{len(all_tables)}: {table_name}")
        
        # En modo job, mostrar estado pero NO saltar tablas
        completion_status = tracking_manager.get_table_completion_status(table_name)
        
        
        # En modo job, procesar TODAS las tablas sin importar el estado
        
        # Analizar campos de la tabla
        table_analysis = analyze_table_fields_across_companies(table_name)
        
        if table_analysis is None:
            # Registrar estado 0 para todas las compañías (tabla no existe)
            for _, company in pending_companies.iterrows():
                tracking_manager.update_status(
                    company_id=company['company_id'],
                    table_name=table_name,
                    status=0,
                    notes="Tabla no existe en esta compañía"
                )
            continue
        
        # Identificar compañías que tienen esta tabla
        companies_with_table = set()
        for company_result in table_analysis['company_results']:
            companies_with_table.add(company_result['company_name'])
        
        # Registrar estado 0 para compañías sin esta tabla
        for _, company in pending_companies.iterrows():
            if company['company_name'] not in companies_with_table:
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
                tracking_manager.update_status(
                    company_id=company_result['company_id'],
                    table_name=table_name,
                    status=2,
                    notes="Error generando SQL - sin campos válidos"
                )
                continue
            
            # Ejecutar vista directamente en BigQuery con reconexión automática
            client = create_bigquery_client()  # Crear cliente inicial
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        # Recrear cliente en caso de error de autenticación
                        client = create_bigquery_client()
                        import time
                        time.sleep(2)
                    
                    query_job = client.query(sql_content)
                    query_job.result()  # Esperar a que termine
                    
                    # Registrar éxito
                    tracking_manager.update_status(
                        company_id=company_result['company_id'],
                        table_name=table_name,
                        status=1,
                        notes="Vista Silver creada exitosamente"
                    )
                    
                    # Guardar archivo SQL
                    filename = f"{output_dir}/{table_name}_{company_name.replace(' ', '_')}.sql"
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(sql_content)
                    company_sql_files.append(filename)
                    
                    break  # Salir del loop de reintentos si fue exitoso
    except Exception as e:
                    if attempt == max_retries - 1:
                        tracking_manager.update_status(
                            company_id=company_result['company_id'],
                            table_name=table_name,
                            status=2,
                            notes=f"Error: {str(e)}"
                        )
                    continue
        
        # Guardar resultados
        all_results[table_name] = {
            'analysis': table_analysis,
            'sql_files': company_sql_files
        }
        
    
    # Generar resumen final
    summary_filename = f"{output_dir}/generation_summary.md"
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Resumen de Generación de Vistas Silver\n\n")
        f.write(f"**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Tablas procesadas:** {len(all_results)}\n\n")
        
        for table_name, result in all_results.items():
            f.write(f"## {table_name}\n")
            f.write(f"- Archivos SQL: {len(result['sql_files'])}\n")
            f.write(f"- Campos comunes: {len(result['analysis']['field_consensus'])}\n")
            f.write(f"- Conflictos de tipo: {len(result['analysis']['type_conflicts'])}\n\n")
    
    # Mostrar resumen final
    processed_count = 0
    skipped_count = 0
    
    for table_name in all_tables:
        completion_status = tracking_manager.get_table_completion_status(table_name)
        
        if completion_status['is_fully_consolidated']:
            skipped_count += 1
        else:
            processed_count += 1
    
    
    print(f"PROCESO COMPLETADO: {len(all_results)} tablas procesadas")
    return all_results, output_dir

if __name__ == "__main__":
    print("INICIANDO GENERATE SILVER VIEWS JOB")
    # Ejecutar generación (VERSIÓN JOB - SIN INTERACCIÓN)
    results, output_dir = generate_all_silver_views(force_recreate=True)
    print("JOB TERMINADO")

    print(f"\n✅ Script completado exitosamente!")
    print(f"📁 Revisa los archivos en: {output_dir}")    