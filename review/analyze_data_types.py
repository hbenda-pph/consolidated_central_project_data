# -*- coding: utf-8 -*-
"""
Analyze Data Types Differences

Este script analiza las diferencias de tipos de datos entre compañías
para las mismas tablas y genera la normalización apropiada con CAST.
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import warnings
from collections import defaultdict, Counter
import sys
import os

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import *

warnings.filterwarnings('ignore')

print("✅ Librerías importadas correctamente")

# Crear cliente de BigQuery
try:
    client = bigquery.Client(project=PROJECT_SOURCE)
    print(f"✅ Cliente BigQuery creado exitosamente para proyecto: {PROJECT_SOURCE}")
except Exception as e:
    print(f"❌ Error al crear cliente BigQuery: {str(e)}")
    raise

def get_companies_info():
    """Obtiene información de las compañías activas"""
    try:
        query = f"""
            SELECT company_id, company_name, company_project_id
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            WHERE company_bigquery_status IS NOT NULL
            ORDER BY company_id
            LIMIT {MAX_COMPANIES_FOR_TEST}  # Limitar para prueba
        """
        print(f"🔍 Ejecutando consulta: {query}")
        result = client.query(query).result()
        companies_df = pd.DataFrame([dict(row) for row in result])
        print(f"✅ Obtenidas {len(companies_df)} compañías")
        return companies_df
    except Exception as e:
        print(f"❌ Error obteniendo información de compañías: {str(e)}")
        return pd.DataFrame()

def get_table_fields_with_types(project_id, table_name):
    """Obtiene campos con sus tipos de datos de una tabla específica"""
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            ordinal_position,
            column_default
        FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    try:
        return pd.DataFrame([dict(row) for row in client.query(query).result()])
    except Exception as e:
        print(f"⚠️  Error obteniendo campos de {project_id}.{dataset_name}.{table_name}: {str(e)}")
        return pd.DataFrame()

def analyze_table_data_types(table_name):
    """
    Analiza las diferencias de tipos de datos para una tabla específica
    """
    print(f"\n🔍 ANALIZANDO TIPOS DE DATOS PARA: {table_name}")
    print("=" * 80)
    
    try:
        companies_df = get_companies_info()
        
        if companies_df.empty:
            print("❌ No se pudieron obtener las compañías")
            return None
            
        print(f"📋 Compañías a analizar: {len(companies_df)}")
        
    except Exception as e:
        print(f"❌ Error en análisis inicial: {str(e)}")
        return None
    
    field_type_analysis = defaultdict(list)
    field_presence = Counter()
    
    # Recopilar información de tipos por campo
    for _, company in companies_df.iterrows():
        project_id = company['company_project_id']
        company_name = company['company_name']
        
        fields_df = get_table_fields_with_types(project_id, table_name)
        
        if fields_df.empty:
            print(f"  ⚠️  {company_name}: Tabla '{table_name}' no encontrada")
            continue
        
        print(f"  ✅ {company_name}: {len(fields_df)} campos")
        
        # Filtrar campos _fivetran (campos del ETL que deben quedarse solo en Bronze)
        filtered_fields_df = fields_df[~fields_df['column_name'].str.startswith('_fivetran')]
        
        # Analizar cada campo
        for _, field in filtered_fields_df.iterrows():
            field_name = field['column_name']
            data_type = field['data_type']
            
            field_type_analysis[field_name].append({
                'company_name': company_name,
                'project_id': project_id,
                'data_type': data_type,
                'is_nullable': field['is_nullable']
            })
            
            field_presence[field_name] += 1
    
    if not field_type_analysis:
        print(f"  ❌ No se encontraron datos para la tabla '{table_name}'")
        return None
    
    # Analizar diferencias de tipos
    type_conflicts = {}
    field_consensus = {}
    
    print(f"\n📊 ANÁLISIS DE TIPOS DE DATOS:")
    print(f"  Total de campos únicos: {len(field_type_analysis)}")
    print(f"  Compañías analizadas: {len(companies_df)}")
    
    for field_name, type_info_list in field_type_analysis.items():
        # Obtener tipos únicos para este campo
        unique_types = list(set([info['data_type'] for info in type_info_list]))
        
        if len(unique_types) > 1:
            # Hay conflicto de tipos
            type_conflicts[field_name] = {
                'types': unique_types,
                'companies': type_info_list,
                'consensus_type': determine_consensus_type(unique_types, type_info_list)
            }
            
            print(f"\n  ⚠️  CONFLICTO EN CAMPO: {field_name}")
            print(f"      Tipos encontrados: {', '.join(unique_types)}")
            print(f"      Tipo consenso: {type_conflicts[field_name]['consensus_type']}")
            
            # Mostrar detalles por compañía
            for info in type_info_list:
                print(f"        - {info['company_name']}: {info['data_type']}")
        else:
            # No hay conflicto, todos tienen el mismo tipo
            field_consensus[field_name] = {
                'type': unique_types[0],
                'companies': type_info_list
            }
    
    print(f"\n📈 RESUMEN:")
    print(f"  Campos sin conflicto: {len(field_consensus)}")
    print(f"  Campos con conflicto: {len(type_conflicts)}")
    
    return {
        'table_name': table_name,
        'total_fields': len(field_type_analysis),
        'field_consensus': field_consensus,
        'type_conflicts': type_conflicts,
        'companies_analyzed': len(companies_df)
    }

def determine_consensus_type(types, type_info_list):
    """
    Determina el tipo de consenso para un campo con conflictos
    """
    # Estrategia de resolución de conflictos
    type_priority = {
        'STRING': 1,      # Más flexible, puede contener cualquier cosa
        'INT64': 2,       # Números enteros
        'FLOAT64': 3,     # Números decimales
        'BOOL': 4,        # Booleanos
        'DATE': 5,        # Fechas
        'DATETIME': 6,    # Fecha y hora
        'TIMESTAMP': 7,   # Timestamp
        'JSON': 8,        # JSON
        'BYTES': 9        # Bytes
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
    
    # Fallback
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

def generate_enhanced_silver_view_sql(table_analysis, company_result):
    """
    Genera SQL mejorado para vista Silver con normalización de tipos
    """
    table_name = table_analysis['table_name']
    company_name = company_result['company_name']
    project_id = company_result['project_id']
    
    # Obtener campos de esta compañía
    company_fields_df = get_table_fields_with_types(project_id, table_name)
    company_fields = {row['column_name']: row['data_type'] for _, row in company_fields_df.iterrows()}
    
    silver_fields = []
    
    # Procesar campos comunes (sin conflictos)
    for field_name, field_info in table_analysis['field_consensus'].items():
        target_type = field_info['type']
        source_type = company_fields.get(field_name, target_type)
        
        if source_type == target_type:
            silver_fields.append(f"    {field_name}")
        else:
            cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            silver_fields.append(f"    {cast_expression}")
    
    # Procesar campos con conflictos de tipo
    for field_name, conflict_info in table_analysis['type_conflicts'].items():
        target_type = conflict_info['consensus_type']
        source_type = company_fields.get(field_name, target_type)
        
        if source_type == target_type:
            silver_fields.append(f"    {field_name}")
        else:
            cast_expression = generate_cast_for_field(field_name, source_type, target_type)
            silver_fields.append(f"    {cast_expression}")
    
    # Procesar campos faltantes (con valores por defecto)
    all_fields = set(table_analysis['field_consensus'].keys()) | set(table_analysis['type_conflicts'].keys())
    missing_fields = all_fields - set(company_fields.keys())
    
    for field_name in missing_fields:
        # Determinar tipo objetivo
        if field_name in table_analysis['field_consensus']:
            target_type = table_analysis['field_consensus'][field_name]['type']
        else:
            target_type = table_analysis['type_conflicts'][field_name]['consensus_type']
        
        default_value = get_default_value_for_type(target_type)
        silver_fields.append(f"    {default_value} as {field_name}")
    
    # Metadata fields
    for metadata_field in METADATA_FIELDS:
        if metadata_field == 'source_project':
            silver_fields.append(f"    '{project_id}' as {metadata_field}")
        elif metadata_field == 'silver_processed_at':
            silver_fields.append(f"    CURRENT_TIMESTAMP() as {metadata_field}")
        elif metadata_field == 'company_name':
            silver_fields.append(f"    '{company_name}' as {metadata_field}")
    
    # Crear SQL
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    view_name = f"vw_{table_name}"
    
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

def test_data_type_analysis():
    """
    Función de prueba para analizar tipos de datos
    """
    print("🧪 PRUEBA DE ANÁLISIS DE TIPOS DE DATOS")
    print("=" * 60)
    
    # Probar con tabla 'call' que ya conoces
    test_table = 'call'
    
    analysis_result = analyze_table_data_types(test_table)
    
    if analysis_result:
        print(f"\n✅ ANÁLISIS COMPLETADO PARA: {test_table}")
        
        # Generar SQL de ejemplo
        companies_df = get_companies_info()
        if not companies_df.empty:
            first_company = companies_df.iloc[0]
            
            print(f"\n🔧 SQL DE EJEMPLO PARA: {first_company['company_name']}")
            print("=" * 60)
            
            sql_example = generate_enhanced_silver_view_sql(analysis_result, first_company)
            print(sql_example)
            
            # Guardar en archivo
            with open(f"test_{test_table}_enhanced_silver_view.sql", 'w') as f:
                f.write(sql_example)
            
            print(f"\n💾 SQL guardado en: test_{test_table}_enhanced_silver_view.sql")
        
        return analysis_result
    else:
        print(f"❌ No se pudo analizar la tabla '{test_table}'")
        return None

def main():
    """Función principal para ejecutar análisis de tipos de datos"""
    try:
        print("🔍 Iniciando análisis de tipos de datos...")
        
        # Verificar configuración
        print(f"📋 Configuración:")
        print(f"  - Proyecto fuente: {PROJECT_SOURCE}")
        print(f"  - Dataset: {DATASET_NAME}")
        print(f"  - Tabla: {TABLE_NAME}")
        print(f"  - Límite compañías: {MAX_COMPANIES_FOR_TEST}")
        
        # Ejecutar análisis de prueba
        result = test_data_type_analysis()
        
        if result:
            print(f"\n🎯 ANÁLISIS COMPLETADO")
            print(f"📊 Campos sin conflicto: {len(result['field_consensus'])}")
            print(f"⚠️  Campos con conflicto: {len(result['type_conflicts'])}")
            
            if result['type_conflicts']:
                print(f"\n⚠️  CAMPOS CON CONFLICTOS DE TIPO:")
                for field_name, conflict in result['type_conflicts'].items():
                    print(f"  - {field_name}: {', '.join(conflict['types'])} → {conflict['consensus_type']}")
            
            return True
        else:
            print(f"\n❌ No se pudo completar el análisis")
            return False
            
    except Exception as e:
        print(f"💥 Error inesperado en main(): {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    main()
