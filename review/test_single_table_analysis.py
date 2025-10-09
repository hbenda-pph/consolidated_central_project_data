# -*- coding: utf-8 -*-
"""
Test Single Table Analysis - Consolidated Central Project Data

Script de prueba para analizar una sola tabla y generar su vista Silver.
Útil para probar la lógica antes de ejecutar el script completo.
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import warnings
from collections import defaultdict, Counter
warnings.filterwarnings('ignore')

# Configuración
PROJECT_SOURCE = "platform-partners-qua"
DATASET_NAME = "settings"
TABLE_NAME = "companies"
TEST_TABLE = "call"  # Tabla de prueba
MAX_COMPANIES_FOR_TEST = 5

# Crear cliente BigQuery
client = bigquery.Client(project=PROJECT_SOURCE)
print(f"✅ Cliente BigQuery creado para proyecto: {PROJECT_SOURCE}")

def get_companies_info():
    """Obtiene información de las compañías activas"""
    query = f"""
        SELECT company_id, company_name, company_project_id
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status IS NOT NULL
        ORDER BY company_id
        LIMIT {MAX_COMPANIES_FOR_TEST}  # Solo las primeras 5 para prueba
    """
    return pd.DataFrame([dict(row) for row in client.query(query).result()])

def get_table_fields_with_types(project_id, table_name):
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    query = f"""
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    try:
        return pd.DataFrame([dict(row) for row in client.query(query).result()])
    except:
        return pd.DataFrame()

# Obtener compañías de prueba
companies = get_companies_info()
print(f"📋 Compañías de prueba: {len(companies)}")

# Analizar la tabla
results = []
all_fields = set()

for _, company in companies.iterrows():
    project_id = company['company_project_id']
    company_name = company['company_name']
    
    fields_df = get_table_fields_with_types(project_id, TEST_TABLE)
    
    if fields_df.empty:
        print(f"  ⚠️  {company_name}: Tabla '{TEST_TABLE}' no encontrada")
        continue
    
    # Filtrar campos _fivetran (campos del ETL que deben quedarse solo en Bronze)
    filtered_fields_df = fields_df[~fields_df['column_name'].str.startswith('_fivetran')]
    fields = filtered_fields_df['column_name'].tolist()
    all_fields.update(fields)
    
    results.append({
        'company_name': company_name,
        'project_id': project_id,
        'fields': fields,
        'field_count': len(fields),
        'fields_df': filtered_fields_df
    })
    
    print(f"  ✅ {company_name}: {len(fields)} campos")

if not results:
    print(f"❌ No se encontraron datos para la tabla '{TEST_TABLE}'")
    exit()

# Analizar campos comunes y únicos
field_frequency = Counter()
for result in results:
    field_frequency.update(result['fields'])

total_companies = len(results)
common_fields = []
partial_fields = []

for field, count in field_frequency.items():
    if count == total_companies:
        common_fields.append(field)
    else:
        partial_fields.append(field)

print(f"\n📊 ANÁLISIS DE CAMPOS PARA '{TEST_TABLE}':")
print(f"  Total de compañías: {total_companies}")
print(f"  Total de campos únicos: {len(all_fields)}")
print(f"  Campos comunes: {len(common_fields)}")
print(f"  Campos parciales: {len(partial_fields)}")

print(f"\n✅ CAMPOS COMUNES:")
for field in sorted(common_fields):
    print(f"    - {field}")

print(f"\n⚠️  CAMPOS PARCIALES:")
for field in sorted(partial_fields):
    count = field_frequency[field]
    print(f"    - {field}: {count}/{total_companies} compañías")

# Analizar tipos de datos
print(f"\n🔍 ANALIZANDO TIPOS DE DATOS...")
field_type_analysis = defaultdict(list)

for result in results:
    company_name = result['company_name']
    project_id = result['project_id']
    fields_df = result['fields_df']
    
    for _, field in fields_df.iterrows():
        field_name = field['column_name']
        data_type = field['data_type']
        
        field_type_analysis[field_name].append({
            'company_name': company_name,
            'project_id': project_id,
            'data_type': data_type
        })

# Detectar conflictos de tipo
type_conflicts = {}
field_consensus = {}

for field_name, type_info_list in field_type_analysis.items():
    unique_types = list(set([info['data_type'] for info in type_info_list]))
    
    if len(unique_types) > 1:
        type_conflicts[field_name] = {
            'types': unique_types,
            'companies': type_info_list
        }
    else:
        field_consensus[field_name] = {
            'type': unique_types[0],
            'companies': type_info_list
        }

print(f"\n📊 ANÁLISIS DE TIPOS:")
print(f"  Campos sin conflicto: {len(field_consensus)}")
print(f"  Campos con conflicto: {len(type_conflicts)}")

if type_conflicts:
    print(f"\n⚠️  CONFLICTOS DE TIPO:")
    for field_name, conflict in type_conflicts.items():
        print(f"    - {field_name}: {', '.join(conflict['types'])}")
        for info in conflict['companies']:
            print(f"        {info['company_name']}: {info['data_type']}")

# Generar SQL de ejemplo para la primera compañía
if results:
    first_company = results[0]
    company_fields = set(first_company['fields'])
    
    # Generar SQL
    sql_fields = []
    
    # Campos comunes
    for field in sorted(common_fields):
        sql_fields.append(f"    {field}")
    
    # Campos parciales con COALESCE
    for field in sorted(partial_fields):
        if field in company_fields:
            sql_fields.append(f"    {field}")
        else:
            sql_fields.append(f"    NULL as {field}")
    
    # Metadata fields
    sql_fields.append(f"    '{first_company['project_id']}' as source_project")
    sql_fields.append(f"    CURRENT_TIMESTAMP() as silver_processed_at")
    sql_fields.append(f"    '{first_company['company_name']}' as company_name")
    
    # Crear SQL
    project_id = first_company['project_id']
    company_name = first_company['company_name']
    dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
    
    sql = f"""-- Vista Silver para {company_name} - Tabla {TEST_TABLE}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE OR REPLACE VIEW `{project_id}.silver.vw_{TEST_TABLE}` AS (
SELECT
{chr(10).join(sql_fields)}
FROM `{project_id}.{dataset_name}.{TEST_TABLE}`
);
"""
    
    print(sql)
    
    # Guardar en archivo
    with open(f"test_{TEST_TABLE}_silver_view.sql", 'w') as f:
        f.write(sql)
    
    print(f"\n💾 SQL guardado en: test_{TEST_TABLE}_silver_view.sql")

print(f"\n✅ Prueba completada!")

def main():
    """Función principal para ejecutar análisis de prueba"""
    print("🧪 Ejecutando análisis de tabla individual...")
    # El código principal ya se ejecutó arriba
    return True

if __name__ == "__main__":
    main()
