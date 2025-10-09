#!/usr/bin/env python3
"""
Script de diagnóstico para verificar detección de conflictos de tipos
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE

client = bigquery.Client(project=PROJECT_SOURCE)

def get_companies_info():
    """Obtiene lista de compañías activas"""
    query = f"""
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
    
    query_job = client.query(query)
    results = query_job.result()
    return pd.DataFrame([dict(row) for row in results])

def analyze_specific_table(table_name):
    """Analiza tipos de datos de una tabla específica"""
    print(f"\n{'='*80}")
    print(f"DIAGNÓSTICO: Analizando tabla '{table_name}'")
    print(f"{'='*80}\n")
    
    companies_df = get_companies_info()
    print(f"✅ Total de compañías activas: {len(companies_df)}\n")
    
    field_types = {}
    companies_with_table = []
    
    for _, company in companies_df.iterrows():
        company_name = company['company_name']
        project_id = company['company_project_id']
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        
        try:
            query = f"""
                SELECT
                    column_name,
                    data_type
                FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                  AND column_name NOT LIKE '_fivetran%'
                ORDER BY ordinal_position
            """
            
            query_job = client.query(query)
            results = query_job.result()
            fields_df = pd.DataFrame([dict(row) for row in results])
            
            if not fields_df.empty:
                companies_with_table.append(company_name)
                print(f"✅ {company_name} ({project_id})")
                
                for _, field in fields_df.iterrows():
                    field_name = field['column_name']
                    data_type = field['data_type']
                    
                    if field_name not in field_types:
                        field_types[field_name] = {}
                    
                    if data_type not in field_types[field_name]:
                        field_types[field_name][data_type] = []
                    
                    field_types[field_name][data_type].append(company_name)
            else:
                print(f"⚠️  {company_name}: Tabla no encontrada")
                
        except Exception as e:
            print(f"❌ {company_name}: Error - {str(e)}")
    
    print(f"\n{'='*80}")
    print(f"RESUMEN DE ANÁLISIS")
    print(f"{'='*80}\n")
    print(f"📊 Compañías con la tabla: {len(companies_with_table)}")
    print(f"📋 Campos analizados: {len(field_types)}\n")
    
    # Detectar conflictos
    conflicts_found = []
    
    print(f"{'='*80}")
    print(f"CONFLICTOS DE TIPO DETECTADOS")
    print(f"{'='*80}\n")
    
    for field_name, types_dict in field_types.items():
        if len(types_dict) > 1:
            # HAY CONFLICTO
            conflicts_found.append(field_name)
            print(f"⚠️  CAMPO: {field_name}")
            print(f"   Tipos encontrados: {list(types_dict.keys())}")
            print(f"   → CONSENSUS: STRING (por regla)")
            
            for data_type, companies in types_dict.items():
                print(f"      {data_type}: {len(companies)} compañías")
                for company in companies[:3]:  # Mostrar primeras 3
                    print(f"         - {company}")
                if len(companies) > 3:
                    print(f"         ... y {len(companies) - 3} más")
            print()
    
    if not conflicts_found:
        print("✅ NO SE ENCONTRARON CONFLICTOS DE TIPO\n")
    else:
        print(f"\n{'='*80}")
        print(f"TOTAL DE CONFLICTOS: {len(conflicts_found)}")
        print(f"{'='*80}\n")
        print("Campos con conflicto:")
        for field_name in conflicts_found:
            types = list(field_types[field_name].keys())
            print(f"  - {field_name}: {types}")
    
    return field_types, conflicts_found

if __name__ == "__main__":
    import sys
    
    # Tabla a analizar (por defecto: appointment)
    table_name = sys.argv[1] if len(sys.argv) > 1 else "appointment"
    
    print(f"\n🔍 SCRIPT DE DIAGNÓSTICO - DETECCIÓN DE CONFLICTOS")
    print(f"   Proyecto: {PROJECT_SOURCE}")
    print(f"   Tabla: {table_name}")
    
    field_types, conflicts = analyze_specific_table(table_name)
    
    if conflicts:
        print(f"\n⚠️  ACCIÓN REQUERIDA:")
        print(f"   Los campos listados arriba tienen conflictos de tipo.")
        print(f"   Al ejecutar generate_silver_views_job.py, estos campos")
        print(f"   DEBEN convertirse a STRING en TODAS las compañías.")
        print(f"\n🔍 Verifica los logs del Job para confirmar que esto sucede.")
    else:
        print(f"\n✅ La tabla '{table_name}' no tiene conflictos de tipos.")
        print(f"   Todas las compañías tienen tipos consistentes.")

