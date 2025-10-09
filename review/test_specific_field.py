#!/usr/bin/env python3
"""
Script para verificar tipos de UN campo específico en todas las compañías
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE

client = bigquery.Client(project=PROJECT_SOURCE)

def analyze_specific_field(table_name, field_name):
    """Analiza un campo específico en todas las compañías"""
    
    query = f"""
        SELECT
            company_name,
            company_project_id
        FROM `{PROJECT_SOURCE}.settings.companies`
        WHERE company_fivetran_status = TRUE
          AND company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
    """
    
    companies_df = pd.DataFrame([dict(row) for row in client.query(query).result()])
    
    print(f"\n{'='*80}")
    print(f"ANÁLISIS DE CAMPO ESPECÍFICO")
    print(f"{'='*80}")
    print(f"Tabla: {table_name}")
    print(f"Campo: {field_name}")
    print(f"{'='*80}\n")
    
    results = []
    
    for _, company in companies_df.iterrows():
        company_name = company['company_name']
        project_id = company['company_project_id']
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        
        try:
            query = f"""
                SELECT
                    data_type
                FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                  AND column_name = '{field_name}'
            """
            
            result = client.query(query).result()
            rows = list(result)
            
            if rows:
                data_type = rows[0].data_type
                results.append({
                    'company_name': company_name,
                    'project_id': project_id,
                    'data_type': data_type,
                    'has_field': True
                })
                print(f"✅ {company_name:30} → {data_type}")
            else:
                results.append({
                    'company_name': company_name,
                    'project_id': project_id,
                    'data_type': None,
                    'has_field': False
                })
                print(f"⚠️  {company_name:30} → CAMPO NO EXISTE")
                
        except Exception as e:
            print(f"❌ {company_name:30} → Error: {str(e)}")
    
    # Resumen
    print(f"\n{'='*80}")
    print(f"RESUMEN")
    print(f"{'='*80}\n")
    
    companies_with_field = [r for r in results if r['has_field']]
    companies_without_field = [r for r in results if not r['has_field']]
    
    print(f"📊 Total compañías: {len(companies_df)}")
    print(f"✅ Con el campo: {len(companies_with_field)}")
    print(f"⚠️  Sin el campo: {len(companies_without_field)}\n")
    
    if companies_with_field:
        types_dict = {}
        for r in companies_with_field:
            data_type = r['data_type']
            if data_type not in types_dict:
                types_dict[data_type] = []
            types_dict[data_type].append(r['company_name'])
        
        print(f"TIPOS DE DATOS ENCONTRADOS:\n")
        for data_type, companies in types_dict.items():
            print(f"  {data_type}: {len(companies)} compañías")
            for company in companies[:5]:
                print(f"    - {company}")
            if len(companies) > 5:
                print(f"    ... y {len(companies) - 5} más")
            print()
        
        if len(types_dict) > 1:
            print(f"⚠️  CONFLICTO DETECTADO: {list(types_dict.keys())}")
            print(f"✅ SOLUCIÓN: Convertir a STRING en TODAS las compañías\n")
        else:
            print(f"✅ NO HAY CONFLICTO: Todas usan {list(types_dict.keys())[0]}\n")

if __name__ == "__main__":
    import sys
    
    table_name = sys.argv[1] if len(sys.argv) > 1 else "appointment"
    field_name = sys.argv[2] if len(sys.argv) > 2 else "arrival_window_start"
    
    analyze_specific_field(table_name, field_name)

