"""
Script para investigar el status de la tabla estimate_external_link
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE, DATASET_NAME

def debug_table_status():
    """Investiga el status de estimate_external_link"""
    
    client = bigquery.Client(project=PROJECT_SOURCE)
    
    print("🔍 INVESTIGACIÓN: estimate_external_link")
    print("=" * 60)
    
    # 1. Verificar si existe en companies_consolidated
    query1 = f"""
    SELECT 
      company_id,
      table_name,
      consolidated_status,
      created_at,
      updated_at,
      error_message,
      notes
    FROM `{PROJECT_SOURCE}.{DATASET_NAME}.companies_consolidated`
    WHERE table_name = 'estimate_external_link'
    ORDER BY company_id
    """
    
    df1 = client.query(query1).to_dataframe()
    
    if df1.empty:
        print("❌ estimate_external_link NO está en companies_consolidated")
        print("   Esto significa que el script generate_silver_views.py nunca la procesó")
    else:
        print(f"✅ estimate_external_link SÍ está en companies_consolidated ({len(df1)} registros)")
        print("\n📋 Estados encontrados:")
        print(df1.to_string(index=False))
    
    # 2. Verificar si existe en algún proyecto de compañía
    print(f"\n🔍 Verificando existencia en proyectos de compañías...")
    
    # Obtener compañías
    companies_query = f"""
    SELECT company_id, company_name, company_project_id
    FROM `{PROJECT_SOURCE}.{DATASET_NAME}.companies`
    WHERE company_fivetran_status = TRUE 
      AND company_bigquery_status = TRUE
      AND company_project_id IS NOT NULL
    ORDER BY company_id
    """
    
    companies_df = client.query(companies_query).to_dataframe()
    
    table_exists_count = 0
    
    for _, company in companies_df.iterrows():
        project_id = company['company_project_id']
        company_name = company['company_name']
        
        try:
            # Construir nombre del dataset: servicetitan_<project_id> con guiones
            dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
            
            # Verificar si la tabla existe en el dataset servicetitan
            check_query = f"""
            SELECT table_name
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = 'estimate_external_link'
            """
            
            result = client.query(check_query).to_dataframe()
            
            if not result.empty:
                print(f"  ✅ {company_name} ({project_id}): Tabla existe en {dataset_name}")
                table_exists_count += 1
            else:
                print(f"  ❌ {company_name} ({project_id}): Tabla NO existe en {dataset_name}")
                
        except Exception as e:
            print(f"  ⚠️  {company_name} ({project_id}): Error verificando - {str(e)}")
    
    print(f"\n📊 RESUMEN:")
    print(f"   - Tablas encontradas: {table_exists_count}/{len(companies_df)} compañías")
    print(f"   - Registros en companies_consolidated: {len(df1)}")
    
    if table_exists_count == 0:
        print(f"\n🎯 CONCLUSIÓN: estimate_external_link NO existe en ninguna compañía")
        print(f"   - Es una tabla 'fantasma' que aparece en el análisis pero no en datos reales")
        print(f"   - Debería eliminarse de all_unique_tables")
    elif len(df1) == 0:
        print(f"\n🎯 CONCLUSIÓN: estimate_external_link existe pero no fue procesada")
        print(f"   - El script generate_silver_views.py debería procesarla")
    else:
        print(f"\n🎯 CONCLUSIÓN: estimate_external_link está siendo procesada correctamente")

if __name__ == "__main__":
    debug_table_status()
