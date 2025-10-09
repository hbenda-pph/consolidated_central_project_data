"""
Script para investigar por qué estimate_external_link no fue procesada
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE, DATASET_NAME

def debug_processing_issue():
    """Investiga por qué estimate_external_link no fue procesada"""
    
    client = bigquery.Client(project=PROJECT_SOURCE)
    
    print("🔍 INVESTIGACIÓN: ¿Por qué estimate_external_link no fue procesada?")
    print("=" * 70)
    
    # 1. Verificar si está en companies_consolidated
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
        print("   El script generate_silver_views.py nunca la procesó")
        
        # 2. Verificar si está en la lista de tablas a procesar (TABLES_TO_PROCESS)
        print(f"\n🔍 Verificando si está en TABLES_TO_PROCESS...")
        
        # Leer el archivo config.py para ver TABLES_TO_PROCESS
        try:
            with open('config.py', 'r') as f:
                config_content = f.read()
                
            if 'estimate_external_link' in config_content:
                print("✅ estimate_external_link SÍ está en TABLES_TO_PROCESS")
            else:
                print("❌ estimate_external_link NO está en TABLES_TO_PROCESS")
                print("   ¡AHÍ ESTÁ EL PROBLEMA!")
                print("   Necesitas agregar 'estimate_external_link' a TABLES_TO_PROCESS en config.py")
                
        except Exception as e:
            print(f"⚠️  Error leyendo config.py: {str(e)}")
        
        # 3. Verificar qué compañías tienen la tabla
        print(f"\n🔍 Verificando qué compañías tienen la tabla...")
        
        companies_query = f"""
        SELECT company_id, company_name, company_project_id
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.companies`
        WHERE company_fivetran_status = TRUE 
          AND company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
        """
        
        companies_df = client.query(companies_query).to_dataframe()
        
        companies_with_table = []
        
        for _, company in companies_df.iterrows():
            project_id = company['company_project_id']
            company_name = company['company_name']
            
            try:
                dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
                
                check_query = f"""
                SELECT table_name
                FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name = 'estimate_external_link'
                """
                
                result = client.query(check_query).to_dataframe()
                
                if not result.empty:
                    companies_with_table.append({
                        'company_id': company['company_id'],
                        'company_name': company_name,
                        'project_id': project_id,
                        'dataset_name': dataset_name
                    })
                    print(f"  ✅ {company_name} ({company['company_id']}): Tabla existe en {dataset_name}")
                    
            except Exception as e:
                print(f"  ⚠️  {company_name} ({company['company_id']}): Error - {str(e)}")
        
        print(f"\n📊 RESUMEN:")
        print(f"   - Compañías con estimate_external_link: {len(companies_with_table)}")
        print(f"   - Registros en companies_consolidated: 0")
        
        if len(companies_with_table) > 0:
            print(f"\n🎯 CONCLUSIÓN: estimate_external_link existe en {len(companies_with_table)} compañías pero NO fue procesada")
            print(f"   CAUSA PROBABLE: No está en TABLES_TO_PROCESS en config.py")
            print(f"\n💡 SOLUCIÓN:")
            print(f"   1. Agregar 'estimate_external_link' a TABLES_TO_PROCESS en config.py")
            print(f"   2. Re-ejecutar generate_silver_views.py")
            
            print(f"\n📋 Compañías afectadas:")
            for comp in companies_with_table:
                print(f"   - {comp['company_name']} (ID: {comp['company_id']})")
        
    else:
        print(f"✅ estimate_external_link SÍ está en companies_consolidated ({len(df1)} registros)")
        print("\n📋 Estados encontrados:")
        print(df1.to_string(index=False))
        
        # Verificar si hay errores
        errors = df1[df1['consolidated_status'] == 2]
        if not errors.empty:
            print(f"\n❌ Errores encontrados:")
            for _, error in errors.iterrows():
                print(f"   - {error['company_id']}: {error['error_message']}")
        else:
            print(f"\n✅ No hay errores registrados")

if __name__ == "__main__":
    debug_processing_issue()
