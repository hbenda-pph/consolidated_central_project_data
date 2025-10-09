"""
Script para investigar el error específico con estimate_external_link
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE, DATASET_NAME

def debug_estimate_error():
    """Investiga el error específico con estimate_external_link"""
    
    client = bigquery.Client(project=PROJECT_SOURCE)
    
    print("🔍 INVESTIGACIÓN: Error específico con estimate_external_link")
    print("=" * 60)
    
    # 1. Obtener compañías que tienen la tabla
    companies_query = f"""
    SELECT company_id, company_name, company_project_id
    FROM `{PROJECT_SOURCE}.{DATASET_NAME}.companies`
    WHERE company_fivetran_status = TRUE 
      AND company_bigquery_status = TRUE
      AND company_project_id IS NOT NULL
    ORDER BY company_id
    """
    
    companies_df = client.query(companies_query).to_dataframe()
    
    print(f"📋 Compañías a verificar: {len(companies_df)}")
    
    # 2. Verificar cada compañía que tiene estimate_external_link
    companies_with_estimate = []
    
    for _, company in companies_df.iterrows():
        project_id = company['company_project_id']
        company_name = company['company_name']
        company_id = company['company_id']
        
        try:
            dataset_name = f"servicetitan_{project_id.replace('-, '_')}"
            
            # Verificar si existe la tabla
            check_query = f"""
            SELECT table_name
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = 'estimate_external_link'
            """
            
            result = client.query(check_query).to_dataframe()
            
            if not result.empty:
                companies_with_estimate.append({
                    'company_id': company_id,
                    'company_name': company_name,
                    'project_id': project_id,
                    'dataset_name': dataset_name
                })
                print(f"  ✅ {company_name} ({company_id}): Tabla existe en {dataset_name}")
                
        except Exception as e:
            print(f"  ⚠️  {company_name} ({company_id}): Error verificando existencia - {str(e)}")
    
    print(f"\n📊 Compañías con estimate_external_link: {len(companies_with_estimate)}")
    
    # 3. Intentar replicar el análisis que hace generate_silver_views.py
    print(f"\n🔍 Intentando replicar el análisis de generate_silver_views.py...")
    
    for comp in companies_with_estimate:
        print(f"\n--- Analizando {comp['company_name']} ({comp['company_id']}) ---")
        
        try:
            # Obtener campos de la tabla (como lo hace el script)
            fields_query = f"""
            SELECT 
              column_name,
              data_type,
              is_nullable
            FROM `{comp['project_id']}.{comp['dataset_name']}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'estimate_external_link'
            ORDER BY ordinal_position
            """
            
            fields_df = client.query(fields_query).to_dataframe()
            
            if fields_df.empty:
                print(f"  ❌ No se pudieron obtener campos de la tabla")
                continue
            
            print(f"  ✅ Campos obtenidos: {len(fields_df)}")
            
            # Filtrar campos _fivetran (como lo hace el script)
            filtered_fields = fields_df[~fields_df['column_name'].str.startswith('_fivetran')]
            print(f"  ✅ Campos filtrados (sin _fivetran): {len(filtered_fields)}")
            
            # Mostrar algunos campos como ejemplo
            if len(filtered_fields) > 0:
                print(f"  📋 Primeros 5 campos:")
                for _, field in filtered_fields.head().iterrows():
                    print(f"     - {field['column_name']} ({field['data_type']})")
            
            # Intentar generar SQL de vista (como lo hace el script)
            print(f"  🔄 Intentando generar SQL de vista...")
            
            # Crear SQL básico
            sql_fields = []
            for _, field in filtered_fields.iterrows():
                sql_fields.append(f"    {field['column_name']}")
            
            sql_content = f"""
CREATE OR REPLACE VIEW `{comp['project_id']}.silver.vw_estimate_external_link` AS
SELECT
{',\n'.join(sql_fields)}
FROM `{comp['project_id']}.{comp['dataset_name']}.estimate_external_link`
"""
            
            print(f"  ✅ SQL generado exitosamente ({len(sql_fields)} campos)")
            
            # Intentar ejecutar la vista (solo para probar)
            print(f"  🔄 Probando ejecución de vista...")
            try:
                query_job = client.query(sql_content)
                query_job.result()  # Esperar a que termine
                print(f"  ✅ Vista creada exitosamente")
                
                # Limpiar la vista de prueba
                cleanup_sql = f"DROP VIEW IF EXISTS `{comp['project_id']}.silver.vw_estimate_external_link`"
                client.query(cleanup_sql).result()
                print(f"  🧹 Vista de prueba eliminada")
                
            except Exception as e:
                print(f"  ❌ Error ejecutando vista: {str(e)}")
            
        except Exception as e:
            print(f"  ❌ Error en análisis: {str(e)}")
    
    print(f"\n🎯 CONCLUSIÓN:")
    print(f"   Si todas las pruebas pasaron, el problema podría ser:")
    print(f"   1. Error de conexión durante el procesamiento original")
    print(f"   2. Problema de timing/concurrencia")
    print(f"   3. Error en la función analyze_table_fields_across_companies()")
    print(f"   4. El script se cayó justo antes de procesar esta tabla")

if __name__ == "__main__":
    debug_estimate_error()
