"""
Script para verificar el orden de procesamiento y por qué estimate_external_link no fue procesada
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE, DATASET_NAME, TABLES_TO_PROCESS

def debug_processing_order():
    """Investiga el orden de procesamiento"""
    
    client = bigquery.Client(project=PROJECT_SOURCE)
    
    print("🔍 INVESTIGACIÓN: Orden de procesamiento")
    print("=" * 50)
    
    # 1. Mostrar TABLES_TO_PROCESS en orden
    print(f"📋 TABLES_TO_PROCESS (orden de procesamiento):")
    for i, table in enumerate(TABLES_TO_PROCESS, 1):
        print(f"   {i:2d}. {table}")
    
    # 2. Verificar dónde está estimate_external_link
    if 'estimate_external_link' in TABLES_TO_PROCESS:
        position = TABLES_TO_PROCESS.index('estimate_external_link') + 1
        print(f"\n🎯 estimate_external_link está en posición {position} de {len(TABLES_TO_PROCESS)}")
        
        # 3. Verificar qué tablas fueron procesadas
        print(f"\n🔍 Verificando qué tablas fueron procesadas...")
        
        processed_query = f"""
        SELECT DISTINCT table_name, COUNT(*) as company_count
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.companies_consolidated`
        GROUP BY table_name
        ORDER BY table_name
        """
        
        processed_df = client.query(processed_query).to_dataframe()
        
        print(f"📊 Tablas procesadas ({len(processed_df)}):")
        for _, row in processed_df.iterrows():
            print(f"   ✅ {row['table_name']} ({row['company_count']} compañías)")
        
        # 4. Encontrar la última tabla procesada
        if not processed_df.empty:
            last_processed = processed_df.iloc[-1]['table_name']
            print(f"\n🔚 Última tabla procesada: {last_processed}")
            
            # Verificar si estimate_external_link viene después
            try:
                last_position = TABLES_TO_PROCESS.index(last_processed) + 1
                estimate_position = TABLES_TO_PROCESS.index('estimate_external_link') + 1
                
                if estimate_position > last_position:
                    print(f"🎯 estimate_external_link (posición {estimate_position}) viene DESPUÉS de {last_processed} (posición {last_position})")
                    print(f"   ✅ CONFIRMADO: El script se cayó antes de procesar estimate_external_link")
                else:
                    print(f"⚠️  estimate_external_link (posición {estimate_position}) viene ANTES de {last_processed} (posición {last_position})")
                    print(f"   ❌ PROBLEMA: Debería haber sido procesada")
                    
            except ValueError as e:
                print(f"⚠️  Error verificando posiciones: {str(e)}")
        
        # 5. Verificar si hay errores en el procesamiento de estimate_external_link
        print(f"\n🔍 Verificando si hay errores específicos...")
        
        # Buscar en logs o errores
        error_query = f"""
        SELECT 
          company_id,
          table_name,
          consolidated_status,
          error_message,
          created_at,
          updated_at
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.companies_consolidated`
        WHERE table_name = 'estimate_external_link'
        ORDER BY company_id
        """
        
        error_df = client.query(error_query).to_dataframe()
        
        if error_df.empty:
            print("❌ No hay registros de estimate_external_link en companies_consolidated")
            print("   Esto confirma que el script se cayó antes de procesarla")
        else:
            print(f"✅ Hay registros de estimate_external_link:")
            print(error_df.to_string(index=False))
    
    else:
        print("❌ estimate_external_link NO está en TABLES_TO_PROCESS")
        print("   Necesitas agregarlo a config.py")

if __name__ == "__main__":
    debug_processing_order()
