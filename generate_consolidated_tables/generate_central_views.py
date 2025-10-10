#!/usr/bin/env python3
"""
Paso 4: Crear vistas en silver del proyecto central
Simplemente expone las tablas consolidadas de bronze como vistas en silver
"""

from google.cloud import bigquery
from datetime import datetime

PROJECT_CENTRAL = "pph-central"
DATASET_BRONZE = "bronze"
DATASET_SILVER = "silver"

def create_central_views():
    """
    Crea vistas en silver que apuntan a las tablas consolidadas en bronze
    """
    client = bigquery.Client()
    
    # Obtener lista de tablas consolidadas
    tables_query = f"""
    SELECT table_name 
    FROM `{PROJECT_CENTRAL}.{DATASET_BRONZE}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'consolidated_%'
    ORDER BY table_name
    """
    
    tables = client.query(tables_query).result()
    total_tables = 0
    success_count = 0
    error_count = 0
    
    print("=" * 80)
    print("🚀 CREAR VISTAS SILVER CENTRALIZADAS")
    print("=" * 80)
    print()
    
    for row in tables:
        total_tables += 1
        table_name = row.table_name
        base_name = table_name.replace('consolidated_', '')
        view_name = f"vw_{base_name}"
        
        print(f"[{total_tables}] {view_name}")
        
        # Crear vista
        view_query = f"""
        /*
         * Vista Silver centralizada para {base_name}
         * Generada automáticamente
         */
        CREATE OR REPLACE VIEW `{PROJECT_CENTRAL}.{DATASET_SILVER}.{view_name}`
        AS 
        SELECT * FROM `{PROJECT_CENTRAL}.{DATASET_BRONZE}.{table_name}`
        """
        
        try:
            client.query(view_query).result()
            print(f"  ✅ Vista creada")
            success_count += 1
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:250]}")
            error_count += 1
    
    # Resumen
    print()
    print("=" * 80)
    print("🎯 RESUMEN FINAL")
    print("=" * 80)
    print(f"✅ Vistas creadas exitosamente: {success_count}")
    print(f"❌ Vistas con errores: {error_count}")
    print(f"📊 Total procesadas: {total_tables}")
    print(f"⏱️ Fecha fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    if error_count == 0:
        print("✨ ¡Todas las vistas se crearon exitosamente!")
    else:
        print("⚠️ Algunas vistas tuvieron errores. Revisa los logs arriba.")
    
    return success_count, error_count

if __name__ == "__main__":
    create_central_views()
