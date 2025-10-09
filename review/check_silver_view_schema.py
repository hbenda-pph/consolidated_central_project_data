#!/usr/bin/env python3
"""
Script para verificar si las vistas Silver tienen company_project_id y company_id
"""

from google.cloud import bigquery

PROJECT_ID = "shape-mhs-1"  # Ejemplo de compañía
DATASET = "silver"
VIEW_NAME = "vw_appointment"

client = bigquery.Client()

query = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{VIEW_NAME}'
    ORDER BY ordinal_position
"""

print(f"🔍 Verificando schema de: {PROJECT_ID}.{DATASET}.{VIEW_NAME}")
print("=" * 80)

try:
    results = client.query(query).result()
    columns = [row.column_name for row in results]
    
    print(f"\n📋 Total de columnas: {len(columns)}\n")
    
    has_company_project_id = 'company_project_id' in columns
    has_company_id = 'company_id' in columns
    
    print("Primeras 10 columnas:")
    for i, col in enumerate(columns[:10], 1):
        marker = ""
        if col == 'company_project_id':
            marker = " ⚠️  <-- METADATA (debería estar solo en consolidada)"
        elif col == 'company_id':
            marker = " ⚠️  <-- METADATA (debería estar solo en consolidada)"
        print(f"  {i:2}. {col}{marker}")
    
    if len(columns) > 10:
        print(f"  ... y {len(columns) - 10} columnas más")
    
    print("\n" + "=" * 80)
    print("RESULTADO:")
    print("=" * 80)
    
    if has_company_project_id or has_company_id:
        print("❌ LA VISTA TIENE CAMPOS DE METADATA (INCORRECTO)")
        print(f"   company_project_id: {'SÍ' if has_company_project_id else 'NO'}")
        print(f"   company_id: {'SÍ' if has_company_id else 'NO'}")
        print("\n✅ SOLUCIÓN ACTUAL:")
        print("   Paso 3 usa: SELECT * EXCEPT(company_project_id, company_id)")
        print("   Esto evita duplicados temporalmente")
        print("\n🔮 SOLUCIÓN FUTURA:")
        print("   Eliminar estos campos del Paso 2 (generate_silver_views)")
        print("   y agregarlos solo en el Paso 3 (consolidación)")
    else:
        print("✅ LA VISTA NO TIENE CAMPOS DE METADATA (CORRECTO)")
        print("   Los campos company_project_id y company_id NO están presentes")
        print("\n✅ SOLUCIÓN ACTUAL:")
        print("   Paso 3 usa: SELECT company_project_id AS ..., company_id AS ..., *")
        print("   Esto agrega los campos correctamente")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")


