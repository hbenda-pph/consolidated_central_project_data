#!/usr/bin/env python3
"""
Script para verificar dónde están los metadatos de consolidated_tables
"""

from google.cloud import bigquery

# Proyectos posibles
projects = [
    'platform-partners-des',
    'platform-partners-pro',
    'platform-partners-qua',
    'pph-central'
]

client = bigquery.Client()

print("🔍 BUSCANDO TABLA metadata_consolidated_tables")
print("=" * 80)

for project in projects:
    table_path = f"{project}.management.metadata_consolidated_tables"
    print(f"\n📋 Verificando: {table_path}")
    
    try:
        # Intentar contar registros
        query = f"""
            SELECT COUNT(*) as total
            FROM `{table_path}`
            WHERE is_active = TRUE
        """
        
        result = client.query(query).result()
        total = list(result)[0].total
        
        print(f"   ✅ ENCONTRADA: {total} tablas activas")
        
        # Mostrar primeras 5 tablas
        query2 = f"""
            SELECT table_name, partition_fields, cluster_fields
            FROM `{table_path}`
            WHERE is_active = TRUE
            ORDER BY table_name
            LIMIT 5
        """
        
        result2 = client.query(query2).result()
        print(f"   📋 Primeras 5 tablas:")
        for row in result2:
            print(f"      - {row.table_name}: partition={row.partition_fields}, cluster={row.cluster_fields}")
        
    except Exception as e:
        error_msg = str(e)
        if "Not found" in error_msg or "does not exist" in error_msg:
            print(f"   ❌ NO EXISTE")
        elif "Access Denied" in error_msg or "Permission denied" in error_msg:
            print(f"   ⚠️  SIN PERMISOS")
        else:
            print(f"   ❌ Error: {error_msg[:100]}")

print("\n" + "=" * 80)
print("💡 RECOMENDACIÓN:")
print("   Usa el proyecto donde encontraste la tabla con más registros")
print("   y actualiza PROJECT_CENTRAL en consolidated_tables_job.py")

