"""
Script de prueba para crear una tabla consolidada
Permite probar el sistema con una tabla específica antes de ejecutar todas
"""

from consolidated_tables_create import ConsolidatedTableCreator
from config import TABLES_TO_PROCESS

def test_single_table(table_name):
    """Prueba la creación de una tabla consolidada específica"""
    print(f"🧪 PRUEBA: Creando tabla consolidada para {table_name}")
    print("=" * 60)
    
    creator = ConsolidatedTableCreator()
    
    # Crear solo una tabla
    success = creator.create_consolidated_table(table_name)
    
    if success:
        print(f"\n✅ PRUEBA EXITOSA: {table_name}")
        print("📋 La tabla se creó correctamente en pph-central.bronze")
        print("🔍 Verifica en BigQuery Console:")
        print(f"   - Proyecto: pph-central")
        print(f"   - Dataset: bronze")
        print(f"   - Tabla: consolidated_{table_name}")
    else:
        print(f"\n❌ PRUEBA FALLIDA: {table_name}")
        print("🔍 Revisa los errores anteriores")
    
    return success

def main():
    """Función principal de prueba"""
    print("🧪 SCRIPT DE PRUEBA - TABLA CONSOLIDADA")
    print("=" * 60)
    
    # Tablas disponibles para prueba
    available_tables = [table for table in TABLES_TO_PROCESS if table >= 'i']
    
    print(f"📋 Tablas disponibles para prueba ({len(available_tables)}):")
    for i, table in enumerate(available_tables[:10], 1):  # Mostrar solo las primeras 10
        print(f"   {i:2d}. {table}")
    
    if len(available_tables) > 10:
        print(f"   ... y {len(available_tables) - 10} más")
    
    # Probar con la primera tabla disponible
    test_table = available_tables[0] if available_tables else 'inventory_bill'
    
    print(f"\n🎯 Probando con tabla: {test_table}")
    
    # Ejecutar prueba
    success = test_single_table(test_table)
    
    if success:
        print(f"\n🚀 LISTO PARA EJECUTAR TODAS LAS TABLAS")
        print("💡 Si la prueba fue exitosa, puedes ejecutar:")
        print("   python consolidated_tables_create.py")
    else:
        print(f"\n⚠️  REVISAR ERRORES ANTES DE CONTINUAR")
        print("💡 Corrige los errores antes de ejecutar todas las tablas")

if __name__ == "__main__":
    main()


