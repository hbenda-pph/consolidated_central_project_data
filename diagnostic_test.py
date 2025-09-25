# -*- coding: utf-8 -*-
"""
Diagnostic Test - Consolidated Central Project Data

Script de diagnóstico para verificar conectividad y configuración.
Ejecuta verificaciones básicas del sistema antes de procesar datos.
"""

import sys
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def test_import_config():
    """Prueba la importación de configuración"""
    print("1️⃣  Verificando importación de configuración...")
    try:
        import config
        print(f"   ✅ Configuración importada correctamente")
        print(f"   📋 Proyecto fuente: {config.PROJECT_SOURCE}")
        print(f"   📋 Dataset: {config.DATASET_NAME}")
        print(f"   📋 Tabla: {config.TABLE_NAME}")
        return True
    except Exception as e:
        print(f"   ❌ Error importando configuración: {e}")
        return False

def test_bigquery_client():
    """Prueba la creación del cliente BigQuery"""
    print("2️⃣  Verificando cliente BigQuery...")
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project="platform-partners-qua")
        print(f"   ✅ Cliente BigQuery creado correctamente")
        print(f"   📋 Proyecto: {client.project}")
        return True
    except Exception as e:
        print(f"   ❌ Error creando cliente BigQuery: {e}")
        return False

def test_companies_table_access():
    """Prueba el acceso a la tabla de compañías"""
    print("3️⃣  Verificando acceso a tabla de compañías...")
    try:
        import config
        from google.cloud import bigquery
        client = bigquery.Client(project=config.PROJECT_SOURCE)
        
        query = f"""
            SELECT COUNT(*) as total
            FROM `{config.PROJECT_SOURCE}.{config.DATASET_NAME}.{config.TABLE_NAME}`
        """
        
        result = client.query(query).result()
        count = next(result).total
        
        print(f"   ✅ Acceso a tabla verificado")
        print(f"   📊 Total de compañías: {count}")
        return True
    except Exception as e:
        print(f"   ❌ Error accediendo a tabla de compañías: {e}")
        return False

def test_companies_sample():
    """Prueba obtención de muestra de compañías"""
    print("4️⃣  Verificando muestra de compañías...")
    try:
        import config
        from google.cloud import bigquery
        client = bigquery.Client(project=config.PROJECT_SOURCE)
        
        query = f"""
            SELECT company_id, company_name, company_project_id
            FROM `{config.PROJECT_SOURCE}.{config.DATASET_NAME}.{config.TABLE_NAME}`
            WHERE company_bigquery_status IS NOT NULL
            LIMIT 3
        """
        
        result = client.query(query).result()
        companies = list(result)
        
        print(f"   ✅ Muestra obtenida correctamente")
        print(f"   📊 Compañías encontradas: {len(companies)}")
        
        for company in companies:
            print(f"      - {company.company_name} ({company.company_project_id})")
        
        return True
    except Exception as e:
        print(f"   ❌ Error obteniendo muestra de compañías: {e}")
        return False

def test_company_tables_access():
    """Prueba acceso a tablas de una compañía"""
    print("5️⃣  Verificando acceso a tablas de compañía...")
    try:
        import config
        from google.cloud import bigquery
        client = bigquery.Client(project=config.PROJECT_SOURCE)
        
        # Obtener una compañía de prueba
        query = f"""
            SELECT company_project_id, company_name
            FROM `{config.PROJECT_SOURCE}.{config.DATASET_NAME}.{config.TABLE_NAME}`
            WHERE company_bigquery_status IS NOT NULL
            LIMIT 1
        """
        
        result = client.query(query).result()
        company = next(result)
        project_id = company.company_project_id
        company_name = company.company_name
        
        print(f"   📋 Compañía de prueba: {company_name}")
        print(f"   📋 Proyecto: {project_id}")
        
        # Verificar acceso a dataset
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        dataset_ref = client.dataset(dataset_name, project=project_id)
        
        try:
            dataset = client.get_dataset(dataset_ref)
            tables = list(client.list_tables(dataset))
            
            print(f"   ✅ Acceso a dataset verificado")
            print(f"   📊 Tablas encontradas: {len(tables)}")
            
            # Mostrar algunas tablas
            table_names = [table.table_id for table in tables[:5]]
            print(f"   📋 Primeras tablas: {', '.join(table_names)}")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error accediendo a dataset: {e}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error en prueba de acceso: {e}")
        return False

def test_dependencies():
    """Prueba dependencias de Python"""
    print("6️⃣  Verificando dependencias de Python...")
    
    required_modules = [
        'google.cloud.bigquery',
        'pandas',
        'numpy',
        'datetime'
    ]
    
    all_ok = True
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"   ✅ {module}")
        except ImportError:
            print(f"   ❌ {module} - No instalado")
            all_ok = False
    
    return all_ok

def generate_diagnostic_report(results):
    """Genera reporte de diagnóstico"""
    print("\n" + "="*60)
    print("📊 REPORTE DE DIAGNÓSTICO")
    print("="*60)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    success_rate = (passed_tests / total_tests) * 100
    
    print(f"Pruebas realizadas: {total_tests}")
    print(f"Pruebas exitosas: {passed_tests}")
    print(f"Pruebas fallidas: {total_tests - passed_tests}")
    print(f"Tasa de éxito: {success_rate:.1f}%")
    
    print(f"\nDetalles por prueba:")
    for test_name, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {test_name}")
    
    if success_rate == 100:
        print(f"\n🎯 ¡SISTEMA LISTO!")
        print(f"💡 Todas las verificaciones pasaron exitosamente.")
        print(f"🚀 Puedes proceder con el procesamiento de datos.")
    elif success_rate >= 80:
        print(f"\n⚠️  SISTEMA PARCIALMENTE LISTO")
        print(f"💡 La mayoría de verificaciones pasaron.")
        print(f"🔧 Revisa los errores antes de continuar.")
    else:
        print(f"\n❌ SISTEMA NO LISTO")
        print(f"💡 Múltiples errores detectados.")
        print(f"🔧 Corrige los problemas antes de continuar.")
    
    return success_rate

def main():
    """Función principal de diagnóstico"""
    print("🔍 DIAGNÓSTICO DEL SISTEMA")
    print("="*60)
    print("Este script verificará la conectividad y configuración")
    print("necesaria para ejecutar el proceso de consolidación.")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Ejecutar todas las pruebas
    results = {
        "Importación de configuración": test_import_config(),
        "Cliente BigQuery": test_bigquery_client(),
        "Acceso a tabla de compañías": test_companies_table_access(),
        "Muestra de compañías": test_companies_sample(),
        "Acceso a tablas de compañía": test_company_tables_access(),
        "Dependencias de Python": test_dependencies()
    }
    
    # Generar reporte
    success_rate = generate_diagnostic_report(results)
    
    # Guardar reporte
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"diagnostic_report_{timestamp}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("REPORTE DE DIAGNÓSTICO\n")
        f.write("="*60 + "\n")
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Tasa de éxito: {success_rate:.1f}%\n\n")
        
        for test_name, result in results.items():
            status = "PASS" if result else "FAIL"
            f.write(f"{status}: {test_name}\n")
    
    print(f"\n📄 Reporte guardado: {report_file}")
    
    return success_rate >= 80

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
