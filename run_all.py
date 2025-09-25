# -*- coding: utf-8 -*-
"""
Run All - Consolidated Central Project Data

Script maestro para ejecutar todo el proceso de consolidación.
Proporciona una interfaz unificada para diferentes comandos.
"""

import sys
import os
from datetime import datetime

def show_help():
    """Muestra ayuda del script"""
    print("🚀 CONSOLIDATED CENTRAL PROJECT DATA")
    print("="*50)
    print("Script maestro para ejecutar el proceso de consolidación")
    print()
    print("USO:")
    print("  python run_all.py <comando>")
    print()
    print("COMANDOS DISPONIBLES:")
    print("  test        - Ejecutar análisis de prueba (tabla individual)")
    print("  silver      - Generar vistas Silver para todas las tablas")
    print("  consolidated - Generar vistas consolidadas centrales")
    print("  all         - Ejecutar proceso completo")
    print("  config      - Mostrar configuración actual")
    print("  help        - Mostrar esta ayuda")
    print()
    print("EJEMPLOS:")
    print("  python run_all.py test")
    print("  python run_all.py silver")
    print("  python run_all.py all")
    print()

def show_config():
    """Muestra la configuración actual"""
    print("⚙️  CONFIGURACIÓN ACTUAL")
    print("="*50)
    
    try:
        import config
        
        print(f"Proyecto fuente: {config.PROJECT_SOURCE}")
        print(f"Proyecto central: {config.CENTRAL_PROJECT}")
        print(f"Dataset: {config.DATASET_NAME}")
        print(f"Tabla: {config.TABLE_NAME}")
        print(f"Campos metadata: {len(config.METADATA_FIELDS)}")
        
        if hasattr(config, 'MAX_COMPANIES_FOR_TEST'):
            print(f"Límite compañías (test): {config.MAX_COMPANIES_FOR_TEST}")
        
        print()
        print("Archivos de configuración encontrados:")
        config_files = ['config.py', 'requirements.txt']
        for file in config_files:
            if os.path.exists(file):
                print(f"  ✅ {file}")
            else:
                print(f"  ❌ {file}")
                
    except Exception as e:
        print(f"❌ Error cargando configuración: {e}")

def run_test():
    """Ejecuta análisis de prueba"""
    print("🧪 EJECUTANDO ANÁLISIS DE PRUEBA")
    print("="*50)
    
    try:
        import test_single_table_analysis
        result = test_single_table_analysis.main()
        
        if result:
            print("\n✅ Análisis de prueba completado exitosamente")
        else:
            print("\n❌ Análisis de prueba falló")
            
        return result
        
    except Exception as e:
        print(f"❌ Error ejecutando análisis de prueba: {e}")
        return False

def run_silver():
    """Ejecuta generación de vistas Silver"""
    print("🔧 GENERANDO VISTAS SILVER")
    print("="*50)
    
    try:
        import generate_silver_views
        result = generate_silver_views.generate_all_silver_views()
        
        if result:
            print("\n✅ Generación de vistas Silver completada")
        else:
            print("\n❌ Generación de vistas Silver falló")
            
        return result
        
    except Exception as e:
        print(f"❌ Error generando vistas Silver: {e}")
        return False

def run_consolidated():
    """Ejecuta generación de vistas consolidadas"""
    print("🔗 GENERANDO VISTAS CONSOLIDADAS")
    print("="*50)
    
    try:
        import generate_central_consolidated_views
        result = generate_central_consolidated_views.generate_all_consolidated_views()
        
        if result:
            print("\n✅ Generación de vistas consolidadas completada")
        else:
            print("\n❌ Generación de vistas consolidadas falló")
            
        return result
        
    except Exception as e:
        print(f"❌ Error generando vistas consolidadas: {e}")
        return False

def run_all():
    """Ejecuta proceso completo"""
    print("🚀 EJECUTANDO PROCESO COMPLETO")
    print("="*50)
    print("Este proceso ejecutará todas las etapas de consolidación")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    results = {}
    
    # Etapa 1: Análisis de prueba
    print("ETAPA 1: Análisis de prueba")
    results['test'] = run_test()
    print()
    
    if not results['test']:
        print("❌ Proceso detenido: Análisis de prueba falló")
        return False
    
    # Etapa 2: Generación Silver
    print("ETAPA 2: Generación de vistas Silver")
    results['silver'] = run_silver()
    print()
    
    if not results['silver']:
        print("❌ Proceso detenido: Generación Silver falló")
        return False
    
    # Etapa 3: Generación Consolidada
    print("ETAPA 3: Generación de vistas consolidadas")
    results['consolidated'] = run_consolidated()
    print()
    
    # Resumen final
    print("📊 RESUMEN DEL PROCESO COMPLETO")
    print("="*50)
    
    total_stages = len(results)
    successful_stages = sum(results.values())
    
    for stage, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {stage.upper()}")
    
    print(f"\nEtapas completadas: {successful_stages}/{total_stages}")
    print(f"Tasa de éxito: {(successful_stages/total_stages)*100:.1f}%")
    
    if successful_stages == total_stages:
        print(f"\n🎯 ¡PROCESO COMPLETADO EXITOSAMENTE!")
        print(f"💡 Todas las etapas se ejecutaron correctamente.")
    else:
        print(f"\n⚠️  PROCESO COMPLETADO CON ERRORES")
        print(f"💡 Revisa los errores arriba antes de continuar.")
    
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return successful_stages == total_stages

def main():
    """Función principal"""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if command == "help":
        show_help()
    elif command == "config":
        show_config()
    elif command == "test":
        run_test()
    elif command == "silver":
        run_silver()
    elif command == "consolidated":
        run_consolidated()
    elif command == "all":
        run_all()
    else:
        print(f"❌ Comando desconocido: {command}")
        print("💡 Usa 'python run_all.py help' para ver comandos disponibles")

if __name__ == "__main__":
    main()
