# -*- coding: utf-8 -*-
"""
Run All Scripts - Consolidated Central Project Data

Script maestro que ejecuta todo el proceso de generación de vistas Silver
y consolidadas usando la configuración centralizada.
"""

import sys
import os
from datetime import datetime
import logging

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import *
from generate_silver_views import generate_all_silver_views
from generate_central_consolidated_views import generate_all_consolidated_views

# Configurar logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(f'consolidation_log_{datetime.now().strftime(TIMESTAMP_FORMAT)}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """
    Función principal que ejecuta todo el proceso
    """
    logger.info("🚀 INICIANDO PROCESO COMPLETO DE CONSOLIDACIÓN")
    logger.info("=" * 80)
    
    try:
        # Validar configuración
        logger.info("1️⃣ Validando configuración...")
        validate_config()
        logger.info("✅ Configuración validada correctamente")
        
        # Mostrar configuración
        logger.info(f"📋 Configuración:")
        logger.info(f"   Proyecto fuente: {PROJECT_SOURCE}")
        logger.info(f"   Proyecto central: {CENTRAL_PROJECT}")
        logger.info(f"   Tablas a procesar: {len(TABLES_TO_PROCESS)}")
        
        # Paso 1: Generar vistas Silver
        logger.info("\n2️⃣ Generando vistas Silver...")
        logger.info("=" * 50)
        silver_results, silver_output_dir = generate_all_silver_views()
        logger.info(f"✅ Vistas Silver generadas en: {silver_output_dir}")
        
        # Paso 2: Generar vistas consolidadas
        logger.info("\n3️⃣ Generando vistas consolidadas...")
        logger.info("=" * 50)
        consolidated_output_dir, consolidated_files = generate_all_consolidated_views()
        logger.info(f"✅ Vistas consolidadas generadas en: {consolidated_output_dir}")
        
        # Resumen final
        logger.info("\n🎯 PROCESO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info(f"📊 Resumen:")
        logger.info(f"   Tablas procesadas: {len(silver_results)}")
        logger.info(f"   Vistas Silver: {silver_output_dir}")
        logger.info(f"   Vistas consolidadas: {consolidated_output_dir}")
        logger.info(f"   Archivos generados: {len(consolidated_files)}")
        
        # Instrucciones finales
        logger.info(f"\n📋 PRÓXIMOS PASOS:")
        logger.info(f"1. Revisar archivos en: {silver_output_dir}")
        logger.info(f"2. Revisar archivos en: {consolidated_output_dir}")
        logger.info(f"3. Ejecutar vistas Silver en cada proyecto de compañía")
        logger.info(f"4. Ejecutar vistas consolidadas en proyecto central: {CENTRAL_PROJECT}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante la ejecución: {str(e)}")
        logger.error("🔍 Revisa los logs para más detalles")
        return False

def run_test_mode():
    """
    Ejecuta el proceso en modo de prueba (solo algunas compañías)
    """
    logger.info("🧪 EJECUTANDO EN MODO DE PRUEBA")
    logger.info("=" * 50)
    
    # Importar y ejecutar script de prueba
    try:
        from test_single_table_analysis import *
        logger.info("✅ Prueba completada")
        return True
    except Exception as e:
        logger.error(f"❌ Error en modo de prueba: {str(e)}")
        return False

def show_help():
    """
    Muestra la ayuda del script
    """
    print("""
🔧 CONSOLIDATED CENTRAL PROJECT DATA - Script Maestro

Uso:
    python run_all.py [comando]

Comandos:
    all         - Ejecuta todo el proceso (vistas Silver + consolidadas)
    silver      - Solo genera vistas Silver
    consolidated - Solo genera vistas consolidadas  
    test        - Ejecuta modo de prueba
    config      - Muestra la configuración actual
    help        - Muestra esta ayuda

Ejemplos:
    python run_all.py all
    python run_all.py test
    python run_all.py config
""")

def show_config():
    """
    Muestra la configuración actual
    """
    print("📋 CONFIGURACIÓN ACTUAL:")
    print("=" * 50)
    print(f"Proyecto fuente: {PROJECT_SOURCE}")
    print(f"Proyecto central: {CENTRAL_PROJECT}")
    print(f"Dataset: {DATASET_NAME}.{TABLE_NAME}")
    print(f"Tablas a procesar: {len(TABLES_TO_PROCESS)}")
    print(f"Dataset Silver: {SILVER_DATASET}")
    print(f"Max compañías para prueba: {MAX_COMPANIES_FOR_TEST}")
    print(f"Nivel de logging: {LOG_LEVEL}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "all":
        success = main()
        sys.exit(0 if success else 1)
        
    elif command == "test":
        success = run_test_mode()
        sys.exit(0 if success else 1)
        
    elif command == "config":
        show_config()
        sys.exit(0)
        
    elif command == "help":
        show_help()
        sys.exit(0)
        
    elif command == "silver":
        logger.info("🔄 Generando solo vistas Silver...")
        try:
            silver_results, silver_output_dir = generate_all_silver_views()
            logger.info(f"✅ Vistas Silver generadas en: {silver_output_dir}")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            sys.exit(1)
            
    elif command == "consolidated":
        logger.info("🔄 Generando solo vistas consolidadas...")
        try:
            consolidated_output_dir, consolidated_files = generate_all_consolidated_views()
            logger.info(f"✅ Vistas consolidadas generadas en: {consolidated_output_dir}")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            sys.exit(1)
    
    else:
        print(f"❌ Comando desconocido: {command}")
        show_help()
        sys.exit(1)
