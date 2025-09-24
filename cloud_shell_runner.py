# -*- coding: utf-8 -*-
"""
Cloud Shell Runner - Consolidated Central Project Data

Script maestro para ejecutar todo el proceso de consolidación en Cloud Shell.
Incluye gestión de sesiones, rollback automático en caso de error y monitoreo.
"""

import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import subprocess

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import *
from execution_manager import ExecutionManager
from rollback_manager import RollbackManager
from monitoring_manager import MonitoringManager

class CloudShellRunner:
    """
    Ejecutor principal para Cloud Shell con gestión completa
    """
    
    def __init__(self):
        self.logger = self.setup_logging()
        self.execution_manager = None
        self.rollback_manager = None
        self.monitoring_manager = MonitoringManager()
        
        self.logger.info("🚀 CloudShellRunner iniciado")
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger(__name__)
    
    def check_prerequisites(self):
        """Verifica prerrequisitos antes de ejecutar"""
        self.logger.info("🔍 Verificando prerrequisitos...")
        
        checks = []
        
        # Verificar que estamos en Cloud Shell o con gcloud configurado
        try:
            result = subprocess.run(['gcloud', 'config', 'get-value', 'project'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                project = result.stdout.strip()
                checks.append(f"✅ Proyecto gcloud configurado: {project}")
            else:
                checks.append("❌ gcloud no configurado")
                return False
        except:
            checks.append("❌ gcloud no disponible")
            return False
        
        # Verificar que las dependencias están instaladas
        try:
            import google.cloud.bigquery
            import pandas
            checks.append("✅ Dependencias Python instaladas")
        except ImportError as e:
            checks.append(f"❌ Dependencias faltantes: {e}")
            return False
        
        # Verificar que el proyecto fuente existe y es accesible
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=PROJECT_SOURCE)
            # Probar una consulta simple
            query = f"SELECT COUNT(*) FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}` LIMIT 1"
            client.query(query).result()
            checks.append(f"✅ Proyecto fuente accesible: {PROJECT_SOURCE}")
        except Exception as e:
            checks.append(f"❌ Proyecto fuente no accesible: {e}")
            return False
        
        # Mostrar resultados
        for check in checks:
            self.logger.info(check)
        
        return all("✅" in check for check in checks)
    
    def run_test_analysis(self):
        """Ejecuta análisis de prueba con una tabla"""
        self.logger.info("🧪 Iniciando análisis de prueba")
        
        # Crear gestor de ejecución
        self.execution_manager = ExecutionManager("test_analysis")
        
        try:
            # Ejecutar análisis de tipos de datos
            success1, output1 = self.execution_manager.execute_safe(
                "python analyze_data_types.py",
                "Análisis de tipos de datos",
                "echo 'No rollback necesario para análisis'"
            )
            
            if not success1:
                self.logger.error("❌ Análisis de tipos falló")
                return False
            
            # Ejecutar análisis de tabla individual
            success2, output2 = self.execution_manager.execute_safe(
                "python test_single_table_analysis.py",
                "Análisis de tabla individual",
                "echo 'No rollback necesario para análisis'"
            )
            
            if not success2:
                self.logger.error("❌ Análisis de tabla individual falló")
                return False
            
            self.logger.info("✅ Análisis de prueba completado exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"💥 Error en análisis de prueba: {str(e)}")
            return False
        finally:
            if self.execution_manager:
                self.execution_manager.print_session_summary()
                self.execution_manager.cleanup_session()
    
    def run_silver_generation(self):
        """Ejecuta generación de vistas Silver"""
        self.logger.info("🔄 Iniciando generación de vistas Silver")
        
        # Crear gestor de ejecución
        self.execution_manager = ExecutionManager("silver_generation")
        
        try:
            # Ejecutar generación de vistas Silver
            success, output = self.execution_manager.execute_safe(
                "python generate_silver_views.py",
                "Generación de vistas Silver",
                "python rollback_manager.py silver --execute"
            )
            
            if not success:
                self.logger.error("❌ Generación de vistas Silver falló")
                self.logger.info("🔄 Iniciando rollback automático...")
                self.auto_rollback("silver")
                return False
            
            self.logger.info("✅ Generación de vistas Silver completada")
            
            # Validar vistas creadas
            self.logger.info("🔍 Validando vistas Silver creadas...")
            self.validate_silver_views()
            
            return True
            
        except Exception as e:
            self.logger.error(f"💥 Error en generación Silver: {str(e)}")
            self.auto_rollback("silver")
            return False
        finally:
            if self.execution_manager:
                self.execution_manager.print_session_summary()
                self.execution_manager.cleanup_session()
    
    def run_consolidated_generation(self):
        """Ejecuta generación de vistas consolidadas"""
        self.logger.info("🔄 Iniciando generación de vistas consolidadas")
        
        # Crear gestor de ejecución
        self.execution_manager = ExecutionManager("consolidated_generation")
        
        try:
            # Ejecutar generación de vistas consolidadas
            success, output = self.execution_manager.execute_safe(
                "python generate_central_consolidated_views.py",
                "Generación de vistas consolidadas",
                "python rollback_manager.py consolidated --execute"
            )
            
            if not success:
                self.logger.error("❌ Generación de vistas consolidadas falló")
                self.logger.info("🔄 Iniciando rollback automático...")
                self.auto_rollback("consolidated")
                return False
            
            self.logger.info("✅ Generación de vistas consolidadas completada")
            
            # Validar vistas creadas
            self.logger.info("🔍 Validando vistas consolidadas creadas...")
            self.validate_consolidated_views()
            
            return True
            
        except Exception as e:
            self.logger.error(f"💥 Error en generación consolidada: {str(e)}")
            self.auto_rollback("consolidated")
            return False
        finally:
            if self.execution_manager:
                self.execution_manager.print_session_summary()
                self.execution_manager.cleanup_session()
    
    def run_full_process(self):
        """Ejecuta el proceso completo"""
        self.logger.info("🚀 Iniciando proceso completo de consolidación")
        
        steps = [
            ("Análisis de tipos de datos", self.run_test_analysis),
            ("Generación de vistas Silver", self.run_silver_generation),
            ("Generación de vistas consolidadas", self.run_consolidated_generation)
        ]
        
        for step_name, step_function in steps:
            self.logger.info(f"📋 Ejecutando paso: {step_name}")
            
            if not step_function():
                self.logger.error(f"❌ Paso falló: {step_name}")
                self.logger.info("🔄 Iniciando rollback completo...")
                self.auto_rollback("all")
                return False
            
            self.logger.info(f"✅ Paso completado: {step_name}")
        
        self.logger.info("🎯 Proceso completo ejecutado exitosamente")
        return True
    
    def auto_rollback(self, rollback_type):
        """Ejecuta rollback automático"""
        try:
            self.rollback_manager = RollbackManager()
            
            if rollback_type == "silver":
                success = self.rollback_manager.rollback_silver_views(dry_run=False)
            elif rollback_type == "consolidated":
                success = self.rollback_manager.rollback_consolidated_views(dry_run=False)
            elif rollback_type == "all":
                success = self.rollback_manager.rollback_all(dry_run=False)
            else:
                self.logger.error(f"❌ Tipo de rollback desconocido: {rollback_type}")
                return False
            
            if success:
                self.logger.info("✅ Rollback automático completado exitosamente")
            else:
                self.logger.error("❌ Rollback automático falló")
            
            return success
            
        except Exception as e:
            self.logger.error(f"💥 Error en rollback automático: {str(e)}")
            return False
    
    def validate_silver_views(self):
        """Valida las vistas Silver creadas"""
        try:
            report = self.monitoring_manager.monitor_silver_views()
            self.monitoring_manager.print_monitoring_summary(report)
            return report['summary']['silver_errors'] == 0
        except Exception as e:
            self.logger.error(f"❌ Error validando vistas Silver: {str(e)}")
            return False
    
    def validate_consolidated_views(self):
        """Valida las vistas consolidadas creadas"""
        try:
            report = self.monitoring_manager.monitor_consolidated_views()
            self.monitoring_manager.print_monitoring_summary(report)
            return report['summary']['consolidated_errors'] == 0
        except Exception as e:
            self.logger.error(f"❌ Error validando vistas consolidadas: {str(e)}")
            return False
    
    def show_help(self):
        """Muestra ayuda del sistema"""
        help_text = """
🚀 CLOUD SHELL RUNNER - CONSOLIDATED CENTRAL PROJECT DATA

Sistema de gestión completa para consolidación de datos en BigQuery.

COMANDOS DISPONIBLES:
  test                    - Ejecutar análisis de prueba con una tabla
  silver                  - Generar vistas Silver (con rollback automático)
  consolidated            - Generar vistas consolidadas (con rollback automático)
  all                     - Ejecutar proceso completo (con rollback automático)
  validate                - Validar vistas existentes
  rollback <type>         - Ejecutar rollback (silver|consolidated|all)
  monitor                 - Monitorear estado de las vistas
  sessions                - Listar sesiones disponibles
  help                    - Mostrar esta ayuda

EJEMPLOS DE USO:
  python cloud_shell_runner.py test
  python cloud_shell_runner.py silver
  python cloud_shell_runner.py all
  python cloud_shell_runner.py validate
  python cloud_shell_runner.py rollback silver
  python cloud_shell_runner.py monitor

CARACTERÍSTICAS:
  ✅ Verificación automática de prerrequisitos
  ✅ Logging detallado de todas las operaciones
  ✅ Rollback automático en caso de error
  ✅ Validación de vistas creadas
  ✅ Monitoreo de estado del sistema
  ✅ Gestión de sesiones con historial

PRERREQUISITOS:
  - gcloud configurado y autenticado
  - Dependencias Python instaladas (requirements.txt)
  - Acceso al proyecto BigQuery fuente
  - Permisos para crear vistas en proyectos de compañías
        """
        print(help_text)

def main():
    """Función principal"""
    if len(sys.argv) < 2:
        runner = CloudShellRunner()
        runner.show_help()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    runner = CloudShellRunner()
    
    try:
        if command == "help":
            runner.show_help()
            
        elif command == "test":
            if not runner.check_prerequisites():
                runner.logger.error("❌ Prerrequisitos no cumplidos")
                sys.exit(1)
            runner.run_test_analysis()
            
        elif command == "silver":
            if not runner.check_prerequisites():
                runner.logger.error("❌ Prerrequisitos no cumplidos")
                sys.exit(1)
            runner.run_silver_generation()
            
        elif command == "consolidated":
            if not runner.check_prerequisites():
                runner.logger.error("❌ Prerrequisitos no cumplidos")
                sys.exit(1)
            runner.run_consolidated_generation()
            
        elif command == "all":
            if not runner.check_prerequisites():
                runner.logger.error("❌ Prerrequisitos no cumplidos")
                sys.exit(1)
            runner.run_full_process()
            
        elif command == "validate":
            runner.validate_silver_views()
            runner.validate_consolidated_views()
            
        elif command == "rollback":
            if len(sys.argv) < 3:
                print("❌ Debe especificar el tipo de rollback (silver|consolidated|all)")
                sys.exit(1)
            
            rollback_type = sys.argv[2].lower()
            runner.auto_rollback(rollback_type)
            
        elif command == "monitor":
            runner.monitoring_manager.print_monitoring_summary(
                runner.monitoring_manager.generate_monitoring_report()
            )
            
        elif command == "sessions":
            rollback_manager = RollbackManager()
            rollback_manager.print_sessions_list()
            
        else:
            print(f"❌ Comando desconocido: {command}")
            runner.show_help()
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"💥 Error inesperado: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
