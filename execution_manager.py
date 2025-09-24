# -*- coding: utf-8 -*-
"""
Execution Manager - Consolidated Central Project Data

Sistema de gestión de ejecución con logging detallado, monitoreo y rollback.
Permite ejecutar, monitorear y revertir operaciones de manera segura.
"""

import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import subprocess
from google.cloud import bigquery

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import *

class ExecutionManager:
    """
    Gestor de ejecución con capacidades de rollback
    """
    
    def __init__(self, session_name=None):
        self.session_name = session_name or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.session_dir = f"execution_sessions/{self.session_name}"
        self.log_file = f"{self.session_dir}/execution.log"
        self.operations_log = f"{self.session_dir}/operations.json"
        self.rollback_script = f"{self.session_dir}/rollback.sql"
        
        # Crear directorio de sesión
        os.makedirs(self.session_dir, exist_ok=True)
        
        # Configurar logging
        self.setup_logging()
        
        # Inicializar log de operaciones
        self.operations = []
        
        self.logger.info(f"🚀 ExecutionManager iniciado - Sesión: {self.session_name}")
        self.logger.info(f"📁 Directorio de sesión: {self.session_dir}")
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def log_operation(self, operation_type, details, status="STARTED"):
        """Registra una operación en el log de operaciones"""
        operation = {
            'timestamp': datetime.now().isoformat(),
            'type': operation_type,
            'details': details,
            'status': status,
            'session': self.session_name
        }
        self.operations.append(operation)
        
        # Guardar en archivo
        with open(self.operations_log, 'w', encoding='utf-8') as f:
            json.dump(self.operations, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"📝 Operación registrada: {operation_type} - {status}")
    
    def execute_safe(self, command, description, rollback_command=None):
        """
        Ejecuta un comando de manera segura con logging y rollback
        """
        self.log_operation("EXECUTION", {
            "command": command,
            "description": description,
            "rollback_command": rollback_command
        }, "STARTED")
        
        try:
            self.logger.info(f"🔄 Ejecutando: {description}")
            self.logger.info(f"📋 Comando: {command}")
            
            # Ejecutar comando
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log_operation("EXECUTION", {
                    "command": command,
                    "description": description,
                    "output": result.stdout
                }, "SUCCESS")
                
                self.logger.info(f"✅ Comando ejecutado exitosamente: {description}")
                return True, result.stdout
            
            else:
                self.log_operation("EXECUTION", {
                    "command": command,
                    "description": description,
                    "error": result.stderr,
                    "return_code": result.returncode
                }, "FAILED")
                
                self.logger.error(f"❌ Error en comando: {description}")
                self.logger.error(f"Error: {result.stderr}")
                return False, result.stderr
                
        except Exception as e:
            self.log_operation("EXECUTION", {
                "command": command,
                "description": description,
                "exception": str(e)
            }, "EXCEPTION")
            
            self.logger.error(f"💥 Excepción en comando: {description} - {str(e)}")
            return False, str(e)
    
    def generate_rollback_script(self):
        """Genera script de rollback basado en operaciones ejecutadas"""
        rollback_statements = []
        
        # Ordenar operaciones por timestamp (más reciente primero)
        sorted_operations = sorted(self.operations, 
                                 key=lambda x: x['timestamp'], 
                                 reverse=True)
        
        for op in sorted_operations:
            if op['type'] == 'VIEW_CREATION' and op['status'] == 'SUCCESS':
                details = op['details']
                rollback_stmt = f"-- Rollback para vista creada en {op['timestamp']}\n"
                rollback_stmt += f"DROP VIEW IF EXISTS `{details['project_id']}.{details['dataset']}.{details['view_name']}`;\n"
                rollback_statements.append(rollback_stmt)
        
        # Escribir script de rollback
        with open(self.rollback_script, 'w', encoding='utf-8') as f:
            f.write(f"-- Script de Rollback para sesión: {self.session_name}\n")
            f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Total de operaciones: {len(self.operations)}\n\n")
            
            for stmt in rollback_statements:
                f.write(stmt + "\n")
        
        self.logger.info(f"📄 Script de rollback generado: {self.rollback_script}")
        return rollback_statements
    
    def create_view_with_rollback(self, project_id, dataset, view_name, sql_content, description):
        """Crea una vista con registro para rollback"""
        full_view_name = f"`{project_id}.{dataset}.{view_name}`"
        
        # Registrar operación
        self.log_operation("VIEW_CREATION", {
            "project_id": project_id,
            "dataset": dataset,
            "view_name": view_name,
            "description": description,
            "sql_preview": sql_content[:200] + "..." if len(sql_content) > 200 else sql_content
        }, "STARTED")
        
        try:
            # Crear cliente BigQuery
            client = bigquery.Client(project=project_id)
            
            # Ejecutar SQL
            query_job = client.query(sql_content)
            query_job.result()  # Esperar a que termine
            
            self.log_operation("VIEW_CREATION", {
                "project_id": project_id,
                "dataset": dataset,
                "view_name": view_name,
                "description": description
            }, "SUCCESS")
            
            self.logger.info(f"✅ Vista creada exitosamente: {full_view_name}")
            return True
            
        except Exception as e:
            self.log_operation("VIEW_CREATION", {
                "project_id": project_id,
                "dataset": dataset,
                "view_name": view_name,
                "description": description,
                "error": str(e)
            }, "FAILED")
            
            self.logger.error(f"❌ Error creando vista {full_view_name}: {str(e)}")
            return False
    
    def validate_view_exists(self, project_id, dataset, view_name):
        """Valida que una vista existe y es accesible"""
        try:
            client = bigquery.Client(project=project_id)
            table_ref = client.dataset(dataset).table(view_name)
            table = client.get_table(table_ref)
            
            self.logger.info(f"✅ Vista validada: {project_id}.{dataset}.{view_name}")
            return True, f"Vista existe con {len(table.schema)} campos"
            
        except Exception as e:
            self.logger.error(f"❌ Error validando vista: {project_id}.{dataset}.{view_name} - {str(e)}")
            return False, str(e)
    
    def get_session_summary(self):
        """Obtiene resumen de la sesión"""
        total_ops = len(self.operations)
        successful_ops = len([op for op in self.operations if op['status'] == 'SUCCESS'])
        failed_ops = len([op for op in self.operations if op['status'] == 'FAILED'])
        
        summary = {
            'session_name': self.session_name,
            'start_time': self.operations[0]['timestamp'] if self.operations else None,
            'end_time': datetime.now().isoformat(),
            'total_operations': total_ops,
            'successful_operations': successful_ops,
            'failed_operations': failed_ops,
            'success_rate': (successful_ops / total_ops * 100) if total_ops > 0 else 0,
            'session_dir': self.session_dir,
            'log_file': self.log_file,
            'operations_log': self.operations_log,
            'rollback_script': self.rollback_script
        }
        
        return summary
    
    def print_session_summary(self):
        """Imprime resumen de la sesión"""
        summary = self.get_session_summary()
        
        print(f"\n📊 RESUMEN DE SESIÓN: {summary['session_name']}")
        print("=" * 60)
        print(f"⏱️  Inicio: {summary['start_time']}")
        print(f"⏱️  Fin: {summary['end_time']}")
        print(f"📋 Total operaciones: {summary['total_operations']}")
        print(f"✅ Exitosas: {summary['successful_operations']}")
        print(f"❌ Fallidas: {summary['failed_operations']}")
        print(f"📈 Tasa de éxito: {summary['success_rate']:.1f}%")
        print(f"📁 Directorio: {summary['session_dir']}")
        print(f"📄 Log: {summary['log_file']}")
        print(f"🔄 Rollback: {summary['rollback_script']}")
        
        if summary['failed_operations'] > 0:
            print(f"\n⚠️  OPERACIONES FALLIDAS:")
            for op in self.operations:
                if op['status'] == 'FAILED':
                    print(f"  - {op['type']}: {op['details'].get('description', 'Sin descripción')}")
    
    def cleanup_session(self):
        """Limpia archivos temporales de la sesión"""
        try:
            # Generar script de rollback final
            self.generate_rollback_script()
            
            # Crear archivo de resumen
            summary_file = f"{self.session_dir}/session_summary.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(self.get_session_summary(), f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"🧹 Sesión finalizada y limpiada: {self.session_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Error limpiando sesión: {str(e)}")

def main():
    """Función principal para demostrar el uso"""
    if len(sys.argv) < 2:
        print("Uso: python execution_manager.py <comando>")
        print("Comandos disponibles:")
        print("  test - Ejecutar prueba con análisis de una tabla")
        print("  silver - Generar vistas Silver")
        print("  consolidated - Generar vistas consolidadas")
        print("  all - Ejecutar todo el proceso")
        print("  rollback - Ejecutar rollback de la última sesión")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    # Crear gestor de ejecución
    manager = ExecutionManager()
    
    try:
        if command == "test":
            manager.logger.info("🧪 Iniciando prueba con análisis de tabla")
            success, output = manager.execute_safe(
                "python test_single_table_analysis.py",
                "Análisis de tabla individual",
                "echo 'No rollback necesario para análisis'"
            )
            
        elif command == "silver":
            manager.logger.info("🔄 Iniciando generación de vistas Silver")
            success, output = manager.execute_safe(
                "python generate_silver_views.py",
                "Generación de vistas Silver",
                "python rollback_manager.py silver"
            )
            
        elif command == "consolidated":
            manager.logger.info("🔄 Iniciando generación de vistas consolidadas")
            success, output = manager.execute_safe(
                "python generate_central_consolidated_views.py",
                "Generación de vistas consolidadas",
                "python rollback_manager.py consolidated"
            )
            
        elif command == "all":
            manager.logger.info("🚀 Iniciando proceso completo")
            
            # Paso 1: Análisis
            success1, _ = manager.execute_safe(
                "python analyze_data_types.py",
                "Análisis de tipos de datos"
            )
            
            # Paso 2: Vistas Silver
            success2, _ = manager.execute_safe(
                "python generate_silver_views.py",
                "Generación de vistas Silver"
            )
            
            # Paso 3: Vistas consolidadas
            success3, _ = manager.execute_safe(
                "python generate_central_consolidated_views.py",
                "Generación de vistas consolidadas"
            )
            
            if success1 and success2 and success3:
                manager.logger.info("🎯 Proceso completo ejecutado exitosamente")
            else:
                manager.logger.error("❌ Proceso completo falló en algún paso")
        
        else:
            print(f"❌ Comando desconocido: {command}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        manager.logger.info("⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        manager.logger.error(f"💥 Error inesperado: {str(e)}")
    finally:
        # Mostrar resumen y limpiar
        manager.print_session_summary()
        manager.cleanup_session()

if __name__ == "__main__":
    main()
