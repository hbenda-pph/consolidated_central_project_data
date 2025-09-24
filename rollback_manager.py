# -*- coding: utf-8 -*-
"""
Rollback Manager - Consolidated Central Project Data

Sistema de rollback para revertir operaciones ejecutadas.
Permite eliminar vistas creadas y restaurar estado anterior.
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

class RollbackManager:
    """
    Gestor de rollback para operaciones de consolidación
    """
    
    def __init__(self, session_name=None):
        self.session_name = session_name
        self.logger = self.setup_logging()
        
        if session_name:
            self.session_dir = f"execution_sessions/{session_name}"
            self.operations_log = f"{self.session_dir}/operations.json"
        else:
            # Buscar la sesión más reciente
            self.session_dir, self.session_name = self.find_latest_session()
            self.operations_log = f"{self.session_dir}/operations.json"
        
        self.logger.info(f"🔄 RollbackManager iniciado - Sesión: {self.session_name}")
        self.logger.info(f"📁 Directorio de sesión: {self.session_dir}")
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger(__name__)
    
    def find_latest_session(self):
        """Encuentra la sesión más reciente"""
        sessions_dir = "execution_sessions"
        if not os.path.exists(sessions_dir):
            self.logger.error("❌ No se encontró directorio de sesiones")
            return None, None
        
        sessions = [d for d in os.listdir(sessions_dir) if os.path.isdir(os.path.join(sessions_dir, d))]
        if not sessions:
            self.logger.error("❌ No se encontraron sesiones")
            return None, None
        
        # Ordenar por nombre (que incluye timestamp)
        sessions.sort(reverse=True)
        latest_session = sessions[0]
        latest_dir = os.path.join(sessions_dir, latest_session)
        
        self.logger.info(f"📋 Sesión más reciente encontrada: {latest_session}")
        return latest_dir, latest_session
    
    def load_operations(self):
        """Carga las operaciones de la sesión"""
        if not os.path.exists(self.operations_log):
            self.logger.error(f"❌ No se encontró archivo de operaciones: {self.operations_log}")
            return []
        
        try:
            with open(self.operations_log, 'r', encoding='utf-8') as f:
                operations = json.load(f)
            self.logger.info(f"📋 Cargadas {len(operations)} operaciones")
            return operations
        except Exception as e:
            self.logger.error(f"❌ Error cargando operaciones: {str(e)}")
            return []
    
    def rollback_silver_views(self, dry_run=True):
        """Hace rollback de todas las vistas Silver creadas"""
        operations = self.load_operations()
        if not operations:
            return False
        
        # Filtrar operaciones de creación de vistas Silver
        silver_operations = [
            op for op in operations 
            if (op['type'] == 'VIEW_CREATION' and 
                op['status'] == 'SUCCESS' and
                'vw_normalized_' in op['details'].get('view_name', ''))
        ]
        
        if not silver_operations:
            self.logger.info("ℹ️  No se encontraron vistas Silver para hacer rollback")
            return True
        
        self.logger.info(f"🔄 Iniciando rollback de {len(silver_operations)} vistas Silver")
        
        rollback_statements = []
        successful_rollbacks = 0
        failed_rollbacks = 0
        
        for op in silver_operations:
            details = op['details']
            project_id = details['project_id']
            dataset = details['dataset']
            view_name = details['view_name']
            
            full_view_name = f"`{project_id}.{dataset}.{view_name}`"
            drop_statement = f"DROP VIEW IF EXISTS {full_view_name};"
            
            rollback_statements.append(drop_statement)
            
            if not dry_run:
                try:
                    # Crear cliente BigQuery
                    client = bigquery.Client(project=project_id)
                    
                    # Ejecutar DROP VIEW
                    query_job = client.query(drop_statement)
                    query_job.result()
                    
                    successful_rollbacks += 1
                    self.logger.info(f"✅ Vista eliminada: {full_view_name}")
                    
                except Exception as e:
                    failed_rollbacks += 1
                    self.logger.error(f"❌ Error eliminando vista {full_view_name}: {str(e)}")
            else:
                self.logger.info(f"🔍 [DRY RUN] Eliminaría: {full_view_name}")
        
        # Guardar script de rollback
        rollback_script = f"{self.session_dir}/rollback_silver_views.sql"
        with open(rollback_script, 'w', encoding='utf-8') as f:
            f.write(f"-- Script de Rollback para Vistas Silver\n")
            f.write(f"-- Sesión: {self.session_name}\n")
            f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Total de vistas: {len(rollback_statements)}\n\n")
            
            for stmt in rollback_statements:
                f.write(stmt + "\n")
        
        if dry_run:
            self.logger.info(f"🔍 [DRY RUN] Se eliminarían {len(rollback_statements)} vistas Silver")
            self.logger.info(f"📄 Script generado: {rollback_script}")
        else:
            self.logger.info(f"✅ Rollback completado: {successful_rollbacks} exitosas, {failed_rollbacks} fallidas")
        
        return failed_rollbacks == 0
    
    def rollback_consolidated_views(self, dry_run=True):
        """Hace rollback de todas las vistas consolidadas creadas"""
        operations = self.load_operations()
        if not operations:
            return False
        
        # Filtrar operaciones de creación de vistas consolidadas
        consolidated_operations = [
            op for op in operations 
            if (op['type'] == 'VIEW_CREATION' and 
                op['status'] == 'SUCCESS' and
                'vw_consolidated_' in op['details'].get('view_name', ''))
        ]
        
        if not consolidated_operations:
            self.logger.info("ℹ️  No se encontraron vistas consolidadas para hacer rollback")
            return True
        
        self.logger.info(f"🔄 Iniciando rollback de {len(consolidated_operations)} vistas consolidadas")
        
        rollback_statements = []
        successful_rollbacks = 0
        failed_rollbacks = 0
        
        for op in consolidated_operations:
            details = op['details']
            project_id = details['project_id']
            dataset = details['dataset']
            view_name = details['view_name']
            
            full_view_name = f"`{project_id}.{dataset}.{view_name}`"
            drop_statement = f"DROP VIEW IF EXISTS {full_view_name};"
            
            rollback_statements.append(drop_statement)
            
            if not dry_run:
                try:
                    # Crear cliente BigQuery
                    client = bigquery.Client(project=project_id)
                    
                    # Ejecutar DROP VIEW
                    query_job = client.query(drop_statement)
                    query_job.result()
                    
                    successful_rollbacks += 1
                    self.logger.info(f"✅ Vista eliminada: {full_view_name}")
                    
                except Exception as e:
                    failed_rollbacks += 1
                    self.logger.error(f"❌ Error eliminando vista {full_view_name}: {str(e)}")
            else:
                self.logger.info(f"🔍 [DRY RUN] Eliminaría: {full_view_name}")
        
        # Guardar script de rollback
        rollback_script = f"{self.session_dir}/rollback_consolidated_views.sql"
        with open(rollback_script, 'w', encoding='utf-8') as f:
            f.write(f"-- Script de Rollback para Vistas Consolidadas\n")
            f.write(f"-- Sesión: {self.session_name}\n")
            f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Total de vistas: {len(rollback_statements)}\n\n")
            
            for stmt in rollback_statements:
                f.write(stmt + "\n")
        
        if dry_run:
            self.logger.info(f"🔍 [DRY RUN] Se eliminarían {len(rollback_statements)} vistas consolidadas")
            self.logger.info(f"📄 Script generado: {rollback_script}")
        else:
            self.logger.info(f"✅ Rollback completado: {successful_rollbacks} exitosas, {failed_rollbacks} fallidas")
        
        return failed_rollbacks == 0
    
    def rollback_all(self, dry_run=True):
        """Hace rollback de todas las operaciones"""
        self.logger.info(f"🔄 Iniciando rollback completo {'(DRY RUN)' if dry_run else ''}")
        
        # Rollback de vistas consolidadas primero (dependencias)
        consolidated_success = self.rollback_consolidated_views(dry_run)
        
        # Rollback de vistas Silver
        silver_success = self.rollback_silver_views(dry_run)
        
        if dry_run:
            self.logger.info("🔍 [DRY RUN] Rollback completo simulado")
        else:
            if consolidated_success and silver_success:
                self.logger.info("🎯 Rollback completo ejecutado exitosamente")
            else:
                self.logger.error("❌ Rollback completo falló en algunos pasos")
        
        return consolidated_success and silver_success
    
    def list_sessions(self):
        """Lista todas las sesiones disponibles"""
        sessions_dir = "execution_sessions"
        if not os.path.exists(sessions_dir):
            self.logger.info("ℹ️  No se encontró directorio de sesiones")
            return []
        
        sessions = []
        for session_name in os.listdir(sessions_dir):
            session_path = os.path.join(sessions_dir, session_name)
            if os.path.isdir(session_path):
                operations_file = os.path.join(session_path, "operations.json")
                if os.path.exists(operations_file):
                    try:
                        with open(operations_file, 'r', encoding='utf-8') as f:
                            operations = json.load(f)
                        
                        sessions.append({
                            'name': session_name,
                            'path': session_path,
                            'operations_count': len(operations),
                            'successful_ops': len([op for op in operations if op['status'] == 'SUCCESS']),
                            'failed_ops': len([op for op in operations if op['status'] == 'FAILED']),
                            'start_time': operations[0]['timestamp'] if operations else None
                        })
                    except:
                        continue
        
        # Ordenar por nombre (timestamp)
        sessions.sort(key=lambda x: x['name'], reverse=True)
        return sessions
    
    def print_sessions_list(self):
        """Imprime lista de sesiones disponibles"""
        sessions = self.list_sessions()
        
        if not sessions:
            print("ℹ️  No se encontraron sesiones")
            return
        
        print(f"\n📋 SESIONES DISPONIBLES ({len(sessions)}):")
        print("=" * 80)
        
        for i, session in enumerate(sessions, 1):
            status = "✅" if session['failed_ops'] == 0 else "⚠️" if session['failed_ops'] < session['successful_ops'] else "❌"
            
            print(f"{i:2d}. {status} {session['name']}")
            print(f"    📊 Operaciones: {session['operations_count']} ({session['successful_ops']} exitosas, {session['failed_ops']} fallidas)")
            print(f"    ⏱️  Inicio: {session['start_time']}")
            print(f"    📁 Directorio: {session['path']}")
            print()

def main():
    """Función principal para rollback"""
    if len(sys.argv) < 2:
        print("Uso: python rollback_manager.py <comando> [opciones]")
        print("\nComandos disponibles:")
        print("  list                    - Lista todas las sesiones disponibles")
        print("  silver [session_name]   - Rollback de vistas Silver (dry run por defecto)")
        print("  consolidated [session_name] - Rollback de vistas consolidadas (dry run por defecto)")
        print("  all [session_name]      - Rollback completo (dry run por defecto)")
        print("  execute <script_file>   - Ejecuta un script de rollback específico")
        print("\nOpciones:")
        print("  --execute               - Ejecuta realmente (no dry run)")
        print("\nEjemplos:")
        print("  python rollback_manager.py list")
        print("  python rollback_manager.py silver")
        print("  python rollback_manager.py all --execute")
        print("  python rollback_manager.py silver session_20240924_143022 --execute")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    session_name = None
    dry_run = True
    
    # Procesar argumentos
    if len(sys.argv) > 2:
        for arg in sys.argv[2:]:
            if arg == '--execute':
                dry_run = False
            elif not arg.startswith('--'):
                session_name = arg
    
    # Crear gestor de rollback
    manager = RollbackManager(session_name)
    
    if not manager.session_name:
        print("❌ No se pudo determinar la sesión para rollback")
        sys.exit(1)
    
    try:
        if command == "list":
            manager.print_sessions_list()
            
        elif command == "silver":
            if dry_run:
                print("🔍 Ejecutando rollback de vistas Silver (DRY RUN)")
            else:
                print("⚠️  Ejecutando rollback de vistas Silver (REAL)")
                response = input("¿Estás seguro? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Rollback cancelado")
                    sys.exit(0)
            
            success = manager.rollback_silver_views(dry_run)
            if not success:
                sys.exit(1)
                
        elif command == "consolidated":
            if dry_run:
                print("🔍 Ejecutando rollback de vistas consolidadas (DRY RUN)")
            else:
                print("⚠️  Ejecutando rollback de vistas consolidadas (REAL)")
                response = input("¿Estás seguro? (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Rollback cancelado")
                    sys.exit(0)
            
            success = manager.rollback_consolidated_views(dry_run)
            if not success:
                sys.exit(1)
                
        elif command == "all":
            if dry_run:
                print("🔍 Ejecutando rollback completo (DRY RUN)")
            else:
                print("⚠️  Ejecutando rollback completo (REAL)")
                response = input("¿Estás seguro? Esto eliminará TODAS las vistas creadas (yes/no): ")
                if response.lower() != 'yes':
                    print("❌ Rollback cancelado")
                    sys.exit(0)
            
            success = manager.rollback_all(dry_run)
            if not success:
                sys.exit(1)
                
        elif command == "execute":
            if len(sys.argv) < 3:
                print("❌ Debe especificar el archivo de script")
                sys.exit(1)
            
            script_file = sys.argv[2]
            if not os.path.exists(script_file):
                print(f"❌ Archivo no encontrado: {script_file}")
                sys.exit(1)
            
            print(f"🔄 Ejecutando script: {script_file}")
            # Aquí se podría implementar la ejecución del script
            print("⚠️  Funcionalidad de ejecución de script pendiente de implementar")
            
        else:
            print(f"❌ Comando desconocido: {command}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n⚠️  Rollback interrumpido por el usuario")
    except Exception as e:
        print(f"💥 Error inesperado: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
