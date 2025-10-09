# -*- coding: utf-8 -*-
"""
Consolidation Status Manager - Consolidated Central Project Data

⚠️ ARCHIVO OBSOLETO - Movido a review/

RAZÓN: Este manager trabaja a nivel de COMPAÑÍA (status general por compañía),
pero el proceso de consolidación trabaja a nivel de TABLA × COMPAÑÍA.

PROBLEMA:
- Una compañía puede tener 40 tablas exitosas y 2 con error
- ¿El status de la compañía es COMPLETED o ERROR?
- Demasiado general para tracking granular

REEMPLAZO:
- Usar: consolidation_tracking_manager.py
- Tracking por tabla y compañía (más granular y útil)
- Permite saber exactamente qué tabla falló en qué compañía

FUNCIONALIDAD ORIGINAL:
Sistema de gestión de estados de consolidación para compañías.
Permite actualizar y consultar el estado del proceso de consolidación.
"""

import sys
import os
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import logging

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import *

class ConsolidationStatusManager:
    """
    Gestor de estados de consolidación
    """
    
    def __init__(self):
        self.logger = self.setup_logging()
        self.client = bigquery.Client(project=PROJECT_SOURCE)
        self.companies_table = f"{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}"
        
        # Estados de consolidación
        self.STATUS = {
            'PENDING': 0,        # Por consolidar
            'COMPLETED': 1,      # Consolidación exitosa
            'ERROR': 2           # Error en el proceso
        }
        
        self.logger.info("📊 ConsolidationStatusManager iniciado")
        self.logger.info(f"📋 Tabla companies: {self.companies_table}")
        
        # Verificar que la tabla existe
        try:
            table = self.client.get_table(self.companies_table)
            self.logger.info(f"✅ Tabla companies encontrada: {len(table.schema)} campos")
        except Exception as e:
            self.logger.error(f"❌ Tabla companies NO encontrada: {str(e)}")
            raise
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger(__name__)
    
    def update_company_status(self, company_id, status, error_message=None):
        """
        Actualiza el estado de consolidación de una compañía
        
        Args:
            company_id (int): ID de la compañía
            status (int): Nuevo estado (0, 1, o 2)
            error_message (str, optional): Mensaje de error si status = 2
        """
        try:
            update_query = f"""
                UPDATE `{self.companies_table}`
                SET 
                    company_consolidated_status = {status},
                    updated_at = CURRENT_TIMESTAMP()
                WHERE company_id = {company_id}
            """
            
            # Si hay error, podríamos agregar un campo adicional para el mensaje
            if error_message and status == self.STATUS['ERROR']:
                # Por ahora, solo logueamos el error
                self.logger.error(f"Error en compañía {company_id}: {error_message}")
            
            result = self.client.query(update_query).result()
            self.logger.info(f"✅ Estado actualizado para compañía {company_id}: {status}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error actualizando estado de compañía {company_id}: {str(e)}")
            return False
    
    def update_multiple_companies_status(self, company_ids, status, error_message=None):
        """
        Actualiza el estado de múltiples compañías
        
        Args:
            company_ids (list): Lista de IDs de compañías
            status (int): Nuevo estado
            error_message (str, optional): Mensaje de error
        """
        try:
            if not company_ids:
                self.logger.warning("⚠️  Lista de compañías vacía")
                return True
            
            # Crear lista de IDs para la consulta
            ids_str = ', '.join(map(str, company_ids))
            
            update_query = f"""
                UPDATE `{self.companies_table}`
                SET 
                    company_consolidated_status = {status},
                    updated_at = CURRENT_TIMESTAMP()
                WHERE company_id IN ({ids_str})
            """
            
            # Debug: Mostrar la consulta
            self.logger.info(f"🔍 Consulta UPDATE: {update_query}")
            
            result = self.client.query(update_query).result()
            self.logger.info(f"✅ Estado actualizado para {len(company_ids)} compañías: {status}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error actualizando estados múltiples: {str(e)}")
            self.logger.error(f"🔍 Consulta que falló: {update_query}")
            return False
    
    def get_companies_by_status(self, status):
        """
        Obtiene compañías por estado de consolidación
        
        Args:
            status (int): Estado a filtrar
            
        Returns:
            pd.DataFrame: DataFrame con las compañías
        """
        try:
            query = f"""
                SELECT 
                    company_id,
                    company_name,
                    company_project_id,
                    company_consolidated_status,
                    updated_at
                FROM `{self.companies_table}`
                WHERE company_consolidated_status = {status}
                ORDER BY company_id
            """
            
            self.logger.info(f"🔄 Ejecutando consulta: {query}")
            self.logger.info(f"📊 Tabla: {self.companies_table}")
            query_job = self.client.query(query)
            self.logger.info(f"📋 Job creado: {query_job.job_id}")
            
            result = query_job.result()
            self.logger.info(f"✅ Consulta completada, procesando resultados...")
            
            companies_df = pd.DataFrame([dict(row) for row in result])
            self.logger.info(f"📊 DataFrame creado con {len(companies_df)} filas")
            
            status_name = {v: k for k, v in self.STATUS.items()}[status]
            self.logger.info(f"📋 Compañías con estado {status_name} ({status}): {len(companies_df)}")
            
            return companies_df
            
        except Exception as e:
            self.logger.error(f"❌ Error obteniendo compañías por estado: {str(e)}")
            return pd.DataFrame()
    
    def get_consolidation_summary(self):
        """
        Obtiene resumen del estado de consolidación
        
        Returns:
            dict: Resumen con conteos por estado
        """
        try:
            query = f"""
                SELECT 
                    company_consolidated_status,
                    COUNT(*) as count
                FROM `{self.companies_table}`
                GROUP BY company_consolidated_status
                ORDER BY company_consolidated_status
            """
            
            result = self.client.query(query).result()
            
            summary = {
                'PENDING': 0,
                'COMPLETED': 0,
                'ERROR': 0,
                'TOTAL': 0
            }
            
            for row in result:
                status = row.company_consolidated_status
                count = row.count
                summary['TOTAL'] += count
                
                if status == self.STATUS['PENDING']:
                    summary['PENDING'] = count
                elif status == self.STATUS['COMPLETED']:
                    summary['COMPLETED'] = count
                elif status == self.STATUS['ERROR']:
                    summary['ERROR'] = count
            
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ Error obteniendo resumen: {str(e)}")
            return {}
    
    def print_consolidation_summary(self):
        """Imprime resumen del estado de consolidación"""
        summary = self.get_consolidation_summary()
        
        if not summary:
            print("❌ No se pudo obtener el resumen")
            return
        
        print("\n📊 RESUMEN DE ESTADO DE CONSOLIDACIÓN")
        print("=" * 50)
        print(f"📋 Total de compañías: {summary['TOTAL']}")
        print(f"⏳ Por consolidar: {summary['PENDING']}")
        print(f"✅ Consolidadas exitosamente: {summary['COMPLETED']}")
        print(f"❌ Con errores: {summary['ERROR']}")
        
        if summary['TOTAL'] > 0:
            pending_pct = (summary['PENDING'] / summary['TOTAL']) * 100
            completed_pct = (summary['COMPLETED'] / summary['TOTAL']) * 100
            error_pct = (summary['ERROR'] / summary['TOTAL']) * 100
            
            print(f"\n📈 Porcentajes:")
            print(f"  - Por consolidar: {pending_pct:.1f}%")
            print(f"  - Completadas: {completed_pct:.1f}%")
            print(f"  - Con errores: {error_pct:.1f}%")
    
    def reset_all_statuses(self, confirm=False):
        """
        Resetea todos los estados a 'PENDING'
        
        Args:
            confirm (bool): Confirmación para evitar ejecución accidental
        """
        if not confirm:
            print("⚠️  Esta acción reseteará TODOS los estados a 'PENDING'")
            print("✅ Continuando automáticamente en modo job...")
        
        try:
            query = f"""
                UPDATE `{self.companies_table}`
                SET 
                    company_consolidated_status = {self.STATUS['PENDING']},
                    updated_at = CURRENT_TIMESTAMP()
            """
            
            result = self.client.query(query).result()
            self.logger.info("✅ Todos los estados reseteados a PENDING")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error reseteando estados: {str(e)}")
            return False
    
    def get_companies_for_consolidation(self, limit=None):
        """
        Obtiene compañías pendientes de consolidación
        
        Args:
            limit (int, optional): Límite de compañías a retornar
            
        Returns:
            pd.DataFrame: Compañías pendientes
        """
        try:
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query = f"""
                SELECT 
                    company_id,
                    company_name,
                    company_project_id
                FROM `{self.companies_table}`
                WHERE company_consolidated_status = {self.STATUS['PENDING']}
                ORDER BY company_id
                {limit_clause}
            """
            
            self.logger.info(f"🔄 Ejecutando consulta: {query}")
            query_job = self.client.query(query)
            result = query_job.result()
            companies_df = pd.DataFrame([dict(row) for row in result])
            
            self.logger.info(f"📋 Compañías pendientes obtenidas: {len(companies_df)}")
            return companies_df
            
        except Exception as e:
            self.logger.error(f"❌ Error obteniendo compañías pendientes: {str(e)}")
            return pd.DataFrame()

def main():
    """Función principal para gestión de estados"""
    if len(sys.argv) < 2:
        print("Uso: python consolidation_status_manager.py <comando> [opciones]")
        print("\nComandos disponibles:")
        print("  summary                    - Mostrar resumen de estados")
        print("  pending                    - Mostrar compañías pendientes")
        print("  completed                  - Mostrar compañías completadas")
        print("  errors                     - Mostrar compañías con errores")
        print("  update <company_id> <status> - Actualizar estado de compañía")
        print("  reset                      - Resetear todos los estados")
        print("\nEstados disponibles:")
        print("  0 - Por consolidar (PENDING)")
        print("  1 - Consolidación exitosa (COMPLETED)")
        print("  2 - Error en el proceso (ERROR)")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    manager = ConsolidationStatusManager()
    
    try:
        if command == "summary":
            manager.print_consolidation_summary()
            
        elif command == "pending":
            companies = manager.get_companies_by_status(manager.STATUS['PENDING'])
            print(f"\n📋 COMPAÑÍAS PENDIENTES ({len(companies)}):")
            for _, company in companies.iterrows():
                print(f"  - {company['company_id']}: {company['company_name']} ({company['company_project_id']})")
            
        elif command == "completed":
            companies = manager.get_companies_by_status(manager.STATUS['COMPLETED'])
            print(f"\n✅ COMPAÑÍAS COMPLETADAS ({len(companies)}):")
            for _, company in companies.iterrows():
                print(f"  - {company['company_id']}: {company['company_name']} ({company['company_project_id']})")
            
        elif command == "errors":
            companies = manager.get_companies_by_status(manager.STATUS['ERROR'])
            print(f"\n❌ COMPAÑÍAS CON ERRORES ({len(companies)}):")
            for _, company in companies.iterrows():
                print(f"  - {company['company_id']}: {company['company_name']} ({company['company_project_id']})")
            
        elif command == "update":
            if len(sys.argv) < 4:
                print("❌ Debe especificar company_id y status")
                sys.exit(1)
            
            company_id = int(sys.argv[2])
            status = int(sys.argv[3])
            
            if status not in [0, 1, 2]:
                print("❌ Status debe ser 0, 1, o 2")
                sys.exit(1)
            
            success = manager.update_company_status(company_id, status)
            if success:
                print(f"✅ Estado actualizado para compañía {company_id}")
            else:
                print(f"❌ Error actualizando estado")
                sys.exit(1)
            
        elif command == "reset":
            manager.reset_all_statuses()
            
        else:
            print(f"❌ Comando desconocido: {command}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n⚠️  Operación interrumpida por el usuario")
    except Exception as e:
        print(f"💥 Error inesperado: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
