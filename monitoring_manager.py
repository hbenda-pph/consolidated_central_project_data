# -*- coding: utf-8 -*-
"""
Monitoring Manager - Consolidated Central Project Data

Sistema de monitoreo y validación para verificar el estado de las vistas creadas.
Permite validar que las vistas existen, son accesibles y tienen el esquema correcto.
"""

import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import *

class MonitoringManager:
    """
    Gestor de monitoreo y validación de vistas
    """
    
    def __init__(self):
        self.logger = self.setup_logging()
        self.client = bigquery.Client(project=PROJECT_SOURCE)
        self.logger.info("📊 MonitoringManager iniciado")
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger(__name__)
    
    def get_companies_info(self):
        """Obtiene información de las compañías activas"""
        query = f"""
            SELECT company_id, company_name, company_project_id
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            WHERE company_bigquery_status IS NOT NULL
            ORDER BY company_id
            LIMIT {MAX_COMPANIES_FOR_TEST}
        """
        return pd.DataFrame([dict(row) for row in self.client.query(query).result()])
    
    def validate_silver_view(self, project_id, view_name, expected_fields=None):
        """Valida una vista Silver específica"""
        try:
            # Obtener información de la vista
            table_ref = self.client.dataset('silver', project=project_id).table(view_name)
            table = self.client.get_table(table_ref)
            
            validation_result = {
                'project_id': project_id,
                'view_name': view_name,
                'exists': True,
                'field_count': len(table.schema),
                'fields': [field.name for field in table.schema],
                'size_bytes': table.num_bytes,
                'num_rows': table.num_rows,
                'created': table.created.isoformat() if table.created else None,
                'modified': table.modified.isoformat() if table.modified else None,
                'errors': []
            }
            
            # Validar campos esperados si se proporcionan
            if expected_fields:
                missing_fields = set(expected_fields) - set(validation_result['fields'])
                extra_fields = set(validation_result['fields']) - set(expected_fields)
                
                if missing_fields:
                    validation_result['errors'].append(f"Campos faltantes: {list(missing_fields)}")
                
                if extra_fields:
                    validation_result['errors'].append(f"Campos extra: {list(extra_fields)}")
            
            # Probar consulta básica
            try:
                test_query = f"SELECT COUNT(*) as row_count FROM `{project_id}.silver.{view_name}` LIMIT 1"
                query_job = self.client.query(test_query)
                result = query_job.result()
                row_count = list(result)[0].row_count
                validation_result['query_test'] = True
                validation_result['actual_rows'] = row_count
            except Exception as e:
                validation_result['query_test'] = False
                validation_result['errors'].append(f"Error en consulta de prueba: {str(e)}")
            
            return validation_result
            
        except Exception as e:
            return {
                'project_id': project_id,
                'view_name': view_name,
                'exists': False,
                'errors': [f"Vista no existe o no es accesible: {str(e)}"]
            }
    
    def validate_consolidated_view(self, project_id, view_name, expected_companies=None):
        """Valida una vista consolidada específica"""
        try:
            # Obtener información de la vista
            table_ref = self.client.dataset('central-silver', project=project_id).table(view_name)
            table = self.client.get_table(table_ref)
            
            validation_result = {
                'project_id': project_id,
                'view_name': view_name,
                'exists': True,
                'field_count': len(table.schema),
                'fields': [field.name for field in table.schema],
                'size_bytes': table.num_bytes,
                'num_rows': table.num_rows,
                'created': table.created.isoformat() if table.created else None,
                'modified': table.modified.isoformat() if table.modified else None,
                'errors': []
            }
            
            # Probar consulta básica
            try:
                test_query = f"SELECT COUNT(*) as row_count FROM `{project_id}.central-silver.{view_name}` LIMIT 1"
                query_job = self.client.query(test_query)
                result = query_job.result()
                row_count = list(result)[0].row_count
                validation_result['query_test'] = True
                validation_result['actual_rows'] = row_count
            except Exception as e:
                validation_result['query_test'] = False
                validation_result['errors'].append(f"Error en consulta de prueba: {str(e)}")
            
            # Validar que contiene datos de las compañías esperadas
            if expected_companies and validation_result['query_test']:
                try:
                    companies_query = f"""
                        SELECT DISTINCT company_name, COUNT(*) as row_count
                        FROM `{project_id}.central-silver.{view_name}`
                        GROUP BY company_name
                        ORDER BY company_name
                    """
                    query_job = self.client.query(companies_query)
                    result = query_job.result()
                    
                    company_data = {row.company_name: row.row_count for row in result}
                    validation_result['company_data'] = company_data
                    
                    missing_companies = set(expected_companies) - set(company_data.keys())
                    if missing_companies:
                        validation_result['errors'].append(f"Compañías faltantes: {list(missing_companies)}")
                        
                except Exception as e:
                    validation_result['errors'].append(f"Error validando datos por compañía: {str(e)}")
            
            return validation_result
            
        except Exception as e:
            return {
                'project_id': project_id,
                'view_name': view_name,
                'exists': False,
                'errors': [f"Vista no existe o no es accesible: {str(e)}"]
            }
    
    def monitor_silver_views(self, table_name=None):
        """Monitorea todas las vistas Silver"""
        self.logger.info("🔍 Iniciando monitoreo de vistas Silver")
        
        companies_df = self.get_companies_info()
        monitoring_results = []
        
        # Obtener campos esperados si se especifica una tabla
        expected_fields = None
        if table_name:
            # Aquí se podría obtener los campos esperados del análisis previo
            pass
        
        for _, company in companies_df.iterrows():
            project_id = company['company_project_id']
            company_name = company['company_name']
            
            if table_name:
                view_name = f"vw_normalized_{table_name}"
                result = self.validate_silver_view(project_id, view_name, expected_fields)
                result['company_name'] = company_name
                result['table_name'] = table_name
                monitoring_results.append(result)
            else:
                # Monitorear todas las vistas Silver (esto requeriría listar las vistas)
                self.logger.info(f"⚠️  Monitoreo de todas las vistas no implementado para {company_name}")
        
        return monitoring_results
    
    def monitor_consolidated_views(self):
        """Monitorea todas las vistas consolidadas"""
        self.logger.info("🔍 Iniciando monitoreo de vistas consolidadas")
        
        companies_df = self.get_companies_info()
        expected_companies = companies_df['company_name'].tolist()
        
        monitoring_results = []
        
        # Lista de tablas conocidas (se podría obtener dinámicamente)
        known_tables = ['call', 'campaign', 'customer', 'job', 'invoice', 'appointment']
        
        for table_name in known_tables:
            view_name = f"vw_consolidated_{table_name}"
            result = self.validate_consolidated_view(
                PROJECT_SOURCE, 
                view_name, 
                expected_companies
            )
            result['table_name'] = table_name
            monitoring_results.append(result)
        
        return monitoring_results
    
    def generate_monitoring_report(self, silver_results=None, consolidated_results=None):
        """Genera reporte de monitoreo"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'silver_views': silver_results or [],
            'consolidated_views': consolidated_results or [],
            'summary': {
                'total_silver_views': len(silver_results) if silver_results else 0,
                'total_consolidated_views': len(consolidated_results) if consolidated_results else 0,
                'silver_errors': 0,
                'consolidated_errors': 0
            }
        }
        
        # Calcular errores
        if silver_results:
            report['summary']['silver_errors'] = len([r for r in silver_results if r['errors']])
        
        if consolidated_results:
            report['summary']['consolidated_errors'] = len([r for r in consolidated_results if r['errors']])
        
        return report
    
    def print_monitoring_summary(self, report):
        """Imprime resumen de monitoreo"""
        summary = report['summary']
        
        print(f"\n📊 REPORTE DE MONITOREO - {report['timestamp']}")
        print("=" * 60)
        
        print(f"📋 Vistas Silver: {summary['total_silver_views']}")
        print(f"   ✅ Sin errores: {summary['total_silver_views'] - summary['silver_errors']}")
        print(f"   ❌ Con errores: {summary['silver_errors']}")
        
        print(f"\n📋 Vistas Consolidadas: {summary['total_consolidated_views']}")
        print(f"   ✅ Sin errores: {summary['total_consolidated_views'] - summary['consolidated_errors']}")
        print(f"   ❌ Con errores: {summary['consolidated_errors']}")
        
        # Mostrar errores detallados
        if summary['silver_errors'] > 0:
            print(f"\n❌ ERRORES EN VISTAS SILVER:")
            for result in report['silver_views']:
                if result['errors']:
                    print(f"  - {result['company_name']}.{result['view_name']}: {', '.join(result['errors'])}")
        
        if summary['consolidated_errors'] > 0:
            print(f"\n❌ ERRORES EN VISTAS CONSOLIDADAS:")
            for result in report['consolidated_views']:
                if result['errors']:
                    print(f"  - {result['view_name']}: {', '.join(result['errors'])}")
        
        # Guardar reporte
        report_file = f"monitoring_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Reporte guardado: {report_file}")
    
    def validate_specific_table(self, table_name):
        """Valida una tabla específica en todas las compañías"""
        self.logger.info(f"🔍 Validando tabla específica: {table_name}")
        
        silver_results = self.monitor_silver_views(table_name)
        consolidated_results = self.monitor_consolidated_views()
        
        # Filtrar solo la tabla específica en consolidadas
        table_consolidated = [r for r in consolidated_results if r.get('table_name') == table_name]
        
        report = self.generate_monitoring_report(silver_results, table_consolidated)
        self.print_monitoring_summary(report)
        
        return report

def main():
    """Función principal para monitoreo"""
    if len(sys.argv) < 2:
        print("Uso: python monitoring_manager.py <comando> [opciones]")
        print("\nComandos disponibles:")
        print("  silver [table_name]     - Monitorear vistas Silver (opcional: tabla específica)")
        print("  consolidated            - Monitorear vistas consolidadas")
        print("  all                     - Monitorear todo")
        print("  table <table_name>      - Validar tabla específica")
        print("\nEjemplos:")
        print("  python monitoring_manager.py silver")
        print("  python monitoring_manager.py silver call")
        print("  python monitoring_manager.py consolidated")
        print("  python monitoring_manager.py table call")
        print("  python monitoring_manager.py all")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    # Crear gestor de monitoreo
    monitor = MonitoringManager()
    
    try:
        if command == "silver":
            table_name = sys.argv[2] if len(sys.argv) > 2 else None
            results = monitor.monitor_silver_views(table_name)
            report = monitor.generate_monitoring_report(silver_results=results)
            monitor.print_monitoring_summary(report)
            
        elif command == "consolidated":
            results = monitor.monitor_consolidated_views()
            report = monitor.generate_monitoring_report(consolidated_results=results)
            monitor.print_monitoring_summary(report)
            
        elif command == "all":
            silver_results = monitor.monitor_silver_views()
            consolidated_results = monitor.monitor_consolidated_views()
            report = monitor.generate_monitoring_report(silver_results, consolidated_results)
            monitor.print_monitoring_summary(report)
            
        elif command == "table":
            if len(sys.argv) < 3:
                print("❌ Debe especificar el nombre de la tabla")
                sys.exit(1)
            
            table_name = sys.argv[2]
            report = monitor.validate_specific_table(table_name)
            
        else:
            print(f"❌ Comando desconocido: {command}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n⚠️  Monitoreo interrumpido por el usuario")
    except Exception as e:
        print(f"💥 Error inesperado: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
