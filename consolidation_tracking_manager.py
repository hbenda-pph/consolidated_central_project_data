"""
Gestor para la tabla companies_consolidated
Maneja el tracking detallado del estado de consolidación por compañía y tabla
"""

import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from config import PROJECT_SOURCE, DATASET_NAME

class ConsolidationTrackingManager:
    """Maneja el tracking de consolidación por compañía y tabla"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_SOURCE)
        self.table_id = f"{PROJECT_SOURCE}.{DATASET_NAME}.companies_consolidated"
        self.ensure_table_exists()
    
    def ensure_table_exists(self):
        """Crea la tabla si no existe"""
        try:
            # Verificar si la tabla existe
            table = self.client.get_table(self.table_id)
            print(f"✅ Tabla companies_consolidated existe: {self.table_id}")
        except Exception:
            # Crear la tabla
            schema = [
                bigquery.SchemaField("company_id", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("consolidated_status", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("notes", "STRING", mode="NULLABLE")
            ]
            
            table = bigquery.Table(self.table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
            table.clustering_fields = ["company_id", "table_name"]
            
            table = self.client.create_table(table)
            print(f"✅ Tabla companies_consolidated creada: {self.table_id}")
    
    def update_status(self, company_id, table_name, status, error_message=None, notes=None):
        """
        Actualiza o inserta el estado de consolidación
        
        Args:
            company_id: ID de la compañía
            table_name: Nombre de la tabla
            status: 0=No existe, 1=Éxito, 2=Error
            error_message: Mensaje de error si status=2
            notes: Observaciones adicionales
        """
        try:
            # Verificar si ya existe un registro
            query = f"""
            SELECT company_id, table_name 
            FROM `{self.table_id}`
            WHERE company_id = {company_id} AND table_name = '{table_name}'
            LIMIT 1
            """
            
            existing = self.client.query(query).to_dataframe()
            
            if existing.empty:
                # Insertar nuevo registro
                insert_query = f"""
                INSERT INTO `{self.table_id}` 
                (company_id, table_name, consolidated_status, created_at, updated_at, error_message, notes)
                VALUES ({company_id}, '{table_name}', {status}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
                        {f"'{error_message}'" if error_message else 'NULL'}, 
                        {f"'{notes}'" if notes else 'NULL'})
                """
            else:
                # Actualizar registro existente
                update_query = f"""
                UPDATE `{self.table_id}`
                SET consolidated_status = {status},
                    updated_at = CURRENT_TIMESTAMP(),
                    error_message = {f"'{error_message}'" if error_message else 'NULL'},
                    notes = {f"'{notes}'" if notes else 'NULL'}
                WHERE company_id = {company_id} AND table_name = '{table_name}'
                """
                insert_query = update_query
            
            self.client.query(insert_query).result()
            return True
            
        except Exception as e:
            print(f"❌ Error actualizando estado {company_id}-{table_name}: {str(e)}")
            return False
    
    def get_table_completion_status(self, table_name):
        """
        Obtiene el estado de completitud de una tabla específica
        
        Returns:
            dict: {
                'total_companies': int,
                'success_count': int,
                'error_count': int,
                'missing_count': int,
                'completion_rate': float,
                'is_fully_consolidated': bool
            }
        """
        try:
            query = f"""
            SELECT 
                consolidated_status,
                COUNT(*) as count
            FROM `{self.table_id}`
            WHERE table_name = '{table_name}'
            GROUP BY consolidated_status
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                return {
                    'total_companies': 0,
                    'success_count': 0,
                    'error_count': 0,
                    'missing_count': 0,
                    'completion_rate': 0.0,
                    'is_fully_consolidated': False
                }
            
            # Contar por estado
            success_count = df[df['consolidated_status'] == 1]['count'].sum() if 1 in df['consolidated_status'].values else 0
            error_count = df[df['consolidated_status'] == 2]['count'].sum() if 2 in df['consolidated_status'].values else 0
            missing_count = df[df['consolidated_status'] == 0]['count'].sum() if 0 in df['consolidated_status'].values else 0
            
            total_companies = success_count + error_count + missing_count
            completion_rate = (success_count / total_companies * 100) if total_companies > 0 else 0.0
            is_fully_consolidated = (error_count == 0 and missing_count == 0 and total_companies > 0)
            
            return {
                'total_companies': total_companies,
                'success_count': success_count,
                'error_count': error_count,
                'missing_count': missing_count,
                'completion_rate': completion_rate,
                'is_fully_consolidated': is_fully_consolidated
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo estado de tabla {table_name}: {str(e)}")
            return {
                'total_companies': 0,
                'success_count': 0,
                'error_count': 0,
                'missing_count': 0,
                'completion_rate': 0.0,
                'is_fully_consolidated': False
            }
    
    def get_tables_to_process(self, all_tables):
        """
        Filtra las tablas que necesitan procesamiento
        
        Args:
            all_tables: Lista de todas las tablas disponibles
            
        Returns:
            list: Tablas que no están 100% consolidadas
        """
        tables_to_process = []
        
        for table_name in all_tables:
            status = self.get_table_completion_status(table_name)
            
            if not status['is_fully_consolidated']:
                tables_to_process.append(table_name)
                print(f"🔄 Tabla {table_name}: {status['completion_rate']:.1f}% completada "
                      f"({status['success_count']}/{status['total_companies']}) - PROCESAR")
            else:
                print(f"✅ Tabla {table_name}: 100% consolidada - SALTAR")
        
        return tables_to_process
    
    def get_company_table_status(self, company_id, table_name):
        """
        Obtiene el estado específico de una compañía-tabla
        
        Returns:
            dict: Estado actual o None si no existe
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.table_id}`
            WHERE company_id = {company_id} AND table_name = '{table_name}'
            ORDER BY updated_at DESC
            LIMIT 1
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                return None
            
            return {
                'company_id': df.iloc[0]['company_id'],
                'table_name': df.iloc[0]['table_name'],
                'consolidated_status': df.iloc[0]['consolidated_status'],
                'created_at': df.iloc[0]['created_at'],
                'updated_at': df.iloc[0]['updated_at'],
                'error_message': df.iloc[0]['error_message'],
                'notes': df.iloc[0]['notes']
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo estado {company_id}-{table_name}: {str(e)}")
            return None
    
    def print_consolidation_report(self):
        """Imprime un reporte completo de consolidación"""
        try:
            query = f"""
            SELECT 
                table_name,
                consolidated_status,
                COUNT(*) as count,
                MAX(updated_at) as last_updated
            FROM `{self.table_id}`
            GROUP BY table_name, consolidated_status
            ORDER BY table_name, consolidated_status
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                print("📊 No hay datos de consolidación disponibles")
                return
            
            print("\n" + "="*80)
            print("📊 REPORTE DE CONSOLIDACIÓN POR TABLA")
            print("="*80)
            
            current_table = None
            for _, row in df.iterrows():
                if row['table_name'] != current_table:
                    current_table = row['table_name']
                    print(f"\n📋 Tabla: {current_table}")
                    print(f"   Última actualización: {row['last_updated']}")
                
                status_text = {0: "No existe", 1: "Éxito", 2: "Error"}
                print(f"   {status_text[row['consolidated_status']]}: {row['count']} compañías")
            
            print("\n" + "="*80)
            
        except Exception as e:
            print(f"❌ Error generando reporte: {str(e)}")

if __name__ == "__main__":
    # Prueba del manager
    manager = ConsolidationTrackingManager()
    manager.print_consolidation_report()
