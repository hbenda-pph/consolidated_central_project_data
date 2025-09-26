"""
Script para crear tablas consolidadas en central-bronze
Con particionado y clusterizado basado en metadatos
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from config import PROJECT_SOURCE, TABLES_TO_PROCESS
from consolidated_metadata_manager import ConsolidatedMetadataManager
from consolidation_tracking_manager import ConsolidationTrackingManager

class ConsolidatedTableCreator:
    """Creador de tablas consolidadas optimizadas"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_SOURCE)
        self.metadata_manager = ConsolidatedMetadataManager()
        self.tracking_manager = ConsolidationTrackingManager()
        self.central_bronze_dataset = f"{PROJECT_SOURCE}.central-bronze"
    
    def get_companies_with_silver_views(self, table_name):
        """
        Obtiene compañías que tienen vistas Silver para una tabla específica
        
        Returns:
            list: Lista de diccionarios con información de compañías
        """
        try:
            # Obtener compañías que tienen status = 1 (éxito) para esta tabla
            query = f"""
            SELECT DISTINCT
                cc.company_id,
                c.company_name,
                c.company_project_id
            FROM `{PROJECT_SOURCE}.settings.companies_consolidated` cc
            JOIN `{PROJECT_SOURCE}.settings.companies` c
                ON cc.company_id = c.company_id
            WHERE cc.table_name = '{table_name}'
              AND cc.consolidated_status = 1
              AND c.company_fivetran_status = TRUE
              AND c.company_bigquery_status = TRUE
            ORDER BY cc.company_id
            """
            
            df = self.client.query(query).to_dataframe()
            
            companies = []
            for _, row in df.iterrows():
                companies.append({
                    'company_id': row['company_id'],
                    'company_name': row['company_name'],
                    'project_id': row['company_project_id']
                })
            
            return companies
            
        except Exception as e:
            print(f"❌ Error obteniendo compañías para {table_name}: {str(e)}")
            return []
    
    def create_consolidated_table(self, table_name):
        """
        Crea tabla consolidada para una tabla específica
        
        Args:
            table_name: Nombre de la tabla a consolidar
        """
        print(f"\n🔄 Creando tabla consolidada: {table_name}")
        
        # 1. Obtener compañías con vistas Silver
        companies = self.get_companies_with_silver_views(table_name)
        
        if not companies:
            print(f"  ⚠️  No hay compañías con vistas Silver para {table_name}")
            return False
        
        print(f"  📋 Compañías encontradas: {len(companies)}")
        
        # 2. Obtener metadatos de la tabla
        metadata = self.metadata_manager.get_table_metadata(table_name)
        print(f"  📊 Metadatos: particionado={metadata['partition_fields'][0]}, cluster={metadata['cluster_fields']}")
        
        # 3. Generar SQL de creación de tabla
        create_sql = self.generate_create_table_sql(table_name, companies, metadata)
        
        # 4. Ejecutar creación de tabla
        try:
            print(f"  🔄 Ejecutando creación de tabla...")
            query_job = self.client.query(create_sql)
            query_job.result()
            print(f"  ✅ Tabla consolidada creada: {self.central_bronze_dataset}.{table_name}")
            return True
            
        except Exception as e:
            print(f"  ❌ Error creando tabla consolidada: {str(e)}")
            return False
    
    def generate_create_table_sql(self, table_name, companies, metadata):
        """
        Genera SQL para crear tabla consolidada
        
        Args:
            table_name: Nombre de la tabla
            companies: Lista de compañías
            metadata: Metadatos de particionado y clusterizado
            
        Returns:
            str: SQL para crear la tabla
        """
        # Construir UNION ALL parts
        union_parts = []
        
        for company in companies:
            project_id = company['project_id']
            company_id = company['company_id']
            
            union_part = f"""
            SELECT 
                '{project_id}' AS company_project_id,
                {company_id} AS company_id,
                *
            FROM `{project_id}.silver.vw_{table_name}`"""
            
            union_parts.append(union_part)
        
        # Configurar particionado
        partition_field = metadata['partition_fields'][0]  # Usar el primer campo disponible
        
        # Configurar clusterizado
        cluster_fields = metadata['cluster_fields']
        cluster_sql = f"CLUSTER BY {', '.join(cluster_fields)}" if cluster_fields else ""
        
        # SQL completo
        sql = f"""
        CREATE OR REPLACE TABLE `{self.central_bronze_dataset}.{table_name}`
        PARTITION BY DATE({partition_field})
        {cluster_sql}
        AS
        {' UNION ALL '.join(union_parts)}
        """
        
        return sql
    
    def create_all_consolidated_tables(self):
        """
        Crea todas las tablas consolidadas
        """
        print("🚀 INICIANDO CREACIÓN DE TABLAS CONSOLIDADAS")
        print("=" * 60)
        
        # Obtener tablas que están 100% consolidadas
        tables_to_process = []
        
        for table_name in TABLES_TO_PROCESS:
            completion_status = self.tracking_manager.get_table_completion_status(table_name)
            
            if completion_status['is_fully_consolidated']:
                tables_to_process.append(table_name)
                print(f"✅ {table_name}: 100% consolidada - PROCESAR")
            else:
                print(f"⏭️  {table_name}: {completion_status['completion_rate']:.1f}% completada - SALTAR")
        
        if not tables_to_process:
            print("⚠️  No hay tablas 100% consolidadas para procesar")
            return
        
        print(f"\n📋 Tablas a procesar: {len(tables_to_process)}")
        print("=" * 60)
        
        # Crear cada tabla consolidada
        success_count = 0
        error_count = 0
        
        for table_name in tables_to_process:
            if self.create_consolidated_table(table_name):
                success_count += 1
            else:
                error_count += 1
        
        # Resumen final
        print(f"\n🎯 CREACIÓN COMPLETADA")
        print(f"✅ Tablas creadas exitosamente: {success_count}")
        print(f"❌ Tablas con errores: {error_count}")
        print(f"📊 Total procesadas: {len(tables_to_process)}")

if __name__ == "__main__":
    creator = ConsolidatedTableCreator()
    creator.create_all_consolidated_tables()
