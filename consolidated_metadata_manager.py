"""
Gestor de metadatos para tablas consolidadas
Maneja la configuración de particionado y clusterizado
"""

from google.cloud import bigquery
import pandas as pd
from config import PROJECT_SOURCE, PROJECT_CENTRAL

class ConsolidatedMetadataManager:
    """Maneja metadatos de tablas consolidadas"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_CENTRAL)
        self.metadata_table = f"{PROJECT_CENTRAL}.management.metadata_consolidated_tables"
    
    def get_table_metadata(self, table_name):
        """
        Obtiene metadatos de una tabla específica
        
        Returns:
            dict: {
                'partition_fields': ['created_on', 'updated_on'],
                'cluster_fields': ['company_id'],
                'update_strategy': 'incremental'
            }
        """
        try:
            query = f"""
            SELECT 
                partition_fields,
                cluster_fields,
                update_strategy
            FROM `{self.metadata_table}`
            WHERE table_name = '{table_name}'
            LIMIT 1
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                # Retornar valores por defecto si no existe configuración
                return self.get_default_metadata(table_name)
            
            row = df.iloc[0]
            return {
                'partition_fields': row['partition_fields'],
                'cluster_fields': row['cluster_fields'],
                'update_strategy': row['update_strategy']
            }
            
        except Exception as e:
            print(f"⚠️  Error obteniendo metadatos para {table_name}: {str(e)}")
            return self.get_default_metadata(table_name)
    
    def get_default_metadata(self, table_name):
        """
        Retorna metadatos por defecto para una tabla
        
        Returns:
            dict: Configuración por defecto
        """
        return {
            'partition_fields': ['created_on', 'updated_on', 'date_created'],
            'cluster_fields': ['company_id'],
            'update_strategy': 'incremental'
        }
    
    def analyze_partition_fields(self, table_name):
        """
        Analiza automáticamente campos de particionado disponibles
        
        Returns:
            list: Array de campos en orden de prioridad
        """
        try:
            # Obtener compañías para analizar
            companies_query = f"""
            SELECT DISTINCT company_project_id
            FROM `{PROJECT_SOURCE}.settings.companies`
            WHERE company_fivetran_status = TRUE 
              AND company_bigquery_status = TRUE
              AND company_project_id IS NOT NULL
            LIMIT 3
            """
            
            companies_df = self.client.query(companies_query).to_dataframe()
            
            if companies_df.empty:
                return ['created_on', 'updated_on', 'date_created']
            
            # Analizar campos en la primera compañía disponible
            project_id = companies_df.iloc[0]['company_project_id']
            dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
            
            # Buscar campos TIMESTAMP
            fields_query = f"""
            SELECT column_name
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
              AND data_type IN ('TIMESTAMP', 'DATETIME', 'DATE')
            ORDER BY ordinal_position
            """
            
            fields_df = self.client.query(fields_query).to_dataframe()
            
            if fields_df.empty:
                return ['created_on', 'updated_on', 'date_created']
            
            # Ordenar campos por prioridad
            available_fields = fields_df['column_name'].tolist()
            priority_fields = ['created_on', 'updated_on', 'date_created', 'modified_on', 'timestamp']
            
            ordered_fields = []
            
            # Agregar campos en orden de prioridad
            for field in priority_fields:
                if field in available_fields:
                    ordered_fields.append(field)
            
            # Agregar campos restantes
            for field in available_fields:
                if field not in ordered_fields:
                    ordered_fields.append(field)
            
            return ordered_fields[:5]  # Máximo 5 campos
            
        except Exception as e:
            print(f"⚠️  Error analizando campos de particionado para {table_name}: {str(e)}")
            return ['created_on', 'updated_on', 'date_created']
    
    def update_table_metadata(self, table_name, partition_fields=None, cluster_fields=None, update_strategy=None):
        """
        Actualiza metadatos de una tabla
        
        Args:
            table_name: Nombre de la tabla
            partition_fields: Array de campos de particionado
            cluster_fields: Array de campos de clusterizado
            update_strategy: Estrategia de actualización
        """
        try:
            # Obtener metadatos actuales
            current_metadata = self.get_table_metadata(table_name)
            
            # Usar valores proporcionados o mantener los actuales
            new_partition_fields = partition_fields or current_metadata['partition_fields']
            new_cluster_fields = cluster_fields or current_metadata['cluster_fields']
            new_update_strategy = update_strategy or current_metadata['update_strategy']
            
            # Verificar que cluster_fields no exceda 4 campos
            if len(new_cluster_fields) > 4:
                new_cluster_fields = new_cluster_fields[:4]
                print(f"⚠️  cluster_fields limitado a 4 campos para {table_name}")
            
            # Insertar o actualizar metadatos
            upsert_query = f"""
            MERGE `{self.metadata_table}` T
            USING (
                SELECT 
                    '{table_name}' as table_name,
                    {new_partition_fields} as partition_fields,
                    {new_cluster_fields} as cluster_fields,
                    '{new_update_strategy}' as update_strategy,
                    CURRENT_TIMESTAMP() as updated_at
            ) S
            ON T.table_name = S.table_name
            WHEN MATCHED THEN
                UPDATE SET 
                    partition_fields = S.partition_fields,
                    cluster_fields = S.cluster_fields,
                    update_strategy = S.update_strategy,
                    updated_at = S.updated_at
            WHEN NOT MATCHED THEN
                INSERT (table_name, partition_fields, cluster_fields, update_strategy, created_at, updated_at)
                VALUES (S.table_name, S.partition_fields, S.cluster_fields, S.update_strategy, S.updated_at, S.updated_at)
            """
            
            self.client.query(upsert_query).result()
            print(f"✅ Metadatos actualizados para {table_name}")
            
        except Exception as e:
            print(f"❌ Error actualizando metadatos para {table_name}: {str(e)}")
    
    def get_all_tables_metadata(self):
        """
        Obtiene metadatos de todas las tablas configuradas
        
        Returns:
            pd.DataFrame: DataFrame con todos los metadatos
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.metadata_table}`
            ORDER BY table_name
            """
            
            return self.client.query(query).to_dataframe()
            
        except Exception as e:
            print(f"❌ Error obteniendo metadatos: {str(e)}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Prueba del manager
    manager = ConsolidatedMetadataManager()
    
    # Probar con una tabla
    test_table = "call"
    metadata = manager.get_table_metadata(test_table)
    print(f"Metadatos para {test_table}: {metadata}")
    
    # Analizar campos de particionado
    partition_fields = manager.analyze_partition_fields(test_table)
    print(f"Campos de particionado para {test_table}: {partition_fields}")
