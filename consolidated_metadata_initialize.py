"""
Script para inicializar la tabla de metadatos con todas las tablas existentes
Usa la información de all_unique_tables para poblar la tabla inicialmente
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from config import PROJECT_SOURCE, TABLES_TO_PROCESS
from consolidated_metadata_manager import ConsolidatedMetadataManager

class MetadataInitializer:
    """Inicializador de tabla de metadatos"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_SOURCE)
        self.metadata_manager = ConsolidatedMetadataManager()
    
    def get_all_unique_tables(self):
        """
        Obtiene todas las tablas únicas de todas las compañías
        Similar a como se hace en generate_silver_views.py
        
        Returns:
            list: Lista de nombres de tablas únicas
        """
        try:
            # Obtener compañías activas
            companies_query = f"""
            SELECT DISTINCT company_project_id
            FROM `{PROJECT_SOURCE}.settings.companies`
            WHERE company_fivetran_status = TRUE 
              AND company_bigquery_status = TRUE
              AND company_project_id IS NOT NULL
            """
            
            companies_df = self.client.query(companies_query).to_dataframe()
            
            if companies_df.empty:
                print("⚠️  No se encontraron compañías activas")
                return []
            
            print(f"📋 Compañías encontradas: {len(companies_df)}")
            
            # Obtener todas las tablas de todas las compañías
            all_tables = set()
            
            for _, row in companies_df.iterrows():
                project_id = row['company_project_id']
                dataset_name = f"servicetitan_{project_id.replace('_', '-')}"
                
                try:
                    # Obtener tablas del dataset
                    tables_query = f"""
                    SELECT table_name
                    FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
                    WHERE table_type = 'BASE TABLE'
                      AND table_name NOT LIKE '_fivetran%'
                    """
                    
                    tables_df = self.client.query(tables_query).to_dataframe()
                    
                    for _, table_row in tables_df.iterrows():
                        table_name = table_row['table_name']
                        all_tables.add(table_name)
                        
                except Exception as e:
                    print(f"⚠️  Error obteniendo tablas de {project_id}: {str(e)}")
                    continue
            
            # Convertir a lista y ordenar
            unique_tables = sorted(list(all_tables))
            print(f"📊 Tablas únicas encontradas: {len(unique_tables)}")
            
            return unique_tables
            
        except Exception as e:
            print(f"❌ Error obteniendo tablas únicas: {str(e)}")
            return []
    
    def initialize_metadata_table(self, table_names=None):
        """
        Inicializa la tabla de metadatos con todas las tablas
        
        Args:
            table_names: Lista de nombres de tablas (opcional, si no se proporciona usa all_unique_tables)
        """
        print("🚀 INICIALIZANDO TABLA DE METADATOS")
        print("=" * 60)
        
        # Obtener tablas si no se proporcionan
        if table_names is None:
            table_names = self.get_all_unique_tables()
        
        if not table_names:
            print("❌ No se encontraron tablas para inicializar")
            return
        
        print(f"📋 Tablas a inicializar: {len(table_names)}")
        print("=" * 60)
        
        # Inicializar cada tabla
        success_count = 0
        error_count = 0
        
        for table_name in table_names:
            try:
                print(f"🔄 Inicializando: {table_name}")
                
                # Analizar campos de particionado automáticamente
                partition_fields = self.metadata_manager.analyze_partition_fields(table_name)
                print(f"  📊 Campos de particionado: {partition_fields}")
                
                # Configuración por defecto
                cluster_fields = ['company_id']
                update_strategy = 'incremental'
                
                # Actualizar metadatos
                self.metadata_manager.update_table_metadata(
                    table_name=table_name,
                    partition_fields=partition_fields,
                    cluster_fields=cluster_fields,
                    update_strategy=update_strategy
                )
                
                success_count += 1
                print(f"  ✅ {table_name} inicializada")
                
            except Exception as e:
                error_count += 1
                print(f"  ❌ Error inicializando {table_name}: {str(e)}")
        
        # Resumen final
        print(f"\n🎯 INICIALIZACIÓN COMPLETADA")
        print(f"✅ Tablas inicializadas: {success_count}")
        print(f"❌ Tablas con errores: {error_count}")
        print(f"📊 Total procesadas: {len(table_names)}")
    
    def show_initialization_summary(self):
        """
        Muestra resumen de la inicialización
        """
        print("\n📊 RESUMEN DE METADATOS INICIALIZADOS")
        print("=" * 60)
        
        df = self.metadata_manager.get_all_tables_metadata()
        
        if df.empty:
            print("⚠️  No hay metadatos configurados")
            return
        
        print(f"📋 Total de tablas configuradas: {len(df)}")
        print()
        
        # Mostrar algunas tablas como ejemplo
        for i, (_, row) in enumerate(df.head(10).iterrows()):
            print(f"📋 {row['table_name']}")
            print(f"  🗂️  Particionado: {row['partition_fields']}")
            print(f"  🔗 Clusterizado: {row['cluster_fields']}")
            print(f"  🔄 Estrategia: {row['update_strategy']}")
            print()
        
        if len(df) > 10:
            print(f"... y {len(df) - 10} tablas más")
    
    def initialize_from_tables_to_process(self):
        """
        Inicializa solo las tablas definidas en TABLES_TO_PROCESS
        """
        print("🚀 INICIALIZANDO METADATOS DE TABLES_TO_PROCESS")
        print("=" * 60)
        
        print(f"📋 Tablas en TABLES_TO_PROCESS: {len(TABLES_TO_PROCESS)}")
        for table_name in TABLES_TO_PROCESS:
            print(f"  - {table_name}")
        
        self.initialize_metadata_table(TABLES_TO_PROCESS)
    
    def interactive_initialization(self):
        """
        Inicialización interactiva
        """
        print("🔧 INICIALIZACIÓN INTERACTIVA DE METADATOS")
        print("=" * 60)
        
        print("💡 Opciones:")
        print("  1. Inicializar todas las tablas únicas (all_unique_tables)")
        print("  2. Inicializar solo TABLES_TO_PROCESS")
        print("  3. Inicializar tabla específica")
        print("  4. Mostrar resumen actual")
        print("  5. Salir")
        
        while True:
            try:
                choice = input("\n🔧 Selecciona una opción (1-5): ").strip()
                
                if choice == '1':
                    self.initialize_metadata_table()
                    self.show_initialization_summary()
                elif choice == '2':
                    self.initialize_from_tables_to_process()
                    self.show_initialization_summary()
                elif choice == '3':
                    self.initialize_single_table_interactive()
                elif choice == '4':
                    self.show_initialization_summary()
                elif choice == '5':
                    print("👋 Saliendo...")
                    break
                else:
                    print("❌ Opción inválida. Intenta de nuevo.")
                    
            except KeyboardInterrupt:
                print("\n👋 Saliendo...")
                break
            except Exception as e:
                print(f"❌ Error: {str(e)}")
    
    def initialize_single_table_interactive(self):
        """
        Inicialización interactiva de una tabla específica
        """
        table_name = input("🔧 Nombre de la tabla a inicializar: ").strip()
        
        if not table_name:
            print("❌ Nombre de tabla requerido")
            return
        
        try:
            self.initialize_metadata_table([table_name])
        except Exception as e:
            print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    initializer = MetadataInitializer()
    
    # Ejecutar inicialización interactiva
    initializer.interactive_initialization()
