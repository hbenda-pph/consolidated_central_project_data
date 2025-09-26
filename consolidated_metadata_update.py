"""
Script para actualizar metadatos de tablas consolidadas
Permite configurar particionado y clusterizado por tabla
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from config import PROJECT_SOURCE, TABLES_TO_PROCESS
from consolidated_metadata_manager import ConsolidatedMetadataManager

class MetadataUpdater:
    """Actualizador de metadatos de tablas consolidadas"""
    
    def __init__(self):
        self.metadata_manager = ConsolidatedMetadataManager()
    
    def update_table_metadata(self, table_name, partition_fields=None, cluster_fields=None, update_strategy=None):
        """
        Actualiza metadatos de una tabla específica
        
        Args:
            table_name: Nombre de la tabla
            partition_fields: Array de campos de particionado (opcional)
            cluster_fields: Array de campos de clusterizado (opcional)
            update_strategy: Estrategia de actualización (opcional)
        """
        print(f"🔄 Actualizando metadatos para: {table_name}")
        
        # Si no se especifican campos, analizar automáticamente
        if partition_fields is None:
            partition_fields = self.metadata_manager.analyze_partition_fields(table_name)
            print(f"  📊 Campos de particionado detectados: {partition_fields}")
        
        if cluster_fields is None:
            cluster_fields = ['company_id']  # Por defecto
            print(f"  📊 Campos de clusterizado por defecto: {cluster_fields}")
        
        if update_strategy is None:
            update_strategy = 'incremental'
            print(f"  📊 Estrategia por defecto: {update_strategy}")
        
        # Actualizar metadatos
        self.metadata_manager.update_table_metadata(
            table_name=table_name,
            partition_fields=partition_fields,
            cluster_fields=cluster_fields,
            update_strategy=update_strategy
        )
    
    def update_multiple_tables(self, table_configs):
        """
        Actualiza metadatos de múltiples tablas
        
        Args:
            table_configs: Lista de diccionarios con configuración
                [
                    {
                        'table_name': 'call',
                        'partition_fields': ['created_on', 'updated_on'],
                        'cluster_fields': ['company_id', 'location_id'],
                        'update_strategy': 'incremental'
                    },
                    ...
                ]
        """
        print("🚀 ACTUALIZANDO METADATOS DE MÚLTIPLES TABLAS")
        print("=" * 60)
        
        for config in table_configs:
            table_name = config['table_name']
            partition_fields = config.get('partition_fields')
            cluster_fields = config.get('cluster_fields')
            update_strategy = config.get('update_strategy')
            
            self.update_table_metadata(
                table_name=table_name,
                partition_fields=partition_fields,
                cluster_fields=cluster_fields,
                update_strategy=update_strategy
            )
            print()
    
    def update_all_tables_default(self):
        """
        Actualiza todas las tablas con configuración por defecto
        """
        print("🚀 ACTUALIZANDO TODAS LAS TABLAS CON CONFIGURACIÓN POR DEFECTO")
        print("=" * 60)
        
        for table_name in TABLES_TO_PROCESS:
            self.update_table_metadata(table_name)
            print()
    
    def show_current_metadata(self):
        """
        Muestra metadatos actuales de todas las tablas
        """
        print("📊 METADATOS ACTUALES DE TABLAS CONSOLIDADAS")
        print("=" * 60)
        
        df = self.metadata_manager.get_all_tables_metadata()
        
        if df.empty:
            print("⚠️  No hay metadatos configurados")
            return
        
        for _, row in df.iterrows():
            print(f"📋 {row['table_name']}")
            print(f"  🗂️  Particionado: {row['partition_fields']}")
            print(f"  🔗 Clusterizado: {row['cluster_fields']}")
            print(f"  🔄 Estrategia: {row['update_strategy']}")
            print(f"  📅 Actualizado: {row['updated_at']}")
            print()
    
    def interactive_update(self):
        """
        Actualización interactiva de metadatos
        """
        print("🔧 ACTUALIZACIÓN INTERACTIVA DE METADATOS")
        print("=" * 60)
        
        # Mostrar tablas disponibles
        print("📋 Tablas disponibles:")
        for i, table_name in enumerate(TABLES_TO_PROCESS, 1):
            print(f"  {i}. {table_name}")
        
        print("\n💡 Opciones:")
        print("  1. Actualizar tabla específica")
        print("  2. Actualizar todas las tablas con configuración por defecto")
        print("  3. Mostrar metadatos actuales")
        print("  4. Salir")
        
        while True:
            try:
                choice = input("\n🔧 Selecciona una opción (1-4): ").strip()
                
                if choice == '1':
                    self.update_single_table_interactive()
                elif choice == '2':
                    self.update_all_tables_default()
                elif choice == '3':
                    self.show_current_metadata()
                elif choice == '4':
                    print("👋 Saliendo...")
                    break
                else:
                    print("❌ Opción inválida. Intenta de nuevo.")
                    
            except KeyboardInterrupt:
                print("\n👋 Saliendo...")
                break
            except Exception as e:
                print(f"❌ Error: {str(e)}")
    
    def update_single_table_interactive(self):
        """
        Actualización interactiva de una tabla específica
        """
        print("\n📋 Tablas disponibles:")
        for i, table_name in enumerate(TABLES_TO_PROCESS, 1):
            print(f"  {i}. {table_name}")
        
        try:
            table_choice = input("\n🔧 Selecciona número de tabla: ").strip()
            table_index = int(table_choice) - 1
            
            if 0 <= table_index < len(TABLES_TO_PROCESS):
                table_name = TABLES_TO_PROCESS[table_index]
                
                print(f"\n🔄 Actualizando: {table_name}")
                
                # Campos de particionado
                partition_input = input("🗂️  Campos de particionado (separados por coma, Enter para auto-detectar): ").strip()
                partition_fields = None
                if partition_input:
                    partition_fields = [field.strip() for field in partition_input.split(',')]
                
                # Campos de clusterizado
                cluster_input = input("🔗 Campos de clusterizado (separados por coma, Enter para ['company_id']): ").strip()
                cluster_fields = None
                if cluster_input:
                    cluster_fields = [field.strip() for field in cluster_input.split(',')]
                
                # Estrategia
                strategy_input = input("🔄 Estrategia (incremental/full_refresh, Enter para 'incremental'): ").strip()
                update_strategy = strategy_input if strategy_input else None
                
                # Actualizar
                self.update_table_metadata(
                    table_name=table_name,
                    partition_fields=partition_fields,
                    cluster_fields=cluster_fields,
                    update_strategy=update_strategy
                )
                
            else:
                print("❌ Número de tabla inválido")
                
        except ValueError:
            print("❌ Número inválido")
        except Exception as e:
            print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    updater = MetadataUpdater()
    
    # Ejecutar actualización interactiva
    updater.interactive_update()
