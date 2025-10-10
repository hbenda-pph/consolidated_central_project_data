#!/usr/bin/env python3
"""
Script para habilitar todos los Scheduled Queries consolidados
Ejecutar después de que el Job principal haya creado las tablas y schedules
"""

from google.cloud import bigquery_datatransfer_v1
from datetime import datetime

PROJECT_CENTRAL = "pph-central"

def enable_all_scheduled_queries():
    """
    Habilita todos los scheduled queries con prefijo sq_consolidated_
    """
    print("=" * 80)
    print("🔄 HABILITANDO SCHEDULED QUERIES CONSOLIDADOS")
    print("=" * 80)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Proyecto: {PROJECT_CENTRAL}")
    print("=" * 80)
    
    try:
        # Cliente de Data Transfer
        transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient()
        parent = f"projects/{PROJECT_CENTRAL}/locations/us"
        
        # Listar todos los scheduled queries
        list_request = bigquery_datatransfer_v1.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"]
        )
        
        # Filtrar los que empiezan con sq_consolidated_
        schedules_to_enable = []
        for config in transfer_client.list_transfer_configs(request=list_request):
            if config.display_name.startswith("sq_consolidated_"):
                schedules_to_enable.append(config)
        
        if not schedules_to_enable:
            print("\n⚠️  No se encontraron Scheduled Queries con prefijo 'sq_consolidated_'")
            print("   Verifica que el Job principal haya ejecutado correctamente")
            return
        
        print(f"\n📋 Scheduled Queries encontrados: {len(schedules_to_enable)}")
        print()
        
        # Habilitar cada uno
        enabled_count = 0
        error_count = 0
        
        for config in schedules_to_enable:
            try:
                # Actualizar solo el campo disabled
                config.disabled = False
                update_mask = {"paths": ["disabled"]}
                
                transfer_client.update_transfer_config(
                    transfer_config=config,
                    update_mask=update_mask
                )
                
                print(f"  ✅ {config.display_name}")
                enabled_count += 1
                
            except Exception as e:
                print(f"  ❌ {config.display_name}: {str(e)[:100]}")
                error_count += 1
        
        # Resumen
        print()
        print("=" * 80)
        print("🎯 RESUMEN")
        print("=" * 80)
        print(f"✅ Schedules habilitados: {enabled_count}")
        print(f"❌ Errores: {error_count}")
        print(f"📊 Total procesados: {len(schedules_to_enable)}")
        print("=" * 80)
        
        if enabled_count > 0:
            print()
            print("🎉 ¡LISTO! Todos los Scheduled Queries están ahora ACTIVOS")
            print("⏰ Correrán cada 6 horas desde este momento")
            print(f"📅 Próxima ejecución: ~{datetime.now().strftime('%H:%M')} + 6 horas")
            print()
        
        return enabled_count, error_count
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    
    try:
        enabled, errors = enable_all_scheduled_queries()
        
        if errors > 0:
            print(f"\n⚠️  Completado con {errors} error(es)")
            sys.exit(1)
        else:
            print("\n✅ ¡Todos los schedules habilitados exitosamente!")
            sys.exit(0)
            
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        sys.exit(1)

