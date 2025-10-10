#!/usr/bin/env python3
"""
Script para habilitar todos los Scheduled Queries consolidados
Ejecutar después de que el Job principal haya creado las tablas y schedules
"""

from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timedelta
import time
import pytz

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
                # Calcular start_time: Hoy 7pm CST/CDT
                # America/Chicago maneja automáticamente DST (Daylight Saving Time)
                chicago_tz = pytz.timezone('America/Chicago')
                now_chicago = datetime.now(chicago_tz)
                
                # Establecer a las 7pm hora de Chicago
                target_time = now_chicago.replace(hour=19, minute=0, second=0, microsecond=0)
                
                # Si ya pasaron las 7pm, programar para mañana
                if now_chicago.hour >= 19:
                    target_time += timedelta(days=1)
                
                # Convertir a UTC para BigQuery
                target_time_utc = target_time.astimezone(pytz.UTC)
                
                # Convertir a timestamp Unix
                start_timestamp = Timestamp()
                start_timestamp.FromSeconds(int(target_time_utc.timestamp()))
                
                # Crear ScheduleOptions con start_time
                schedule_options = bigquery_datatransfer_v1.ScheduleOptions(
                    start_time=start_timestamp
                )
                
                config.disabled = False
                config.schedule_options = schedule_options
                
                update_mask = {"paths": ["disabled", "schedule_options"]}
                
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
            # Mostrar hora programada
            chicago_tz = pytz.timezone('America/Chicago')
            now_chicago = datetime.now(chicago_tz)
            target_time = now_chicago.replace(hour=19, minute=0, second=0, microsecond=0)
            if now_chicago.hour >= 19:
                target_time += timedelta(days=1)
            
            target_time_utc = target_time.astimezone(pytz.UTC)
            
            print()
            print("🎉 ¡LISTO! Todos los Scheduled Queries están ahora ACTIVOS")
            print(f"⏰ Primera ejecución (Chicago): {target_time.strftime('%Y-%m-%d %I:%M %p %Z')}")
            print(f"⏰ Primera ejecución (UTC):     {target_time_utc.strftime('%Y-%m-%d %I:%M %p %Z')}")
            print("📅 Luego correrán cada 6 horas automáticamente")
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

