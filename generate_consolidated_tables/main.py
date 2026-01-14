#!/usr/bin/env python3
"""
Entry point para Cloud Run Job - Generate Consolidated Tables

Este script llama a generate_consolidated_tables.py en modo automático
para ejecución en Cloud Run Job.
Soporta paralelismo por rango de compañías cuando se ejecuta con múltiples tareas.
"""

import os
from generate_consolidated_tables import create_all_consolidated_tables

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 CLOUD RUN JOB - CREATE CONSOLIDATED TABLES")
    print("⚙️  MODO: AUTOMÁTICO (Sin interacción)")
    print("=" * 80)
    
    # Detectar si estamos en modo paralelo (Cloud Run Jobs con múltiples tareas)
    task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', '0'))
    task_count = int(os.environ.get('CLOUD_RUN_TASK_COUNT', '1'))
    is_parallel = task_count > 1
    
    # IMPORTANTE: En modo paralelo, dividimos las TABLAS entre tareas (no las compañías)
    # Cada tabla consolidada necesita TODAS las compañías en un UNION ALL
    # Por lo tanto, cada tarea procesa un subconjunto de tablas pero con todas las compañías
    company_id_filter = None
    if is_parallel:
        print(f"\n{'='*80}")
        print(f"🚀 MODO PARALELO ACTIVADO")
        print(f"   Tarea: {task_index + 1}/{task_count}")
        print(f"   Esta tarea procesará un subconjunto de tablas")
        print(f"   Cada tabla se creará con TODAS las compañías disponibles")
        print(f"{'='*80}\n")
    
    # Ejecutar creación de tablas con scheduled queries
    stats = create_all_consolidated_tables(
        create_schedules=True,  # Crear scheduled queries automáticamente
        company_id_filter=company_id_filter  # Filtrar compañías si está en modo paralelo
    )
    
    print(f"\n✅ CLOUD RUN JOB COMPLETADO!")
    if is_parallel:
        print(f"📊 Tarea {task_index + 1}/{task_count} - Tablas creadas: {stats['success_count']}")
        print(f"❌ Tarea {task_index + 1}/{task_count} - Errores: {stats['error_count']}")
    else:
        print(f"📊 Tablas creadas: {stats['success_count']}")
        print(f"❌ Errores: {stats['error_count']}")
    print("=" * 80)
    
    # Exit code basado en resultado
    import sys
    sys.exit(1 if stats['error_count'] > 0 else 0)

