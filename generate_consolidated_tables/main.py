#!/usr/bin/env python3
"""
Entry point para Cloud Run Job - Generate Consolidated Tables

Este script llama a generate_consolidated_tables.py en modo automático
para ejecución en Cloud Run Job.
"""

from generate_consolidated_tables import create_all_consolidated_tables

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 CLOUD RUN JOB - CREATE CONSOLIDATED TABLES")
    print("⚙️  MODO: AUTOMÁTICO (Sin interacción)")
    print("=" * 80)
    
    # Ejecutar creación de tablas con scheduled queries
    stats = create_all_consolidated_tables(
        create_schedules=True  # Crear scheduled queries automáticamente
    )
    
    print(f"\n✅ CLOUD RUN JOB COMPLETADO!")
    print(f"📊 Tablas creadas: {stats['success_count']}")
    print(f"❌ Errores: {stats['error_count']}")
    print("=" * 80)
    
    # Exit code basado en resultado
    import sys
    sys.exit(1 if stats['error_count'] > 0 else 0)

