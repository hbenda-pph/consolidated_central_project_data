#!/usr/bin/env python3
"""
Entry point para Cloud Run Job - Generate Silver Views

Este script llama a generate_silver_views.py en modo NO INTERACTIVO
para ejecución en Cloud Run Job.

Usa metadata_consolidated_tables como fuente de verdad (use_metadata=True).
"""

from generate_silver_views import generate_all_silver_views

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 CLOUD RUN JOB - GENERATE SILVER VIEWS")
    print("⚙️  MODO: NO INTERACTIVO")
    print("📋 FUENTE: metadata_consolidated_tables")
    print("=" * 80)
    
    # Ejecutar en modo forzado, sin interacción, desde la letra 'a'
    # Para reiniciar desde otra letra, modificar el parámetro start_from_letter
    # use_metadata=True: usa metadata_consolidated_tables como fuente de verdad
    results, output_dir = generate_all_silver_views(
        force_mode=True,           # Procesa todas las compañías sin confirmación
        start_from_letter='a',     # Desde qué tabla iniciar (útil para reiniciar)
        use_metadata=True          # Usa metadata_consolidated_tables como fuente de verdad
    )
    
    print(f"\n✅ CLOUD RUN JOB COMPLETADO!")
    print(f"📁 Output: {output_dir}")
    print("=" * 80)

