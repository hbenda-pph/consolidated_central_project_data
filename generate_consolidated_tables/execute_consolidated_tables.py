#!/usr/bin/env python3
"""
Script ejecutable para crear todas las tablas consolidadas
Ejecuta el proceso completo de consolidación en pph-central.bronze
"""

import sys
import os
from datetime import datetime

# Agregar el directorio actual al path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from consolidated_tables_create import ConsolidatedTableCreator
from config import TABLES_TO_PROCESS

def main():
    """Función principal para crear todas las tablas consolidadas"""
    print("🚀 CREACIÓN DE TABLAS CONSOLIDADAS")
    print("=" * 60)
    print(f"⏰ Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Proyecto destino: pph-central.bronze")
    print(f"📋 Total de tablas a procesar: {len(TABLES_TO_PROCESS)}")
    print("=" * 60)
    
    try:
        # Crear instancia del creador
        creator = ConsolidatedTableCreator()
        
        # Ejecutar creación de todas las tablas
        creator.create_all_consolidated_tables()
        
        print("\n" + "=" * 60)
        print("🎉 PROCESO COMPLETADO")
        print(f"⏰ Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        print("\n📋 PRÓXIMOS PASOS:")
        print("1. ✅ Verificar tablas creadas en pph-central.bronze")
        print("2. 🔄 Crear vistas consolidadas en pph-central.silver")
        print("3. 🧪 Probar consultas en las vistas finales")
        
    except KeyboardInterrupt:
        print("\n⚠️  PROCESO INTERRUMPIDO POR EL USUARIO")
        print("🔄 Puedes reanudar ejecutando el script nuevamente")
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        print("🔍 Revisa los logs anteriores para más detalles")
        sys.exit(1)

if __name__ == "__main__":
    main()
