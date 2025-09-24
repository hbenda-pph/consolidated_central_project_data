# -*- coding: utf-8 -*-
"""
Cloud Shell Setup - Consolidated Central Project Data

Script de configuración e instalación para Cloud Shell.
Verifica y configura el entorno necesario para ejecutar el proceso de consolidación.
"""

import sys
import os
import subprocess
import json
from pathlib import Path

def run_command(command, description, check_output=False):
    """Ejecuta un comando y maneja errores"""
    print(f"🔄 {description}...")
    try:
        if check_output:
            result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
            return True, result.stdout.strip()
        else:
            result = subprocess.run(command, shell=True, check=True)
            return True, ""
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        return False, str(e)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False, str(e)

def check_gcloud_auth():
    """Verifica autenticación de gcloud"""
    print("\n🔐 Verificando autenticación de gcloud...")
    
    # Verificar si gcloud está instalado
    success, output = run_command("gcloud --version", "Verificando gcloud", True)
    if not success:
        print("❌ gcloud no está instalado o no está en PATH")
        return False
    
    print(f"✅ gcloud versión: {output.split()[0]}")
    
    # Verificar autenticación
    success, output = run_command("gcloud auth list --filter=status:ACTIVE --format='value(account)'", 
                                 "Verificando autenticación", True)
    if not success or not output:
        print("❌ No hay cuentas autenticadas activas")
        print("💡 Ejecuta: gcloud auth login")
        return False
    
    print(f"✅ Autenticado como: {output}")
    return True

def check_gcloud_config():
    """Verifica configuración de gcloud"""
    print("\n⚙️  Verificando configuración de gcloud...")
    
    # Verificar proyecto configurado
    success, project = run_command("gcloud config get-value project", 
                                  "Obteniendo proyecto configurado", True)
    if not success or not project:
        print("❌ No hay proyecto configurado")
        print("💡 Ejecuta: gcloud config set project YOUR_PROJECT_ID")
        return False
    
    print(f"✅ Proyecto configurado: {project}")
    
    # Verificar región
    success, region = run_command("gcloud config get-value compute/region", 
                                 "Obteniendo región configurada", True)
    if success and region:
        print(f"✅ Región configurada: {region}")
    else:
        print("⚠️  Región no configurada (opcional)")
    
    return True

def check_python_environment():
    """Verifica el entorno de Python"""
    print("\n🐍 Verificando entorno de Python...")
    
    # Verificar versión de Python
    python_version = sys.version_info
    print(f"✅ Python versión: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version < (3, 7):
        print("❌ Se requiere Python 3.7 o superior")
        return False
    
    # Verificar pip
    success, output = run_command("pip --version", "Verificando pip", True)
    if not success:
        print("❌ pip no está disponible")
        return False
    
    print(f"✅ pip versión: {output}")
    return True

def install_dependencies():
    """Instala dependencias de Python"""
    print("\n📦 Instalando dependencias de Python...")
    
    # Verificar si requirements.txt existe
    if not os.path.exists("requirements.txt"):
        print("❌ Archivo requirements.txt no encontrado")
        return False
    
    # Instalar dependencias
    success, output = run_command("pip install -r requirements.txt", 
                                 "Instalando dependencias")
    if not success:
        print("❌ Error instalando dependencias")
        return False
    
    print("✅ Dependencias instaladas exitosamente")
    return True

def verify_bigquery_access():
    """Verifica acceso a BigQuery"""
    print("\n🔍 Verificando acceso a BigQuery...")
    
    try:
        # Intentar importar la librería
        from google.cloud import bigquery
        print("✅ Librería google-cloud-bigquery importada correctamente")
        
        # Intentar crear cliente (esto verificará autenticación)
        client = bigquery.Client()
        print("✅ Cliente BigQuery creado exitosamente")
        
        # Verificar permisos básicos
        try:
            # Listar datasets (operación básica)
            datasets = list(client.list_datasets(max_results=1))
            print("✅ Permisos de lectura verificados")
        except Exception as e:
            print(f"⚠️  Permisos de lectura limitados: {e}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Error importando BigQuery: {e}")
        return False
    except Exception as e:
        print(f"❌ Error accediendo a BigQuery: {e}")
        return False

def create_directories():
    """Crea directorios necesarios"""
    print("\n📁 Creando directorios necesarios...")
    
    directories = [
        "execution_sessions",
        "logs",
        "output"
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✅ Directorio creado/verificado: {directory}")
        except Exception as e:
            print(f"❌ Error creando directorio {directory}: {e}")
            return False
    
    return True

def verify_config_file():
    """Verifica archivo de configuración"""
    print("\n⚙️  Verificando archivo de configuración...")
    
    if not os.path.exists("config.py"):
        print("❌ Archivo config.py no encontrado")
        return False
    
    try:
        # Intentar importar configuración
        import config
        print("✅ Archivo config.py importado correctamente")
        
        # Verificar configuraciones críticas
        required_configs = [
            'PROJECT_SOURCE',
            'DATASET_NAME', 
            'TABLE_NAME',
            'METADATA_FIELDS'
        ]
        
        for config_name in required_configs:
            if hasattr(config, config_name):
                print(f"✅ Configuración encontrada: {config_name}")
            else:
                print(f"❌ Configuración faltante: {config_name}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error importando configuración: {e}")
        return False

def run_basic_test():
    """Ejecuta prueba básica del sistema"""
    print("\n🧪 Ejecutando prueba básica...")
    
    try:
        # Importar módulos de prueba
        import test_single_table_analysis
        import generate_silver_views
        import analyze_data_types
        import diagnostic_test
        
        print("✅ Módulos de prueba importados correctamente")
        print("💡 Para ejecutar diagnóstico completo, usa: python diagnostic_test.py")
        print("💡 Para ejecutar prueba completa, usa: python cloud_shell_runner.py test")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en prueba básica: {e}")
        return False

def generate_setup_report(results):
    """Genera reporte de configuración"""
    print("\n📊 REPORTE DE CONFIGURACIÓN")
    print("=" * 50)
    
    total_checks = len(results)
    passed_checks = sum(results.values())
    
    print(f"Verificaciones realizadas: {total_checks}")
    print(f"Verificaciones exitosas: {passed_checks}")
    print(f"Verificaciones fallidas: {total_checks - passed_checks}")
    print(f"Tasa de éxito: {(passed_checks/total_checks)*100:.1f}%")
    
    print(f"\nDetalles:")
    for check_name, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {check_name}")
    
    if passed_checks == total_checks:
        print(f"\n🎯 ¡Configuración completada exitosamente!")
        print(f"💡 Puedes proceder con: python cloud_shell_runner.py test")
    else:
        print(f"\n⚠️  Configuración incompleta. Revisa los errores arriba.")
        print(f"💡 Una vez corregidos, ejecuta este script nuevamente.")

def main():
    """Función principal de configuración"""
    print("🚀 CONFIGURACIÓN DE CLOUD SHELL")
    print("=" * 50)
    print("Este script verificará y configurará el entorno necesario")
    print("para ejecutar el proceso de consolidación de datos.")
    print()
    
    results = {}
    
    # Ejecutar verificaciones
    results["Autenticación gcloud"] = check_gcloud_auth()
    results["Configuración gcloud"] = check_gcloud_config()
    results["Entorno Python"] = check_python_environment()
    results["Dependencias Python"] = install_dependencies()
    results["Acceso BigQuery"] = verify_bigquery_access()
    results["Archivos de configuración"] = verify_config_file()
    results["Directorios necesarios"] = create_directories()
    results["Prueba básica"] = run_basic_test()
    
    # Generar reporte
    generate_setup_report(results)
    
    # Guardar reporte
    report_file = f"setup_report_{subprocess.run(['date', '+%Y%m%d_%H%M%S'], 
                                                capture_output=True, text=True).stdout.strip()}.json"
    
    with open(report_file, 'w') as f:
        json.dump({
            'timestamp': subprocess.run(['date'], capture_output=True, text=True).stdout.strip(),
            'results': results,
            'total_checks': len(results),
            'passed_checks': sum(results.values()),
            'success_rate': (sum(results.values())/len(results))*100
        }, f, indent=2)
    
    print(f"\n📄 Reporte guardado: {report_file}")

if __name__ == "__main__":
    main()
