"""
Cloud Run Job para ejecutar generate_silver_views.py
Evita timeouts y maneja ejecuciones largas
"""

import subprocess
import sys
import os
from datetime import datetime

def deploy_cloud_run_job():
    """
    Despliega Cloud Run Job para generar vistas Silver
    """
    print("🚀 Desplegando Cloud Run Job para generar vistas Silver")
    
    # Configuración
    PROJECT_ID = "platform-partners-des"
    REGION = "us-east1"
    JOB_NAME = "generate-silver-views-job"
    IMAGE_NAME = f"gcr.io/{PROJECT_ID}/silver-views-generator"
    
    print(f"📋 Configuración:")
    print(f"   Proyecto: {PROJECT_ID}")
    print(f"   Región: {REGION}")
    print(f"   Job: {JOB_NAME}")
    print(f"   Imagen: {IMAGE_NAME}")
    
    try:
        # 1. Construir imagen Docker
        print("\n📦 Construyendo imagen Docker...")
        build_cmd = [
            "gcloud", "builds", "submit", 
            "--tag", IMAGE_NAME,
            "."
        ]
        
        result = subprocess.run(build_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error construyendo imagen: {result.stderr}")
            return False
        
        print("✅ Imagen construida exitosamente")
        
        # 2. Crear/actualizar Cloud Run Job
        print("\n🔧 Creando Cloud Run Job...")
        
        # Verificar si el job ya existe
        check_cmd = ["gcloud", "run", "jobs", "describe", JOB_NAME, "--region", REGION]
        check_result = subprocess.run(check_cmd, capture_output=True, text=True)
        
        if check_result.returncode == 0:
            # Job existe, actualizar
            print("📝 Job existe, actualizando...")
            create_cmd = [
                "gcloud", "run", "jobs", "replace", 
                "--region", REGION,
                "--image", IMAGE_NAME,
                "--memory", "4Gi",
                "--cpu", "2",
                "--max-retries", "3",
                "--parallelism", "1",
                "--task-count", "1",
                "--set-env-vars", "PYTHONUNBUFFERED=1"
            ]
        else:
            # Job no existe, crear
            print("🆕 Creando nuevo job...")
            create_cmd = [
                "gcloud", "run", "jobs", "create", JOB_NAME,
                "--image", IMAGE_NAME,
                "--region", REGION,
                "--memory", "4Gi",
                "--cpu", "2",
                "--max-retries", "3",
                "--parallelism", "1",
                "--task-count", "1",
                "--set-env-vars", "PYTHONUNBUFFERED=1"
            ]
        
        result = subprocess.run(create_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error creando job: {result.stderr}")
            return False
        
        print("✅ Cloud Run Job creado/actualizado exitosamente")
        
        # 3. Ejecutar job
        print("\n▶️  Ejecutando job...")
        execute_cmd = [
            "gcloud", "run", "jobs", "execute", JOB_NAME,
            "--region", REGION,
            "--wait"
        ]
        
        result = subprocess.run(execute_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error ejecutando job: {result.stderr}")
            return False
        
        print("✅ Job ejecutado exitosamente")
        
        # 4. Mostrar logs
        print("\n📊 Obteniendo logs...")
        logs_cmd = [
            "gcloud", "run", "jobs", "logs", JOB_NAME,
            "--region", REGION,
            "--limit", "50"
        ]
        
        result = subprocess.run(logs_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("📋 Logs del job:")
            print(result.stdout)
        else:
            print(f"⚠️  No se pudieron obtener logs: {result.stderr}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        return False

def execute_job_only():
    """
    Solo ejecuta el job (sin desplegar)
    """
    print("🚀 Ejecutando Cloud Run Job existente")
    
    PROJECT_ID = "pph-central"
    REGION = "us-east1"
    JOB_NAME = "generate-silver-views-job"
    
    try:
        # Ejecutar job
        execute_cmd = [
            "gcloud", "run", "jobs", "execute", JOB_NAME,
            "--region", REGION,
            "--wait"
        ]
        
        result = subprocess.run(execute_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Error ejecutando job: {result.stderr}")
            return False
        
        print("✅ Job ejecutado exitosamente")
        
        # Mostrar logs
        print("\n📊 Obteniendo logs...")
        logs_cmd = [
            "gcloud", "run", "jobs", "logs", JOB_NAME,
            "--region", REGION,
            "--limit", "50"
        ]
        
        result = subprocess.run(logs_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("📋 Logs del job:")
            print(result.stdout)
        
        return True
        
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        return False

def show_job_status():
    """
    Muestra el estado del job
    """
    print("📊 Estado del Cloud Run Job")
    
    PROJECT_ID = "pph-central"
    REGION = "us-east1"
    JOB_NAME = "generate-silver-views-job"
    
    try:
        # Describir job
        describe_cmd = [
            "gcloud", "run", "jobs", "describe", JOB_NAME,
            "--region", REGION,
            "--format", "value(status.conditions[0].type,status.conditions[0].status)"
        ]
        
        result = subprocess.run(describe_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"🔍 Estado: {result.stdout.strip()}")
        else:
            print(f"⚠️  No se pudo obtener estado: {result.stderr}")
        
        # Listar ejecuciones
        list_cmd = [
            "gcloud", "run", "jobs", "executions", "list",
            "--job", JOB_NAME,
            "--region", REGION,
            "--limit", "5"
        ]
        
        result = subprocess.run(list_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("\n📈 Ejecuciones recientes:")
            print(result.stdout)
        
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command in ["deploy", "d"]:
            deploy_cloud_run_job()
        elif command in ["execute", "e"]:
            execute_job_only()
        elif command in ["status", "s"]:
            show_job_status()
        else:
            print("❌ Comando inválido. Usa: deploy, execute, o status")
    else:
        print("🚀 Cloud Run Job para generar vistas Silver")
        print("\n💡 Comandos disponibles:")
        print("  python generate_silver_views_job.py deploy   - Desplegar y ejecutar job")
        print("  python generate_silver_views_job.py execute  - Solo ejecutar job existente")
        print("  python generate_silver_views_job.py status   - Ver estado del job")
        print("\n📋 Configuración:")
        print("  Proyecto: pph-central")
        print("  Región: us-east1")
        print("  Job: generate-silver-views-job")
        print("  Memoria: 4Gi, CPU: 2")
