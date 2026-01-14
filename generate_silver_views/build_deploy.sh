#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA GENERATE SILVER VIEWS JOB (Cloud Run Job)
# Multi-Environment: DEV, QUA, PRO
# =============================================================================

set -e  # Salir si hay algún error

# =============================================================================
# CONFIGURACIÓN DE AMBIENTES
# =============================================================================

# Detectar proyecto activo de gcloud
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

# Si se proporciona parámetro, usarlo; si no, detectar automáticamente
if [ -n "$1" ]; then
    # Parámetro proporcionado explícitamente
    ENVIRONMENT="$1"
    ENVIRONMENT=$(echo "$ENVIRONMENT" | tr '[:upper:]' '[:lower:]')  # Convertir a minúsculas
    
    # Validar ambiente
    if [[ ! "$ENVIRONMENT" =~ ^(dev|qua|pro)$ ]]; then
        echo "❌ Error: Ambiente inválido '$ENVIRONMENT'"
        echo "Uso: ./build_deploy.sh [dev|qua|pro]"
        echo ""
        echo "Ejemplos:"
        echo "  ./build_deploy.sh dev    # Deploy en DEV (platform-partners-des)"
        echo "  ./build_deploy.sh qua    # Deploy en QUA (platform-partners-qua)"
        echo "  ./build_deploy.sh pro    # Deploy en PRO (platform-partners-pro)"
        echo ""
        echo "O ejecuta sin parámetros para usar el proyecto activo de gcloud"
        exit 1
    fi
else
    # Detectar automáticamente según el proyecto activo
    echo "🔍 Detectando ambiente desde proyecto activo de gcloud..."
    
    case "$CURRENT_PROJECT" in
        platform-partners-des)
            ENVIRONMENT="dev"
            echo "✅ Detectado: DEV (platform-partners-des)"
            ;;
        platform-partners-qua)
            ENVIRONMENT="qua"
            echo "✅ Detectado: QUA (platform-partners-qua)"
            ;;
        platform-partners-pro)
            ENVIRONMENT="pro"
            echo "✅ Detectado: PRO (platform-partners-pro)"
            ;;
        *)
            echo "⚠️  Proyecto activo: ${CURRENT_PROJECT}"
            echo "⚠️  No se reconoce el proyecto. Usando DEV por defecto."
            ENVIRONMENT="dev"
            ;;
    esac
fi

# Configuración según ambiente
case "$ENVIRONMENT" in
    dev)
        PROJECT_ID="platform-partners-des"
        JOB_NAME="generate-silver-views-job-dev"
        SERVICE_ACCOUNT="data-analytics@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        JOB_NAME="generate-silver-views-job-qua"
        SERVICE_ACCOUNT="data-analytics@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="constant-height-455614-i0"  # Project ID real de PRO (necesario para build y comandos gcloud)
        PROJECT_NAME="platform-partners-pro"    # Project name (para variable de entorno GCP_PROJECT)
        JOB_NAME="generate-silver-views-job"
        SERVICE_ACCOUNT="data-analytics@${PROJECT_ID}.iam.gserviceaccount.com"
        ;;
esac

# Para DEV y QUA, PROJECT_NAME = PROJECT_ID (son iguales)
if [ -z "$PROJECT_NAME" ]; then
    PROJECT_NAME="${PROJECT_ID}"
fi

REGION="us-east1"
IMAGE_NAME="generate-silver-views-job"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
MEMORY="8Gi"
CPU="4"
MAX_RETRIES="3"
TASK_TIMEOUT="7200"

# Configuración de paralelismo (Cloud Run Jobs)
# PARALLELISM: Número de tareas que se ejecutan simultáneamente
# TASKS: Número total de tareas a ejecutar
# Ejemplo: Si tienes 30 compañías y TASKS=3, cada tarea procesará ~10 compañías
# Para desactivar paralelismo, establecer ambos a 1
PARALLELISM="3"  # Ejecutar 3 tareas en paralelo
TASKS="3"         # Total de 3 tareas

echo "🚀 Iniciando Build & Deploy para Generate Silver Views Job"
echo "==========================================================="
echo "🌍 AMBIENTE: ${ENVIRONMENT^^}"
echo "📋 Configuración:"
echo "   Proyecto ID: ${PROJECT_ID}"
if [ "$ENVIRONMENT" = "pro" ]; then
    echo "   Proyecto Name: ${PROJECT_NAME}"
fi
echo "   Job Name: ${JOB_NAME}"
echo "   Región: ${REGION}"
echo "   Imagen: ${IMAGE_TAG}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo "   Memoria: ${MEMORY}"
echo "   CPU: ${CPU}"
echo "   Timeout: ${TASK_TIMEOUT}s"
if [ "$TASKS" != "1" ]; then
    echo "   🚀 Paralelismo: ${PARALLELISM} tareas simultáneas, ${TASKS} tareas totales"
fi
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado."
    echo "   Ejecuta este script desde el directorio generate_silver_views/"
    exit 1
fi

# Verificar que gcloud está configurado
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI no está instalado o no está en el PATH"
    exit 1
fi

# Verificar proyecto activo
CURRENT_PROJECT=$(gcloud config get-value project)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo "⚠️  Proyecto actual: ${CURRENT_PROJECT}"
    echo "🔧 Configurando proyecto a: ${PROJECT_ID}"
    gcloud config set project ${PROJECT_ID}
fi

echo ""
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "=========================================="
gcloud builds submit --tag ${IMAGE_TAG} --project=${PROJECT_ID}

if [ $? -eq 0 ]; then
    echo "✅ Build exitoso!"
else
    echo "❌ Error en el build"
    exit 1
fi

echo ""
echo "🚀 PASO 2: CREATE/UPDATE JOB"
echo "============================="

# Verificar si el job ya existe
if gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} &> /dev/null; then
    echo "📝 Job existe, actualizando..."
    # Construir comando base
    UPDATE_CMD="gcloud run jobs update ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --service-account ${SERVICE_ACCOUNT} \
        --memory ${MEMORY} \
        --cpu ${CPU} \
        --max-retries ${MAX_RETRIES} \
        --task-timeout ${TASK_TIMEOUT} \
        --set-env-vars PYTHONUNBUFFERED=1"
    
    # Agregar paralelismo si está configurado
    if [ "$TASKS" != "1" ]; then
        UPDATE_CMD="${UPDATE_CMD} --parallelism ${PARALLELISM} --tasks ${TASKS}"
    fi
    
    eval ${UPDATE_CMD}
else
    echo "🆕 Job no existe, creando..."
    # Construir comando base
    CREATE_CMD="gcloud run jobs create ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --service-account ${SERVICE_ACCOUNT} \
        --memory ${MEMORY} \
        --cpu ${CPU} \
        --max-retries ${MAX_RETRIES} \
        --task-timeout ${TASK_TIMEOUT} \
        --set-env-vars PYTHONUNBUFFERED=1"
    
    # Agregar paralelismo si está configurado
    if [ "$TASKS" != "1" ]; then
        CREATE_CMD="${CREATE_CMD} --parallelism ${PARALLELISM} --tasks ${TASKS}"
    fi
    
    eval ${CREATE_CMD}
fi

if [ $? -eq 0 ]; then
    echo "✅ Job creado/actualizado exitosamente!"
else
    echo "❌ Error creando/actualizando job"
    exit 1
fi

echo ""
echo "🎉 ¡DEPLOY COMPLETADO EXITOSAMENTE!"
echo "===================================="
echo ""
echo "🌍 AMBIENTE: ${ENVIRONMENT^^}"
echo "📊 Para ejecutar el Job:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Para ver logs del último Job:"
echo "   gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}\" --limit=50 --format=\"table(timestamp,severity,textPayload)\" --project=${PROJECT_ID}"
echo ""
echo "📋 Para ver detalles del Job:"
echo "   gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔄 Para deploy en otros ambientes:"
echo "   ./build_deploy.sh dev    # Deploy en DEV (desarrollo)"
echo "   ./build_deploy.sh qua    # Deploy en QUA (validación)"
echo "   ./build_deploy.sh pro    # Deploy en PRO (producción)"
echo ""
echo "📝 Notas:"
echo "   - DEV: Para desarrollo y testing"
echo "   - QUA: Para validación y QA"
echo "   - PRO: Para producción con datos reales"
echo ""
