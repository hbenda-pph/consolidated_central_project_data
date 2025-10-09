#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA GENERATE SILVER VIEWS JOB
# =============================================================================

set -e  # Salir si hay algún error

# Configuración
PROJECT_ID="platform-partners-des"
JOB_NAME="generate-silver-views-job"
REGION="us-east1"
SERVICE_ACCOUNT="data-analytics@${PROJECT_ID}.iam.gserviceaccount.com"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${JOB_NAME}"
MEMORY="8Gi"
CPU="4"
MAX_RETRIES="3"
PARALLELISM="2"
TASK_TIMEOUT="7200"

echo "🚀 Iniciando Build & Deploy para Generate Silver Views Job"
echo "======================================================="
echo "📋 Configuración:"
echo "   Proyecto: ${PROJECT_ID}"
echo "   Job: ${JOB_NAME}"
echo "   Región: ${REGION}"
echo "   Imagen: ${IMAGE_TAG}"
echo "   Memory: ${MEMORY}"
echo "   CPU:    ${CPU}"
echo "   Max Retries:  ${MAX_RETRIES}"
echo "   Parallelism:  ${PARALLELISM}"
echo "   Task Timeout: ${TASK_TIMEOUT}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado. Ejecuta este script desde generate_silver_views/"
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
gcloud builds submit --tag ${IMAGE_TAG}

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
if gcloud run jobs describe ${JOB_NAME} --region=${REGION} &> /dev/null; then
    echo "📝 Job existe, actualizando..."
    gcloud run jobs update ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --memory ${MEMORY} \
        --cpu ${CPU} \
        --max-retries ${MAX_RETRIES} \
        --parallelism ${PARALLELISM} \
        --task-timeout ${TASK_TIMEOUT} \
        --set-env-vars PYTHONUNBUFFERED=1 \
        --service-account ${SERVICE_ACCOUNT}
else
    echo "🆕 Job no existe, creando..."
    gcloud run jobs create ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --memory ${MEMORY} \
        --cpu ${CPU} \
        --max-retries ${MAX_RETRIES} \
        --parallelism ${PARALLELISM} \
        --task-timeout ${TASK_TIMEOUT} \
        --set-env-vars PYTHONUNBUFFERED=1 \
        --service-account ${SERVICE_ACCOUNT}
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
echo "📊 Para ejecutar el job:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION}"
echo ""
echo "🔧 Para ver logs del job:"
echo "   gcloud run jobs logs ${JOB_NAME} --region=${REGION}"
echo ""
echo "🛑 Para eliminar el job:"
echo "   gcloud run jobs delete ${JOB_NAME} --region=${REGION}"