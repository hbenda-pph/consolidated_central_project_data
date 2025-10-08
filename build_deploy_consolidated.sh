#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA CONSOLIDATED TABLES JOB
# =============================================================================

set -e  # Salir si hay algún error

# Configuración
PROJECT_ID="pph-central"
JOB_NAME="create-consolidated-tables-job"
REGION="us-east1"
SERVICE_ACCOUNT="data-analytics@platform-partners-pro.iam.gserviceaccount.com"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${JOB_NAME}"

echo "🚀 Iniciando Build & Deploy para Consolidated Tables Job"
echo "======================================================="
echo "📋 Configuración:"
echo "   Proyecto: ${PROJECT_ID}"
echo "   Job: ${JOB_NAME}"
echo "   Región: ${REGION}"
echo "   Imagen: ${IMAGE_TAG}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "consolidated_tables_job.py" ]; then
    echo "❌ Error: consolidated_tables_job.py no encontrado. Ejecuta este script desde el directorio consolidated_central_project_data/"
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

# Crear Dockerfile temporal específico para consolidated tables
cat > Dockerfile.consolidated << 'EOF'
FROM python:3.11-slim

WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar scripts necesarios
COPY consolidated_tables_job.py .

# Comando por defecto
CMD ["python", "consolidated_tables_job.py"]
EOF

gcloud builds submit --tag ${IMAGE_TAG} -f Dockerfile.consolidated

# Limpiar Dockerfile temporal
rm Dockerfile.consolidated

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
        --memory 8Gi \
        --cpu 4 \
        --max-retries 3 \
        --parallelism 1 \
        --task-timeout 3600 \
        --set-env-vars PYTHONUNBUFFERED=1 \
        --service-account ${SERVICE_ACCOUNT}
else
    echo "🆕 Job no existe, creando..."
    gcloud run jobs create ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --memory 8Gi \
        --cpu 4 \
        --max-retries 3 \
        --parallelism 1 \
        --task-timeout 3600 \
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
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Para ver logs del job:"
echo "   gcloud run jobs logs ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🛑 Para eliminar el job:"
echo "   gcloud run jobs delete ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"

