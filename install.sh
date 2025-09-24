#!/bin/bash

# Script de instalación rápida para Cloud Shell
# Consolidated Central Project Data

echo "🚀 INSTALANDO CONSOLIDATED CENTRAL PROJECT DATA"
echo "=============================================="

# Verificar que estamos en Cloud Shell
if [ -z "$CLOUD_SHELL" ]; then
    echo "⚠️  Este script está diseñado para Cloud Shell"
    echo "   Continúa manualmente si estás en otro entorno"
fi

# Crear directorio de trabajo
echo "📁 Creando directorio de trabajo..."
mkdir -p ~/consolidated_central_project_data
cd ~/consolidated_central_project_data

# Instalar dependencias de Python
echo "📦 Instalando dependencias de Python..."
pip install --upgrade pip
pip install -r requirements.txt

# Verificar autenticación de Google Cloud
echo "🔐 Verificando autenticación..."
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
    echo "❌ No hay autenticación activa. Ejecutando gcloud auth login..."
    gcloud auth login
fi

# Mostrar configuración actual
echo "📋 Configuración actual de gcloud:"
gcloud config list

# Verificar proyecto
PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT" ]; then
    echo "❌ No hay proyecto configurado. Configura uno con:"
    echo "   gcloud config set project TU-PROYECTO"
    exit 1
fi

echo "✅ Proyecto configurado: $PROJECT"

# Verificar permisos de BigQuery
echo "🔍 Verificando permisos de BigQuery..."
if ! bq ls > /dev/null 2>&1; then
    echo "❌ No se pueden acceder a los datasets de BigQuery"
    echo "   Verifica que tienes permisos de BigQuery Data Viewer"
    exit 1
fi

echo "✅ Permisos de BigQuery verificados"

# Crear directorio para archivos generados
echo "📁 Creando directorios de salida..."
mkdir -p generated_views

# Mostrar instrucciones
echo ""
echo "🎯 INSTALACIÓN COMPLETADA"
echo "========================="
echo ""
echo "📋 Próximos pasos:"
echo "1. Edita config.py con tus valores específicos:"
echo "   - PROJECT_SOURCE: Tu proyecto fuente"
echo "   - CENTRAL_PROJECT: Tu proyecto central"
echo ""
echo "2. Ejecuta una prueba:"
echo "   python run_all.py test"
echo ""
echo "3. Genera todas las vistas:"
echo "   python run_all.py all"
echo ""
echo "4. Para ayuda:"
echo "   python run_all.py help"
echo ""
echo "📁 Archivos disponibles:"
echo "   - config.py: Configuración"
echo "   - run_all.py: Script maestro"
echo "   - README.md: Documentación completa"
echo ""
echo "✅ ¡Listo para usar!"
