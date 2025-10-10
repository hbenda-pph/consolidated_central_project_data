#!/bin/bash

# =============================================================================
# SCRIPT PARA OTORGAR PERMISOS A data-consolidation SERVICE ACCOUNT
# =============================================================================

set -e

SERVICE_ACCOUNT="data-consolidation@pph-central.iam.gserviceaccount.com"

echo "🔐 Otorgando permisos a ${SERVICE_ACCOUNT}"
echo "================================================================"

# Array de proyectos de compañías (actualizar según sea necesario)
COMPANY_PROJECTS=(
    "shape-mhs-1"
    "shape-chc-2"
    "shape-tucson-3"
    "shape-otm-4"
    "shape-aone-5"
    "shape-lba-6"
    "shape-lbca-7"
    "shape-dear-8"
    "shape-hhwi-9"
    "shape-cls-10"
    "shape-hecs-11"
    "shape-jsp-12"
    "shape-ico-13"
    "shape-indy-14"
    "shape-lex-15"
    "shape-hze-16"
    "shape-ncva-17"
    "shape-ahs-18"
    "shape-ppp-19"
    "shape-mgy-20"
    "shape-ns-21"
    "shape-sst-22"
    "shape-jfsp-23"
    "shape-pthc-24"
    "shape-cos-26"
    "shape-gem-27"
    "shape-newe-28"
    "shape-acga-29"
    "shape-jrb-30"
    "shape-ida-31"
)

echo ""
echo "📋 Proyectos a procesar: ${#COMPANY_PROJECTS[@]}"
echo ""

# Contador
SUCCESS=0
FAILED=0

# Otorgar permisos BigQuery Data Viewer en cada proyecto
for PROJECT in "${COMPANY_PROJECTS[@]}"; do
    echo "🔄 Procesando: ${PROJECT}"
    
    if gcloud projects add-iam-policy-binding ${PROJECT} \
        --member="serviceAccount:${SERVICE_ACCOUNT}" \
        --role="roles/bigquery.dataViewer" \
        --quiet 2>/dev/null; then
        echo "   ✅ Permisos otorgados"
        ((SUCCESS++))
    else
        echo "   ⚠️  Error (puede que ya exista o no tengas acceso)"
        ((FAILED++))
    fi
    echo ""
done

# Resumen
echo "================================================================"
echo "🎯 RESUMEN"
echo "================================================================"
echo "✅ Exitosos: ${SUCCESS}"
echo "❌ Fallidos: ${FAILED}"
echo "📊 Total: ${#COMPANY_PROJECTS[@]}"
echo "================================================================"

if [ ${FAILED} -eq 0 ]; then
    echo "✅ ¡Todos los permisos otorgados exitosamente!"
else
    echo "⚠️  Algunos proyectos fallaron. Revisa los logs arriba."
fi

