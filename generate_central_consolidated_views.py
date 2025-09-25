# -*- coding: utf-8 -*-
"""
Generate Central Consolidated Views

Este script genera las vistas consolidadas en el proyecto central
que unen todas las vistas Silver de las compañías.

Paso 2: Crear vistas consolidadas en central-silver
"""

from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
warnings.filterwarnings('ignore')

# Configuración
PROJECT_SOURCE = "platform-partners-qua"
CENTRAL_PROJECT = "platform-partners-des"  # Ajustar según tu proyecto central
DATASET_NAME = "settings"
TABLE_NAME = "companies"

print(f"🔧 Configuración:")
print(f"   Proyecto fuente: {PROJECT_SOURCE}")
print(f"   Proyecto central: {CENTRAL_PROJECT}")
print(f"   Dataset: {DATASET_NAME}")

# Crear cliente
client = bigquery.Client(project=PROJECT_SOURCE)

def get_companies_info():
    """Obtiene información de todas las compañías activas"""
    query = f"""
        SELECT company_id, company_name, company_project_id
        FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status IS NOT NULL
        ORDER BY company_id
    """
    return pd.DataFrame([dict(row) for row in client.query(query).result()])

def generate_consolidated_view_sql(table_name, companies_df):
    """
    Genera el SQL para la vista consolidada de una tabla específica
    """
    print(f"🔧 Generando vista consolidada para: {table_name}")
    
    # Crear UNION ALL para cada compañía
    union_parts = []
    
    for _, company in companies_df.iterrows():
        project_id = company['company_project_id']
        company_name = company['company_name']
        
        # Verificar que la vista Silver existe (esto sería ideal validar)
        silver_view = f"`{project_id}.silver.vw_{table_name}`"
        
        union_parts.append(f"SELECT *, '{project_id}' AS company_project_id, {company['company_id']} AS company_id FROM {silver_view}")
    
    # Crear SQL completo
    sql = f"""-- Vista Consolidada para tabla: {table_name}
-- Generada automáticamente el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Consolida datos de {len(companies_df)} compañías

CREATE OR REPLACE VIEW `{CENTRAL_PROJECT}.central-silver.vw_consolidated_{table_name}` AS (
{chr(10).join([f"  {part}" if i == 0 else f"  UNION ALL{chr(10)}  {part}" for i, part in enumerate(union_parts)])}
);

-- Comentarios:
-- Esta vista consolida datos normalizados de todas las compañías
-- Los campos han sido normalizados en las vistas Silver de cada compañía
-- Campos faltantes se han rellenado con valores por defecto apropiados
"""
    
    return sql

def generate_all_consolidated_views():
    """
    Genera todas las vistas consolidadas
    """
    # Lista de tablas (misma que en el script anterior)
    tables_to_process = [
        'appointment', 'appointment_assignment', 'booking', 'business_unit', 'call', 
        'campaign', 'campaign_category', 'campaign_cost', 'campaign_phone_number',
        'customer', 'customer_contact', 'employee', 'estimate', 'estimate_item',
        'inventory_bill', 'inventory_bill_item', 'invoice', 'invoice_item', 'job',
        'job_hold_reason', 'job_split', 'job_type', 'job_type_business_unit_id',
        'job_type_skill', 'lead', 'location', 'location_contact', 'membership',
        'non_job_appointment', 'payment', 'payment_applied_to', 'project',
        'project_status', 'project_sub_status', 'technician', 'zone', 'zone_city',
        'zone_service_day', 'zone_technician', 'zone_zip', 'zone_business_unit',
        'estimate_external_link'
    ]
    
    # Obtener información de compañías
    companies_df = get_companies_info()
    print(f"📋 Compañías activas: {len(companies_df)}")
    
    # Crear directorio de salida
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"central_consolidated_views_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"🚀 GENERANDO VISTAS CONSOLIDADAS")
    print(f"📁 Directorio: {output_dir}")
    print("=" * 80)
    
    generated_files = []
    
    for table_name in tables_to_process:
        try:
            # Generar SQL para la vista consolidada
            sql_content = generate_consolidated_view_sql(table_name, companies_df)
            
            # Guardar archivo
            filename = f"{output_dir}/central_consolidated_{table_name}.sql"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            
            generated_files.append(filename)
            print(f"  ✅ {table_name}: {filename}")
            
        except Exception as e:
            print(f"  ❌ Error en {table_name}: {str(e)}")
    
    # Crear archivo maestro que ejecute todas las vistas
    master_filename = f"{output_dir}/EXECUTE_ALL_CONSOLIDATED_VIEWS.sql"
    with open(master_filename, 'w', encoding='utf-8') as f:
        f.write(f"-- Script maestro para ejecutar todas las vistas consolidadas\n")
        f.write(f"-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Proyecto central: {CENTRAL_PROJECT}\n\n")
        
        f.write(f"-- IMPORTANTE: Asegúrate de que todas las vistas Silver estén creadas\n")
        f.write(f"-- en cada proyecto de compañía antes de ejecutar este script.\n\n")
        
        for filename in generated_files:
            table_name = filename.split('_')[-1].replace('.sql', '')
            f.write(f"-- Ejecutar vista consolidada para: {table_name}\n")
            f.write(f"-- Archivo: {filename}\n")
            f.write(f"\\include {filename}\n\n")
    
    # Crear resumen
    summary_filename = f"{output_dir}/CONSOLIDATION_SUMMARY.md"
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Resumen de Vistas Consolidadas\n\n")
        f.write(f"**Proyecto central:** {CENTRAL_PROJECT}\n")
        f.write(f"**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Compañías:** {len(companies_df)}\n\n")
        
        f.write(f"## Tablas Procesadas\n\n")
        f.write(f"Total: {len(generated_files)} vistas consolidadas\n\n")
        
        for filename in generated_files:
            table_name = filename.split('_')[-1].replace('.sql', '')
            f.write(f"- `{table_name}`: {filename}\n")
        
        f.write(f"\n## Instrucciones de Ejecución\n\n")
        f.write(f"1. **Prerrequisito:** Asegúrate de que todas las vistas Silver estén creadas\n")
        f.write(f"2. **Ejecución individual:** Ejecuta cada archivo `.sql` en el proyecto central\n")
        f.write(f"3. **Ejecución masiva:** Usa el archivo `EXECUTE_ALL_CONSOLIDATED_VIEWS.sql`\n\n")
        
        f.write(f"## Estructura de las Vistas\n\n")
        f.write(f"Cada vista consolidada:\n")
        f.write(f"- Se crea en: `{CENTRAL_PROJECT}.central-silver.vw_consolidated_{{table_name}}`\n")
        f.write(f"- Consolida datos de {len(companies_df)} compañías\n")
        f.write(f"- Incluye campos normalizados y metadata\n")
        f.write(f"- Usa UNION ALL para máxima performance\n")
    
    print(f"\n🎯 GENERACIÓN COMPLETADA")
    print(f"📁 Directorio: {output_dir}")
    print(f"📄 Archivos generados: {len(generated_files)}")
    print(f"📋 Script maestro: {master_filename}")
    print(f"📊 Resumen: {summary_filename}")
    
    return output_dir, generated_files

if __name__ == "__main__":
    output_dir, files = generate_all_consolidated_views()
    print(f"\n✅ Script completado exitosamente!")
    print(f"📁 Revisa los archivos en: {output_dir}")
