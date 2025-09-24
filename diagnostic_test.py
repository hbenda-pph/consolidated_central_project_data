# -*- coding: utf-8 -*-
"""
Diagnostic Test - Consolidated Central Project Data

Script de diagnóstico para verificar la configuración y conexión a BigQuery.
Útil para identificar problemas antes de ejecutar el proceso principal.
"""

import sys
import os
from google.cloud import bigquery
import pandas as pd

# Agregar el directorio actual al path para importar config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_config_import():
    """Prueba la importación de configuración"""
    print("🔍 Probando importación de configuración...")
    
    try:
        from config import *
        print("✅ Configuración importada correctamente")
        print(f"  - PROJECT_SOURCE: {PROJECT_SOURCE}")
        print(f"  - DATASET_NAME: {DATASET_NAME}")
        print(f"  - TABLE_NAME: {TABLE_NAME}")
        print(f"  - MAX_COMPANIES_FOR_TEST: {MAX_COMPANIES_FOR_TEST}")
        return True
    except Exception as e:
        print(f"❌ Error importando configuración: {e}")
        return False

def test_bigquery_client():
    """Prueba la creación del cliente BigQuery"""
    print("\n🔍 Probando cliente BigQuery...")
    
    try:
        from config import PROJECT_SOURCE
        client = bigquery.Client(project=PROJECT_SOURCE)
        print(f"✅ Cliente BigQuery creado para proyecto: {PROJECT_SOURCE}")
        return True, client
    except Exception as e:
        print(f"❌ Error creando cliente BigQuery: {e}")
        return False, None

def test_companies_table(client):
    """Prueba el acceso a la tabla de compañías"""
    print("\n🔍 Probando acceso a tabla de compañías...")
    
    try:
        from config import PROJECT_SOURCE, DATASET_NAME, TABLE_NAME
        
        query = f"""
            SELECT COUNT(*) as total_companies
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            WHERE company_bigquery_status IS NOT NULL
        """
        
        print(f"📋 Ejecutando consulta: {query}")
        result = client.query(query).result()
        total_companies = list(result)[0].total_companies
        
        print(f"✅ Tabla accesible. Total de compañías activas: {total_companies}")
        return True
        
    except Exception as e:
        print(f"❌ Error accediendo a tabla de compañías: {e}")
        return False

def test_sample_companies(client):
    """Prueba obtener una muestra de compañías"""
    print("\n🔍 Probando obtención de muestra de compañías...")
    
    try:
        from config import PROJECT_SOURCE, DATASET_NAME, TABLE_NAME, MAX_COMPANIES_FOR_TEST
        
        query = f"""
            SELECT company_id, company_name, company_project_id
            FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
            WHERE company_bigquery_status IS NOT NULL
            ORDER BY company_id
            LIMIT {MAX_COMPANIES_FOR_TEST}
        """
        
        result = client.query(query).result()
        companies_df = pd.DataFrame([dict(row) for row in result])
        
        print(f"✅ Obtenidas {len(companies_df)} compañías:")
        for _, company in companies_df.iterrows():
            print(f"  - {company['company_name']} ({company['company_project_id']})")
        
        return True, companies_df
        
    except Exception as e:
        print(f"❌ Error obteniendo compañías: {e}")
        return False, None

def test_sample_table_access(client, companies_df):
    """Prueba el acceso a una tabla de muestra"""
    print("\n🔍 Probando acceso a tabla de muestra...")
    
    if companies_df.empty:
        print("⚠️  No hay compañías para probar")
        return False
    
    # Usar la primera compañía
    first_company = companies_df.iloc[0]
    project_id = first_company['company_project_id']
    company_name = first_company['company_name']
    
    try:
        dataset_name = f"servicetitan_{project_id.replace('-', '_')}"
        table_name = 'call'  # Tabla común
        
        query = f"""
            SELECT COUNT(*) as total_tables
            FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table_name}'
        """
        
        print(f"📋 Probando acceso a: {project_id}.{dataset_name}.{table_name}")
        result = client.query(query).result()
        total_tables = list(result)[0].total_tables
        
        if total_tables > 0:
            print(f"✅ Tabla '{table_name}' encontrada en {company_name}")
            
            # Probar acceso a campos
            fields_query = f"""
                SELECT column_name, data_type
                FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                LIMIT 5
            """
            
            fields_result = client.query(fields_query).result()
            fields_df = pd.DataFrame([dict(row) for row in fields_result])
            
            print(f"✅ Campos de muestra obtenidos ({len(fields_df)} campos):")
            for _, field in fields_df.iterrows():
                print(f"  - {field['column_name']}: {field['data_type']}")
            
            return True
        else:
            print(f"⚠️  Tabla '{table_name}' no encontrada en {company_name}")
            return False
            
    except Exception as e:
        print(f"❌ Error accediendo a tabla de muestra: {e}")
        return False

def main():
    """Función principal de diagnóstico"""
    print("🔧 DIAGNÓSTICO DEL SISTEMA")
    print("=" * 50)
    
    # Paso 1: Importación de configuración
    if not test_config_import():
        print("\n❌ DIAGNÓSTICO FALLIDO: Problema con configuración")
        return False
    
    # Paso 2: Cliente BigQuery
    success, client = test_bigquery_client()
    if not success:
        print("\n❌ DIAGNÓSTICO FALLIDO: Problema con BigQuery")
        return False
    
    # Paso 3: Tabla de compañías
    if not test_companies_table(client):
        print("\n❌ DIAGNÓSTICO FALLIDO: Problema con tabla de compañías")
        return False
    
    # Paso 4: Muestra de compañías
    success, companies_df = test_sample_companies(client)
    if not success:
        print("\n❌ DIAGNÓSTICO FALLIDO: Problema obteniendo compañías")
        return False
    
    # Paso 5: Acceso a tabla de muestra
    if not test_sample_table_access(client, companies_df):
        print("\n⚠️  DIAGNÓSTICO PARCIAL: Problema con acceso a tablas de compañías")
        return False
    
    print("\n🎯 DIAGNÓSTICO COMPLETADO EXITOSAMENTE")
    print("✅ El sistema está listo para ejecutar el proceso de consolidación")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
