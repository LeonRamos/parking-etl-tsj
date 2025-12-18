from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 1. Configuración de rutas para que Airflow encuentre tus scripts
# Dentro del contenedor, tus scripts están en /opt/airflow/scripts
sys.path.append('/opt/airflow/scripts')

from extract_bronce import extract_to_bronce
from transform_silver import transform_to_silver
from load_gold import load_to_gold

# 2. Argumentos por defecto (Mejores prácticas)
default_args = {
    'owner': 'estudiante_maestria',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # Reintentos automáticos
    'retry_delay': timedelta(minutes=5), # Espera entre reintentos
}

# 3. Definición del DAG
with DAG(
    'parking_etl_bronce_plata_oro', # ID solicitado en la práctica
    default_args=default_args,
    description='Pipeline ETL integral - Estacionamiento TSJ Zapopan',
    schedule_interval='0 2 * * *', # Ejecución diaria a las 2:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['big_data', 'tsj_zapopan'],
) as dag:

    # Tarea 1: Extracción (Capa Bronce)
    extract_task = PythonOperator(
        task_id='extract_to_bronce',
        python_callable=extract_to_bronce,
        doc_md="Extrae datos del CSV y los carga como JSON en Postgres."
    )

    # Tarea 2: Transformación (Capa Plata)
    transform_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
        doc_md="Limpia placas, normaliza tipos y valida datos."
    )

    # Tarea 3: Carga Analítica (Capa Oro)
    load_task = PythonOperator(
        task_id='load_to_gold',
        python_callable=load_to_gold,
        doc_md="Genera tablas de resumen y métricas de negocio."
    )

    # 4. Definición del flujo (Dependencias)
    # Primero extraemos, luego transformamos, finalmente cargamos
    extract_task >> transform_task >> load_task