import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime
import os

# 1. Configuración de la conexión a la base de datos
# El formato es: postgresql://usuario:contraseña@host:puerto/nombre_db
DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"
engine = create_engine('postgresql+psycopg2://airflow:postgres1989@host.docker.internal:5433/parking_db')

def extract_to_bronce():
    file_path = '/opt/airflow/data/reporte_acceso_tsj_zapopan_2021_2025.csv'
    table_name = 'bronce_parking_raw'
    schema_name = 'bronce'
    
    print(f"Iniciando extracción desde: {file_path}")

    # 2. Leer el CSV en pedazos (chunks) de 5,000 registros
    # Esto es vital en Big Data para no agotar la memoria RAM
    chunk_size = 5000
    
    try:
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            # Convertir cada fila del chunk en un formato JSON
            # 'records' crea una lista de diccionarios
            chunk_json = chunk.apply(lambda x: x.to_json(), axis=1)
            
            # Crear el DataFrame final para la tabla Bronce
            df_bronce = pd.DataFrame()
            df_bronce['raw_record'] = chunk_json
            df_bronce['source_file'] = os.path.basename(file_path)
            df_bronce['year'] = chunk['año'] if 'año' in chunk else None
            df_bronce['fecha'] = pd.to_datetime(chunk['fecha']).dt.date
            df_bronce['raw_loaded_at'] = datetime.now()

            # 3. Insertar en la base de datos (PostgreSQL)
            df_bronce.to_sql(
                table_name, 
                engine, 
                schema=schema_name, 
                if_exists='append', 
                index=False,
                method='multi' # Optimiza la inserción masiva
            )
            
            print(f"Chunk {i+1} procesado: {len(chunk)} registros insertados.")

        print("--- Extracción a Capa Bronce finalizada con éxito ---")

    except Exception as e:
        print(f"Error durante la extracción: {e}")
        raise

if __name__ == "__main__":
    extract_to_bronce()