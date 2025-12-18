import pandas as pd
import json
from sqlalchemy import create_engine
import re

# Configuración de conexión
DB_URL = "postgresql+psycopg2://airflow:postgres1989@host.docker.internal:5433/parking_db"
engine = create_engine(DB_URL)

def transform_to_silver():
    print("Iniciando transformación: Bronce -> Plata")
    
    # 1. Extraer datos de la tabla Bronce
    # ELIMINADO 'id' porque no existe en tu tabla Bronce
    query = "SELECT raw_record, source_file, fecha FROM bronce.bronce_parking_raw"
    df_raw = pd.read_sql(query, engine)
    
    if df_raw.empty:
        print("No hay datos en Bronce para procesar.")
        return

    # 2. Convertir la columna JSONB en columnas individuales de Pandas
    df_expanded = pd.json_normalize(df_raw['raw_record'].apply(json.loads))
    
    # CAMBIO: Como no hay 'id' en Bronce, usamos el índice de Pandas para tener una referencia
    df_expanded['bronce_id'] = df_raw.index 

    # --- TRANSFORMACIONES (Hard Skills: Pandas) ---

    # A. Limpieza de Placas
    df_expanded['placa'] = df_expanded['placa'].str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)

    # B. Normalización de Tipo de Vehículo
    categorias_validas = ['sedan', 'moto', 'camioneta', 'carga', 'suv', 'minibus', 'otro']
    df_expanded['tipo_vehiculo'] = df_expanded['tipo_vehiculo'].str.lower().str.strip()
    
    # C. Conversión de Booleanos
    bool_cols = ['engomado', 'bitacora', 'capacidad_sobrepasada', 'descanso_oficial', 'vacacional']
    for col in bool_cols:
        if col in df_expanded.columns: # Agregamos validación por si la columna no viene en el JSON
            df_expanded[col] = df_expanded[col].map({True: True, False: False, 'Sí': True, 'No': False, 1: True, 0: False})

    # D. Creación de Timestamp
    # IMPORTANTE: Asegúrate que dentro del JSON vengan 'fecha' y 'hora'
    df_expanded['timestamp'] = pd.to_datetime(df_expanded['fecha'] + ' ' + df_expanded['hora'])

    # --- VALIDACIONES ---
    df_expanded['validation_errors'] = [[] for _ in range(len(df_expanded))]
    df_expanded['is_valid'] = True

    mask_placa_invalid = (df_expanded['placa'].str.len() < 4) | (df_expanded['placa'].str.len() > 20)
    df_expanded.loc[mask_placa_invalid, 'is_valid'] = False
    df_expanded.loc[mask_placa_invalid, 'validation_errors'].apply(lambda x: x.append("Placa inválida"))

    mask_tipo_invalid = ~df_expanded['tipo_vehiculo'].isin(categorias_validas)
    df_expanded.loc[mask_tipo_invalid, 'is_valid'] = False
    df_expanded.loc[mask_tipo_invalid, 'validation_errors'].apply(lambda x: x.append("Tipo vehículo desconocido"))

    # E. Detección de Duplicados
    df_expanded['duplicate_marker'] = df_expanded.duplicated(subset=['placa', 'timestamp'], keep='first')

    # 3. Cargar a la tabla Plata
    # AJUSTE: Quitamos columnas que podrían no existir o les damos un valor por defecto
    # Si 'barra_acceso' no viene en tu JSON, el script fallará. Asegúrate de que los nombres coincidan.
    
    cols_plata = [
        'fecha', 'hora', 'timestamp', 'tipo_vehiculo', 'placa', 
        'is_valid', 'validation_errors', 'duplicate_marker', 'bronce_id'
    ]
    
    # Convertir lista de errores a string
    df_expanded['validation_errors'] = df_expanded['validation_errors'].apply(lambda x: ", ".join(x))

    df_expanded[cols_plata].to_sql(
        'plata_parking', 
        engine, 
        schema='plata', 
        if_exists='replace', 
        index=False
    )

    print(f"Transformación completada. {len(df_expanded)} registros procesados en la capa PLATA.")

if __name__ == "__main__":
    transform_to_silver()