# PRÁCTICA: ETL CON AIRFLOW Y ARQUITECTURA BRONCE-PLATA-ORO

[![Ciencia de Datos](https://img.shields.io/badge/Ciencia%20de%20Datos-Big%20Data-brightgreen)]()
[![Python](https://img.shields.io/badge/Python-%203.8%2B-blue)]()
[![Big%20Data](https://img.shields.io/badge/Big%20Data-Tecnolog%C3%ADas%20Emergentes-orange)]()
[![GitHub](https://img.shields.io/badge/GitHub-Gest%C3%B3n%20de%20proyectos-lightgrey)]()
[![TecMM](https://img.shields.io/badge/TecMM-zapopan-0D3692?style=for-the-badge&logo=university&logoColor=white&labelColor=101010)]()
---
## Estacionamiento Centro Educativo TSJ Zapopan
**Duración:** 1 semana | **Nivel:** Maestría en Sistemas Computacionales - Big Data  
**Rol:** Ingeniero de Datos | **Herramientas:** Airflow, Docker, PostgreSQL, pgAdmin, Python, Git


##  OBJETIVO

Al finalizar esta práctica, el alumno será capaz de:

1. **Levantar un entorno local integrado** con Docker Compose que incluya:
   - Apache Airflow (webserver + scheduler)
   - PostgreSQL (base de datos)
   - pgAdmin (interfaz de administración SQL)
   - Volúmenes compartidos para datos CSV

2. **Diseñar e implementar un DAG de Airflow** que orqueste un pipeline ETL completo con:
   - Tareas secuenciales con dependencias claras
   - Manejo de errores y reintentos
   - Logging y monitoreo

3. **Implementar la arquitectura medallion (Bronce → Plata → Oro)** con responsabilidades específicas:
   - **Bronce (Raw):** Almacenar datos originales sin transformaciones
   - **Plata (Staging):** Limpiar, validar y estandarizar datos
   - **Oro (Confiable):** Generar modelos analíticos para toma de decisiones

4. **Ejecutar transformaciones de datos** usando Python (pandas) y SQL para:
   - Limpieza y validación de registros
   - Normalización de tipos de datos
   - Manejo de valores faltantes y duplicados
   - Agregaciones por dimensiones de negocio

5. **Generar insights de negocio** analizando patrones de ocupación, uso por tipo de vehículo y comportamiento en períodos especiales

---

##  REQUISITOS PREVIOS

### Software necesario:
- **Docker Desktop** (versión 4.0+) con Docker Compose
- **Python 3.8+** (opcional, para pruebas locales)
- **Git** configurado en tu máquina
- **Navegador web** (Chrome, Firefox)

### Conocimientos requeridos:
- Python básico (funciones, librerías como pandas)
- SQL básico (SELECT, INSERT, CREATE TABLE, JOIN)
- Conceptos de DAGs en Airflow (dependencias, operadores)
- Uso de la terminal/línea de comandos

### Cuentas necesarias:
- GitHub (para versionado del código)
- Editor de código (VS Code recomendado)


## ESTRUCTURA DEL REPOSITORIO GITHUB

Tu repositorio debe organizarse así:

```
parking-etl-tsj/
│
├── README.md                          # Instrucciones de inicio
├── .gitignore                         # Archivos a ignorar en Git
├── docker-compose.yml                 # Configuración de servicios
│
├── dags/
│   └── parking_etl_dag.py            # DAG principal de Airflow
│
├── scripts/
│   ├── transform_silver.py           # Lógica de transformación a plata
│   ├── load_gold.py                  # Lógica de carga a oro
│   └── utils.py                      # Funciones auxiliares
│
├── sql/
│   ├── init_database.sql             # Crear tablas bronce, plata, oro
│   ├── ddl_bronce.sql                # Definición tabla bronce
│   ├── ddl_plata.sql                 # Definición tabla plata
│   ├── ddl_oro.sql                   # Definición tabla oro
│   └── queries_validacion.sql        # Consultas de verificación
│
├── data/
│   ├── parking_access.csv            # CSV principal (ignorado en .gitignore)
│   └── sample_5rows.csv              # Muestra para pruebas
│
├── tests/
│   └── test_transformations.py       # Tests unitarios
│
├── logs/                              # Logs de Airflow (gitignored)
├── plugins/                           # Plugins personalizados de Airflow
│
└── docs/
    └── GUIA_PASO_A_PASO.md           # Esta guía

```
---

![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)
## PASO 1: PREPARAR EL ENTORNO LOCAL

### 1.1 Clonar o crear el repositorio

```bash
# Opción A: Si clonas desde un repo existente
git clone https://github.com/tu-usuario/parking-etl-tsj.git
cd parking-etl-tsj

# Opción B: Crear un repo nuevo localmente
mkdir parking-etl-tsj
cd parking-etl-tsj
git init
```

### 1.2 Crear la estructura de directorios

```bash
mkdir -p dags scripts sql data tests logs plugins docs
touch README.md .gitignore
```

### 1.3 Configurar .gitignore

Crea archivo `.gitignore`:

```
# Datos
data/*.csv
!data/sample_5rows.csv

# Airflow
logs/
airflow.db
airflow.cfg

# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
venv/
.venv/

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# Secrets
.env
secrets.yaml
```

### 1.4 Descargar el CSV original

1. Coloca el archivo **`reporte_acceso_tsj_zapopan_2021_2025.csv`** en la carpeta `./data/`
2. Verifica que pese ~7MB y tenga 92,572 registros

---
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
## PASO 2: CONFIGURAR DOCKER COMPOSE


### 2.1 Crear `docker-compose.yml`

Crea el archivo en la raíz del proyecto:

```yaml
version: '3.8'

services:
  # PostgreSQL Database
  postgres:
    image: postgres:14-alpine
    container_name: parking-postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: parking_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/init_database.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - parking-network

  # pgAdmin - Interfaz web para PostgreSQL
  pgadmin:
    image: dpage/pgadmin4:7.5
    container_name: parking-pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@parking-tsj.local
      PGADMIN_DEFAULT_PASSWORD: admin123
    ports:
      - "8081:80"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - parking-network

  # Apache Airflow - Scheduler
  airflow-scheduler:
    image: apache/airflow:2.7.1-python3.11
    container_name: parking-airflow-scheduler
    environment:
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__PLUGINS_FOLDER: /opt/airflow/plugins
      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - parking-network
    command: scheduler

  # Apache Airflow - Webserver
  airflow-webserver:
    image: apache/airflow:2.7.1-python3.11
    container_name: parking-airflow-webserver
    environment:
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__PLUGINS_FOLDER: /opt/airflow/plugins
      AIRFLOW__LOGGING__BASE_LOG_FOLDER: /opt/airflow/logs
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
      AIRFLOW_WWW_USER_USERNAME: admin
      AIRFLOW_WWW_USER_PASSWORD: admin123
      AIRFLOW_WWW_USER_EMAIL: admin@airflow.local
      AIRFLOW_WWW_USER_FIRSTNAME: Admin
      AIRFLOW_WWW_USER_LASTNAME: User
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
      airflow-scheduler:
        condition: service_started
    networks:
      - parking-network
    command: webserver

volumes:
  postgres_data:
    driver: local

networks:
  parking-network:
    driver: bridge
```

### 2.2 Verificar Docker Compose

```bash
# Validar sintaxis
docker-compose config

# Listar servicios que se levantarán
docker-compose ps
```

---
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
## PASO 3: CREAR ESQUEMA DE TABLAS

### 3.1 Crear `sql/init_database.sql`

```sql
/*
 * SCRIPT DE INICIALIZACIÓN - PARKING ETL
 * Crea las tablas para las 3 capas: Bronce, Plata y Oro
 */

-- ============================================================
-- CAPA BRONCE (Raw): Almacenamiento de datos originales
-- ============================================================

CREATE TABLE IF NOT EXISTS bronce_parking_raw (
    id SERIAL PRIMARY KEY,
    raw_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    year INT,
    fecha DATE,
    raw_record JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bronce_year ON bronce_parking_raw(year);
CREATE INDEX idx_bronce_fecha ON bronce_parking_raw(fecha);
CREATE INDEX idx_bronce_loaded_at ON bronce_parking_raw(raw_loaded_at);

-- ============================================================
-- CAPA PLATA (Staging): Datos limpios y validados
-- ============================================================

CREATE TABLE IF NOT EXISTS plata_parking (
    id BIGSERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    hora TIME NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    tipo_vehiculo VARCHAR(50),
    placa VARCHAR(20) NOT NULL,
    barra_acceso INT,
    engomado VARCHAR(10),
    bitacora VARCHAR(10),
    foto VARCHAR(255),
    capacidad_sobrepasada VARCHAR(10),
    descanso_oficial VARCHAR(10),
    vacacional VARCHAR(10),
    año INT NOT NULL,
    
    -- Campos de validación
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT[],
    duplicate_marker VARCHAR(50),
    
    -- Campos de auditoría
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronce_id INT REFERENCES bronce_parking_raw(id) ON DELETE SET NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_plata_fecha ON plata_parking(fecha);
CREATE INDEX idx_plata_timestamp ON plata_parking(timestamp);
CREATE INDEX idx_plata_tipo_vehiculo ON plata_parking(tipo_vehiculo);
CREATE INDEX idx_plata_placa ON plata_parking(placa);
CREATE INDEX idx_plata_valid ON plata_parking(is_valid);
CREATE INDEX idx_plata_año ON plata_parking(año);

-- ============================================================
-- CAPA ORO (Confiable): Modelo analítico para decisiones
-- ============================================================

-- Tabla de hechos: Accesos por hora
CREATE TABLE IF NOT EXISTS oro_accesos_hora (
    id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    hora INT NOT NULL CHECK (hora >= 0 AND hora <= 23),
    tipo_vehiculo VARCHAR(50),
    total_accesos INT DEFAULT 0,
    capacidad_sobrepasada_count INT DEFAULT 0,
    descanso_oficial BOOLEAN DEFAULT FALSE,
    vacacional BOOLEAN DEFAULT FALSE,
    año INT NOT NULL,
    
    -- Campos de auditoría
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (fecha, hora, tipo_vehiculo)
);

CREATE INDEX idx_oro_accesos_fecha ON oro_accesos_hora(fecha);
CREATE INDEX idx_oro_accesos_año ON oro_accesos_hora(año);
CREATE INDEX idx_oro_accesos_tipo ON oro_accesos_hora(tipo_vehiculo);

-- Tabla de hechos: Ocupación diaria
CREATE TABLE IF NOT EXISTS oro_ocupacion_diaria (
    id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    tipo_vehiculo VARCHAR(50),
    total_accesos INT DEFAULT 0,
    accesos_por_barra_acceso JSONB,
    capacidad_sobrepasada_events INT DEFAULT 0,
    descanso_oficial BOOLEAN DEFAULT FALSE,
    vacacional BOOLEAN DEFAULT FALSE,
    día_semana VARCHAR(10),
    año INT NOT NULL,
    
    -- Métricas derivadas
    promedio_accesos_hora DECIMAL(10,2),
    pico_hora INT,
    pico_valor INT,
    
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (fecha, tipo_vehiculo)
);

CREATE INDEX idx_oro_ocupacion_fecha ON oro_ocupacion_diaria(fecha);
CREATE INDEX idx_oro_ocupacion_año ON oro_ocupacion_diaria(año);
CREATE INDEX idx_oro_ocupacion_tipo ON oro_ocupacion_diaria(tipo_vehiculo);

-- Tabla de hechos: Resumen por tipo de vehículo
CREATE TABLE IF NOT EXISTS oro_resumen_tipo_vehiculo (
    id SERIAL PRIMARY KEY,
    tipo_vehiculo VARCHAR(50) NOT NULL UNIQUE,
    total_accesos INT DEFAULT 0,
    total_accesos_validos INT DEFAULT 0,
    total_accesos_invalidos INT DEFAULT 0,
    porcentaje_validez DECIMAL(5,2),
    primer_acceso DATE,
    ultimo_acceso DATE,
    días_con_acceso INT,
    accesos_con_engomado INT,
    accesos_con_bitacora INT,
    accesos_descanso_oficial INT,
    accesos_vacacional INT,
    capacidad_sobrepasada_events INT,
    
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_oro_tipo_veh ON oro_resumen_tipo_vehiculo(tipo_vehiculo);

-- Tabla de hechos: Patrones por rango horario
CREATE TABLE IF NOT EXISTS oro_patrones_horarios (
    id SERIAL PRIMARY KEY,
    rango_horario VARCHAR(20) NOT NULL,  -- e.g., "06:00-09:00", "09:00-12:00", etc.
    tipo_vehiculo VARCHAR(50),
    total_accesos INT DEFAULT 0,
    promedio_accesos_dia DECIMAL(10,2),
    capacidad_sobrepasada_events INT DEFAULT 0,
    descanso_oficial BOOLEAN DEFAULT FALSE,
    vacacional BOOLEAN DEFAULT FALSE,
    año INT NOT NULL,
    
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (rango_horario, tipo_vehiculo, año)
);

CREATE INDEX idx_oro_patrones_año ON oro_patrones_horarios(año);

-- Tabla de control: Log de ejecuciones
CREATE TABLE IF NOT EXISTS oro_execution_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    execution_date TIMESTAMP,
    task_start_time TIMESTAMP,
    task_end_time TIMESTAMP,
    task_duration_seconds DECIMAL(10,2),
    status VARCHAR(50),  -- SUCCESS, FAILED, SKIPPED
    error_message TEXT,
    records_processed INT,
    records_inserted INT,
    records_failed INT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_oro_log_exec_date ON oro_execution_log(execution_date);
CREATE INDEX idx_oro_log_status ON oro_execution_log(status);

-- ============================================================
-- TABLA AUXILIAR: Catálogo de tipos de vehículo
-- ============================================================

CREATE TABLE IF NOT EXISTS cat_tipo_vehiculo (
    id SERIAL PRIMARY KEY,
    tipo_vehiculo VARCHAR(50) NOT NULL UNIQUE,
    descripcion VARCHAR(255),
    categoria_general VARCHAR(50),  -- Ligero, Pesado, Motocicleta
    es_activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO cat_tipo_vehiculo (tipo_vehiculo, descripcion, categoria_general)
VALUES 
    ('sedan', 'Automóvil tipo sedán', 'Ligero'),
    ('moto', 'Motocicleta', 'Motocicleta'),
    ('camioneta', 'Camioneta pickup', 'Ligero'),
    ('carga', 'Vehículo de carga', 'Pesado'),
    ('suv', 'Sport Utility Vehicle', 'Ligero'),
    ('minibus', 'Minibús', 'Pesado'),
    ('otro', 'Otro tipo de vehículo', 'Otro')
ON CONFLICT DO NOTHING;

-- ============================================================
-- VISTA: Resumen diario consolidado
-- ============================================================

CREATE OR REPLACE VIEW v_resumen_diario_consolidado AS
SELECT
    od.fecha,
    od.año,
    EXTRACT(DOW FROM od.fecha)::INT as día_semana_num,
    od.día_semana,
    od.descanso_oficial,
    od.vacacional,
    od.tipo_vehiculo,
    od.total_accesos,
    od.capacidad_sobrepasada_events,
    od.pico_hora,
    od.pico_valor,
    od.promedio_accesos_hora
FROM oro_ocupacion_diaria od
ORDER BY od.fecha DESC, od.tipo_vehiculo;

-- ============================================================
-- VISTA: Horarios críticos (picos de ocupación)
-- ============================================================

CREATE OR REPLACE VIEW v_horarios_criticos AS
SELECT
    ah.fecha,
    ah.hora,
    ah.tipo_vehiculo,
    ah.total_accesos,
    ah.capacidad_sobrepasada_count,
    CASE 
        WHEN ah.total_accesos >= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_accesos) FROM oro_accesos_hora) 
        THEN 'Alto'
        WHEN ah.total_accesos >= (SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_accesos) FROM oro_accesos_hora) 
        THEN 'Medio'
        ELSE 'Bajo'
    END as nivel_ocupacion
FROM oro_accesos_hora ah
WHERE ah.capacidad_sobrepasada_count > 0
ORDER BY ah.fecha DESC, ah.hora;

COMMIT;
```

### 3.2 Dividir en archivos SQL individuales (opcional pero recomendado)

**`sql/ddl_bronce.sql`:**
```sql
CREATE TABLE IF NOT EXISTS bronce_parking_raw (
    id SERIAL PRIMARY KEY,
    raw_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    year INT,
    fecha DATE,
    raw_record JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_bronce_year ON bronce_parking_raw(year);
CREATE INDEX idx_bronce_fecha ON bronce_parking_raw(fecha);
```

---
![Python](https://img.shields.io/badge/Python_Script-3776AB?style=for-the-badge&logo=python&logoColor=white)


## PASO 4: CREAR SCRIPTS DE TRANSFORMACIÓN

### 4.1 Crear `scripts/utils.py`

```python
"""
Utilidades para transformación de datos de estacionamiento
"""
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
import json

# Catálogo de tipos de vehículos válidos
TIPOS_VEHICULO_VALIDOS = {
    'sedan': 'sedan',
    'moto': 'moto',
    'motocicleta': 'moto',
    'camioneta': 'camioneta',
    'camión': 'carga',
    'carga': 'carga',
    'suv': 'suv',
    'minibus': 'minibus',
    'pickup': 'camioneta',
    'auto': 'sedan',
    'automóvil': 'sedan'
}

def normalizar_tipo_vehiculo(valor: str) -> Tuple[str, List[str]]:
    """
    Normaliza el tipo de vehículo a un valor estándar.
    
    Args:
        valor: Valor original del CSV
        
    Returns:
        Tupla (tipo_normalizado, lista_errores)
    """
    errores = []
    
    if pd.isna(valor) or str(valor).strip() == '':
        return None, ['tipo_vehiculo_vacio']
    
    valor_limpio = str(valor).strip().lower().replace('á', 'a').replace('ó', 'o')
    
    # Buscar coincidencia exacta o parcial
    if valor_limpio in TIPOS_VEHICULO_VALIDOS:
        return TIPOS_VEHICULO_VALIDOS[valor_limpio], []
    
    # Búsqueda parcial
    for clave, normalizado in TIPOS_VEHICULO_VALIDOS.items():
        if clave in valor_limpio:
            return normalizado, []
    
    # Si no encuentra coincidencia, retornar como está
    errores.append(f'tipo_vehiculo_no_reconocido: {valor}')
    return valor_limpio, errores

def validar_placa(placa: str) -> Tuple[bool, List[str]]:
    """
    Valida que la placa cumpla con formato básico.
    Esperado: XXX-###-X o XXX-### (placas mexicanas)
    
    Args:
        placa: Valor de placa del CSV
        
    Returns:
        Tupla (es_válida, lista_errores)
    """
    errores = []
    
    if pd.isna(placa) or str(placa).strip() == '':
        return False, ['placa_vacia']
    
    placa_limpia = str(placa).strip().upper()
    
    # Longitud mínima
    if len(placa_limpia) < 4:
        errores.append('placa_muy_corta')
        return False, errores
    
    # Longitud máxima
    if len(placa_limpia) > 20:
        errores.append('placa_muy_larga')
        return False, errores
    
    # Verificar que contenga al menos una letra y un número
    has_letter = any(c.isalpha() for c in placa_limpia)
    has_number = any(c.isdigit() for c in placa_limpia)
    
    if not (has_letter and has_number):
        errores.append('placa_formato_inválido')
        return False, errores
    
    return True, []

def validar_timestamp(fecha_str: str, hora_str: str) -> Tuple[str, List[str]]:
    """
    Valida y convierte fecha + hora a timestamp ISO.
    
    Args:
        fecha_str: Fecha en formato YYYY-MM-DD
        hora_str: Hora en formato HH:MM:SS
        
    Returns:
        Tupla (timestamp_ISO, lista_errores)
    """
    errores = []
    
    try:
        if pd.isna(fecha_str) or pd.isna(hora_str):
            return None, ['fecha_o_hora_vacia']
        
        fecha_limpia = str(fecha_str).strip()
        hora_limpia = str(hora_str).strip()
        
        timestamp_str = f"{fecha_limpia} {hora_limpia}"
        ts = pd.to_datetime(timestamp_str, format='%Y-%m-%d %H:%M:%S')
        
        return ts.isoformat(), []
    except Exception as e:
        errores.append(f'timestamp_parsing_error: {str(e)}')
        return None, errores

def normalizar_booleano(valor: str) -> Tuple[bool, List[str]]:
    """
    Normaliza valores sí/no a booleanos.
    
    Args:
        valor: 'sí', 'no', 'yes', 'no', True, False, etc.
        
    Returns:
        Tupla (booleano, lista_errores)
    """
    if pd.isna(valor):
        return False, []
    
    valor_lower = str(valor).strip().lower()
    
    if valor_lower in ['sí', 'si', 'yes', 'true', '1', 'verdadero']:
        return True, []
    elif valor_lower in ['no', 'false', '0', 'falso']:
        return False, []
    else:
        return False, [f'booleano_no_reconocido: {valor}']

def detectar_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta duplicados usando placa + fecha + hora.
    Marca con duplicate_marker para trazabilidad.
    
    Args:
        df: DataFrame de plata
        
    Returns:
        DataFrame con columna duplicate_marker
    """
    df['duplicate_marker'] = df.duplicated(
        subset=['placa', 'timestamp'],
        keep=False
    ).apply(lambda x: 'DUPLICADO' if x else None)
    
    return df

def enriquecer_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade columnas derivadas útiles.
    
    Args:
        df: DataFrame de plata
        
    Returns:
        DataFrame enriquecido
    """
    # Día de la semana
    df['día_semana'] = pd.to_datetime(df['fecha']).dt.day_name()
    
    # Franja horaria
    def asignar_franja(hora):
        h = int(hora) if pd.notna(hora) else -1
        if h >= 6 and h < 9:
            return 'Matutina (6-9)'
        elif h >= 9 and h < 12:
            return 'Mañana (9-12)'
        elif h >= 12 and h < 15:
            return 'Mediodía (12-15)'
        elif h >= 15 and h < 18:
            return 'Tarde (15-18)'
        elif h >= 18 and h < 21:
            return 'Atardecer (18-21)'
        else:
            return 'Nocturno (21-6)'
    
    df['franja_horaria'] = df['hora'].apply(asignar_franja)
    
    return df

def generar_resumen_validacion(df: pd.DataFrame) -> Dict:
    """
    Genera resumen estadístico de validaciones.
    
    Args:
        df: DataFrame procesado
        
    Returns:
        Diccionario con estadísticas
    """
    total_registros = len(df)
    registros_validos = df['is_valid'].sum()
    registros_invalidos = total_registros - registros_validos
    duplicados = df['duplicate_marker'].notna().sum()
    
    return {
        'total_registros': total_registros,
        'registros_validos': registros_validos,
        'registros_invalidos': registros_invalidos,
        'porcentaje_validez': round((registros_validos / total_registros * 100), 2) if total_registros > 0 else 0,
        'duplicados_detectados': duplicados,
        'tipos_vehiculo_unicos': df['tipo_vehiculo'].nunique(),
        'placas_unicas': df['placa'].nunique(),
        'rango_fechas': f"{df['fecha'].min()} a {df['fecha'].max()}",
    }
```

### 4.2 Crear `scripts/transform_silver.py`

```python
"""
Transformación de Bronce a Plata
Limpieza, validación y estandarización de datos
"""
import pandas as pd
import json
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import (
    normalizar_tipo_vehiculo,
    validar_placa,
    validar_timestamp,
    normalizar_booleano,
    detectar_duplicados,
    enriquecer_datos,
    generar_resumen_validacion
)

def transform_bronce_to_plata(limit: int = None) -> dict:
    """
    Lee registros de bronce, aplica transformaciones y carga a plata.
    
    Args:
        limit: Número máximo de registros a procesar (para pruebas)
        
    Returns:
        Diccionario con estadísticas del proceso
    """
    print("="*80)
    print("INICIANDO TRANSFORMACIÓN: BRONCE → PLATA")
    print("="*80)
    
    # Conectar a PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    try:
        # 1. Leer registros de bronce (raw JSON)
        print("\n[1/5] Leyendo datos de bronce...")
        query_bronce = "SELECT id, raw_record FROM bronce_parking_raw ORDER BY id"
        if limit:
            query_bronce += f" LIMIT {limit}"
        
        df_raw = pd.read_sql(query_bronce, con=engine)
        print(f"  → Registros leídos: {len(df_raw)}")
        
        if len(df_raw) == 0:
            print("  ⚠️  No hay datos en bronce. Proceso finalizado.")
            return {'status': 'NO_DATA', 'records_processed': 0}
        
        # 2. Parsear JSON y crear DataFrame
        print("\n[2/5] Parseando JSON y estructurando datos...")
        records = []
        for idx, row in df_raw.iterrows():
            try:
                record = json.loads(row['raw_record'])
                record['bronce_id'] = row['id']
                records.append(record)
            except json.JSONDecodeError as e:
                print(f"  ⚠️  Error parseando registro {row['id']}: {e}")
                continue
        
        df = pd.DataFrame(records)
        print(f"  → Registros parseados: {len(df)}")
        
        # 3. Aplicar transformaciones
        print("\n[3/5] Aplicando transformaciones...")
        
        # Validar y normalizar cada columna
        df['timestamp_validado'], df['errores_timestamp'] = zip(*df.apply(
            lambda r: validar_timestamp(r.get('fecha'), r.get('hora')),
            axis=1
        ))
        
        df['tipo_vehiculo_normalizado'], df['errores_tipo'] = zip(*df.get('tipo_vehiculo', '').apply(
            normalizar_tipo_vehiculo
        ))
        
        df['placa_valida'], df['errores_placa'] = zip(*df.get('placa', '').apply(
            validar_placa
        ))
        
        df['engomado_bool'], _ = zip(*df.get('engomado', 'no').apply(normalizar_booleano))
        df['bitacora_bool'], _ = zip(*df.get('bitacora', 'no').apply(normalizar_booleano))
        df['capacidad_sobrepasada_bool'], _ = zip(*df.get('capacidad_sobrepasada', 'no').apply(normalizar_booleano))
        df['descanso_oficial_bool'], _ = zip(*df.get('descanso_oficial', 'no').apply(normalizar_booleano))
        df['vacacional_bool'], _ = zip(*df.get('vacacional', 'no').apply(normalizar_booleano))
        
        # Parsear hora a INT
        df['hora'] = pd.to_datetime(df.get('hora', ''), format='%H:%M:%S', errors='coerce').dt.hour
        
        # Parsear fecha
        df['fecha'] = pd.to_datetime(df.get('fecha', ''), format='%Y-%m-%d', errors='coerce').dt.date
        
        print(f"  → Transformaciones aplicadas")
        
        # 4. Consolidar errores de validación
        print("\n[4/5] Consolidando validaciones...")
        
        def consolidar_errores(row):
            errores = []
            errores.extend(row.get('errores_timestamp', []))
            errores.extend(row.get('errores_tipo', []))
            errores.extend(row.get('errores_placa', []))
            return errores if errores else []
        
        df['validation_errors'] = df.apply(consolidar_errores, axis=1)
        df['is_valid'] = df['validation_errors'].apply(lambda x: len(x) == 0)
        
        print(f"  → Registros válidos: {df['is_valid'].sum()}")
        print(f"  → Registros con errores: {(~df['is_valid']).sum()}")
        
        # Detectar duplicados
        df = detectar_duplicados(df)
        
        # Enriquecer datos
        df = enriquecer_datos(df)
        
        # 5. Cargar a tabla plata
        print("\n[5/5] Cargando a tabla PLATA...")
        
        df_plata = pd.DataFrame({
            'fecha': df['fecha'],
            'hora': df['hora'],
            'timestamp': df['timestamp_validado'],
            'tipo_vehiculo': df['tipo_vehiculo_normalizado'],
            'placa': df.get('placa', ''),
            'barra_acceso': df.get('barra_acceso', '').apply(lambda x: int(x) if str(x).isdigit() else None),
            'engomado': df['engomado_bool'],
            'bitacora': df['bitacora_bool'],
            'foto': df.get('foto', ''),
            'capacidad_sobrepasada': df['capacidad_sobrepasada_bool'],
            'descanso_oficial': df['descanso_oficial_bool'],
            'vacacional': df['vacacional_bool'],
            'año': df.get('año', '').apply(lambda x: int(x) if str(x).isdigit() else None),
            'is_valid': df['is_valid'],
            'validation_errors': df['validation_errors'].apply(lambda x: x if x else None),
            'duplicate_marker': df['duplicate_marker'],
            'bronce_id': df['bronce_id'],
            'processed_at': datetime.utcnow(),
        })
        
        # Insertar en PostgreSQL (row by row para mayor control)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        inserted = 0
        failed = 0
        
        for idx, row in df_plata.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO plata_parking 
                    (fecha, hora, timestamp, tipo_vehiculo, placa, barra_acceso, engomado, 
                     bitacora, foto, capacidad_sobrepasada, descanso_oficial, vacacional, año,
                     is_valid, validation_errors, duplicate_marker, bronce_id, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
                inserted += 1
            except Exception as e:
                failed += 1
                print(f"    !!Error insertando¡¡ registro {idx}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"  → Registros insertados exitosamente: {inserted}")
        print(f"  → Registros con error: {failed}")
        
        # Generar resumen
        resumen = generar_resumen_validacion(df)
        
        print("\n" + "="*80)
        print("RESUMEN DE VALIDACIÓN:")
        print("="*80)
        for clave, valor in resumen.items():
            print(f"  {clave}: {valor}")
        print("="*80)
        
        return {
            'status': 'SUCCESS',
            'records_processed': len(df),
            'records_inserted': inserted,
            'records_failed': failed,
            'resumen': resumen
        }
        
    except Exception as e:
        print(f"\n ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'FAILED', 'error': str(e)}

if __name__ == '__main__':
    # Para pruebas locales
    resultado = transform_bronce_to_plata()
    print(f"\nResultado: {resultado}")
```

### 4.3 Crear `scripts/load_gold.py`

```python
"""
Carga de Plata a Oro
Generación de tablas agregadas para análisis
"""
import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_plata_to_oro() -> dict:
    """
    Lee datos de plata, aplica agregaciones y carga tablas oro.
    
    Returns:
        Diccionario con estadísticas del proceso
    """
    print("="*80)
    print("INICIANDO CARGA: PLATA → ORO")
    print("="*80)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Limpiar tablas oro (opcional, según política de actualización)
        print("\n[1/4] Preparando tablas ORO...")
        cursor.execute("TRUNCATE TABLE oro_accesos_hora CASCADE;")
        cursor.execute("TRUNCATE TABLE oro_ocupacion_diaria CASCADE;")
        cursor.execute("TRUNCATE TABLE oro_resumen_tipo_vehiculo CASCADE;")
        cursor.execute("TRUNCATE TABLE oro_patrones_horarios CASCADE;")
        conn.commit()
        print("  → Tablas truncadas")
        
        # 2. Cargar oro_accesos_hora (agregación por fecha-hora-tipo)
        print("\n[2/4] Generando ORO - Accesos por Hora...")
        query_accesos_hora = """
            INSERT INTO oro_accesos_hora 
            (fecha, hora, tipo_vehiculo, total_accesos, capacidad_sobrepasada_count, 
             descanso_oficial, vacacional, año)
            SELECT
                fecha,
                hora,
                tipo_vehiculo,
                COUNT(*) as total_accesos,
                SUM(CASE WHEN capacidad_sobrepasada = true THEN 1 ELSE 0 END) as capacidad_sobrepasada_count,
                MAX(descanso_oficial) as descanso_oficial,
                MAX(vacacional) as vacacional,
                año
            FROM plata_parking
            WHERE is_valid = TRUE
            GROUP BY fecha, hora, tipo_vehiculo, año
            ON CONFLICT (fecha, hora, tipo_vehiculo) DO UPDATE SET
                total_accesos = EXCLUDED.total_accesos,
                capacidad_sobrepasada_count = EXCLUDED.capacidad_sobrepasada_count,
                descanso_oficial = EXCLUDED.descanso_oficial,
                vacacional = EXCLUDED.vacacional,
                updated_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query_accesos_hora)
        accesos_hora_count = cursor.rowcount
        conn.commit()
        print(f"  → Registros creados/actualizados: {accesos_hora_count}")
        
        # 3. Cargar oro_ocupacion_diaria
        print("\n[3/4] Generando ORO - Ocupación Diaria...")
        query_ocupacion_diaria = """
            INSERT INTO oro_ocupacion_diaria
            (fecha, tipo_vehiculo, total_accesos, capacidad_sobrepasada_events,
             descanso_oficial, vacacional, día_semana, año, promedio_accesos_hora, pico_hora, pico_valor)
            SELECT
                fecha,
                tipo_vehiculo,
                SUM(total_accesos) as total_accesos,
                SUM(capacidad_sobrepasada_count) as capacidad_sobrepasada_events,
                MAX(descanso_oficial) as descanso_oficial,
                MAX(vacacional) as vacacional,
                TO_CHAR(fecha, 'Day') as día_semana,
                año,
                ROUND(SUM(total_accesos)::numeric / COUNT(*), 2) as promedio_accesos_hora,
                (ARRAY_AGG(hora ORDER BY total_accesos DESC))[1] as pico_hora,
                MAX(total_accesos) as pico_valor
            FROM oro_accesos_hora
            GROUP BY fecha, tipo_vehiculo, año
            ON CONFLICT (fecha, tipo_vehiculo) DO UPDATE SET
                total_accesos = EXCLUDED.total_accesos,
                capacidad_sobrepasada_events = EXCLUDED.capacidad_sobrepasada_events,
                descanso_oficial = EXCLUDED.descanso_oficial,
                vacacional = EXCLUDED.vacacional,
                promedio_accesos_hora = EXCLUDED.promedio_accesos_hora,
                pico_hora = EXCLUDED.pico_hora,
                pico_valor = EXCLUDED.pico_valor,
                updated_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query_ocupacion_diaria)
        ocupacion_diaria_count = cursor.rowcount
        conn.commit()
        print(f"  → Registros creados/actualizados: {ocupacion_diaria_count}")
        
        # 4. Cargar oro_resumen_tipo_vehiculo
        print("\n[4/4] Generando ORO - Resumen por Tipo de Vehículo...")
        query_resumen_tipo = """
            INSERT INTO oro_resumen_tipo_vehiculo
            (tipo_vehiculo, total_accesos, total_accesos_validos, total_accesos_invalidos,
             porcentaje_validez, primer_acceso, ultimo_acceso, días_con_acceso,
             accesos_con_engomado, accesos_con_bitacora, accesos_descanso_oficial,
             accesos_vacacional, capacidad_sobrepasada_events)
            SELECT
                tipo_vehiculo,
                COUNT(*) as total_accesos,
                SUM(CASE WHEN is_valid = true THEN 1 ELSE 0 END) as total_accesos_validos,
                SUM(CASE WHEN is_valid = false THEN 1 ELSE 0 END) as total_accesos_invalidos,
                ROUND(
                    100.0 * SUM(CASE WHEN is_valid = true THEN 1 ELSE 0 END) / COUNT(*),
                    2
                ) as porcentaje_validez,
                MIN(fecha) as primer_acceso,
                MAX(fecha) as ultimo_acceso,
                COUNT(DISTINCT fecha) as días_con_acceso,
                SUM(CASE WHEN engomado = true THEN 1 ELSE 0 END) as accesos_con_engomado,
                SUM(CASE WHEN bitacora = true THEN 1 ELSE 0 END) as accesos_con_bitacora,
                SUM(CASE WHEN descanso_oficial = true THEN 1 ELSE 0 END) as accesos_descanso_oficial,
                SUM(CASE WHEN vacacional = true THEN 1 ELSE 0 END) as accesos_vacacional,
                SUM(CASE WHEN capacidad_sobrepasada = true THEN 1 ELSE 0 END) as capacidad_sobrepasada_events
            FROM plata_parking
            GROUP BY tipo_vehiculo
            ON CONFLICT (tipo_vehiculo) DO UPDATE SET
                total_accesos = EXCLUDED.total_accesos,
                total_accesos_validos = EXCLUDED.total_accesos_validos,
                total_accesos_invalidos = EXCLUDED.total_accesos_invalidos,
                porcentaje_validez = EXCLUDED.porcentaje_validez,
                primer_acceso = EXCLUDED.primer_acceso,
                ultimo_acceso = EXCLUDED.ultimo_acceso,
                días_con_acceso = EXCLUDED.días_con_acceso,
                accesos_con_engomado = EXCLUDED.accesos_con_engomado,
                accesos_con_bitacora = EXCLUDED.accesos_con_bitacora,
                accesos_descanso_oficial = EXCLUDED.accesos_descanso_oficial,
                accesos_vacacional = EXCLUDED.accesos_vacacional,
                capacidad_sobrepasada_events = EXCLUDED.capacidad_sobrepasada_events,
                updated_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query_resumen_tipo)
        resumen_tipo_count = cursor.rowcount
        conn.commit()
        
        print(f"  → Registros creados/actualizados: {resumen_tipo_count}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print(" CARGA A ORO COMPLETADA EXITOSAMENTE")
        print("="*80)
        print(f"  Accesos por hora: {accesos_hora_count}")
        print(f"  Ocupación diaria: {ocupacion_diaria_count}")
        print(f"  Resumen por tipo: {resumen_tipo_count}")
        print("="*80 + "\n")
        
        return {
            'status': 'SUCCESS',
            'accesos_hora': accesos_hora_count,
            'ocupacion_diaria': ocupacion_diaria_count,
            'resumen_tipo': resumen_tipo_count
        }
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return {'status': 'FAILED', 'error': str(e)}

if __name__ == '__main__':
    resultado = load_plata_to_oro()
    print(f"Resultado: {resultado}")
```

---
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)

## PASO 5: CREAR EL DAG DE AIRFLOW

### 5.1 Crear `dags/parking_etl_dag.py`

```python
"""
DAG Principal: Estacionamiento TSJ Zapopan - ETL Bronce → Plata → Oro
Orquestación de pipeline de datos con Airflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import json
import logging

# Configurar ruta de scripts
sys.path.insert(0, '/opt/airflow/scripts')

from transform_silver import transform_bronce_to_plata
from load_gold import load_plata_to_oro

# Logger
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN DEL DAG
# ============================================================

DEFAULT_ARGS = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# ============================================================
# TAREAS PERSONALIZADAS (PYTHON OPERATORS)
# ============================================================

def task_extract_to_bronce(**context):
    """
    TAREA 1: EXTRACT → BRONCE
    Lee el CSV del estacionamiento y carga los registros en la tabla bronce.
    """
    logger.info("="*80)
    logger.info("INICIANDO EXTRACCIÓN A BRONCE")
    logger.info("="*80)
    
    DATA_PATH = "/opt/airflow/data/reporte_acceso_tsj_zapopan_2021_2025.csv"
    
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"CSV no encontrado en {DATA_PATH}")
    
    logger.info(f"Leyendo CSV: {DATA_PATH}")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Leer CSV en chunks para manejar grandes volúmenes
        chunk_size = 5000
        total_rows = 0
        
        for chunk_num, df_chunk in enumerate(pd.read_csv(DATA_PATH, chunksize=chunk_size)):
            logger.info(f"Procesando chunk {chunk_num + 1}...")
            
            for _, row in df_chunk.iterrows():
                # Convertir fila a JSON para bronce
                raw_json = json.dumps(row.dropna().to_dict(), default=str)
                
                # Extraer año y fecha
                year = None
                fecha = None
                if 'año' in row and pd.notna(row['año']):
                    year = int(row['año'])
                if 'fecha' in row and pd.notna(row['fecha']):
                    try:
                        fecha = pd.to_datetime(row['fecha']).date()
                    except:
                        pass
                
                cursor.execute("""
                    INSERT INTO bronce_parking_raw 
                    (raw_loaded_at, source_file, year, fecha, raw_record)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    datetime.utcnow(),
                    os.path.basename(DATA_PATH),
                    year,
                    fecha,
                    raw_json
                ))
                
                total_rows += 1
            
            conn.commit()
            logger.info(f"  Chunk {chunk_num + 1}: {len(df_chunk)} registros insertados")
        
        cursor.close()
        conn.close()
        
        logger.info("="*80)
        logger.info(f" EXTRACCIÓN COMPLETADA: {total_rows} registros cargados en BRONCE")
        logger.info("="*80)
        
        # Guardar info en XComs para tareas siguientes
        context['task_instance'].xcom_push(
            key='bronce_record_count',
            value=total_rows
        )
        
        return {
            'status': 'SUCCESS',
            'records_loaded': total_rows
        }
        
    except Exception as e:
        logger.error(f" ERROR en extracción: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        raise

def task_transform_to_plata(**context):
    """
    TAREA 2: TRANSFORM → PLATA
    Limpia, valida y estandariza datos de bronce.
    """
    logger.info("="*80)
    logger.info("INICIANDO TRANSFORMACIÓN A PLATA")
    logger.info("="*80)
    
    # Obtener valor de XCom (opcional)
    bronce_count = context['task_instance'].xcom_pull(
        task_ids='extract_to_bronce',
        key='bronce_record_count'
    )
    logger.info(f"Registros en bronce: {bronce_count}")
    
    # Ejecutar función de transformación
    resultado = transform_bronce_to_plata()
    
    if resultado['status'] == 'SUCCESS':
        context['task_instance'].xcom_push(
            key='plata_record_count',
            value=resultado['records_inserted']
        )
        return resultado
    else:
        raise Exception(f"Error en transformación: {resultado}")

def task_load_to_oro(**context):
    """
    TAREA 3: LOAD → ORO
    Genera tablas agregadas para análisis (hechos y dimensiones).
    """
    logger.info("="*80)
    logger.info("INICIANDO CARGA A ORO")
    logger.info("="*80)
    
    plata_count = context['task_instance'].xcom_pull(
        task_ids='transform_to_silver',
        key='plata_record_count'
    )
    logger.info(f"Registros en plata: {plata_count}")
    
    resultado = load_plata_to_oro()
    
    if resultado['status'] == 'SUCCESS':
        context['task_instance'].xcom_push(
            key='oro_load_summary',
            value=resultado
        )
        return resultado
    else:
        raise Exception(f"Error en carga oro: {resultado}")

def task_validar_datos(**context):
    """
    TAREA 4: VALIDACIÓN
    Ejecuta consultas de control para verificar integridad de datos.
    """
    logger.info("="*80)
    logger.info("EJECUTANDO VALIDACIONES")
    logger.info("="*80)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    try:
        # Query 1: Contar registros por capa
        query1 = """
            SELECT 
                'bronce' as capa, COUNT(*) as total
            FROM bronce_parking_raw
            UNION
            SELECT 'plata', COUNT(*) FROM plata_parking
            UNION
            SELECT 'oro_accesos_hora', COUNT(*) FROM oro_accesos_hora
        """
        df1 = pd.read_sql(query1, con=engine)
        logger.info("Registros por capa:")
        for _, row in df1.iterrows():
            logger.info(f"  {row['capa']}: {row['total']}")
        
        # Query 2: Porcentaje de validez
        query2 = """
            SELECT 
                SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as validos,
                COUNT(*) as total,
                ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) as porcentaje
            FROM plata_parking
        """
        df2 = pd.read_sql(query2, con=engine)
        logger.info(f"Calidad de datos PLATA:")
        logger.info(f"  Válidos: {df2.iloc[0]['validos']}")
        logger.info(f"  Total: {df2.iloc[0]['total']}")
        logger.info(f"  Porcentaje validez: {df2.iloc[0]['porcentaje']}%")
        
        logger.info("="*80)
        logger.info(" VALIDACIONES COMPLETADAS")
        logger.info("="*80)
        
        return {'status': 'SUCCESS', 'validations': df1.to_dict()}
        
    except Exception as e:
        logger.error(f" ERROR en validación: {e}")
        raise

# ============================================================
# DEFINICIÓN DEL DAG
# ============================================================

with DAG(
    dag_id='parking_etl_bronce_plata_oro',
    default_args=DEFAULT_ARGS,
    description='ETL Pipeline - Estacionamiento TSJ Zapopan (Bronce → Plata → Oro)',
    schedule_interval='0 2 * * *',  # Ejecuta diariamente a las 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['parking', 'etl', 'bigdata'],
) as dag:
    
    # TAREA 1: Verificar que PostgreSQL esté disponible
    t_check_db = BashOperator(
        task_id='check_database',
        bash_command='echo "Verificando conexión a PostgreSQL"',
        doc='Verifica que PostgreSQL está disponible'
    )
    
    # TAREA 2: Extract → Bronce
    t_extract = PythonOperator(
        task_id='extract_to_bronce',
        python_callable=task_extract_to_bronce,
        provide_context=True,
        doc='Lee CSV y carga en tabla bronce sin transformaciones'
    )
    
    # TAREA 3: Transform → Plata
    t_transform = PythonOperator(
        task_id='transform_to_silver',
        python_callable=task_transform_to_plata,
        provide_context=True,
        doc='Limpia, valida y normaliza datos'
    )
    
    # TAREA 4: Load → Oro
    t_load = PythonOperator(
        task_id='load_to_gold',
        python_callable=task_load_to_oro,
        provide_context=True,
        doc='Genera tablas agregadas para análisis'
    )
    
    # TAREA 5: Validar integridad
    t_validate = PythonOperator(
        task_id='validate_data',
        python_callable=task_validar_datos,
        provide_context=True,
        doc='Ejecuta queries de validación'
    )
    
    # DEFINIR DEPENDENCIAS
    t_check_db >> t_extract >> t_transform >> t_load >> t_validate

# ============================================================
# DOCUMENTACIÓN DEL DAG
# ============================================================

dag.doc_md = """
# Pipeline ETL: Estacionamiento TSJ Zapopan

## Descripción
Pipeline de datos que implementa la arquitectura **medallion** (bronce → plata → oro) para análisis de accesos al estacionamiento del Centro Educativo TSJ Zapopan.

## Flujo de datos
1. **BRONCE (Raw):** Importa CSV completo sin transformaciones
2. **PLATA (Staging):** Limpia, valida y normaliza datos
3. **ORO (Confiable):** Genera tablas analíticas agregadas

## Cronograma
- Ejecución automática: **Diariamente a las 2:00 AM**
- Duración estimada: **15-30 minutos** (depende del volumen)

## Responsables
- Data Engineer: Alumno de Maestría en Big Data
- Base de datos: PostgreSQL 14
- Orquestación: Apache Airflow 2.7.1

## Contacto
Para reportar problemas, contactar al instructor del curso.
"""
```

---
![docker-compose up](https://img.shields.io/badge/docker--compose-up-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## PASO 6: LEVANTAR EL ENTORNO

### 6.1 Estructura final verificada

```bash
# Verificar que todo esté en su lugar
parking-etl-tsj/
├── docker-compose.yml          
├── dags/parking_etl_dag.py     
├── scripts/*.py                
├── sql/init_database.sql       
├── data/
│   └── reporte_acceso_tsj_zapopan_2021_2025.csv  
└── ...
```

### 6.2 Levantar servicios

```bash
# Iniciar Docker Compose
docker-compose up -d

# Verificar servicios
docker-compose ps

# Ver logs en tiempo real
docker-compose logs -f airflow-webserver

# Esperar ~1-2 minutos para que Airflow inicialice
```

### 6.3 Acceso a interfaces

**Airflow Web UI:**
- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin123`

**pgAdmin:**
- URL: http://localhost:8081
- Email: `admin@parking-tsj.local`
- Contraseña: `admin123`

**PostgreSQL:**
- Host: `localhost`
- Puerto: `5432`
- Usuario: `airflow`
- Contraseña: `airflow`
- Base de datos: `parking_db`

---
![Trigger DAG](https://img.shields.io/badge/Trigger-DAG-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)

## PASO 7: EJECUTAR EL DAG

### 7.1 En Airflow Web UI

1. Ir a http://localhost:8080
2. Encontrar DAG `parking_etl_bronce_plata_oro`
3. Click en el DAG
4. Click en botón **play** (▶) para "Trigger DAG"
5. Observar ejecución de tareas en tiempo real

### 7.2 Verificar ejecución

```bash
# Ver logs
docker-compose logs airflow-scheduler

# Acceder a shell de Airflow
docker-compose exec airflow-webserver bash
```

---
![pgAdmin](https://img.shields.io/badge/pgAdmin-4D6DB3?style=for-the-badge&logo=postgresql&logoColor=white)

## PASO 8: VALIDAR DATOS EN PGADMIN

### 8.1 Conectar a PostgreSQL desde pgAdmin

1. Abrir http://localhost:8081
2. Login con admin@parking-tsj.local / admin123
3. Servers → Register → New Server
   - Name: `PostgreSQL Parking`
   - Host: `postgres`
   - Username: `airflow`
   - Password: `airflow`
   - Database: `parking_db`
4. Click "Save"

### 8.2 Ejecutar consultas de validación

**Contar registros por capa:**
```sql
SELECT 'BRONCE' as capa, COUNT(*) as total FROM bronce_parking_raw
UNION
SELECT 'PLATA', COUNT(*) FROM plata_parking
UNION
SELECT 'ORO - Accesos Hora', COUNT(*) FROM oro_accesos_hora
UNION
SELECT 'ORO - Ocupación Diaria', COUNT(*) FROM oro_ocupacion_diaria
UNION
SELECT 'ORO - Resumen Tipo', COUNT(*) FROM oro_resumen_tipo_vehiculo;
```

**Top 10 horas con mayor ocupación:**
```sql
SELECT 
    fecha,
    hora,
    tipo_vehiculo,
    total_accesos,
    capacidad_sobrepasada_count,
    CASE WHEN capacidad_sobrepasada_count > 0 THEN ' CRÍTICO' ELSE ' OK' END as estado
FROM oro_accesos_hora
ORDER BY capacidad_sobrepasada_count DESC, total_accesos DESC
LIMIT 10;
```

**Resumen por tipo de vehículo:**
```sql
SELECT
    tipo_vehiculo,
    total_accesos,
    total_accesos_validos,
    porcentaje_validez,
    primer_acceso,
    ultimo_acceso,
    días_con_acceso,
    capacidad_sobrepasada_events
FROM oro_resumen_tipo_vehiculo
ORDER BY total_accesos DESC;
```

**Patrones de ocupación por franja horaria:**
```sql
SELECT
    EXTRACT(HOUR FROM timestamp) as hora,
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp) >= 6 AND EXTRACT(HOUR FROM timestamp) < 9 THEN 'Matutina (6-9)'
        WHEN EXTRACT(HOUR FROM timestamp) >= 9 AND EXTRACT(HOUR FROM timestamp) < 12 THEN 'Mañana (9-12)'
        WHEN EXTRACT(HOUR FROM timestamp) >= 12 AND EXTRACT(HOUR FROM timestamp) < 15 THEN 'Mediodía (12-15)'
        WHEN EXTRACT(HOUR FROM timestamp) >= 15 AND EXTRACT(HOUR FROM timestamp) < 18 THEN 'Tarde (15-18)'
        ELSE 'Nocturno'
    END as franja,
    COUNT(*) as total_accesos,
    COUNT(DISTINCT DATE(timestamp)) as días_únicos
FROM plata_parking
WHERE is_valid = TRUE
GROUP BY EXTRACT(HOUR FROM timestamp), franja
ORDER BY hora;
```

---
![Documentación](https://img.shields.io/badge/Documentación-4A90E2?style=for-the-badge&logo=readthedocs&logoColor=white)

##  ENTREGABLES ESPERADOS

### 1. Repositorio GitHub

Tu repositorio debe contener:

- `README.md` con instrucciones de inicio (este documento)
- `docker-compose.yml` funcional
- `dags/parking_etl_dag.py` con DAG completo
- `scripts/` con `transform_silver.py`, `load_gold.py`, `utils.py`
- `sql/init_database.sql` con todas las tablas
- `.gitignore` configurado correctamente
- `data/sample_5rows.csv` (muestra para pruebas)
- `tests/test_transformations.py` (opcional pero recomendado)

### 2. Evidencias de ejecución

Capturar y documentar:

- Captura de Airflow Web UI mostrando DAG ejecutado exitosamente
- Captura de pgAdmin con:
  - Tabla bronce con datos cargados
  - Tabla plata con datos transformados
  - Tablas oro con agregaciones
- Exportar 2-3 consultas SQL con resultados mostrando insights

### 3. Documento de análisis (5 páginas)

Explica:

- **Diseño de la solución:** ¿Por qué decidiste esta arquitectura?
- **Transformaciones aplicadas:** Lógica de limpieza y validación
- **Decisiones de modelado:** ¿Cómo diseñaste las tablas oro?
- **Insights generados:** ¿Qué patrones detectaste en los datos?
- **Desafíos resueltos:** Problemas encontrados y cómo los solucionaste

### 4. Plan de mejora (opcional)

Proposiciones para evolucionar:

- Incrementar frecuencia de ejecución (@hourly, @30min)
- Agregar checks de data quality (Great Expectations)
- Implementar alertas por capacidad sobrepasada
- Crear dashboards con Metabase o Superset
- Implementar versionado de datos (data lineage)

---
![Criterios de Evaluación](https://img.shields.io/badge/Criterios%20de-Evaluación-4CAF50?style=for-the-badge&logo=markdown&logoColor=white)

## CRITERIOS DE EVALUACIÓN

| Criterio | Peso | Descripción |
|----------|------|-------------|
| **Entorno Docker funcional** | 15% | Airflow, PostgreSQL, pgAdmin levantados correctamente |
| **DAG implementado correctamente** | 20% | Dependencias, operadores, manejo de errores |
| **Transformaciones en PLATA** | 25% | Limpieza, validación, normalización de datos |
| **Capa ORO (agregaciones)** | 20% | Tablas analíticas con métricas útiles |
| **Documentación y código limpio** | 10% | README, comments, estructura ordenada |
| **Análisis e insights** | 10% | Interpretación de resultados y patrones |

---

## TROUBLESHOOTING

### Problema: "docker: command not found"
**Solución:** Instalar Docker Desktop o Docker Engine según tu SO

### Problema: Puerto 8080 ya en uso
**Solución:** Cambiar en docker-compose.yml
```yaml
ports:
  - "8090:8080"  # Usar puerto 8090 en lugar de 8080
```

### Problema: PostgreSQL no inicia
**Solución:**
```bash
docker-compose down -v  # Eliminar volúmenes
docker-compose up -d    # Reiniciar
```

### Problema: DAG no aparece en Airflow
**Solución:**
```bash
# Verificar permisos
chmod 777 dags/
# Esperar 30 segundos y refrescar página
```

### Problema: "No module named 'utils'"
**Solución:** Verificar que `sys.path` en DAG incluya `/opt/airflow/scripts`

---

## REFERENCIAS Y RECURSOS

- **Apache Airflow:** https://airflow.apache.org/docs/
- **PostgreSQL:**  https://www.postgresql.org/docs/
- **Pandas:** https://pandas.pydata.org/docs/
- **Medallion Architecture:** https://www.databricks.com/blog/2022/06/24/onelake-medallion-lakehouse-architecture.html
- **Data Engineering Fundamentals:** https://www.datacamp.com/

---

## SOPORTE

Para dudas o problemas:
1. Revisar logs: `docker-compose logs -f airflow-webserver`
2. Consultar documentación oficial de las herramientas
3. Contactar al instructor del curso

---

**¡Éxito en tu práctica de Big Data! **

