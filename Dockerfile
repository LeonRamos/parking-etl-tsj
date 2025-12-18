# Usamos la imagen oficial de Airflow como base
FROM apache/airflow:2.7.1

# Cambiamos a usuario root para instalar dependencias del sistema si fuera necesario
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Volvemos al usuario airflow para instalar las librerías de Python
USER airflow

# Instalamos las librerías necesarias para el proyecto
# pandas: para manejar el CSV
# psycopg2-binary: para conectar con Postgres
# sqlalchemy: para cargar datos a la DB
RUN pip install --no-cache-dir \
    pandas \
    psycopg2-binary \
    sqlalchemy