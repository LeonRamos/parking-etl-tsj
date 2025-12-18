-- 1. Crear la base de datos para Airflow 
-- (Postgres no permite 'CREATE DATABASE' dentro de una transacción, se ejecuta solo)
CREATE DATABASE airflow;

-- 2. Nos aseguramos de que el usuario tenga permisos
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- 3. Conectarse a la base de datos del proyecto para crear los esquemas
\c parking_db;

-- 4. Crear los esquemas de la arquitectura Medallion
CREATE SCHEMA IF NOT EXISTS bronce;
CREATE SCHEMA IF NOT EXISTS plata;
CREATE SCHEMA IF NOT EXISTS oro;

-- 5. Crear tabla de aterrizaje en capa Bronce
CREATE TABLE IF NOT EXISTS bronce.bronce_parking_raw (
    id SERIAL PRIMARY KEY,
    raw_record JSONB NOT NULL,
    source_file VARCHAR(255),
    year INT,
    fecha DATE,
    raw_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comentario para verificar en pgAdmin
COMMENT ON SCHEMA bronce IS 'Capa de datos crudos de Zapopan';