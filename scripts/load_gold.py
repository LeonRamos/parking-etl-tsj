import pandas as pd
from sqlalchemy import create_engine, text

# Configuración de conexión
DB_URL = "postgresql+psycopg2://airflow:postgres1989@host.docker.internal:5433/parking_db"
engine = create_engine(DB_URL)

def load_to_gold():
    print("Iniciando generación de Capa Oro (Modelos Analíticos)...")
    
    try:
        with engine.begin() as conn:
            # Borramos tablas previas
            conn.execute(text("DROP TABLE IF EXISTS oro.oro_accesos_hora CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS oro.oro_ocupacion_diaria CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS oro.oro_resumen_tipo_vehiculo CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS oro.oro_patrones_horarios CASCADE;"))

            # 1. oro_accesos_hora
            conn.execute(text("""
                CREATE TABLE oro.oro_accesos_hora AS
                SELECT 
                    EXTRACT(HOUR FROM timestamp::TIMESTAMP) as hora_del_dia,
                    COUNT(*) as total_accesos
                FROM plata.plata_parking
                WHERE is_valid = TRUE
                GROUP BY EXTRACT(HOUR FROM timestamp::TIMESTAMP)
                ORDER BY hora_del_dia;
            """))

            # 2. oro_ocupacion_diaria
            conn.execute(text("""
                CREATE TABLE oro.oro_ocupacion_diaria AS
                SELECT 
                    fecha::DATE as fecha_clean,
                    COUNT(*) as total_accesos,
                    COUNT(DISTINCT placa) as vehiculos_unicos
                FROM plata.plata_parking
                WHERE is_valid = TRUE
                GROUP BY fecha::DATE
                ORDER BY fecha_clean;
            """))

            # 3. oro_resumen_tipo_vehiculo
            conn.execute(text("""
                CREATE TABLE oro.oro_resumen_tipo_vehiculo AS
                SELECT 
                    tipo_vehiculo,
                    COUNT(*) as cantidad,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM plata.plata_parking), 2) as porcentaje_del_total
                FROM plata.plata_parking
                GROUP BY tipo_vehiculo
                ORDER BY cantidad DESC;
            """))

            # 4. oro_patrones_horarios (CORREGIDO EL GROUP BY)
            conn.execute(text("""
                CREATE TABLE oro.oro_patrones_horarios AS
                SELECT 
                    CASE 
                        WHEN EXTRACT(DOW FROM fecha::DATE) IN (0, 6) THEN 'Fin de Semana'
                        ELSE 'Día Laboral'
                    END as tipo_dia,
                    EXTRACT(HOUR FROM timestamp::TIMESTAMP) as hora_buscada,
                    COUNT(*) as flujo_vehicular
                FROM plata.plata_parking
                WHERE is_valid = TRUE
                GROUP BY 
                    CASE WHEN EXTRACT(DOW FROM fecha::DATE) IN (0, 6) THEN 'Fin de Semana' ELSE 'Día Laboral' END,
                    EXTRACT(HOUR FROM timestamp::TIMESTAMP)
                ORDER BY tipo_dia, flujo_vehicular DESC;
            """))

        print("--- Capa Oro generada con éxito ---")

    except Exception as e:
        print(f"Error al generar la Capa Oro: {e}")
        raise

if __name__ == "__main__":
    load_to_gold()