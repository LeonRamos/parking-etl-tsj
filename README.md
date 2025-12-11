
[![Ciencia de Datos](https://img.shields.io/badge/Ciencia%20de%20Datos-Big%20Data-brightgreen)]()
[![Python](https://img.shields.io/badge/Python-%203.8%2B-blue)]()
[![Big%20Data](https://img.shields.io/badge/Big%20Data-Tecnolog%C3%ADas%20Emergentes-orange)]()
[![GitHub](https://img.shields.io/badge/GitHub-Gest%C3%B3n%20de%20proyectos-lightgrey)]()
[![TecMM](https://img.shields.io/badge/TecMM-zapopan-0D3692?style=for-the-badge&logo=university&logoColor=white&labelColor=101010)]()
## Descripción

# parking-etl-tsj
>Este Proyecto busca que el alumno desarrolle competencias integrales en Data Engineering mediante la implementación de un pipeline ETL (Extract, Transform, Load) completo que procesa datos reales de acceso al estacionamiento de una institución educativa.  El estudiante actuará como Ingeniero de Datos responsable de:  Diseñar y construir un sistema de orquestación de datos automatizado  Aplicar principios de arquitectura medallion (bronce-plata-oro) para calidad de datos  Implementar transformaciones de datos con validación y limpieza  Generar modelos analíticos para toma de decisiones  Esta aproximación basada en proyectos reales prepara al estudiante para desafíos encontrados en la industria moderna de Data Science y Big Data.

## Guía Rápida 

#  Pipeline ETL Estacionamiento TSJ Zapopan

**Proyecto de Big Data - MSC - TSJ**

Esta solución completa de Data Engineering que implementa la arquitectura medallón (Bronce → Plata → Oro) para analizar accesos al estacionamiento del Centro Educativo TSJ Zapopan.

##  Características

- **Pipeline ETL completo** orquestado con Apache Airflow
- **Arquitectura medallion** (bronce raw → plata staging → oro confiable)
- **92,572 registros** de accesos de estacionamiento (2021-2025)
- **Transformaciones automáticas** de limpieza y validación
- **Análisis de ocupación** por hora, día y tipo de vehículo
- **Entorno local con Docker** listo para desarrollo

##  Requisitos Previos

- Docker Desktop 4.0+
- Docker Compose
- Git
- Navegador web moderno
- ~2GB de espacio en disco

##  Inicio Rápido (5 minutos)

### 1. Clonar repositorio
```bash
git clone https://github.com/tu-usuario/parking-etl-tsj.git
cd parking-etl-tsj
```

### 2. Preparar datos
```bash
# Colocar el CSV en data/
cp reporte_acceso_tsj_zapopan_2021_2025.csv data/
```

### 3. Levantar servicios
```bash
docker-compose up -d
```

### 4. Verificar servicios
```bash
docker-compose ps
```

Espera 1-2 minutos para que Airflow inicialice completamente.

### 5. Acceder a interfaces

| Herramienta | URL | Usuario/Contraseña |
|------------|-----|-------------------|
| **Airflow** | http://localhost:8080 | admin / admin123 |
| **pgAdmin** | http://localhost:8081 | admin@parking-tsj.local / admin123 |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

##  Ejecutar el DAG

1. Abrir http://localhost:8080
2. Ir a DAGs → `parking_etl_bronce_plata_oro`
3. Click en **trigger** (▶ botón)
4. Monitorear ejecución de tareas
5. Esperar ~5-10 minutos hasta completarse

##  Estructura de Archivos

```
parking-etl-tsj/
├── docker-compose.yml              # Configuración de servicios
├── README.md                        # Este archivo
├── PRACTICA_ETL_AIRFLOW.md         # Guía detallada completa
│
├── dags/
│   └── parking_etl_dag.py          # DAG principal de Airflow
│
├── scripts/
│   ├── utils.py                    # Utilidades de transformación
│   ├── transform_silver.py         # Bronce → Plata
│   └── load_gold.py                # Plata → Oro
│
├── sql/
│   ├── init_database.sql           # Script de inicialización
│   ├── ddl_bronce.sql              # Tabla bronce
│   ├── ddl_plata.sql               # Tabla plata
│   ├── ddl_oro.sql                 # Tablas oro
│   └── queries_validacion.sql      # Queries de verificación
│
├── data/
│   ├── reporte_acceso_tsj_zapopan_2021_2025.csv  # CSV principal
│   └── sample_5rows.csv                          # Muestra para pruebas
│
├── tests/
│   └── test_transformations.py     # Tests unitarios
│
├── logs/                           # Logs de Airflow (auto-generado)
└── plugins/                        # Plugins personalizados

```

##  Arquitectura Medallion

### Capa Bronce (Raw)
- **Tabla:** `bronce_parking_raw`
- **Propósito:** Almacenamiento original sin transformaciones
- **Formato:** JSON + metadatos de carga
- **Responsabilidad:** Trazabilidad completa

### Capa Plata (Staging)
- **Tabla:** `plata_parking`
- **Propósito:** Datos limpios y validados
- **Transformaciones:**
  - Normalización de tipos de vehículos
  - Validación de placas
  - Conversión de tipos de datos
  - Detección de duplicados
- **Calidad:** ~95% de registros válidos

### Capa Oro (Confiable)
- **Tablas analíticas:**
  - `oro_accesos_hora` - Agregación por hora
  - `oro_ocupacion_diaria` - Métricas diarias
  - `oro_resumen_tipo_vehiculo` - Análisis por tipo
  - `oro_patrones_horarios` - Patrones de uso
- **Propósito:** Modelos listos para análisis

##  Consultas Útiles

### Registros por capa
```sql
SELECT 'BRONCE' as capa, COUNT(*) as total FROM bronce_parking_raw
UNION ALL
SELECT 'PLATA', COUNT(*) FROM plata_parking
UNION ALL
SELECT 'ORO', COUNT(*) FROM oro_accesos_hora;
```

### Horas con mayor ocupación
```sql
SELECT fecha, hora, tipo_vehiculo, total_accesos
FROM oro_accesos_hora
WHERE capacidad_sobrepasada_count > 0
ORDER BY capacidad_sobrepasada_count DESC
LIMIT 10;
```

### Resumen por tipo de vehículo
```sql
SELECT 
    tipo_vehiculo,
    total_accesos,
    porcentaje_validez,
    capacidad_sobrepasada_events
FROM oro_resumen_tipo_vehiculo
ORDER BY total_accesos DESC;
```

## Comandos Útiles

### Ver estado de servicios
```bash
docker-compose ps
```

### Ver logs en tiempo real
```bash
docker-compose logs -f airflow-webserver
```

### Detener servicios
```bash
docker-compose stop
```

### Eliminar todo (limpieza total)
```bash
docker-compose down -v
```

### Acceder a shell de PostgreSQL
```bash
docker-compose exec postgres psql -U airflow -d parking_db
```

### Ejecutar script SQL
```bash
docker-compose exec postgres psql -U airflow -d parking_db -f /docker-entrypoint-initdb.d/init.sql
```

##  Documentación Completa

Consulta **PRACTICA_ETL_AIRFLOW.md** para:
- Pasos detallados de implementación
- Explicación de cada componente
- Scripts completos y comentados
- Troubleshooting extendido
- Referencias y recursos

##  Troubleshooting Rápido

### "Docker command not found"
→ Instalar Docker Desktop desde https://docker.com

### Puerto 8080 en uso
→ Cambiar en docker-compose.yml: `ports: ["8090:8080"]`

### PostgreSQL no inicia
```bash
docker-compose down -v
docker-compose up -d
```

### DAG no aparece en Airflow
```bash
chmod 777 dags/
# Esperar 30 segundos y refrescar
```

### No hay datos en tablas
```bash
# Verificar que el CSV esté en data/
ls -lh data/
# Ejecutar DAG manualmente
```

## Dataset

**Reporte de Acceso - TSJ Zapopan**
- **Período:** 2021-2025
- **Registros:** 92,572
- **Columnas:** 12
  - fecha (DATE)
  - hora (TIME)
  - tipo_vehiculo (VARCHAR)
  - placa (VARCHAR)
  - barra_acceso (INT)
  - engomado (BOOLEAN)
  - bitacora (BOOLEAN)
  - foto (VARCHAR)
  - capacidad_sobrepasada (BOOLEAN)
  - descanso_oficial (BOOLEAN)
  - vacacional (BOOLEAN)
  - año (INT)

##  Aprendizajes Esperados

- Levantar entorno Docker con múltiples servicios
- Diseñar DAGs en Apache Airflow
- Implementar ETL con Python y pandas
- Modelar datos en PostgreSQL
- Arquitectura medallion (bronce-plata-oro)
- Transformación y limpieza de datos
- Versionado con Git
- Análisis de datos para toma de decisiones

## Entregables

- Repositorio GitHub con código
- DAG ejecutado exitosamente
- Capturas de pgAdmin con datos
- Documento de análisis (1-2 págs)
- Mejoras propuestas para evolución

## Criterios de Evaluación

| Rubro | % |
|-------|-----|
| Entorno funcional | 15% |
| DAG correcto | 20% |
| Transformaciones PLATA | 25% |
| Capa ORO | 20% |
| Documentación | 10% |
| Análisis e insights | 10% |

## Enlaces Útiles

- [Apache Airflow Docs](https://airflow.apache.org/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/)
- [Docker Documentation](https://docs.docker.com/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/onelake-medallion-lakehouse-architecture.html)

##  Soporte

Para problemas o dudas:
1. Consultar **[PRACTICA_ETL_AIRFLOW.md](/PRACTICA_ETL_AIRFLOW.md)** (sección Troubleshooting)
2. Revisar logs: `docker-compose logs -f`
3. Contactar al profesor


---

**¡Éxito en tu proyecto de Big Data!**

![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![pgAdmin](https://img.shields.io/badge/pgAdmin-4D6DB3?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)


