#!/bin/bash

# ============================================================
# SCRIPT DE INSTALACIÓN - PARKING ETL
# Automatiza el setup del entorno Docker
# ============================================================

set -e  # Salir ante cualquier error

echo "=================================================="
echo "🚀 INSTALADOR - PIPELINE ETL ESTACIONAMIENTO"
echo "=================================================="
echo ""

# Verificar que Docker está instalado
if ! command -v docker &> /dev/null; then
    echo " ERROR: Docker no está instalado"
    echo "   Descargar desde: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Verificar que Docker Compose está instalado
if ! command -v docker-compose &> /dev/null; then
    echo " ERROR: Docker Compose no está instalado"
    echo "   Descargar desde: https://docs.docker.com/compose/install/"
    exit 1
fi

echo " Docker y Docker Compose detectados"
echo ""

# Crear directorios necesarios
echo "[1/5] Creando estructura de directorios..."
mkdir -p dags scripts sql data tests logs plugins postgres_data pgadmin_data
echo " Directorios creados"
echo ""

# Crear archivo .gitignore si no existe
if [ ! -f .gitignore ]; then
    echo "[2/5] Creando .gitignore..."
    cat > .gitignore << 'EOF'
# Datos
data/*.csv
!data/sample_5rows.csv

# Airflow
logs/
airflow.db
airflow.cfg
postgres_data/
pgadmin_data/

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
EOF
    echo " .gitignore creado"
else
    echo " .gitignore ya existe"
fi
echo ""

# Verificar que existe docker-compose.yml
if [ ! -f docker-compose.yml ]; then
    echo " ERROR: docker-compose.yml no encontrado"
    echo "   Por favor asegurate de estar en la carpeta raíz del proyecto"
    exit 1
fi

# Validar docker-compose.yml
echo "[3/5] Validando docker-compose.yml..."
docker-compose config > /dev/null
echo " Configuración válida"
echo ""

# Levantar servicios
echo "[4/5] Levantando servicios (esto puede tomar 1-2 minutos)..."
docker-compose up -d
echo " Servicios iniciados"
echo ""

# Esperar a que PostgreSQL esté listo
echo "[5/5] Esperando a que PostgreSQL esté disponible..."
sleep 10

for i in {1..30}; do
    if docker-compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1; then
        echo " PostgreSQL está listo"
        break
    fi
    echo "   Intento $i/30..."
    sleep 2
done

echo ""
echo "=================================================="
echo " INSTALACIÓN COMPLETADA"
echo "=================================================="
echo ""
echo " Pasos siguientes:"
echo ""
echo "1️  Coloca el CSV en:"
echo "    cp reporte_acceso_tsj_zapopan_2021_2025.csv data/"
echo ""
echo "2️  Accede a Airflow:"
echo "     http://localhost:8080"
echo "     Usuario: admin"
echo "     Contraseña: admin123"
echo ""
echo "3️  Accede a pgAdmin:"
echo "     http://localhost:8081"
echo "     Email: admin@parking-tsj.local"
echo "     Contraseña: admin123"
echo ""
echo "4️  PostgreSQL disponible en:"
echo "      localhost:5432"
echo "     Usuario: airflow"
echo "     Contraseña: airflow"
echo ""
echo "5️  Ejecuta el DAG:"
echo "    - Abre Airflow"
echo "    - Ve a DAGs"
echo "    - Busca 'parking_etl_bronce_plata_oro'"
echo "    - Click en el botón play (▶)"
echo ""
echo " Documentación:"
echo "    - README.md → Guía rápida"
echo "    - PRACTICA_ETL_AIRFLOW.md → Guía completa"
echo "    - RESUMEN_EJECUTIVO.md → Descripción general"
echo ""
echo " Ver logs:"
echo "    docker-compose logs -f airflow-webserver"
echo ""
echo "  Ver estado de servicios:"
echo "    docker-compose ps"
echo ""
echo " Detener servicios:"
echo "    docker-compose stop"
echo ""
echo "=================================================="

