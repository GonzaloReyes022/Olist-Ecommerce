# Imagen oficial de Airflow
# - Ya tiene Airflow instalado
# - Python 3.11 (versión estable)
# - Mantenida por la comunidad de Airflow
FROM apache/airflow:2.8.1-python3.11

# Cambiamos a usuario root para instalar dependencias del sistema
USER root

# Instalamos herramientas necesarias
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    # ¿Por qué gcc/g++? Para compilar librerías Python que lo necesiten
    && apt-get clean

# Volvemos al usuario airflow (salimos de root)
USER airflow

# Copiamos archivo de dependencias Python
COPY requirements.txt /requirements.txt

# Instalamos librerías Python
RUN pip install --no-cache-dir -r /requirements.txt

# ¿Por qué --no-cache-dir?
# Para que la imagen de Docker sea más pequeña. Normalmente pip guarda
# en ram una copia de lo instalado por si llegamos a querer instalarlo de
# nuevo y no tener que descagarlo desde 0