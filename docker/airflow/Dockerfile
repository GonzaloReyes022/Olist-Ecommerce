# Imagen oficial de Airflow
# - Ya tiene Airflow instalado
# - Python 3.11 (versión estable)
# - Mantenida por la comunidad de Airflow
FROM apache/airflow:2.8.1-python3.11

# Cambiamos a usuario root para instalar dependencias del sistema
USER root

# Instalamos herramientas necesarias
# Por qué docker-ce-cli y NO docker.io:
#   docker.io (paquete Debian) queda en API v1.41, el daemon del host
#   exige mínimo v1.44+. docker-ce-cli del repo oficial de Docker
#   siempre viene actualizado y habla la misma API que el daemon.
RUN apt-get update && apt-get install -y \
        gcc g++ ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
       | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo \
       "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
        https://download.docker.com/linux/debian bookworm stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Agregar el usuario airflow al grupo docker
# Sin esto, el usuario airflow (no-root) no puede usar el socket de Docker
# El GID 999 es el GID estándar del grupo docker en la mayoría de sistemas Linux.
# Si enmbargo en el mio es 968: getent group docker | cut -d: -f3
#
RUN groupmod -g 968 docker 2>/dev/null || groupadd -g 968 docker && \
    usermod -aG docker airflow

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