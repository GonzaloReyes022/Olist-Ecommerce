FROM apache/spark:3.5.1-python3

USER root

# Instalar pip
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Descargar el JAR de Snowflake Spark Connector
RUN curl -L https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.5/spark-snowflake_2.12-2.12.0-spark_3.5.jar \
    -o $SPARK_HOME/jars/spark-snowflake_2.12-2.12.0-spark_3.5.jar

# Descargar dependencias adicionales de Snowflake
RUN curl -L https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.4/snowflake-jdbc-3.14.4.jar \
    -o $SPARK_HOME/jars/snowflake-jdbc-3.14.4.jar

# Descargar JARs de Delta Lake (delta-spark 3.1.0 para Spark 3.5.x)
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    -o $SPARK_HOME/jars/delta-spark_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
    -o $SPARK_HOME/jars/delta-storage-3.1.0.jar

# Copiar e instalar requirements Python
COPY requirements.txt /opt/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/requirements.txt

# Directorio de trabajo y logs
RUN mkdir -p /opt/spark_jobs /opt/logs && chown spark:spark /opt/logs
WORKDIR /opt/spark_jobs

USER spark

# Configurar PYTHONPATH: /opt para que Python resuelva "from spark_jobs.xxx"
ENV PYTHONPATH="/opt:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"