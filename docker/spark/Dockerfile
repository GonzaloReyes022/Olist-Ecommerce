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

# Copiar e instalar requirements Python
COPY requirements.txt /opt/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/requirements.txt

# Directorio de trabajo
RUN mkdir -p /opt/spark-jobs
WORKDIR /opt/spark-jobs

USER spark

# Configurar PYTHONPATH para que encuentre PySpark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"