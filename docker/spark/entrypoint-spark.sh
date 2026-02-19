#!/bin/bash
# Entrypoint para el contenedor Spark.
#
# Problema: los bind mounts (./data:/opt/data) heredan el UID del host (1000).
# El user "spark" (UID 185) no puede escribir en ellos.
#
# Solución: este script corre como root (antes del USER spark del Dockerfile),
# ajusta permisos de las carpetas de datos, y luego ejecuta el comando
# como el user "spark" via gosu/su-exec/exec.

# Asegurar que spark pueda escribir en los directorios de datos
chown -R spark:spark /opt/data 2>/dev/null || true
chown -R spark:spark /opt/logs 2>/dev/null || true

# Ejecutar el comando original como user 
#super importante la cagada esta, sino se me puede quedar colgado spark por mas que mande un docker=compose  down
#entrega el mismo proceso en el que se corre/recibe senales docker para que se ejecute spark
exec gosu spark "$@"

#chown -R spark:spark /opt/data: Root entra a la carpeta y cambia el dueño a "spark" a la fuerza.
#>/dev/null || true:Si la carpeta no existe o está montada como "solo lectura", el comando fallaría y detendría el contenedor.
# Con esto sigue de paso
