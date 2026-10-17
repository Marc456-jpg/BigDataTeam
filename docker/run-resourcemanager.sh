#!/bin/bash
# =====================================================================
#  Arranque del ResourceManager de YARN (mini-paso 1.4e)
#
#  QUE ES: el script que ejecuta el contenedor "resourcemanager" (perfil
#  "yarn"). El ResourceManager es el "jefe de obra" de YARN: decide cuanta
#  memoria y CPU recibe cada tarea de MapReduce y en que NodeManager corre.
#
#  DE DONDE SALE: copia EXACTA del /run.sh de la imagen
#  bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8 (que es una sola
#  linea), con "exec" delante para que Java sea el proceso 1 y se apague
#  ordenadamente (explicacion completa en docker/run-namenode.sh).
# =====================================================================

# Arrancar el ResourceManager:
#   $HADOOP_HOME   -> carpeta de Hadoop dentro del contenedor (/opt/hadoop-3.2.1)
#   --config ...   -> carpeta con los *-site.xml generados desde hadoop.env
#   exec           -> el script se CONVIERTE en el ResourceManager
exec $HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR resourcemanager
