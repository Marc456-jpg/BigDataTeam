#!/bin/bash
# =====================================================================
#  Arranque del NodeManager de YARN (mini-paso 1.4e)
#
#  QUE ES: el script que ejecuta el contenedor "nodemanager" (perfil "yarn").
#  El NodeManager es el "obrero" de YARN: ejecuta las tareas map y reduce
#  que le asigna el ResourceManager (en Hadoop Streaming, lanza nuestros
#  scripts de Python).
#
#  DE DONDE SALE: copia EXACTA del /run.sh de la imagen
#  bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8 (una sola linea), con
#  "exec" delante para que Java sea el proceso 1 y se apague ordenadamente
#  (explicacion completa en docker/run-namenode.sh).
# =====================================================================

# Arrancar el NodeManager ("exec" = el script se CONVIERTE en el NodeManager).
exec $HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR nodemanager
