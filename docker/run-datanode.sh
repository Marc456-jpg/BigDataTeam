#!/bin/bash
# =====================================================================
#  Arranque de los DataNodes (mini-paso 1.4d)
#
#  QUE ES: el script que ejecutan los contenedores "datanode1" y "datanode2"
#  al encenderse (los dos usan este mismo archivo).
#
#  DE DONDE SALE: copia EXACTA del /run.sh de la imagen
#  bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8, con UNA sola diferencia:
#  "exec" delante de la ultima linea, para que Java sea el proceso 1 y se
#  apague ordenadamente (explicacion completa en docker/run-namenode.sh).
# =====================================================================

# 1) Averiguar en que carpeta guarda este DataNode los bloques de datos.
#    La imagen define HDFS_CONF_dfs_datanode_data_dir = "file:///hadoop/dfs/data";
#    "perl" quita el "file://" y deja la ruta /hadoop/dfs/data (nuestro volumen).
datadir=`echo $HDFS_CONF_dfs_datanode_data_dir | perl -pe 's#file://##'`

# 2) Si la carpeta de datos no existe, salir con error (codigo 2).
if [ ! -d $datadir ]; then
  echo "Datanode data directory not found: $datadir"
  exit 2
fi

# 3) Arrancar el DataNode. "exec" = el script se CONVIERTE en el DataNode.
exec $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR datanode
