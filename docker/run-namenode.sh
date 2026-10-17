#!/bin/bash
# =====================================================================
#  Arranque del NameNode (mini-paso 1.4d)
#
#  QUE ES: el script que ejecuta el contenedor "namenode" al encenderse.
#  Lo lanza el docker-compose.yml con:
#      command: ["/bin/bash", "/opt/steam/run-namenode.sh"]
#
#  DE DONDE SALE: es una copia EXACTA del /run.sh que trae la imagen
#  bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8, con UNA sola diferencia:
#  la palabra "exec" delante de la ultima linea.
#
#  POR QUE "exec": sin "exec", bash es el proceso 1 del contenedor y Java
#  (el NameNode) es su hijo. Docker solo avisa ("SIGTERM": apagate) al
#  proceso 1; bash no le pasa el aviso a Java, y Java acaba cortado a la
#  fuerza ("SIGKILL", codigo de salida 137). Con "exec", Java SUSTITUYE a
#  bash y pasa a ser el proceso 1: recibe el aviso, se apaga ordenadamente
#  y el contenedor sale con codigo 143. (Comprobado el 17 oct 2026.)
#
#  Nota: los comentarios van sin tildes para evitar problemas de
#  codificacion de caracteres dentro del contenedor Linux.
# =====================================================================

# 1) Averiguar en que carpeta guarda el NameNode sus metadatos (el "indice"
#    de HDFS). La variable HDFS_CONF_dfs_namenode_name_dir la define la imagen
#    con el formato "file:///hadoop/dfs/name"; "perl" le quita el "file://"
#    y deja solo la ruta: /hadoop/dfs/name (que es nuestro volumen).
namedir=`echo $HDFS_CONF_dfs_namenode_name_dir | perl -pe 's#file://##'`

# 2) Si esa carpeta no existe, algo esta mal montado: avisar y salir con
#    error (codigo 2) en vez de arrancar un NameNode sin donde guardar nada.
if [ ! -d $namedir ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

# 3) El nombre del cluster (CLUSTER_NAME, en nuestro compose: "steam") es
#    obligatorio, porque se usa al formatear. Si falta, salir con error.
if [ -z "$CLUSTER_NAME" ]; then
  echo "Cluster name not specified"
  exit 2
fi

# 4) SOLO LA PRIMERA VEZ: si la carpeta de metadatos esta vacia (volumen
#    nuevo), "formatear" el NameNode, es decir, crear un HDFS vacio con su
#    Cluster ID y su Block Pool ID. En los arranques siguientes la carpeta
#    ya tiene datos y este paso se salta (por eso HDFS conserva su memoria).
if [ "`ls -A $namedir`" == "" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format $CLUSTER_NAME
fi

# 5) Arrancar el NameNode. "exec" = este script se CONVIERTE en el NameNode
#    (no lo lanza como hijo). Es la unica linea distinta del original.
exec $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode
