# =====================================================================
#  Imagen del NodeManager de YARN + Python 3 (mini-paso 1.4e)
#
#  QUE ES: una "receta" de Docker. Parte de la imagen del NodeManager que
#  usa el profesor y le anade Python 3. Docker Compose la "cocina" sola la
#  primera vez que se enciende el perfil "yarn" y la guarda con el nombre
#  steam-bigdata/hadoop-nodemanager-python:1.0 (ver docker-compose.yml).
#
#  POR QUE: Hadoop Streaming ejecuta nuestro mapper y nuestro reducer en
#  Python DENTRO del NodeManager, y la imagen original no trae Python
#  (comprobado el 17 oct 2026 con "which python python3": sin resultado).
#
#  OJO: el Python que se instala es el 3.5 (el de Debian 9). No admite
#  f-strings (f"..."), que llegaron en Python 3.6: el codigo MapReduce
#  debe escribirse compatible con 3.5.
# =====================================================================

# Punto de partida: la imagen original del NodeManager (Hadoop 3.2.1, Java 8,
# Debian 9). Se hereda todo: Hadoop, su configuracion, su arranque y su
# chequeo de salud.
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

# Instalar Python 3. Estos comandos se probaron antes en un contenedor de
# usar y tirar (17 oct 2026) y terminaron con "Python 3.5.3".
#   1) echo ... > sources.list : Debian 9 ("stretch") ya no tiene soporte y
#      sus paquetes se mudaron a archive.debian.org; se cambia la direccion
#      de la "tienda" de paquetes a ese archivo historico.
#   2) apt-get update : descargar el catalogo de paquetes. La opcion
#      Check-Valid-Until=false evita que se queje de que el catalogo es antiguo.
#   3) apt-get install python3 : instalar Python 3 (sin paquetes "recomendados"
#      extra, para que la imagen ocupe lo minimo).
#   4) rm -rf /var/lib/apt/lists/* : borrar el catalogo descargado (ya no hace
#      falta y asi la imagen ocupa menos).
#   5) python3 --version : comprobar que Python quedo instalado; si fallara,
#      la construccion de la imagen se detendria con error.
RUN echo 'deb http://archive.debian.org/debian stretch main' > /etc/apt/sources.list \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && apt-get install -y --no-install-recommends python3 \
 && rm -rf /var/lib/apt/lists/* \
 && python3 --version
