#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
exportar_resultados.py · Copia los resultados pequeños de HDFS (/resultados) a
la carpeta evidence/resultados del repositorio, para entregarlos en GitHub.

Por qué existe: las respuestas a las preguntas Q1-Q3, el MapReduce, el informe
de calidad, las comparativas (CSV/Parquet y MapReduce/RDD), las métricas del ML
y las filas en cuarentena (con su motivo) se guardan en HDFS,
que vive dentro de Docker y no se sube a GitHub. El enunciado pide una carpeta
de evidencias con métricas y resultados. Este script deja una copia legible
(CSV, TSV o JSON) en el repositorio.

Qué se copia y adónde lo dice config/pipeline.yaml (sección «exportar»).
Solo copia resultados pequeños (KB), nunca datos de reseñas: un archivo que
pase del tamaño máximo de la configuración no se copia (avisa). Reemplaza los
archivos cada vez (se puede repetir sin problema). Si un resultado todavía no
existe en HDFS, avisa y sigue con los demás.

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/analisis/exportar_resultados.py

Devuelve 0 si ha copiado todos y 1 si falta alguno.
Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"


def lista_de_resultados(cfg):
    """(ruta en HDFS, nombre del archivo local, ¿es CSV con cabecera?), tal como
    los enumera config/pipeline.yaml (exportar.archivos)."""
    return [(a["hdfs"], a["local"], bool(a["csv"])) for a in cfg["exportar"]["archivos"]]


def main():
    parser = argparse.ArgumentParser(description="Copia /resultados de HDFS a evidence/resultados")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    destino = RAIZ / cfg["exportar"]["carpeta_local"]
    destino.mkdir(parents=True, exist_ok=True)
    max_bytes = int(cfg["exportar"]["max_kb_por_archivo"]) * 1024

    # Spark en modo local, solo para usar la API de ficheros de Hadoop.
    spark = SparkSession.builder.appName("exportar_resultados").master("local[1]").getOrCreate()
    try:
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        Path_ = jvm.org.apache.hadoop.fs.Path
        fs = Path_(ns + "/").getFileSystem(conf)
        IOUtils = jvm.org.apache.commons.io.IOUtils

        def leer(p):
            """Contenido (bytes) de un archivo de HDFS."""
            entrada = fs.open(p)
            try:
                return bytes(IOUtils.toByteArray(entrada))
            finally:
                entrada.close()

        faltan = 0
        for ruta, nombre, con_cabecera in lista_de_resultados(cfg):
            p = Path_(ns + ruta)
            if not fs.exists(p):
                print("[AVISO] No existe {} (todavia no se ha generado)".format(ruta))
                faltan += 1
                continue
            if fs.getFileStatus(p).isDirectory():
                # Carpeta de Spark o Hadoop: se juntan sus trozos part-* en orden.
                # Si son CSV con cabecera, la cabecera se deja solo una vez.
                trozos = sorted(s.getPath().toString() for s in fs.listStatus(p)
                                if s.getPath().getName().startswith("part-"))
                contenido = b""
                for i, t in enumerate(trozos):
                    datos = leer(Path_(t))
                    if con_cabecera and i > 0 and b"\n" in datos:
                        datos = datos.split(b"\n", 1)[1]
                    contenido += datos
            else:
                trozos = [ruta]
                contenido = leer(p)
            if len(contenido) > max_bytes:
                print("[AVISO] {} ocupa {} bytes (maximo {}): no se copia".format(
                    ruta, len(contenido), max_bytes))
                faltan += 1
                continue
            (destino / nombre).write_bytes(contenido)
            lineas = contenido.count(b"\n")
            print("[EXPORTAR] {:<45} -> {} ({} bytes, {} lineas, {} trozo(s))".format(
                ruta, nombre, len(contenido), lineas, len(trozos)))

        carpeta = cfg["exportar"]["carpeta_local"]
        if faltan:
            print("[AVISO] Faltan {} resultado(s); el resto esta en {}".format(faltan, carpeta))
            return 1
        print("[OK] Resultados copiados en {}".format(carpeta))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
