#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
exportar_entrada_mapreduce.py · Prepara la entrada del job de MapReduce (apartado C).

Por qué existe: Hadoop Streaming lee los archivos LÍNEA A LÍNEA y cada línea es
un registro. El CSV original NO sirve tal cual, porque muchas reseñas tienen
saltos de línea dentro del texto (por eso el ETL usa multiLine): Hadoop
partiría esas reseñas en varios trozos. Y el Parquet de la zona curada es un
formato binario que Hadoop Streaming no sabe leer.

Solución: desde la zona curada (datos ya limpios y sin duplicados) se escribe
un archivo de texto con SOLO las dos columnas que necesita el cálculo:

    app_id<TAB>recommended          p. ej. "730<TAB>true"

Una línea por reseña (21,6 millones), repartidas en varios archivos (uno por
tarea map). La misma entrada la usará la versión con RDDs de Spark, para que la
comparación entre MapReduce y Spark sea justa (mismos datos, mismo formato).

Idempotente: escribe en modo «overwrite», así que repetirlo no duplica nada.

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/procesamiento/exportar_entrada_mapreduce.py

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"


def main():
    parser = argparse.ArgumentParser(description="Exporta la entrada de texto del MapReduce")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_mr = cfg["spark"], cfg["mapreduce"]
    origen = ns + cfg["etl"]["salida"]
    destino = ns + cfg_mr["entrada"]

    spark = (
        SparkSession.builder
        .appName("exportar_entrada_mapreduce")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )
    try:
        inicio = time.perf_counter()
        print("[EXPORTAR] {} -> {}".format(origen, destino))
        # Solo 2 columnas: Parquet lee únicamente esas (no carga el texto de
        # las reseñas). concat_ws las une con un tabulador; el booleano se
        # escribe como "true"/"false".
        lineas = (
            spark.read.parquet(origen)
            .select(F.concat_ws("\t", F.col("app_id").cast("string"),
                                F.col("recommended").cast("string")).alias("value"))
        )
        (
            lineas.repartition(int(cfg_mr["archivos_entrada"]))   # 1 archivo por tarea map
            .write.mode("overwrite")                              # idempotente
            .text(destino)
        )
        # Comprobación: cuántas líneas quedaron escritas (debe ser = filas curadas).
        n = spark.read.text(destino).count()
        print("[EXPORTAR] Lineas escritas: {}".format(n))
        print("[OK] Entrada del MapReduce lista en {:.1f} s".format(time.perf_counter() - inicio))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
