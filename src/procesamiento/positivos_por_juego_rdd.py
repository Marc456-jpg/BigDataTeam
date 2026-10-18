#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
positivos_por_juego_rdd.py · El mismo cálculo que el job de MapReduce, pero con
la API de RDDs de Spark (apartado C del enunciado).

Calcula, para cada juego (app_id): cuántas reseñas tiene, cuántas lo
recomiendan y qué porcentaje supone. Lee LA MISMA ENTRADA que el MapReduce
(/curada/mapreduce_entrada, líneas "app_id<TAB>recommended"), para que la
comparación de tiempos y de código sea justa (decisión 47).

Las tres fases, igual que en MapReduce:
  - MAP:     cada línea → (app_id, (1, 1 si recomienda / 0 si no))
  - SHUFFLE: reduceByKey junta los pares del mismo app_id. Diferencia clave con
             nuestro MapReduce: Spark SUMA YA DENTRO DE CADA PARTICIÓN antes de
             enviar nada (lo que en Hadoop sería un «combiner»), así que por la
             red viaja como mucho un par por juego y partición, no 21,6 millones.
  - REDUCE:  suma (reseñas, positivas) de cada juego y calcula el porcentaje.

Al final compara su resultado, línea a línea, con el del MapReduce
(/resultados/mapreduce/positivos_por_juego) y dice si son idénticos.

Idempotente: borra su carpeta de salida antes de escribir.

Uso (desde PowerShell, en C:\\steam-bigdata; con YARN apagado para dejar memoria):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/procesamiento/positivos_por_juego_rdd.py

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"


def a_par(linea):
    """MAP: "730<TAB>true" → [("730", (1, 1))]. Lista vacía si la línea está mal
    (flatMap la descarta), igual que hace el mapper de Hadoop."""
    campos = linea.split("\t")
    if len(campos) != 2 or not campos[0].isdigit() or campos[1] not in ("true", "false"):
        return []
    return [(campos[0], (1, 1 if campos[1] == "true" else 0))]


def sumar(a, b):
    """REDUCE: suma dos parejas (reseñas, positivas)."""
    return (a[0] + b[0], a[1] + b[1])


def a_linea(par):
    """Formato de salida idéntico al del reducer de Hadoop."""
    app_id, (resenas, positivas) = par
    return "\t".join([app_id, str(resenas), str(positivas),
                      "{:.2f}".format(100.0 * positivas / resenas)])


def main():
    parser = argparse.ArgumentParser(description="Porcentaje de resenas positivas por juego con RDDs")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_mr = cfg["spark"], cfg["mapreduce"]

    spark = (
        SparkSession.builder
        .appName("positivos_por_juego_rdd")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .getOrCreate()
    )
    sc = spark.sparkContext
    try:
        # Borrar la salida anterior (idempotencia: saveAsTextFile no sobrescribe).
        Path_ = sc._jvm.org.apache.hadoop.fs.Path
        fs = Path_(ns + "/").getFileSystem(sc._jsc.hadoopConfiguration())
        salida = ns + cfg_mr["salida_rdd"]
        fs.delete(Path_(salida), True)

        inicio = time.perf_counter()
        # ---- El cálculo completo: 4 líneas de código ----------------------
        resultado = (
            sc.textFile(ns + cfg_mr["entrada"])          # 1 partición por archivo (8)
            .flatMap(a_par)                              # MAP
            .reduceByKey(sumar)                          # SHUFFLE + REDUCE (con combiner)
            .map(a_linea)                                # formato de salida
        )
        resultado.coalesce(1).saveAsTextFile(salida)     # 1 archivo, como el MapReduce
        segundos = time.perf_counter() - inicio
        print("[RDD] Calculo y escritura en {:.1f} s -> {}".format(segundos, salida))

        # ---- Comparación con el resultado de MapReduce -------------------
        mio = set(sc.textFile(salida).collect())
        de_mapreduce = set(sc.textFile(ns + cfg_mr["salida"]).collect())
        print("[RDD] Juegos: {} (RDD) / {} (MapReduce)".format(len(mio), len(de_mapreduce)))
        iguales = mio == de_mapreduce
        print("[RDD] Resultados identicos al MapReduce: {}".format("SI" if iguales else "NO"))
        return 0 if iguales else 1
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
