#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verificar_curada.py · Comprueba, contando de verdad, que /curada/reviews está bien.

Por qué existe: el informe del ETL (etl_reviews.json) dice cuántas filas
DEBERÍA haber en la salida (filas_finales), pero ese número se calcula ANTES de
escribir. Este script vuelve a leer lo que de verdad quedó escrito en HDFS y
comprueba cuatro cosas (controles de calidad del apartado E del enunciado):

  1. Número de filas escritas  = filas_finales del informe del ETL.
  2. Número de review_id distintas = número de filas (no quedan duplicados).
  3. Ninguna fila sin las columnas clave (review_id, app_id, recommended, created_at).
  4. Cada carpeta year=AAAA solo contiene reseñas creadas ese año (UTC).

Además imprime cuántas reseñas hay por año, para el documento.

Solo LEE: no cambia nada en HDFS. Lee únicamente las columnas que necesita
(Parquet guarda cada columna por separado), así que es rápido (≈ 1-3 min) y
no carga la columna pesada «review».

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/procesamiento/verificar_curada.py

Devuelve 0 si todo cuadra y 1 si algo no cuadra.
Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Raíz del repositorio (este archivo está en <raíz>/src/procesamiento/).
RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"


def main():
    parser = argparse.ArgumentParser(description="Verifica la zona curada (/curada/reviews)")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_etl = cfg["spark"], cfg["etl"]

    # Misma configuración que el ETL (memoria, UTC), pero con su propio nombre
    # para distinguirla en la web de Spark (http://localhost:8080).
    spark = (
        SparkSession.builder
        .appName("verificar_curada")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )
    try:
        # Lo que dijo el informe del ETL (archivo JSON de varias líneas).
        informe = spark.read.option("multiLine", True).json(ns + cfg_etl["informe_calidad"])
        esperadas = informe.select("calidad.filas_finales").first()[0]

        # Solo las columnas necesarias: review_id, claves, fecha y la partición.
        df = spark.read.parquet(ns + cfg_etl["salida"]).select(
            "review_id", "app_id", "recommended", "created_at", "year")

        # Una sola pasada para filas, claves nulas y años mal colocados.
        r = df.agg(
            F.count(F.lit(1)).alias("filas"),
            F.sum((F.col("review_id").isNull() | F.col("app_id").isNull()
                   | F.col("recommended").isNull() | F.col("created_at").isNull())
                  .cast("int")).alias("sin_clave"),
            F.sum((F.year("created_at") != F.col("year")).cast("int")).alias("anio_mal"),
        ).first()
        distintas = df.select("review_id").distinct().count()
        por_anio = df.groupBy("year").count().orderBy("year").collect()

        print("[VERIFICAR] Filas escritas en {}: {}".format(cfg_etl["salida"], r["filas"]))
        print("[VERIFICAR] Filas segun el informe del ETL: {}".format(esperadas))
        print("[VERIFICAR] review_id distintas: {}".format(distintas))
        print("[VERIFICAR] Filas sin columna clave: {}".format(r["sin_clave"]))
        print("[VERIFICAR] Filas en una carpeta de ano equivocada: {}".format(r["anio_mal"]))
        print("[VERIFICAR] Resenas por ano:")
        for fila in por_anio:
            print("        year={}  {:>10}".format(fila["year"], fila["count"]))

        # Las cuatro comprobaciones: (descripción, ¿se cumple?)
        comprobaciones = [
            ("filas escritas = informe del ETL", r["filas"] == esperadas),
            ("sin duplicados (distintas = filas)", distintas == r["filas"]),
            ("ninguna fila sin columna clave", r["sin_clave"] == 0),
            ("cada fila en la carpeta de su ano", r["anio_mal"] == 0),
        ]
        for texto, ok in comprobaciones:
            print("[{}] {}".format("OK" if ok else "FALLO", texto))
        todo_ok = all(ok for _, ok in comprobaciones)
        print("[VERIFICAR] Resultado: {}".format("TODO CUADRA" if todo_ok else "HAY DIFERENCIAS"))
        return 0 if todo_ok else 1
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
