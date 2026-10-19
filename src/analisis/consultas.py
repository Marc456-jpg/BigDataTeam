#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
consultas.py · Las 3 preguntas analíticas de la inscripción (apartado D),
resueltas con DataFrames de Spark sobre la zona curada.

  Q1 (agrupación + join): los 10 juegos con mayor porcentaje de reseñas
     positivas, entre los que tienen al menos 1.000 reseñas, con sus datos del
     catálogo de Steam (nombre, géneros, desarrolladores, precio).
     Géneros: la LISTA COMPLETA (decisión 49), nunca un «género principal».
  Q2 (agrupación, media y mediana): horas jugadas al escribir la reseña
     (author_playtime_at_review), comparando quien recomienda y quien no.
     Se da también la mediana porque la media la disparan unos pocos jugadores
     con miles de horas.
  Q3 (función de ventana): para cada idioma, los 5 juegos cuyas reseñas de 2020
     sumaron más votos de «útil» (votes_helpful), con DENSE_RANK.
     Solo se lee la carpeta year=2020 (poda de particiones). Se excluyen los
     votos «imposibles» (4.294.967.295 = 2^32-1, un contador roto del origen;
     ver el informe de calidad del ETL).

Cada resultado se guarda en CSV (resultados pequeños) en /resultados/q1..q3 y
se imprime. Idempotente: escribe en modo «overwrite».

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/analisis/consultas.py

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"
VOTO_IMPOSIBLE = 4294967295          # 2^32 - 1: valor de contador roto en el origen


def guardar(df, ruta):
    """Guarda un resultado pequeño como un único CSV con cabecera.

    escape='"': una comilla dentro de un texto se escribe "" (estándar de los
    CSV, el que entiende Excel) y no \" (lo que Spark hace por defecto).
    Ejemplo: Arsi "Hakita" Patala -> "Arsi ""Hakita"" Patala" (tarea 5.7)."""
    (df.coalesce(1).write.mode("overwrite")
     .option("header", True).option("escape", '"')
     .csv(ruta))


def main():
    parser = argparse.ArgumentParser(description="Preguntas analiticas Q1-Q3")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_q = cfg["spark"], cfg["consultas"]

    spark = (
        SparkSession.builder
        .appName("consultas_q1_q3")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )
    try:
        resenas = spark.read.parquet(ns + cfg["etl"]["salida"])
        juegos = spark.read.parquet(ns + cfg["catalogo"]["salida"]).select(
            "app_id", "nombre",
            F.array_join("generos", ", ").alias("generos"),          # lista completa (decisión 49)
            F.array_join("desarrolladores", ", ").alias("desarrolladores"),
            "precio_usd")
        res = ns + cfg["hdfs"]["resultados"]

        # ---------------- Q1: top juegos por % positivo + catálogo ----------
        t = time.perf_counter()
        por_juego = (resenas.groupBy("app_id").agg(
            F.count(F.lit(1)).alias("resenas"),
            F.sum(F.col("recommended").cast("int")).alias("positivas"))
            .filter(F.col("resenas") >= int(cfg_q["q1_min_resenas"]))
            .withColumn("pct_positivas", F.round(100.0 * F.col("positivas") / F.col("resenas"), 2)))
        q1 = (por_juego.join(juegos, "app_id", "left")       # left: no perder juegos sin catálogo
              .orderBy(F.desc("pct_positivas"), F.desc("resenas"))
              .limit(int(cfg_q["q1_top"]))
              .select("app_id", "nombre", "pct_positivas", "resenas", "generos",
                      "desarrolladores", "precio_usd"))
        guardar(q1, res + "/q1")
        print("[Q1] Top {} juegos por % de resenas positivas (min {} resenas) - {:.1f} s".format(
            cfg_q["q1_top"], cfg_q["q1_min_resenas"], time.perf_counter() - t))
        q1.show(truncate=40)

        # ---------------- Q2: horas al reseñar, recomienda o no -------------
        t = time.perf_counter()
        horas = F.col("author_playtime_at_review") / 60.0           # minutos -> horas
        q2 = (resenas.filter(F.col("author_playtime_at_review").isNotNull())
              .groupBy("recommended").agg(
                  F.count(F.lit(1)).alias("resenas"),
                  F.round(F.avg(horas), 1).alias("media_horas"),
                  F.round(F.percentile_approx(horas, 0.5, 10000), 1).alias("mediana_horas"))
              .orderBy("recommended"))
        guardar(q2, res + "/q2")
        print("[Q2] Horas jugadas al escribir la resena (sin nulos) - {:.1f} s".format(time.perf_counter() - t))
        q2.show()

        # ---------------- Q3: top 5 por idioma en 2020 (DENSE_RANK) ---------
        t = time.perf_counter()
        anio = int(cfg_q["q3_anio"])
        votos = (resenas.filter(F.col("year") == anio)                # poda: solo year=2020
                 .filter(F.col("votes_helpful") < VOTO_IMPOSIBLE)
                 .groupBy("language", "app_id")
                 .agg(F.sum("votes_helpful").alias("votos_utiles")))
        ventana = Window.partitionBy("language").orderBy(F.desc("votos_utiles"))
        q3 = (votos.withColumn("puesto", F.dense_rank().over(ventana))
              .filter(F.col("puesto") <= int(cfg_q["q3_top"]))
              .join(F.broadcast(juegos.select("app_id", "nombre")), "app_id", "left")
              .select("language", "puesto", "app_id", "nombre", "votos_utiles")
              .orderBy("language", "puesto", "app_id"))
        guardar(q3, res + "/q3")
        n_idiomas = q3.select("language").distinct().count()
        print("[Q3] Top {} juegos por idioma segun votos utiles, resenas de {} ({} idiomas) - {:.1f} s".format(
            cfg_q["q3_top"], anio, n_idiomas, time.perf_counter() - t))
        q3.filter(F.col("language").isin("english", "spanish", "schinese")).show(30, truncate=40)

        print("[OK] Resultados en {}/q1, /q2 y /q3".format(res))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
