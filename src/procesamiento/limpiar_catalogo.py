#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
limpiar_catalogo.py · Catálogo de juegos: respuestas crudas de la API de Steam
(zona raw, JSON) → tabla limpia en Parquet (zona curada). Mini-paso 3.8.

Entrada: /raw/games/appdetails.jsonl (la crea src/ingesta/descargar_catalogo.py),
una línea por juego: {"app_id": ..., "descargado_utc": ..., "respuesta": {...}}.

Salida: /curada/games, una fila por juego, con estas columnas:
    app_id, en_tienda, nombre, tipo, es_gratis, precio_usd, generos,
    genero_principal, desarrolladores, editores, fecha_salida, descargado_utc

Como en el ETL de las reseñas, el esquema del JSON se declara EXPLÍCITAMENTE
(apartado D: «esquemas definidos explícitamente, no inferidos»): Spark solo
lee los campos que nos interesan y no adivina tipos. Un campo que no venga, o
que venga con otro tipo, queda vacío (null) en vez de romper la lectura.

Idempotente: escribe en modo «overwrite». Es una tabla pequeña (315 filas):
se guarda en un solo archivo y sin particiones (particionar 315 filas solo
crearía archivos diminutos).

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/procesamiento/limpiar_catalogo.py

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"

# Esquema explícito: solo los campos de la respuesta de Steam que usamos.
_NOMBRES = T.ArrayType(T.StringType())
ESQUEMA_CATALOGO = T.StructType([
    T.StructField("app_id", T.LongType()),
    T.StructField("descargado_utc", T.StringType()),
    T.StructField("respuesta", T.StructType([
        T.StructField("success", T.BooleanType()),            # ¿sigue en la tienda?
        T.StructField("data", T.StructType([
            T.StructField("type", T.StringType()),             # "game", "dlc"...
            T.StructField("name", T.StringType()),
            T.StructField("is_free", T.BooleanType()),
            T.StructField("developers", _NOMBRES),
            T.StructField("publishers", _NOMBRES),
            T.StructField("genres", T.ArrayType(T.StructType([
                T.StructField("description", T.StringType())]))),
            T.StructField("price_overview", T.StructType([
                T.StructField("currency", T.StringType()),
                T.StructField("final", T.LongType())])),       # precio en céntimos
            T.StructField("release_date", T.StructType([
                T.StructField("date", T.StringType())])),      # p. ej. "21 Dec, 2017"
        ])),
    ])),
])


def main():
    parser = argparse.ArgumentParser(description="Limpia el catalogo de juegos (raw JSON -> Parquet)")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_cat = cfg["spark"], cfg["catalogo"]

    spark = (
        SparkSession.builder
        .appName("limpiar_catalogo")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .getOrCreate()
    )
    try:
        crudo = spark.read.schema(ESQUEMA_CATALOGO).json(ns + cfg_cat["raw"])
        d = F.col("respuesta.data")
        fecha_texto = d["release_date"]["date"]
        # La fecha viene como texto en inglés, en dos formatos posibles.
        # try_to_timestamp devuelve null (en vez de error) si no encaja,
        # p. ej. "Coming soon".
        fecha = F.coalesce(F.try_to_timestamp(fecha_texto, F.lit("d MMM, yyyy")),
                           F.try_to_timestamp(fecha_texto, F.lit("MMM d, yyyy"))).cast("date")
        generos = F.transform(d["genres"], lambda g: g["description"])
        juegos = crudo.select(
            F.col("app_id").cast("int").alias("app_id"),
            F.coalesce(F.col("respuesta.success"), F.lit(False)).alias("en_tienda"),
            d["name"].alias("nombre"),
            d["type"].alias("tipo"),
            d["is_free"].alias("es_gratis"),
            # Precio de HOY en dólares (los gratuitos no traen precio): 0 si es gratis.
            F.when(d["is_free"], F.lit(0.0))
             .otherwise(d["price_overview"]["final"] / 100.0).alias("precio_usd"),
            generos.alias("generos"),
            generos[0].alias("genero_principal"),               # el primero que lista Steam
            d["developers"].alias("desarrolladores"),
            d["publishers"].alias("editores"),
            fecha.alias("fecha_salida"),
            F.col("descargado_utc"),
        ).dropDuplicates(["app_id"])

        juegos.coalesce(1).write.mode("overwrite").parquet(ns + cfg_cat["salida"])

        # Resumen (sobre lo escrito)
        escrito = spark.read.parquet(ns + cfg_cat["salida"])
        r = escrito.agg(
            F.count(F.lit(1)).alias("juegos"),
            F.sum(F.col("en_tienda").cast("int")).alias("en_tienda"),
            F.sum(F.col("nombre").isNull().cast("int")).alias("sin_nombre"),
            F.sum(F.col("fecha_salida").isNull().cast("int")).alias("sin_fecha"),
            F.sum(F.col("es_gratis").cast("int")).alias("gratis"),
        ).first()
        print("[CATALOGO] Juegos: {} | en la tienda: {} | sin nombre: {} | sin fecha: {} | gratis: {}"
              .format(r["juegos"], r["en_tienda"], r["sin_nombre"], r["sin_fecha"], r["gratis"]))
        print("[CATALOGO] Generos principales mas frecuentes:")
        for fila in (escrito.groupBy("genero_principal").count()
                     .orderBy(F.desc("count")).limit(5).collect()):
            print("        {:<25} {:>4}".format(str(fila["genero_principal"]), fila["count"]))
        print("[OK] Catalogo limpio en {}{}".format(ns, cfg_cat["salida"]))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
