#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
optimizar_q1.py · Optimización de una consulta, medida antes y después (apartado D).

El enunciado pide, para una de las preguntas: mostrar el plan de ejecución
(explain), identificar las etapas y los shuffles en la Spark UI, aplicar una
optimización y medir antes y después. Usamos Q1 (top juegos por % positivo con
el catálogo), escrita de tres formas que dan EL MISMO resultado:

  V1 «ingenua»: primero se juntan las 21,6 M de reseñas con el catálogo
     (join) y después se agrupa. El join se hace con SORT-MERGE JOIN (Spark
     reparte las 21,6 M de filas por app_id por la red = shuffle grande).
  V2 «broadcast»: igual que V1, pero el catálogo (315 filas) se copia entero a
     cada executor (BROADCAST JOIN): las reseñas no se mueven para el join.
  V3 «agregar antes» (la de consultas.py): primero se agrupa por juego
     (21,6 M de filas → 315) y después se hace el join con el catálogo (broadcast).

Para que V1 sea de verdad un sort-merge join hay que desactivar el broadcast
automático (Spark 4 lo hace solo cuando una tabla es pequeña, también durante
la ejecución gracias a AQE): spark.sql.autoBroadcastJoinThreshold = -1 y
spark.sql.adaptive.autoBroadcastJoinThreshold = -1, solo para V1.

Para cada versión: se ejecuta 2 veces y se guarda el mejor tiempo; se guarda
el plan final (explain «formatted») en evidence/fase5/ del proyecto, y se
cuenta cuántos shuffles (Exchange) y qué tipo de join tiene.
Al final se comprueba que las tres dan el mismo top 10.

--pausa N: espera N segundos al terminar, con Spark aún encendido, para poder
abrir la Spark UI (http://localhost:4040) y hacer capturas de Jobs/Stages/SQL.

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/analisis/optimizar_q1.py --pausa 180

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import re
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"
CARPETA_PLANES = RAIZ / "evidence" / "fase5"

# En el plan «formatted», cada shuffle es un nodo «Exchange (n)»; el broadcast
# es «BroadcastExchange (n)» (no es un shuffle: no reparte filas por la red).
PATRON_SHUFFLE = re.compile(r"(?<!Broadcast)Exchange \(\d+\)")


def arbol_final(plan):
    """Del texto del plan, solo el árbol del plan FINAL (el que se ejecutó de
    verdad tras las decisiones de AQE), sin el «Initial Plan» ni los detalles."""
    arbol = plan.split("\n\n")[0]                       # el árbol va antes de los detalles
    return arbol.split("== Initial Plan ==")[0]


def plan_final(df):
    """Plan físico final (tras ejecutar, con AQE ya resuelto) en modo «formatted»."""
    return df.sparkSession.sparkContext._jvm.PythonSQLUtils.explainString(
        df._jdf.queryExecution(), "formatted")


def main():
    parser = argparse.ArgumentParser(description="Optimizacion de Q1 medida antes y despues")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    parser.add_argument("--pausa", type=int, default=0, help="segundos de espera al final (Spark UI)")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_q = cfg["spark"], cfg["consultas"]
    minimo, top = int(cfg_q["q1_min_resenas"]), int(cfg_q["q1_top"])

    spark = (
        SparkSession.builder
        .appName("optimizar_q1")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )
    try:
        def leer():
            resenas = spark.read.parquet(ns + cfg["etl"]["salida"]).select("app_id", "recommended")
            juegos = spark.read.parquet(ns + cfg["catalogo"]["salida"]).select("app_id", "nombre")
            return resenas, juegos

        def metricas(df):
            """Columnas finales comunes a las tres versiones (mismo resultado)."""
            return (df.withColumn("pct_positivas",
                                  F.round(100.0 * F.col("positivas") / F.col("resenas"), 2))
                    .filter(F.col("resenas") >= minimo)
                    .orderBy(F.desc("pct_positivas"), F.desc("resenas"), "app_id")
                    .limit(top)
                    .select("app_id", "nombre", "pct_positivas", "resenas"))

        agregados = [F.count(F.lit(1)).alias("resenas"),
                     F.sum(F.col("recommended").cast("int")).alias("positivas")]

        def v1():
            r, j = leer()
            return metricas(r.join(j, "app_id").groupBy("app_id", "nombre").agg(*agregados))

        def v2():
            r, j = leer()
            return metricas(r.join(F.broadcast(j), "app_id").groupBy("app_id", "nombre").agg(*agregados))

        def v3():
            r, j = leer()
            return metricas(r.groupBy("app_id").agg(*agregados).join(F.broadcast(j), "app_id"))

        versiones = [
            ("V1_join_primero_sortmerge", v1, True),
            ("V2_join_primero_broadcast", v2, False),
            ("V3_agregar_antes_broadcast", v3, False),
        ]
        normal = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

        # Calentamiento: una lectura completa para que la primera versión no
        # pague sola el coste de abrir los archivos por primera vez.
        spark.read.parquet(ns + cfg["etl"]["salida"]).select("app_id").count()

        CARPETA_PLANES.mkdir(parents=True, exist_ok=True)
        resultados, filas_por_version = [], {}
        for nombre, construir, sin_broadcast in versiones:
            valor = "-1" if sin_broadcast else normal
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", valor)
            spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", valor)
            tiempos = []
            for _ in range(2):                                  # 2 veces: mejor tiempo
                spark.sparkContext.setJobDescription(nombre)   # nombre visible en la Spark UI
                df = construir()
                t = time.perf_counter()
                filas = df.collect()
                tiempos.append(time.perf_counter() - t)
            plan = plan_final(df)
            (CARPETA_PLANES / "5.2_plan_{}.txt".format(nombre)).write_text(plan, encoding="utf-8")
            final = arbol_final(plan)
            shuffles = len(PATRON_SHUFFLE.findall(final))
            join = ("SortMergeJoin" if "SortMergeJoin" in final else
                    "BroadcastHashJoin" if "BroadcastHashJoin" in final else "otro")
            resultados.append((nombre, min(tiempos), join, shuffles))
            filas_por_version[nombre] = [tuple(f) for f in filas]
            print("[OPT] {:<28} {:>6.1f} s (mejor de 2: {})  join: {:<17} shuffles: {}".format(
                nombre, min(tiempos), ", ".join("{:.1f}".format(x) for x in tiempos), join, shuffles))

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", normal)
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", normal)

        iguales = len(set(map(tuple, filas_por_version.values()))) == 1
        print("[OPT] Las 3 versiones dan el mismo top {}: {}".format(top, "SI" if iguales else "NO"))
        base = resultados[0][1]
        for nombre, segundos, _, _ in resultados[1:]:
            print("[OPT] {} es {:.1f} veces mas rapida que V1".format(nombre.split("_")[0], base / segundos))
        print("[OPT] Planes guardados en evidence/fase5/5.2_plan_*.txt")

        if args.pausa > 0:
            print("[OPT] Spark sigue encendido {} s: abre http://localhost:4040 (pestanas SQL y Stages)".format(args.pausa))
            time.sleep(args.pausa)
        print("[OK] Optimizacion de Q1 medida")
        return 0 if iguales else 1
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
