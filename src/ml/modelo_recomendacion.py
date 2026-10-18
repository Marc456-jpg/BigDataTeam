#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modelo_recomendacion.py · Modelo de machine learning del proyecto (apartado E):
¿la reseña recomienda el juego? (columna «recommended», sí/no).

Qué hace (decisión 50 del documento explicativo):
  1. Lee de la zona curada SOLO las columnas que necesita (Parquet guarda cada
     columna por separado, así que el texto de las reseñas no se lee).
  2. Opcional: se queda con una muestra al azar (--fraccion 0.1 = el 10 %),
     para medir cuánto tarda antes de lanzarlo con todo.
  3. Construye las variables, todas conocidas EN EL MOMENTO de escribir la
     reseña (si no, el modelo «haría trampa»: fuga de datos):
       - de la reseña: horas jugadas al reseñar (log), longitud del texto (log),
         si faltan las horas, si lo compró en Steam, si lo recibió gratis, si se
         escribió en acceso anticipado, e idioma (una columna sí/no por cada uno
         de los idiomas más frecuentes; el resto queda como «otros»);
       - del catálogo (/curada/games): una columna sí/no por cada género
         presente en al menos el 10 % de los juegos. Nunca un «género
         principal» (decisión 49).
     NO usa el precio ni «es gratis» del catálogo (decisión 51): son los de HOY
     (la API de Steam da los datos actuales), no los de la fecha de la reseña.
     Los géneros sí, porque apenas cambian con el tiempo.
     NO usa playtime_forever, votos, weighted_vote_score, comment_count,
     horas de las 2 últimas semanas, última vez que jugó, ni el nº de juegos o
     de reseñas del autor: se midieron al descargar el dataset, después de la
     reseña.
  4. Separa al azar 80 % para entrenar y 20 % para evaluar (semilla fija:
     repetir el script da la misma separación).
  5. Entrena: VectorAssembler -> StandardScaler -> LogisticRegression.
     El StandardScaler pone cada variable en «desviaciones típicas», así los
     coeficientes se pueden comparar entre sí (cuál pesa más).
  6. Evalúa con AUC: un modelo que dijera siempre «sí» acertaría el 87,4 % sin
     aprender nada; el AUC no se deja engañar así (0,5 = azar, 1 = perfecto).

Escribe (modo «overwrite», se puede repetir):
  - /resultados/ml/regresion_logistica.json : métricas, coeficientes y tiempos;
  - /resultados/ml/modelo_regresion_logistica : el modelo entrenado.

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/ml/modelo_recomendacion.py --fraccion 0.1

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

import yaml
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"

# Variables de la reseña que se calculan siempre (nombre -> descripción).
VARIABLES_RESENA = {
    "log_horas_al_resenar": "log(1 + horas jugadas al escribir la resena)",
    "sin_horas": "1 si no consta cuantas horas llevaba jugadas",
    "log_longitud_texto": "log(1 + caracteres de la resena)",
    "compra_steam": "1 si compro el juego en Steam",
    "recibido_gratis": "1 si recibio el juego gratis",
    "acceso_anticipado": "1 si la resena se escribio en acceso anticipado",
}


def nombre_columna(texto):
    """'Massively Multiplayer' -> 'massively_multiplayer' (nombre válido de columna)."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", texto).strip("_").lower()


def a_numero(columna):
    """Sí/no (boolean) -> 1.0 / 0.0; si falta el dato, 0.0."""
    return F.coalesce(F.col(columna).cast("double"), F.lit(0.0))


def main():
    parser = argparse.ArgumentParser(description="ML: LogisticRegression sobre recommended")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    parser.add_argument("--fraccion", type=float, default=None,
                        help="parte de las resenas a usar (0.1 = 10 %%); por defecto, la de la configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_ml = cfg["spark"], cfg["ml"]
    fraccion = float(args.fraccion if args.fraccion is not None else cfg_ml["fraccion"])
    if not 0 < fraccion <= 1:
        print("[ERROR] --fraccion tiene que estar entre 0 (sin incluir) y 1")
        return 1
    semilla = int(cfg_ml["semilla"])
    tiempos = {}

    spark = (
        SparkSession.builder
        .appName("ml_regresion_logistica")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )
    try:
        inicio_total = time.perf_counter()

        # ---------------- 1) Catálogo: géneros frecuentes y variables -------
        t = time.perf_counter()
        juegos = spark.read.parquet(ns + cfg["catalogo"]["salida"])
        n_juegos = juegos.count()
        conteo = (juegos.select(F.explode("generos").alias("genero"))
                  .groupBy("genero").count().collect())          # pocas filas: al driver
        minimo = float(cfg_ml["min_pct_juegos_genero"])
        frecuentes = [(r["genero"], r["count"])
                      for r in sorted(conteo, key=lambda r: (-r["count"], r["genero"]))
                      if 100.0 * r["count"] / n_juegos >= minimo]
        generos = [g for g, _ in frecuentes]
        print("[ML] Catalogo: {} juegos; generos en al menos el {:g} % de ellos: {}".format(
            n_juegos, minimo, ", ".join("{} ({})".format(g, c) for g, c in frecuentes)))

        lista_generos = F.coalesce(F.col("generos"), F.array().cast("array<string>"))
        columnas_genero = {"genero_" + nombre_columna(g): g for g in generos}
        vars_juego = (juegos.select(
            "app_id",
            *[F.array_contains(lista_generos, g).cast("double").alias(c)
              for c, g in columnas_genero.items()]))

        # ---------------- 2) Reseñas: solo las columnas necesarias ----------
        resenas = (spark.read.parquet(ns + cfg["etl"]["salida"])
                   .select("app_id", "recommended", "author_playtime_at_review", "review_length",
                           "language", "steam_purchase", "received_for_free",
                           "written_during_early_access")
                   .filter(F.col("recommended").isNotNull()))
        if fraccion < 1:
            resenas = resenas.sample(fraction=fraccion, seed=semilla)

        horas = F.col("author_playtime_at_review") / 60.0               # minutos -> horas
        datos = (resenas.select(
            F.col("recommended").cast("double").alias("label"),
            "app_id",
            F.coalesce(F.col("language"), F.lit("desconocido")).alias("idioma"),
            F.log1p(F.greatest(F.coalesce(horas, F.lit(0.0)), F.lit(0.0))).alias("log_horas_al_resenar"),
            F.col("author_playtime_at_review").isNull().cast("double").alias("sin_horas"),
            F.log1p(F.coalesce(F.col("review_length"), F.lit(0)).cast("double")).alias("log_longitud_texto"),
            a_numero("steam_purchase").alias("compra_steam"),
            a_numero("received_for_free").alias("recibido_gratis"),
            a_numero("written_during_early_access").alias("acceso_anticipado"))
            # El catálogo es pequeño (≈ 300 filas): se copia entero a cada executor.
            .join(F.broadcast(vars_juego), "app_id", "left")
            .fillna(0.0, subset=list(columnas_genero)))

        # ---------------- 3) Separar entrenamiento / prueba -----------------
        p_test = float(cfg_ml["proporcion_test"])
        entrenamiento, prueba = datos.randomSplit([1.0 - p_test, p_test], seed=semilla)

        # Idiomas más frecuentes, contados SOLO en entrenamiento (la prueba no
        # debe influir en nada de lo que aprende el modelo).
        # De paso, la suma de todos los idiomas da las filas de entrenamiento.
        por_idioma = sorted(((r["idioma"], r["count"]) for r in
                             entrenamiento.groupBy("idioma").count().collect()),
                            key=lambda x: (-x[1], x[0]))
        filas_entrenamiento = sum(n for _, n in por_idioma)
        top_idiomas = [i for i, _ in por_idioma[:int(cfg_ml["idiomas_top"])]]
        columnas_idioma = {"idioma_" + nombre_columna(i): i for i in top_idiomas}

        def con_idiomas(df):
            return df.select("*", *[(F.col("idioma") == i).cast("double").alias(c)
                                    for c, i in columnas_idioma.items()])

        entrenamiento, prueba = con_idiomas(entrenamiento), con_idiomas(prueba)
        variables = list(VARIABLES_RESENA) + list(columnas_genero) + list(columnas_idioma)
        tiempos["preparar_s"] = round(time.perf_counter() - t, 1)
        print("[ML] Fraccion de resenas usada: {:g} (semilla {}); {} variables".format(
            fraccion, semilla, len(variables)))
        print("[ML] Idiomas con columna propia (el resto = otros): {}".format(", ".join(top_idiomas)))

        # ---------------- 4) Entrenar --------------------------------------
        t = time.perf_counter()
        modelo = Pipeline(stages=[
            VectorAssembler(inputCols=variables, outputCol="variables_crudas"),
            StandardScaler(inputCol="variables_crudas", outputCol="features",
                           withMean=False, withStd=True),
            LogisticRegression(featuresCol="features", labelCol="label",
                               maxIter=int(cfg_ml["iteraciones_max"])),
        ]).fit(entrenamiento)
        tiempos["entrenar_s"] = round(time.perf_counter() - t, 1)
        regresion = modelo.stages[-1]
        iteraciones = regresion.summary.totalIterations
        print("[ML] Entrenado en {} s ({} iteraciones)".format(tiempos["entrenar_s"], iteraciones))

        # ---------------- 5) Evaluar con los datos de prueba ----------------
        t = time.perf_counter()
        predicciones = (modelo.transform(prueba)
                        .select("label", "rawPrediction", "prediction").persist())
        evaluador = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
        auc_roc = evaluador.setMetricName("areaUnderROC").evaluate(predicciones)
        auc_pr = evaluador.setMetricName("areaUnderPR").evaluate(predicciones)
        r = predicciones.agg(
            F.count(F.lit(1)).alias("filas"),
            F.sum("label").alias("positivas"),
            F.sum((F.col("prediction") == F.col("label")).cast("int")).alias("aciertos"),
            F.sum((F.col("prediction") == 0).cast("int")).alias("predice_no")).first()
        predicciones.unpersist()
        tiempos["evaluar_s"] = round(time.perf_counter() - t, 1)

        acierto = r["aciertos"] / r["filas"]
        siempre_si = r["positivas"] / r["filas"]

        # Coeficientes (en desviaciones típicas: comparables entre sí).
        coeficientes = sorted(zip(variables, regresion.coefficients.toArray().tolist()),
                              key=lambda x: -abs(x[1]))
        tiempos["total_s"] = round(time.perf_counter() - inicio_total, 1)

        print("[ML] Filas: entrenamiento {}, prueba {}".format(filas_entrenamiento, r["filas"]))
        print("[ML] AUC ROC (prueba): {:.4f}   (0,5 = azar; 1 = perfecto)".format(auc_roc))
        print("[ML] AUC PR  (prueba): {:.4f}".format(auc_pr))
        print("[ML] Acierto con umbral 0,5: {:.2f} %  frente a decir siempre si: {:.2f} %".format(
            100 * acierto, 100 * siempre_si))
        print("[ML] Resenas de prueba que el modelo predice como NO: {}".format(r["predice_no"]))
        print("[ML] Coeficientes (positivo = mas probable que recomiende), de mayor a menor peso:")
        for nombre, valor in coeficientes:
            print("        {:<34} {:+.4f}".format(nombre, valor))
        print("        {:<34} {:+.4f}".format("(intercepto)", regresion.intercept))
        print("[ML] Tiempos: {}".format(tiempos))
        if fraccion < 1:
            print("[ML] Estimacion orientativa con todas las resenas: <= {:.1f} min "
                  "(tiempo total / fraccion; leer el Parquet cuesta lo mismo con cualquier fraccion)".format(
                      tiempos["total_s"] / fraccion / 60))

        # ---------------- 6) Guardar informe y modelo -----------------------
        informe = {
            "modelo": "LogisticRegression (VectorAssembler + StandardScaler)",
            "objetivo": "recommended",
            "fraccion": fraccion, "semilla": semilla, "proporcion_test": p_test,
            "filas": {"entrenamiento": filas_entrenamiento, "prueba": r["filas"]},
            "metricas_prueba": {"auc_roc": round(auc_roc, 4), "auc_pr": round(auc_pr, 4),
                                "acierto_umbral_0_5": round(acierto, 4),
                                "acierto_diciendo_siempre_si": round(siempre_si, 4),
                                "predice_no": r["predice_no"]},
            "iteraciones": iteraciones,
            "variables": {**VARIABLES_RESENA,
                          **{c: "1 si el juego tiene el genero " + g for c, g in columnas_genero.items()},
                          **{c: "1 si la resena esta en " + i for c, i in columnas_idioma.items()}},
            "coeficientes": {n: round(v, 4) for n, v in coeficientes},
            "intercepto": round(regresion.intercept, 4),
            "tiempos": tiempos,
        }
        texto = json.dumps(informe, indent=2, ensure_ascii=False)
        ruta_informe = ns + cfg_ml["salida_informe"]
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        Path_ = jvm.org.apache.hadoop.fs.Path
        fs = Path_(ns + "/").getFileSystem(conf)
        salida = fs.create(Path_(ruta_informe), True)       # True = reemplazar
        salida.write(bytearray(texto.encode("utf-8")))
        salida.close()
        modelo.write().overwrite().save(ns + cfg_ml["salida_modelo"])
        print("[OK] Informe en {} y modelo en {}".format(cfg_ml["salida_informe"], cfg_ml["salida_modelo"]))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
