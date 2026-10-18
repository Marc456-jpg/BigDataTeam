#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
comparar_formatos.py · Compara el CSV original (zona raw) con el Parquet limpio
(zona curada): cuánto ocupan y cuánto tarda Spark en leerlos.

Por qué existe: el apartado B del enunciado pide justificar la conversión a
Parquet. Este script lo MIDE en nuestro clúster en vez de suponerlo:

  1. TAMAÑO: bytes de cada formato en HDFS (una copia y con sus 2 réplicas).
  2. TIEMPO, MISMA CONSULTA EN LOS DOS FORMATOS: «cuántas reseñas recomiendan
     el juego y cuántas no» (groupBy recommended + count).
       - En el CSV, Spark tiene que leer el archivo ENTERO, fila a fila, con
         una sola tarea (multiLine): ≈ 10 min.
       - En Parquet, Spark lee SOLO la columna «recommended» (cada columna se
         guarda por separado) y en paralelo: segundos.
  3. TIEMPO CON PODA DE PARTICIONES (solo Parquet): la misma consulta, pero solo
     para 2020. Spark ni siquiera abre las carpetas de los otros años.

Solo LEE los datos. El único archivo que escribe es el informe JSON de
resultados en /resultados/comparativa/csv_vs_parquet.json (se reemplaza en
cada ejecución, así que se puede repetir sin problema).

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/procesamiento/comparar_formatos.py

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import json
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# El esquema explícito del CSV se reutiliza del ETL (mismo archivo de la misma
# carpeta), para leer el CSV exactamente igual que lo lee el ETL.
from etl_reviews import ESQUEMA_RAW

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"


def medir(nombre, consulta):
    """Ejecuta una consulta (función sin argumentos), mide los segundos que
    tarda y devuelve (segundos, filas del resultado)."""
    print("[COMPARAR] {} ...".format(nombre))
    inicio = time.perf_counter()
    filas = consulta()
    segundos = time.perf_counter() - inicio
    print("           {:.1f} s".format(segundos))
    return segundos, filas


def main():
    parser = argparse.ArgumentParser(description="Compara CSV (raw) y Parquet (curada)")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_etl = cfg["spark"], cfg["etl"]
    ruta_csv = ns + cfg_etl["csv_raw"]
    ruta_parquet = ns + cfg_etl["salida"]
    ruta_informe = ns + cfg["hdfs"]["resultados"] + "/comparativa/csv_vs_parquet.json"

    spark = (
        SparkSession.builder
        .appName("comparar_formatos")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )
    try:
        # --- 1) Tamaños en HDFS ------------------------------------------------
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        Path_ = jvm.org.apache.hadoop.fs.Path
        fs = Path_(ns + "/").getFileSystem(conf)

        def tamanos(ruta):
            """(MB de una copia, MB contando las réplicas) de un archivo o carpeta."""
            resumen = fs.getContentSummary(Path_(ruta))
            return (round(resumen.getLength() / 1024**2, 1),
                    round(resumen.getSpaceConsumed() / 1024**2, 1))

        csv_mb, csv_mb_replicas = tamanos(ruta_csv)
        pq_mb, pq_mb_replicas = tamanos(ruta_parquet)
        print("[COMPARAR] CSV    : {} MB ({} MB con replicas)".format(csv_mb, csv_mb_replicas))
        print("[COMPARAR] Parquet: {} MB ({} MB con replicas)".format(pq_mb, pq_mb_replicas))

        # --- 2) Misma consulta en los dos formatos -----------------------------
        csv = (
            spark.read.schema(ESQUEMA_RAW)
            .option("header", True).option("multiLine", True)
            .option("quote", '"').option("escape", '"')
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(ruta_csv)
        )
        parquet = spark.read.parquet(ruta_parquet)

        def por_recomendacion(df):
            # collect() obliga a Spark a ejecutar de verdad la consulta.
            return lambda: {str(r["recommended"]): r["count"]
                            for r in df.groupBy("recommended").count().collect()}

        t_csv, res_csv = medir("CSV: resenas por recommended (lee el archivo entero, ~10 min)",
                               por_recomendacion(csv))
        t_pq, res_pq = medir("Parquet: misma consulta (solo la columna recommended)",
                             por_recomendacion(parquet))
        t_pq_2020, res_pq_2020 = medir("Parquet: misma consulta solo 2020 (poda de particiones)",
                                       por_recomendacion(parquet.filter(F.col("year") == 2020)))

        # --- 3) Informe ------------------------------------------------------------
        informe = {
            "tamano_mb": {"csv": csv_mb, "csv_con_replicas": csv_mb_replicas,
                          "parquet": pq_mb, "parquet_con_replicas": pq_mb_replicas,
                          "parquet_entre_csv": round(pq_mb / csv_mb, 3)},
            "consulta": "groupBy(recommended).count()",
            "segundos": {"csv": round(t_csv, 1), "parquet": round(t_pq, 1),
                         "parquet_solo_2020": round(t_pq_2020, 1)},
            "veces_mas_rapido_parquet": round(t_csv / t_pq, 1),
            "resultado": {"csv": res_csv, "parquet": res_pq, "parquet_solo_2020": res_pq_2020},
            "nota": ("El CSV incluye las filas corruptas y las duplicadas que el ETL quito; "
                     "por eso sus cuentas son algo mayores que las del Parquet."),
        }
        texto = json.dumps(informe, indent=2, ensure_ascii=False)
        salida = fs.create(Path_(ruta_informe), True)       # True = reemplazar
        salida.write(bytearray(texto.encode("utf-8")))
        salida.close()
        print(texto)
        print("[OK] Comparativa guardada en {}".format(ruta_informe))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
