#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl_reviews.py · ETL de las reseñas de Steam: zona raw (CSV) → zona curada (Parquet).

Apartados del enunciado que cubre:
  - B: «conversión de los datos a Parquet particionado» y zonas raw/curada/resultados.
  - D: «ETL y limpieza de los datos usando DataFrames, con esquemas definidos
       explícitamente (no inferidos automáticamente)».
  - E: «controles básicos de calidad de datos» y «re-ejecuciones idempotentes».

Cómo funciona (dos pasadas):

  PASADA 1 (lenta, ≈ 10-20 min): lee el CSV de 8 GB de /raw con un esquema
     explícito y lo escribe en Parquet en una zona de paso (staging),
     particionada por año y mes. El CSV tiene saltos de línea DENTRO de las
     reseñas, así que Spark tiene que leerlo de principio a fin con una sola
     tarea (multiLine): esta es la parte lenta. Después ya todo es Parquet.

  MEDICIÓN: mide cuánto ocupa cada partición (año y año+mes) y decide la
     partición final con el criterio de config/pipeline.yaml.

  Entre las dos pasadas se reinicia Spark: así se borran sus archivos
  temporales de la pasada 1 (varios GB) y no se llena el disco.

  PASADA 2 (rápida, ≈ 3-6 min): lee el staging (Parquet, en paralelo), calcula
     el informe de calidad, se queda solo con las filas válidas, quita
     duplicados y escribe el resultado final en /curada/reviews, particionado.
     Al final borra el staging.

Re-ejecutar el script es seguro (idempotente): todas las escrituras usan modo
«overwrite», así que nunca se duplican datos.

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/procesamiento/etl_reviews.py

(«--conf spark.log.level=WARN» quita los ≈ 1.300 mensajes INFO de Spark, para
que se vean bien los mensajes del propio ETL.)

Por qué con spark-submit y no con «python» a secas: en la imagen de Jupyter,
PySpark no está instalado como paquete normal de Python; Jupyter lo prepara al
arrancar sus notebooks. «spark-submit» es el lanzador oficial de Spark: prepara
él mismo el entorno de PySpark y arranca el programa (es lo que se usa en la
vida real para ejecutar trabajos de Spark que no son notebooks).

Nota: los mensajes que imprime van sin tildes a propósito (PowerShell las
muestra mal); los comentarios sí llevan tildes.
"""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Raíz del repositorio (este archivo está en <raíz>/src/procesamiento/).
RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"

# ---------------------------------------------------------------------------
# ESQUEMA EXPLÍCITO DEL CSV (23 columnas, en el mismo orden que el archivo)
# ---------------------------------------------------------------------------
# Spark NO adivina los tipos: se los decimos nosotros. Así la lectura es más
# rápida (no tiene que recorrer el archivo para inferirlos) y los tipos son
# siempre los mismos. Los nombres originales con punto («author.steamid») se
# cambian por guion bajo («author_steamid»), porque el punto en Spark significa
# «campo dentro de una estructura» y obligaría a usar comillas invertidas.
# La última columna, «_corrupt_record», no está en el CSV: Spark guarda ahí el
# texto original de las filas que no puede leer (filas «corruptas»).
ESQUEMA_RAW = T.StructType([
    T.StructField("indice", T.LongType()),               # 1ª columna sin nombre (índice de pandas)
    T.StructField("app_id", T.IntegerType()),             # identificador del juego en Steam
    T.StructField("app_name", T.StringType()),            # nombre del juego
    T.StructField("review_id", T.LongType()),             # identificador de la reseña
    T.StructField("language", T.StringType()),            # idioma de la reseña
    T.StructField("review", T.StringType()),              # texto de la reseña
    T.StructField("timestamp_created", T.LongType()),     # fecha de creación (segundos desde 1970, UTC)
    T.StructField("timestamp_updated", T.LongType()),     # fecha de la última edición
    T.StructField("recommended", T.BooleanType()),        # ¿recomienda el juego? (variable objetivo del ML)
    T.StructField("votes_helpful", T.LongType()),         # votos de «útil» (Long: hay valores enormes)
    T.StructField("votes_funny", T.LongType()),           # votos de «divertida»
    T.StructField("weighted_vote_score", T.DoubleType()), # puntuación ponderada de utilidad (0 a 1)
    T.StructField("comment_count", T.IntegerType()),      # comentarios que ha recibido la reseña
    T.StructField("steam_purchase", T.BooleanType()),     # ¿compró el juego en Steam?
    T.StructField("received_for_free", T.BooleanType()),  # ¿lo recibió gratis?
    T.StructField("written_during_early_access", T.BooleanType()),  # ¿escrita en acceso anticipado?
    T.StructField("author_steamid", T.LongType()),        # identificador del autor
    T.StructField("author_num_games_owned", T.IntegerType()),   # juegos que tiene el autor
    T.StructField("author_num_reviews", T.IntegerType()),       # reseñas que ha escrito el autor
    T.StructField("author_playtime_forever", T.DoubleType()),   # minutos jugados en total (hasta la descarga)
    T.StructField("author_playtime_last_two_weeks", T.DoubleType()),  # minutos en las 2 últimas semanas
    T.StructField("author_playtime_at_review", T.DoubleType()), # minutos jugados al escribir la reseña
    T.StructField("author_last_played", T.DoubleType()),        # última vez que jugó (segundos desde 1970)
    T.StructField("_corrupt_record", T.StringType()),     # texto de las filas que Spark no pudo leer
])


def leer_config(ruta):
    """Lee config/pipeline.yaml y devuelve un diccionario."""
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)


def crear_sesion(cfg_spark, master=None):
    """Crea la sesión de Spark conectada al clúster (o al master indicado)."""
    return (
        SparkSession.builder
        .appName("etl_reviews")
        .master(master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        # Fechas en UTC: el año y el mes no dependen del PC (decisión del 16 oct).
        .config("spark.sql.session.timeZone", cfg_spark["zona_horaria"])
        # 2 copias por bloque al escribir en HDFS (solo hay 2 DataNodes).
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        # Tareas por «shuffle»: 32 en vez de 200 (tenemos 4 núcleos).
        .config("spark.sql.shuffle.partitions", str(cfg_spark["particiones_shuffle"]))
        .getOrCreate()
    )


class HDFS:
    """Pequeña ayuda para preguntar a HDFS (tamaños, borrar, escribir texto)
    usando la librería de Hadoop que Spark ya lleva dentro."""

    def __init__(self, spark, base):
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        self._Path = jvm.org.apache.hadoop.fs.Path
        self._base = base.rstrip("/")             # p. ej. «hdfs://namenode:9000»
        self._fs = self._Path(self._base + "/").getFileSystem(conf)

    def _p(self, ruta):
        """Ruta completa (con «hdfs://namenode:9000» delante)."""
        return self._Path(self._base + ruta)

    def existe(self, ruta):
        return self._fs.exists(self._p(ruta))

    def tamano(self, ruta):
        """Bytes que ocupan los datos de una carpeta (UNA copia, sin réplicas)."""
        return self._fs.getContentSummary(self._p(ruta)).getLength()

    def subcarpetas(self, ruta):
        """Nombres de las subcarpetas directas (p. ej. «year=2020»)."""
        return sorted(e.getPath().getName() for e in self._fs.listStatus(self._p(ruta))
                      if e.isDirectory())

    def borrar(self, ruta):
        if self.existe(ruta):
            self._fs.delete(self._p(ruta), True)

    def poner_copias(self, ruta, copias):
        """Cambia el número de copias (réplicas) de TODOS los archivos de una
        carpeta, recorriendo sus subcarpetas. Devuelve cuántos archivos cambió.

        HDFS borra solo, en segundo plano y en pocos segundos, las copias que
        sobran; el espacio de disco se libera sin hacer nada más."""
        archivos = self._fs.listFiles(self._p(ruta), True)   # True = recursivo
        n = 0
        while archivos.hasNext():
            self._fs.setReplication(archivos.next().getPath(), copias)
            n += 1
        return n

    def escribir_texto(self, ruta, texto):
        """Escribe (o reemplaza) un archivo de texto UTF-8 en HDFS."""
        salida = self._fs.create(self._p(ruta), True)
        salida.write(bytearray(texto.encode("utf-8")))
        salida.close()


def mb(n_bytes):
    return n_bytes / 1024**2


def tamano_grupo_filas(cfg_etl):
    """
    Bytes de cada «grupo de filas» (row group) de los archivos Parquet.

    Un archivo Parquet se guarda por trozos llamados grupos de filas, y quien lo
    lee carga un grupo entero en memoria de golpe. Por defecto miden 128 MB, y
    con 4 tareas a la vez en un executor de 1 GB eso agotaba la memoria
    (OutOfMemoryError del 18 oct). Con grupos de 16 MB cada tarea carga 8 veces
    menos.
    """
    return int(cfg_etl["parquet_grupo_filas_mb"]) * 1024 * 1024


def pasada_1_staging(spark, cfg_etl, ns):
    """CSV (raw) → Parquet de paso, particionado por año y mes."""
    raw = (
        spark.read
        .schema(ESQUEMA_RAW)                       # esquema explícito: nada se adivina
        .option("header", True)                    # la 1ª línea son los nombres: se salta
        .option("multiLine", True)                 # hay reseñas con saltos de línea dentro
        .option("quote", '"')                      # los textos van entre comillas dobles...
        .option("escape", '"')                     # ...y una comilla dentro se escribe ""
        .option("mode", "PERMISSIVE")              # si una fila falla, no se para: se marca
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(ns + cfg_etl["csv_raw"])
    )

    # Columnas derivadas: fechas legibles (tipo timestamp, en UTC), año, mes y
    # longitud del texto (será un predictor del ML).
    datos = (
        raw.drop("indice")
        .withColumn("created_at", F.timestamp_seconds("timestamp_created"))
        .withColumn("updated_at", F.timestamp_seconds("timestamp_updated"))
        .withColumn("author_last_played",
                    F.timestamp_seconds(F.col("author_last_played").cast("long")))
        .withColumn("review_length", F.length("review"))
        .withColumn("year", F.year("created_at"))
        .withColumn("month", F.month("created_at"))
        .drop("timestamp_created", "timestamp_updated")
    )

    # repartition(16, year, month): reparte las filas en 16 tareas de forma que
    # cada (año, mes) cae entero en una sola tarea → 1 archivo por carpeta de mes.
    # La zona de paso se escribe con las 2 copias de siempre y, al terminar,
    # main() la deja en 1 copia (ver HDFS.poner_copias). Un primer intento con
    # .option("dfs.replication", "1") aquí NO funcionó (el 2.º intento en el
    # clúster dejó 2 copias). Explicación más probable: cada executor reutiliza
    # la conexión a HDFS que abrió al leer el CSV, creada con el valor general
    # (2 copias), y es esa conexión la que decide las copias de lo que escribe.
    (
        datos.repartition(int(cfg_etl["particiones_staging"]), "year", "month")
        .write.mode("overwrite")                   # idempotente: reemplaza lo anterior
        .option("parquet.block.size", str(tamano_grupo_filas(cfg_etl)))
        .partitionBy("year", "month")
        .parquet(ns + cfg_etl["staging"])
    )


def medir_y_decidir(hdfs, cfg_etl):
    """Mide el tamaño de cada partición del staging y decide year o year_month."""
    staging = cfg_etl["staging"]
    por_anio = {}
    por_mes = []
    for carpeta_anio in hdfs.subcarpetas(staging):            # «year=2020», ...
        ruta_anio = staging + "/" + carpeta_anio
        por_anio[carpeta_anio] = hdfs.tamano(ruta_anio)
        for carpeta_mes in hdfs.subcarpetas(ruta_anio):       # «month=5», ...
            por_mes.append(hdfs.tamano(ruta_anio + "/" + carpeta_mes))

    mediana_mes = statistics.median(por_mes) if por_mes else 0
    mediana_anio = statistics.median(por_anio.values()) if por_anio else 0
    umbral = cfg_etl["umbral_mb_particion"] * 1024**2

    eleccion = cfg_etl["particion"]
    if eleccion == "auto":
        eleccion = "year_month" if mediana_mes >= umbral else "year"

    medicion = {
        "particiones_anio": len(por_anio),
        "particiones_anio_mes": len(por_mes),
        "mb_por_anio": {k: round(mb(v), 1) for k, v in por_anio.items()},
        "mb_mediana_anio": round(mb(mediana_anio), 1),
        "mb_mediana_anio_mes": round(mb(mediana_mes), 1),
        "mb_min_anio_mes": round(mb(min(por_mes)), 2) if por_mes else 0,
        "mb_max_anio_mes": round(mb(max(por_mes)), 2) if por_mes else 0,
        "umbral_mb": cfg_etl["umbral_mb_particion"],
        "criterio_config": cfg_etl["particion"],
        "particion_elegida": eleccion,
    }
    return eleccion, medicion


def pasada_2_curada(spark, cfg_etl, ns, eleccion):
    """Parquet de paso → informe de calidad + datos limpios en la zona curada."""
    df = spark.read.parquet(ns + cfg_etl["staging"])

    fmin = F.to_timestamp(F.lit(cfg_etl["fecha_minima"]))
    fmax = F.to_timestamp(F.lit(cfg_etl["fecha_maxima"] + " 23:59:59"))

    # Una fila es VÁLIDA si: Spark la pudo leer, tiene las columnas clave y su
    # fecha de creación está en el rango razonable.
    corrupta = F.col("_corrupt_record").isNotNull()
    falta_clave = (F.col("review_id").isNull() | F.col("app_id").isNull()
                   | F.col("recommended").isNull() | F.col("created_at").isNull())
    fuera_rango = (F.col("created_at") < fmin) | (F.col("created_at") > fmax)
    valida = ~corrupta & ~falta_clave & ~fuera_rango

    # --- Informe de calidad: todo se cuenta en UNA sola pasada (agg) ---------
    fila = df.agg(
        F.count(F.lit(1)).alias("filas_leidas"),
        F.sum(corrupta.cast("int")).alias("filas_corruptas"),
        F.sum((~corrupta & falta_clave).cast("int")).alias("filas_sin_columna_clave"),
        F.sum((~corrupta & ~falta_clave & fuera_rango).cast("int")).alias("filas_fecha_fuera_de_rango"),
        F.sum(valida.cast("int")).alias("filas_validas"),
        # Avisos: se cuentan pero NO se borran (son raros, no imposibles).
        # Se mira review_length (un número) y no review (el texto): así esta
        # cuenta no tiene que leer la columna más pesada del dataset.
        F.sum(F.col("review_length").isNull().cast("int")).alias("aviso_review_nula"),
        F.sum(F.col("author_playtime_at_review").isNull().cast("int")).alias("aviso_playtime_at_review_nulo"),
        F.sum((F.col("author_playtime_at_review") < 0).cast("int")).alias("aviso_playtime_negativo"),
        F.sum(((F.col("weighted_vote_score") < 0) | (F.col("weighted_vote_score") > 1)).cast("int"))
         .alias("aviso_weighted_vote_score_fuera_0_1"),
        F.sum((F.col("votes_funny") >= 4294967295).cast("int")).alias("aviso_votes_funny_desbordado"),
        F.sum((F.col("votes_helpful") >= 4294967295).cast("int")).alias("aviso_votes_helpful_desbordado"),
        # Fechas como texto, formateadas por Spark en UTC (no en la zona del PC).
        F.date_format(F.min("created_at"), "yyyy-MM-dd HH:mm:ss").alias("fecha_min"),
        F.date_format(F.max("created_at"), "yyyy-MM-dd HH:mm:ss").alias("fecha_max"),
    ).collect()[0].asDict()

    # Duplicados: misma review_id más de una vez (entre las filas válidas).
    # Contar los distintos solo mueve la columna review_id: es barato.
    validas = df.filter(valida).drop("_corrupt_record")
    distintas = validas.select("review_id").distinct().count()
    fila["review_id_duplicadas"] = fila["filas_validas"] - distintas
    fila["filas_finales"] = distintas

    # --- Datos limpios: válidas y sin duplicados ----------------------------
    # Solo si HAY duplicados se hace el trabajo caro de quitarlos (mover todas
    # las filas por review_id). De cada reseña repetida, se queda la versión
    # más reciente (la de updated_at mayor).
    if fila["review_id_duplicadas"] > 0:
        ventana = Window.partitionBy("review_id").orderBy(F.col("updated_at").desc())
        limpias = (
            validas.withColumn("_n", F.row_number().over(ventana))
            .filter(F.col("_n") == 1).drop("_n")
        )
    else:
        limpias = validas

    columnas_particion = ["year"] if eleccion == "year" else ["year", "month"]
    (
        limpias.repartition(*columnas_particion)   # 1 archivo por partición
        .write.mode("overwrite")                   # idempotente
        .option("parquet.block.size", str(tamano_grupo_filas(cfg_etl)))
        .partitionBy(*columnas_particion)
        .parquet(ns + cfg_etl["salida"])
    )
    return fila


def main():
    parser = argparse.ArgumentParser(description="ETL de las resenas de Steam: raw (CSV) -> curada (Parquet)")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="solo para pruebas (p. ej. local[2])")
    args = parser.parse_args()

    cfg = leer_config(args.config)
    cfg_etl = cfg["etl"]
    ns = cfg["hdfs"]["namenode"]          # p. ej. hdfs://namenode:9000

    t0 = time.time()
    spark = crear_sesion(cfg["spark"], args.master)
    hdfs = HDFS(spark, ns)
    print("[ETL] Spark {} | entrada {}{}".format(spark.version, ns, cfg_etl["csv_raw"]))
    print("[ETL] Web de esta aplicacion: http://localhost:4040")

    try:
        if not hdfs.existe(cfg_etl["csv_raw"]):
            print("[ERROR] No existe {} en HDFS (falta la ingesta, mini-paso 2.2).".format(cfg_etl["csv_raw"]))
            return 2

        # PASADA 1 -----------------------------------------------------------
        print("[1/4] Pasada 1: CSV -> Parquet de paso (la parte lenta) ...")
        t = time.time()
        pasada_1_staging(spark, cfg_etl, ns)
        t_pasada1 = time.time() - t
        print("      hecho en {:.1f} min".format(t_pasada1 / 60))

        # Se reinicia Spark: al cerrarse, borra sus archivos temporales del
        # «shuffle» de la pasada 1 (varios GB) y el disco no se llena.
        spark.stop()
        spark = crear_sesion(cfg["spark"], args.master)
        hdfs = HDFS(spark, ns)

        # La zona de paso es temporal (se borra al final), así que basta con
        # 1 copia: se quitan ≈ 4 GB de disco justo antes de la pasada 2, que es
        # cuando más espacio hace falta (raw + staging + salida final + shuffle).
        # Si fallara, no es grave: se avisa y el ETL sigue con 2 copias.
        try:
            n = hdfs.poner_copias(cfg_etl["staging"], 1)
            print("      zona de paso reducida a 1 copia ({} archivos)".format(n))
        except Exception as error:
            print("[AVISO] No se pudo reducir la zona de paso a 1 copia: {}".format(error))

        # MEDICIÓN -----------------------------------------------------------
        print("[2/4] Midiendo el tamano de las particiones ...")
        eleccion, medicion = medir_y_decidir(hdfs, cfg_etl)
        print("      particiones ano: {} | ano+mes: {}".format(
            medicion["particiones_anio"], medicion["particiones_anio_mes"]))
        print("      mediana ano: {} MB | mediana ano+mes: {} MB (umbral {} MB)".format(
            medicion["mb_mediana_anio"], medicion["mb_mediana_anio_mes"], medicion["umbral_mb"]))
        for anio, tam in medicion["mb_por_anio"].items():
            print("        {:<12} {:>10.1f} MB".format(anio, tam))
        print("      -> particion elegida: {}".format(eleccion))

        # PASADA 2 -----------------------------------------------------------
        print("[3/4] Pasada 2: calidad + limpieza -> {} ...".format(cfg_etl["salida"]))
        t = time.time()
        calidad = pasada_2_curada(spark, cfg_etl, ns, eleccion)
        t_pasada2 = time.time() - t
        print("      hecho en {:.1f} min".format(t_pasada2 / 60))

        # INFORME + LIMPIEZA DEL STAGING --------------------------------------
        print("[4/4] Informe de calidad y borrado de la zona de paso ...")
        informe = {
            "entrada": cfg_etl["csv_raw"],
            "salida": cfg_etl["salida"],
            "calidad": calidad,
            "particion": medicion,
            "tamano_salida_mb": None,
            "minutos": {"pasada_1": round(t_pasada1 / 60, 1), "pasada_2": round(t_pasada2 / 60, 1)},
        }
        hdfs.borrar(cfg_etl["staging"])
        informe["tamano_salida_mb"] = round(mb(hdfs.tamano(cfg_etl["salida"])), 1)
        texto = json.dumps(informe, indent=2, ensure_ascii=False)
        hdfs.escribir_texto(cfg_etl["informe_calidad"], texto)
        print(texto)
        print("[OK] ETL terminado en {:.1f} min. Informe en {}{}".format(
            (time.time() - t0) / 60, ns, cfg_etl["informe_calidad"]))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
