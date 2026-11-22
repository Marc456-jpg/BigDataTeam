# Databricks notebook source
# MAGIC %md
# MAGIC # Steam Reviews · Entrega 2 · De local a la nube (Databricks Free Edition)
# MAGIC
# MAGIC **Equipo:** Manuel Fernández Pérez-Ricarte · Marc Pich-Aguilera Béjar · Nora Cepas Zapico
# MAGIC
# MAGIC Este notebook adapta a Databricks el pipeline de la Entrega 1 (clúster local con Docker:
# MAGIC HDFS + YARN + Spark 4.2.0). Mantiene las **mismas 3 preguntas de negocio** y busca
# MAGIC resultados equivalentes, para poder comparar *qué cambia* al pasar de administrar nuestro
# MAGIC propio clúster a una plataforma gestionada.
# MAGIC
# MAGIC | Zona en HDFS (local) | Equivalente en Databricks (Unity Catalog) |
# MAGIC |---|---|
# MAGIC | `/raw` (CSV de 8,17 GB, replicación 2) | volume `workspace.steam.raw` (CSV reducido de 1,79 GB) |
# MAGIC | `/curada` (Parquet particionado por `year`) | tabla Delta `workspace.steam.reviews` |
# MAGIC | `/quarantine` (JSON) | tabla Delta `workspace.steam.quarantine_reviews` |
# MAGIC | `/resultados` (CSV/JSON) | tablas Delta `q1`, `q2`, `q3` + volume `workspace.steam.resultados` |
# MAGIC
# MAGIC **Datos:** `steam_reviews_databricks.csv`, generado con `reducir_steam_reviews.py` (modo «proyecto»):
# MAGIC las **21.747.371 filas** completas y solo las **13 columnas** que usa el pipeline. El texto de la
# MAGIC reseña se ha sustituido por su longitud (`review_length`), que es lo único que usábamos de él.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parámetros
# MAGIC
# MAGIC En la Entrega 1 todos los parámetros estaban en `config/pipeline.yaml`. Aquí van en un
# MAGIC diccionario al principio del notebook, con los **mismos valores**, para que ningún paso lleve
# MAGIC rutas ni números escritos dentro. Desaparecen los parámetros de infraestructura (master de Spark,
# MAGIC memoria del driver y de los executors, réplicas de HDFS…): en *serverless* no se pueden elegir.

# COMMAND ----------

# ---------------------------------------------------------------------------
# PARÁMETROS DEL PIPELINE (equivalente a config/pipeline.yaml de la Entrega 1)
# ---------------------------------------------------------------------------
CATALOGO = "workspace"   # catálogo de Unity Catalog que trae Free Edition
ESQUEMA = "steam"        # nuestro esquema (equivale a la "raíz" de nuestras zonas de HDFS)

PARAM = {
    # --- Zona raw: el CSV reducido, subido a mano por la interfaz (Databricks no tiene internet)
    "csv_raw": f"/Volumes/{CATALOGO}/{ESQUEMA}/raw/steam_reviews_databricks.csv",
    # Tamaño exacto del CSV en bytes (el del resumen del script de reducción). Sirve para
    # comprobar que la subida está completa, igual que tamano_bytes en la Entrega 1.
    "csv_bytes": 1_786_432_475,
    # Filas que debe tener el CSV (las del dataset original: no hay muestreo)
    "filas_esperadas": 21_747_371,

    # --- Tablas Delta (sustituyen a las carpetas de HDFS)
    "tabla_reviews": f"{CATALOGO}.{ESQUEMA}.reviews",                # zona curada
    "tabla_cuarentena": f"{CATALOGO}.{ESQUEMA}.quarantine_reviews",  # zona cuarentena

    # --- Spark (solo lo que se puede tocar en serverless)
    # Fechas en UTC (decisión del 16 oct): el año y el mes no dependen de la máquina.
    "zona_horaria": "UTC",

    # --- Reglas de calidad del ETL (mismos valores que la Entrega 1)
    "fecha_minima": "2003-09-12",          # Steam nació en 2003
    "fecha_maxima": "2021-12-31",          # el dataset se descargó a principios de 2021
    "voto_imposible": 4_294_967_295,       # 2^32 - 1: contador desbordado en la API de Steam

    # --- Consultas (Q1-Q3)
    "q1_min_resenas": 1000,
    "q1_top": 10,
    "q3_anio": 2020,
    "q3_top": 5,

    # --- ML
    "proporcion_test": 0.2,
    "semilla": 42,
}

# Cifras de la Entrega 1 (local) para comparar al final de cada paso
REF_LOCAL = {
    "spark": "4.2.0",
    "filas_leidas": 21_747_371,
    "filas_corruptas": 7,
    "review_id_duplicadas": 134_927,
    "filas_finales": 21_612_437,
    "minutos_etl": 9.7,
}

for clave, valor in PARAM.items():
    print(f"{clave:<18} = {valor}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Entorno
# MAGIC
# MAGIC En local, antes de nada había que levantar 7 contenedores (`docker compose up`), comprobar
# MAGIC HDFS con `hdfs fsck` y ver en la web de Spark (puerto 8080) que el worker estaba vivo.
# MAGIC Aquí el cómputo *serverless* ya está disponible al abrir el notebook: solo comprobamos la
# MAGIC versión y creamos (si no existen) el esquema y los volumes. `IF NOT EXISTS` hace que la celda
# MAGIC se pueda repetir sin errores (re-ejecución idempotente, igual que en la Entrega 1).

# COMMAND ----------

import os
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# --- Esquema y volumes (idempotente: si ya existen, no hace nada) ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.resultados")

# --- Zona horaria UTC (una de las pocas opciones de Spark que serverless deja cambiar)
spark.conf.set("spark.sql.session.timeZone", PARAM["zona_horaria"])


def leer_conf(clave):
    """Lee una opción de Spark. En serverless algunas no se pueden consultar:
    en ese caso devuelve el motivo en vez de parar el notebook."""
    try:
        return spark.conf.get(clave)
    except Exception as error:
        return f"(no disponible: {type(error).__name__})"


# --- Qué tenemos debajo -----------------------------------------------------
print("Versión de Spark         :", spark.version, f"(local: {REF_LOCAL['spark']})")
print("Databricks Runtime       :", os.environ.get("DATABRICKS_RUNTIME_VERSION", "(no expuesta)"))
print("Zona horaria             :", leer_conf("spark.sql.session.timeZone"))
print("ANSI SQL                 :", leer_conf("spark.sql.ansi.enabled"))
print("AQE (adaptive)           :", leer_conf("spark.sql.adaptive.enabled"))
print("Particiones de shuffle   :", leer_conf("spark.sql.shuffle.partitions"))
print("Umbral broadcast (bytes) :", leer_conf("spark.sql.autoBroadcastJoinThreshold"))
print("Máx. bytes por partición :", leer_conf("spark.sql.files.maxPartitionBytes"))
# En local fijábamos spark.executor.memory=1536m y master=spark://spark-master:7077.
# Aquí no existen: lo intentamos leer para dejarlo documentado.
print("Memoria del executor     :", leer_conf("spark.executor.memory"))
print("Master                   :", leer_conf("spark.master"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingesta: comprobar el CSV en la zona raw
# MAGIC
# MAGIC En la Entrega 1 la ingesta era `hdfs dfs -put` (14 min 37 s para 8 GB) y después `hdfs fsck`
# MAGIC para ver los 61 bloques y sus 2 réplicas. Aquí el CSV se subió por la interfaz de Unity Catalog
# MAGIC y solo podemos ver **el fichero**, no sus bloques ni dónde están sus copias: eso lo gestiona el
# MAGIC proveedor (pregunta 1 del informe). Comprobamos que el tamaño es exactamente el esperado.

# COMMAND ----------

info = dbutils.fs.ls(PARAM["csv_raw"])[0]
print("Ruta     :", info.path)
print(f"Tamaño   : {info.size:,} bytes ({info.size / 1024**3:.2f} GiB)")
print(f"Esperado : {PARAM['csv_bytes']:,} bytes")
assert info.size == PARAM["csv_bytes"], "El CSV del volume no tiene el tamaño esperado: subida incompleta"
print("[OK] El CSV está completo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Esquema explícito y lectura
# MAGIC
# MAGIC Como en la Entrega 1, **nunca `inferSchema`**: los tipos los decimos nosotros (más rápido,
# MAGIC porque Spark no recorre el fichero para adivinarlos, y siempre iguales).
# MAGIC
# MAGIC **Cambios respecto a la Entrega 1:**
# MAGIC - 13 columnas en vez de 23 (el CSV reducido solo trae las que usa el pipeline).
# MAGIC - `review_length` ya viene calculada → desaparece `F.length("review")`.
# MAGIC - El punto de `author.playtime_at_review` se cambia por guion bajo, como antes.
# MAGIC - **Ya no hace falta `multiLine`**: el CSV original tenía saltos de línea dentro del texto de las
# MAGIC   reseñas y Spark tenía que leerlo con **una sola tarea** (la parte lenta del ETL local, que nos
# MAGIC   obligó a hacer dos pasadas). Sin el texto, cada línea es una fila y Spark puede repartir el
# MAGIC   fichero en trozos y leerlos **en paralelo**.

# COMMAND ----------

# ---------------------------------------------------------------------------
# ESQUEMA EXPLÍCITO DEL CSV REDUCIDO (13 columnas, en el mismo orden que el fichero)
# ---------------------------------------------------------------------------
ESQUEMA_RAW = T.StructType([
    T.StructField("app_id", T.IntegerType()),             # juego (Q1, Q3, ML: join con el catálogo)
    T.StructField("review_id", T.LongType()),             # reseña (ETL: duplicados)
    T.StructField("language", T.StringType()),            # idioma (Q3, ML)
    T.StructField("timestamp_created", T.LongType()),     # creación, segundos desde 1970 (UTC)
    T.StructField("timestamp_updated", T.LongType()),     # última edición (duplicados: la más reciente)
    T.StructField("recommended", T.BooleanType()),        # ¿recomienda? (Q1, Q2, etiqueta del ML)
    T.StructField("votes_helpful", T.LongType()),         # votos «útil» (Q3). Long: hay valores enormes
    T.StructField("votes_funny", T.LongType()),           # votos «divertida» (aviso de desbordamiento)
    T.StructField("steam_purchase", T.BooleanType()),     # ¿comprado en Steam? (ML)
    T.StructField("received_for_free", T.BooleanType()),  # ¿recibido gratis? (ML)
    T.StructField("written_during_early_access", T.BooleanType()),  # ¿en acceso anticipado? (ML)
    T.StructField("author_playtime_at_review", T.DoubleType()),     # minutos jugados al reseñar (Q2, ML)
    T.StructField("review_length", T.IntegerType()),      # caracteres del texto (ML)
    # No está en el CSV: Spark guarda aquí el texto de las filas que no puede leer
    T.StructField("_corrupt_record", T.StringType()),
])

raw = (
    spark.read
    .schema(ESQUEMA_RAW)                       # esquema explícito: nada se adivina
    .option("header", True)                    # la 1.ª línea son los nombres: se salta
    .option("mode", "PERMISSIVE")              # si una fila falla, no se para: se marca
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    # Sin multiLine: el CSV reducido ya no tiene texto con saltos de línea
    .csv(PARAM["csv_raw"])
)

raw.printSchema()
display(raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 ¿Cómo reparte Spark la lectura? (particionado de entrada)
# MAGIC
# MAGIC Contamos cuántas filas lee cada tarea con `spark_partition_id()`. En Databricks no hay
# MAGIC `df.rdd.getNumPartitions()` (el notebook usa Spark Connect y no tiene RDDs), así que lo medimos
# MAGIC con una agregación. En local, con `multiLine`, el CSV entero lo leía **1 tarea**.
# MAGIC
# MAGIC Esta celda hace además el **recuento total** de filas y de filas corruptas, y mide el tiempo.

# COMMAND ----------

t0 = time.perf_counter()
por_particion = (
    raw.groupBy(F.spark_partition_id().alias("particion"))
    .agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum(F.col("_corrupt_record").isNotNull().cast("int")).alias("corruptas"),
    )
    .orderBy("particion")
    .toPandas()                                # resultado pequeño: una fila por partición
)
segundos_lectura = time.perf_counter() - t0

filas_leidas = int(por_particion["filas"].sum())
filas_corruptas = int(por_particion["corruptas"].sum())

print(f"Particiones de lectura : {len(por_particion)}  (local con multiLine: 1)")
print(f"Filas por partición    : mín {por_particion['filas'].min():,} · "
      f"mediana {int(por_particion['filas'].median()):,} · máx {por_particion['filas'].max():,}")
print(f"Filas leídas           : {filas_leidas:,}  (esperadas {PARAM['filas_esperadas']:,})")
print(f"Filas corruptas        : {filas_corruptas:,}  (local: {REF_LOCAL['filas_corruptas']})")
print(f"Tiempo                 : {segundos_lectura:.1f} s")
assert filas_leidas == PARAM["filas_esperadas"], "No se han leído todas las filas del CSV"
display(por_particion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ETL: raw (CSV) → curada (Delta)
# MAGIC
# MAGIC Misma lógica que `src/procesamiento/etl_reviews.py` de la Entrega 1:
# MAGIC 1. **Columnas derivadas:** fechas legibles en UTC (`created_at`, `updated_at`), `year` y `month`.
# MAGIC 2. **Informe de calidad** en una sola pasada (`agg`).
# MAGIC 3. **Cuarentena:** las filas inválidas (corruptas, sin columna clave o con fecha fuera de
# MAGIC    2003-09-12 … 2021-12-31) no se tiran: se guardan con su `motivo`.
# MAGIC 4. **Duplicados** por `review_id`: se queda la versión más reciente (`updated_at` mayor),
# MAGIC    con el truco de la Entrega 1 (lista pequeña de ids repetidos en *broadcast*; solo las filas
# MAGIC    repetidas se ordenan).
# MAGIC 5. **Escritura** particionada por `year` (decisión medida en la Entrega 1: mediana 192,5 MB por
# MAGIC    año frente a 24,7 MB por mes).
# MAGIC
# MAGIC **Qué cambia en Databricks:**
# MAGIC - **Una sola pasada.** En local hacían falta dos (CSV → Parquet de paso → limpieza) porque el CSV
# MAGIC   con `multiLine` se leía con 1 tarea y tardaba; además se reiniciaba Spark entre pasadas para
# MAGIC   liberar disco. Aquí el CSV se lee en 14 trozos en segundos y el disco no es problema nuestro.
# MAGIC - **Delta en vez de Parquet en HDFS:** las tablas quedan registradas en Unity Catalog y cada
# MAGIC   escritura crea una **versión** (historial y *time travel*, pregunta 1 del informe).
# MAGIC - **Sin ajustes de memoria:** en local tuvimos que bajar el tamaño de los *row groups* de Parquet
# MAGIC   a 16 MB y subir el executor a 1,5 GB tras varios `OutOfMemoryError`. Aquí no hay nada que tocar.
# MAGIC - **Sin `cache()`:** en serverless no se puede cachear un DataFrame; cada acción vuelve a leer el
# MAGIC   CSV (3-4 s), así que no merece la pena una tabla intermedia.

# COMMAND ----------

t_etl = time.perf_counter()

# --- 4.1 Columnas derivadas --------------------------------------------------
# timestamp_seconds convierte "segundos desde 1970" en una fecha. Como la sesión
# está en UTC, year y month salen en UTC (igual que en la Entrega 1).
datos = (
    raw
    .withColumn("created_at", F.timestamp_seconds("timestamp_created"))
    .withColumn("updated_at", F.timestamp_seconds("timestamp_updated"))
    .withColumn("year", F.year("created_at"))
    .withColumn("month", F.month("created_at"))
    .drop("timestamp_created", "timestamp_updated")
    # Ya NO hace falta: .withColumn("review_length", F.length("review"))
    # → la longitud viene calculada en el CSV reducido.
)

# --- 4.2 Reglas de calidad (las mismas que en local) -------------------------
fmin = F.to_timestamp(F.lit(PARAM["fecha_minima"]))
fmax = F.to_timestamp(F.lit(PARAM["fecha_maxima"] + " 23:59:59"))

corrupta = F.col("_corrupt_record").isNotNull()
falta_clave = (F.col("review_id").isNull() | F.col("app_id").isNull()
               | F.col("recommended").isNull() | F.col("created_at").isNull())
fuera_rango = (F.col("created_at") < fmin) | (F.col("created_at") > fmax)
valida = ~corrupta & ~falta_clave & ~fuera_rango

# Columnas del CSV (sin la de corruptas) para contar sus nulos una a una.
# Motivo: el lector CSV de Spark solo analiza las columnas que la consulta usa
# («column pruning»). Al mirar TODAS aquí, cualquier valor mal formado en
# cualquier columna saldría como corrupta o como nulo, y lo veríamos.
COLUMNAS_CSV = [c for c in datos.columns
                if c not in ("_corrupt_record", "created_at", "updated_at", "year", "month")]

# --- 4.3 Informe de calidad: todo en UNA sola pasada (agg) -------------------
calidad = datos.agg(
    F.count(F.lit(1)).alias("filas_leidas"),
    F.sum(corrupta.cast("int")).alias("filas_corruptas"),
    F.sum((~corrupta & falta_clave).cast("int")).alias("filas_sin_columna_clave"),
    F.sum((~corrupta & ~falta_clave & fuera_rango).cast("int")).alias("filas_fecha_fuera_de_rango"),
    F.sum(valida.cast("int")).alias("filas_validas"),
    # Avisos: se cuentan pero NO se borran (raros, no imposibles)
    F.sum(F.col("review_length").isNull().cast("int")).alias("aviso_review_nula"),
    F.sum(F.col("author_playtime_at_review").isNull().cast("int")).alias("aviso_playtime_at_review_nulo"),
    F.sum((F.col("author_playtime_at_review") < 0).cast("int")).alias("aviso_playtime_negativo"),
    F.sum((F.col("votes_funny") >= PARAM["voto_imposible"]).cast("int")).alias("aviso_votes_funny_desbordado"),
    F.sum((F.col("votes_helpful") >= PARAM["voto_imposible"]).cast("int")).alias("aviso_votes_helpful_desbordado"),
    F.date_format(F.min("created_at"), "yyyy-MM-dd HH:mm:ss").alias("fecha_min"),
    F.date_format(F.max("created_at"), "yyyy-MM-dd HH:mm:ss").alias("fecha_max"),
    # Nulos por columna (comprobación de que no se ha colado ningún valor mal leído)
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}") for c in COLUMNAS_CSV],
).first().asDict()
calidad["filas_en_cuarentena"] = calidad["filas_leidas"] - calidad["filas_validas"]

for clave, valor in calidad.items():
    if not clave.startswith("nulos_"):
        print(f"{clave:<32} {valor}")
print("\nNulos por columna:")
for c in COLUMNAS_CSV:
    print(f"  {c:<30} {calidad['nulos_' + c]:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Cuarentena
# MAGIC En local era una carpeta JSON en `/quarantine/reviews`; aquí es una **tabla Delta**. Se escribe
# MAGIC aunque esté vacía, para que el paso sea siempre el mismo (idempotente: `overwrite`).

# COMMAND ----------

invalidas = (
    datos.filter(~valida)
    .withColumn("motivo",
                F.when(corrupta, F.lit("corrupta"))
                .when(falta_clave, F.lit("sin_columna_clave"))
                .otherwise(F.lit("fecha_fuera_de_rango")))
    # «_corrupt_record» es un nombre reservado de los lectores de Spark: se renombra
    .withColumnRenamed("_corrupt_record", "registro_original")
)
(
    invalidas.write.format("delta")
    .mode("overwrite")                          # idempotente: reemplaza la versión anterior
    .option("overwriteSchema", "true")
    .saveAsTable(PARAM["tabla_cuarentena"])
)
display(spark.table(PARAM["tabla_cuarentena"]).groupBy("motivo").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Duplicados y escritura de la zona curada
# MAGIC Igual que en local (decisión del 19 oct): en vez de ordenar los 21,7 M de filas por `review_id`,
# MAGIC 1. se agrupa **solo** `review_id` para saber cuáles se repiten (≈ 135.000);
# MAGIC 2. esa lista pequeña se envía entera a cada tarea (`F.broadcast`);
# MAGIC 3. las filas que no se repiten pasan sin moverse (`left_anti`);
# MAGIC 4. solo las repetidas se ordenan (ventana) para quedarse con la más reciente.

# COMMAND ----------

validas = datos.filter(valida).drop("_corrupt_record")
columnas = validas.columns

grupos = validas.groupBy("review_id").count()
distintas = grupos.count()
calidad["review_id_duplicadas"] = calidad["filas_validas"] - distintas
calidad["filas_finales"] = distintas
print(f"review_id distintas : {distintas:,}")
print(f"Duplicadas          : {calidad['review_id_duplicadas']:,}  (local: {REF_LOCAL['review_id_duplicadas']:,})")

if calidad["review_id_duplicadas"] > 0:
    repetidas_ids = F.broadcast(grupos.filter(F.col("count") > 1).select("review_id"))
    unicas = validas.join(repetidas_ids, "review_id", "left_anti")
    ventana = Window.partitionBy("review_id").orderBy(F.col("updated_at").desc())
    repetidas = (
        validas.join(repetidas_ids, "review_id", "left_semi")
        .withColumn("_n", F.row_number().over(ventana))
        .filter(F.col("_n") == 1).drop("_n")
    )
    limpias = unicas.select(*columnas).unionByName(repetidas.select(*columnas))
else:
    limpias = validas

# Escritura en Delta, particionada por año. overwriteSchema permite cambiar el
# esquema o la partición si se vuelve a ejecutar con otra decisión.
t_escritura = time.perf_counter()
(
    limpias.write.format("delta")
    .mode("overwrite")                          # idempotente
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(PARAM["tabla_reviews"])
)
segundos_escritura = time.perf_counter() - t_escritura
minutos_etl = (time.perf_counter() - t_etl) / 60
calidad["minutos_etl"] = round(minutos_etl, 2)

print(f"Filas finales       : {calidad['filas_finales']:,}  (local: {REF_LOCAL['filas_finales']:,})")
print(f"Escritura Delta     : {segundos_escritura:.1f} s")
print(f"ETL completo        : {minutos_etl:.1f} min  (local: {REF_LOCAL['minutos_etl']} min)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Informe de calidad en la zona de resultados
# MAGIC En local se escribía en HDFS (`/resultados/calidad/etl_reviews.json`) con la clase `HDFS`, que usaba
# MAGIC `spark.sparkContext._jvm`. Aquí no hay `sparkContext` (Spark Connect): se usa `dbutils.fs.put`
# MAGIC sobre el volume `resultados`.

# COMMAND ----------

import json

ruta_informe = f"/Volumes/{CATALOGO}/{ESQUEMA}/resultados/calidad/etl_reviews.json"
informe = {
    "entrada": PARAM["csv_raw"],
    "salida": PARAM["tabla_reviews"],
    "cuarentena": PARAM["tabla_cuarentena"],
    "particion": "year",
    "calidad": calidad,
}
dbutils.fs.put(ruta_informe, json.dumps(informe, indent=2, ensure_ascii=False), overwrite=True)
print("Informe guardado en", ruta_informe)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar la zona curada
# MAGIC Las 4 reglas de `verificar_curada.py`, contando sobre lo que **de verdad** quedó escrito:
# MAGIC 1. filas escritas = `filas_finales` del informe;
# MAGIC 2. sin duplicados (`review_id` distintas = filas);
# MAGIC 3. ninguna fila sin columna clave;
# MAGIC 4. cada fila en la partición de su año.

# COMMAND ----------

curada = spark.table(PARAM["tabla_reviews"])
r = curada.agg(
    F.count(F.lit(1)).alias("filas"),
    F.countDistinct("review_id").alias("distintas"),
    F.sum((F.col("review_id").isNull() | F.col("app_id").isNull()
           | F.col("recommended").isNull() | F.col("created_at").isNull()).cast("int")).alias("sin_clave"),
    F.sum((F.year("created_at") != F.col("year")).cast("int")).alias("anio_mal"),
).first()

comprobaciones = [
    ("filas escritas = informe del ETL", r["filas"] == calidad["filas_finales"]),
    ("sin duplicados (distintas = filas)", r["distintas"] == r["filas"]),
    ("ninguna fila sin columna clave", r["sin_clave"] == 0),
    ("cada fila en la partición de su año", r["anio_mal"] == 0),
]
print(f"Filas escritas: {r['filas']:,} · distintas: {r['distintas']:,} · "
      f"sin clave: {r['sin_clave']} · año mal: {r['anio_mal']}")
for texto, ok in comprobaciones:
    print(f"[{'OK' if ok else 'FALLO'}] {texto}")
assert all(ok for _, ok in comprobaciones), "La zona curada no cuadra"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Particiones de la tabla Delta: filas, ficheros y tamaño por año
# MAGIC Para comparar con la medición de la Entrega 1 (Parquet por año, mediana 192,5 MB con texto).
# MAGIC La columna oculta `_metadata` dice de qué fichero sale cada fila y cuánto pesa ese fichero.

# COMMAND ----------

ficheros = curada.select("year", "_metadata.file_path", "_metadata.file_size")
por_anio = (
    ficheros.groupBy("year").agg(F.count(F.lit(1)).alias("filas"))
    .join(
        ficheros.drop_duplicates(["file_path"]).groupBy("year").agg(
            F.count(F.lit(1)).alias("ficheros"),
            F.round(F.sum("file_size") / 1024**2, 1).alias("mb"),
        ),
        "year",
    )
    .orderBy("year")
)
display(por_anio)
display(spark.sql(f"DESCRIBE DETAIL {PARAM['tabla_reviews']}")
        .select("format", "partitionColumns", "numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Historial de versiones de Delta (pregunta 1 del informe)
# MAGIC Cada `overwrite` crea una versión nueva y la anterior se puede seguir leyendo
# MAGIC (`VERSION AS OF n`). Esto **no** es replicación (copias de los mismos bloques en varios discos,
# MAGIC como el factor 2 de HDFS) ni una copia de seguridad (copia independiente en otro sitio): es un
# MAGIC registro de cambios dentro de la propia tabla.

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {PARAM['tabla_reviews']}")
    .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Catálogo de juegos (tabla auxiliar del join)
# MAGIC
# MAGIC En la Entrega 1, `descargar_catalogo.py` pedía a la API pública de Steam los datos de los 315
# MAGIC juegos y los guardaba en HDFS; `limpiar_catalogo.py` los pasaba a Parquet. **Databricks Free
# MAGIC Edition no tiene salida a internet**, así que la descarga se hace en el PC
# MAGIC (`descargar_catalogo_local.py`, misma lógica) y el fichero `appdetails.jsonl` se sube a mano al
# MAGIC volume `raw`. Aquí solo se limpia, con el mismo esquema explícito que en local.
# MAGIC
# MAGIC Limitación: los datos son los de la fecha de descarga (noviembre), no los de octubre de la
# MAGIC Entrega 1: algún nombre o precio puede haber cambiado.

# COMMAND ----------

PARAM["catalogo_raw"] = f"/Volumes/{CATALOGO}/{ESQUEMA}/raw/appdetails.jsonl"
PARAM["tabla_juegos"] = f"{CATALOGO}.{ESQUEMA}.games"

# Esquema explícito: solo los campos de la respuesta de Steam que usamos
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

crudo = spark.read.schema(ESQUEMA_CATALOGO).json(PARAM["catalogo_raw"])
d = F.col("respuesta.data")
fecha_texto = d["release_date"]["date"]
# La fecha viene como texto en inglés en dos formatos posibles; try_to_timestamp
# devuelve null (en vez de error) si no encaja, p. ej. "Coming soon".
fecha = F.coalesce(F.try_to_timestamp(fecha_texto, F.lit("d MMM, yyyy")),
                   F.try_to_timestamp(fecha_texto, F.lit("MMM d, yyyy"))).cast("date")
generos = F.transform(d["genres"], lambda g: g["description"])

juegos_limpios = crudo.select(
    F.col("app_id").cast("int").alias("app_id"),
    F.coalesce(F.col("respuesta.success"), F.lit(False)).alias("en_tienda"),
    d["name"].alias("nombre"),
    d["type"].alias("tipo"),
    d["is_free"].alias("es_gratis"),
    # Precio de HOY en dólares (los gratuitos no traen precio): 0 si es gratis
    F.when(d["is_free"], F.lit(0.0)).otherwise(d["price_overview"]["final"] / 100.0).alias("precio_usd"),
    generos.alias("generos"),                               # lista completa (decisión 49)
    F.coalesce(F.size(generos), F.lit(0)).alias("num_generos"),
    d["developers"].alias("desarrolladores"),
    d["publishers"].alias("editores"),
    fecha.alias("fecha_salida"),
    F.col("descargado_utc"),
).dropDuplicates(["app_id"])

# Tabla pequeña (315 filas): sin particiones (solo crearía ficheros diminutos)
(juegos_limpios.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(PARAM["tabla_juegos"]))

juegos_tabla = spark.table(PARAM["tabla_juegos"])
display(juegos_tabla.agg(
    F.count(F.lit(1)).alias("juegos"),
    F.sum(F.col("en_tienda").cast("int")).alias("en_tienda"),
    F.sum(F.col("nombre").isNull().cast("int")).alias("sin_nombre"),
    F.sum(F.col("es_gratis").cast("int")).alias("gratis"),
    F.round(F.avg("num_generos"), 2).alias("media_generos"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Las 3 preguntas de negocio (mismas que en la Entrega 1)
# MAGIC
# MAGIC - **Q1 (agregación + join):** los 10 juegos con mayor % de reseñas positivas (mín. 1.000 reseñas),
# MAGIC   con nombre, géneros, desarrolladores y precio del catálogo.
# MAGIC - **Q2 (agregación, media y mediana):** horas jugadas al reseñar, quien recomienda frente a quien no.
# MAGIC - **Q3 (función de ventana):** por idioma, los 5 juegos cuyas reseñas de 2020 sumaron más votos
# MAGIC   «útil» (`DENSE_RANK`). Solo se lee la partición `year=2020` (poda de particiones).
# MAGIC
# MAGIC Cada resultado se guarda como **tabla Delta** (`q1`, `q2`, `q3`) y como **CSV** en el volume
# MAGIC `resultados` (en local: CSV en `/resultados/q1..q3` de HDFS).

# COMMAND ----------

resenas = spark.table(PARAM["tabla_reviews"])
juegos = juegos_tabla.select(
    "app_id", "nombre",
    F.array_join("generos", ", ").alias("generos"),
    F.array_join("desarrolladores", ", ").alias("desarrolladores"),
    "precio_usd",
)
CARPETA_RESULTADOS = f"/Volumes/{CATALOGO}/{ESQUEMA}/resultados"


def guardar(df, nombre_tabla, nombre_csv):
    """Guarda un resultado pequeño como tabla Delta y como un único CSV.
    El resultado se trae una sola vez al driver (pandas, conserva el orden) y
    desde ahí se escriben las dos cosas. Una tabla Delta NO guarda el orden de
    las filas: al leerla hay que volver a ordenar. El CSV se escribe con pandas
    directamente en el volume (en serverless /Volumes se usa como una carpeta)."""
    pdf = df.toPandas()
    spark.createDataFrame(pdf, schema=df.schema).write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(f"{CATALOGO}.{ESQUEMA}.{nombre_tabla}")
    pdf.to_csv(f"{CARPETA_RESULTADOS}/{nombre_csv}", index=False)
    return pdf


# ---------------- Q1: top juegos por % positivo + catálogo -------------------
t = time.perf_counter()
por_juego = (
    resenas.groupBy("app_id").agg(
        F.count(F.lit(1)).alias("resenas"),
        F.sum(F.col("recommended").cast("int")).alias("positivas"))
    .filter(F.col("resenas") >= PARAM["q1_min_resenas"])
    .withColumn("pct_positivas", F.round(100.0 * F.col("positivas") / F.col("resenas"), 2))
)
q1 = (
    por_juego.join(juegos, "app_id", "left")         # left: no perder juegos sin catálogo
    .orderBy(F.desc("pct_positivas"), F.desc("resenas"))
    .limit(PARAM["q1_top"])
    .select("app_id", "nombre", "pct_positivas", "resenas", "generos", "desarrolladores", "precio_usd")
)
q1_pdf = guardar(q1, "q1", "q1_top_juegos_pct_positivo.csv")
print(f"[Q1] {time.perf_counter() - t:.1f} s")
display(q1_pdf)

# COMMAND ----------

# ---------------- Q2: horas al reseñar, recomienda o no ----------------------
t = time.perf_counter()
horas = F.col("author_playtime_at_review") / 60.0              # minutos -> horas
q2 = (
    resenas.filter(F.col("author_playtime_at_review").isNotNull())
    .groupBy("recommended").agg(
        F.count(F.lit(1)).alias("resenas"),
        F.round(F.avg(horas), 1).alias("media_horas"),
        F.round(F.percentile_approx(horas, 0.5, 10000), 1).alias("mediana_horas"))
    .orderBy("recommended")
)
q2_pdf = guardar(q2, "q2", "q2_horas_al_resenar.csv")
print(f"[Q2] {time.perf_counter() - t:.1f} s  "
      "(local: no recomienda 2.715.949 · 202,5 h · 37,3 h | recomienda 18.870.834 · 139,5 h · 30,8 h)")
display(q2_pdf)

# COMMAND ----------

# ---------------- Q3: top 5 por idioma en 2020 (DENSE_RANK) ------------------
t = time.perf_counter()
votos = (
    resenas.filter(F.col("year") == PARAM["q3_anio"])           # poda: solo year=2020
    .filter(F.col("votes_helpful") < PARAM["voto_imposible"])   # fuera contadores rotos
    .groupBy("language", "app_id")
    .agg(F.sum("votes_helpful").alias("votos_utiles"))
)
ventana_q3 = Window.partitionBy("language").orderBy(F.desc("votos_utiles"))
q3 = (
    votos.withColumn("puesto", F.dense_rank().over(ventana_q3))
    .filter(F.col("puesto") <= PARAM["q3_top"])
    .join(F.broadcast(juegos.select("app_id", "nombre")), "app_id", "left")
    .select("language", "puesto", "app_id", "nombre", "votos_utiles")
    .orderBy("language", "puesto", "app_id")
)
q3_pdf = guardar(q3, "q3", "q3_top5_por_idioma_2020.csv")
print(f"[Q3] {time.perf_counter() - t:.1f} s · idiomas: {q3_pdf['language'].nunique()} (local: 28) · "
      f"filas: {len(q3_pdf)} (local: 141)")
display(q3_pdf[q3_pdf["language"].isin(["english", "spanish", "schinese"])])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Comparación con la Entrega 1
# MAGIC Resultados locales (copiados de `evidence/resultados/`) frente a los de Databricks. Recordatorio:
# MAGIC Databricks tiene **7 reseñas más** (las 7 «corruptas» de local, que lo eran por columnas que el CSV
# MAGIC reducido ya no trae), así que algún porcentaje puede moverse en el segundo decimal.

# COMMAND ----------

import pandas as pd

q1_local = pd.DataFrame({
    "app_id": [1229490, 1144400, 1055540, 1089980, 427520, 1145360, 620, 1118200, 1291340, 546560],
    "pct_local": [99.53, 99.4, 99.14, 99.03, 98.92, 98.88, 98.77, 98.77, 98.71, 98.69],
    "resenas_local": [5584, 5034, 5847, 19400, 108282, 118416, 232329, 26319, 7601, 51099],
})
q1_nube = q1_pdf[["app_id", "nombre", "pct_positivas", "resenas"]].copy()
q1_nube["puesto"] = range(1, len(q1_nube) + 1)
comparacion_q1 = q1_nube.merge(q1_local, on="app_id", how="outer")
comparacion_q1["igual"] = (comparacion_q1["pct_positivas"] == comparacion_q1["pct_local"]) & \
                          (comparacion_q1["resenas"] == comparacion_q1["resenas_local"])
display(comparacion_q1)
print("Q1 idéntica a la local:", bool(comparacion_q1["igual"].all()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optimización de Q1 y planes de ejecución (preguntas 2 y 3 del informe)
# MAGIC
# MAGIC Las mismas tres versiones de `optimizar_q1.py`, que dan el mismo top 10:
# MAGIC - **V1 «join primero, sort-merge»:** se juntan las 21,6 M de reseñas con el catálogo y después se
# MAGIC   agrupa. Se fuerza un *sort-merge join* (reparte las reseñas por `app_id` por la red: shuffle grande).
# MAGIC - **V2 «join primero, broadcast»:** igual, pero el catálogo se copia entero a cada tarea.
# MAGIC - **V3 «agregar antes»:** primero se agrupa (21,6 M → 315 filas) y luego se hace el join (broadcast).
# MAGIC
# MAGIC **Cambio respecto a local:** allí V1 se forzaba poniendo `spark.sql.autoBroadcastJoinThreshold = -1`.
# MAGIC En serverless esa opción ni siquiera se puede leer (ver sección 1), así que V1 se fuerza con la
# MAGIC **pista** (*hint*) `merge` en el join, que dice lo mismo a nivel de consulta.
# MAGIC
# MAGIC Para cada versión: 2 ejecuciones (mejor tiempo, como en local), `explain("extended")` y
# MAGIC `explain("formatted")`, nº de *shuffles* (Exchange) y tipo de join. Los planes se guardan en el
# MAGIC volume `resultados/planes/` para compararlos con `evidence/fase5/` de la Entrega 1.

# COMMAND ----------

import contextlib
import io
import re

CARPETA_PLANES = f"{CARPETA_RESULTADOS}/planes"
os.makedirs(CARPETA_PLANES, exist_ok=True)


def texto_plan(df, modo):
    """Devuelve como texto lo que df.explain(modo) imprimiría por pantalla."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode=modo)
    return buffer.getvalue()


def resumen_plan(plan):
    """Cuenta shuffles y broadcasts y busca el tipo de join en el plan «formatted».
    Se mira la sección de DETALLES (los «Arguments:» de cada nodo), no el árbol:
    - Shuffle de verdad (filas repartidas por la red): «ENSURE_REQUIREMENTS».
      Vale para el «Exchange» de Spark y para el «PhotonShuffleMapStage» de Photon.
    - Broadcast: «BroadcastExchange» en Spark; en Photon el broadcast usa la misma
      maquinaria de shuffle (PhotonShuffleExchangeSink → MapStage → Source) pero su
      MapStage dice «EXECUTOR_BROADCAST». Una primera versión contaba esos nodos
      como shuffles (error corregido el 19 nov)."""
    arbol = plan.split("\n\n")[0]                    # el árbol va antes de los detalles
    shuffles = len(re.findall(r"Arguments: .*ENSURE_REQUIREMENTS", plan))
    broadcasts = (len(re.findall(r"Arguments: EXECUTOR_BROADCAST", plan))
                  + len(re.findall(r"BroadcastExchange \(\d+\)", arbol)))
    joins = sorted(set(re.findall(r"\b(\w*(?:SortMergeJoin|ShuffledHashJoin|BroadcastHashJoin|"
                                  r"BroadcastNestedLoopJoin))\b", arbol)))
    return shuffles, broadcasts, ", ".join(joins) or "otro", "Photon" in arbol


solo = resenas.select("app_id", "recommended")
cat = juegos_tabla.select("app_id", "nombre")
agregados = [F.count(F.lit(1)).alias("resenas"),
             F.sum(F.col("recommended").cast("int")).alias("positivas")]


def metricas(df):
    """Columnas finales comunes a las tres versiones (mismo resultado)."""
    return (df.withColumn("pct_positivas", F.round(100.0 * F.col("positivas") / F.col("resenas"), 2))
            .filter(F.col("resenas") >= PARAM["q1_min_resenas"])
            .orderBy(F.desc("pct_positivas"), F.desc("resenas"), "app_id")
            .limit(PARAM["q1_top"])
            .select("app_id", "nombre", "pct_positivas", "resenas"))


versiones = {
    "V1_join_primero_sortmerge": lambda: metricas(
        solo.join(cat.hint("merge"), "app_id").groupBy("app_id", "nombre").agg(*agregados)),
    "V2_join_primero_broadcast": lambda: metricas(
        solo.join(F.broadcast(cat), "app_id").groupBy("app_id", "nombre").agg(*agregados)),
    "V3_agregar_antes_broadcast": lambda: metricas(
        solo.groupBy("app_id").agg(*agregados).join(F.broadcast(cat), "app_id")),
}

solo.select("app_id").count()                       # calentamiento (como en local)

resumen, filas_por_version, planes = [], {}, {}
for nombre, construir in versiones.items():
    tiempos = []
    for _ in range(2):                              # 2 veces: se queda el mejor tiempo
        df = construir()
        t = time.perf_counter()
        filas = df.collect()
        tiempos.append(time.perf_counter() - t)
    extendido = texto_plan(df, "extended")
    formateado = texto_plan(df, "formatted")
    planes[nombre] = {"extended": extendido, "formatted": formateado}
    with open(f"{CARPETA_PLANES}/plan_{nombre}_extended.txt", "w", encoding="utf-8") as f:
        f.write(extendido)
    with open(f"{CARPETA_PLANES}/plan_{nombre}_formatted.txt", "w", encoding="utf-8") as f:
        f.write(formateado)
    shuffles, broadcasts, join, photon = resumen_plan(formateado)
    filas_por_version[nombre] = [tuple(f) for f in filas]
    resumen.append({"version": nombre, "mejor_s": round(min(tiempos), 2),
                    "tiempos_s": ", ".join(f"{x:.2f}" for x in tiempos),
                    "join": join, "shuffles": shuffles, "broadcasts": broadcasts,
                    "photon": photon})

iguales = len({tuple(v) for v in filas_por_version.values()}) == 1
print("Las 3 versiones dan el mismo top 10:", "SÍ" if iguales else "NO")
print("Local: V1 SortMergeJoin, 2 Exchange, 4,6 s · V2 broadcast 2,0 s · V3 1,1 s")
display(pd.DataFrame(resumen))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Plan completo de V1 (`explain("extended")`)
# MAGIC Plan lógico analizado → optimizado (Catalyst) → físico. Aquí se ven las optimizaciones
# MAGIC lógicas (poda de columnas, *predicate pushdown*…) y si el plan físico usa operadores **Photon**.

# COMMAND ----------

print(planes["V1_join_primero_sortmerge"]["extended"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Planes físicos de V1, V2 y V3 (`explain("formatted")`)

# COMMAND ----------

for nombre in versiones:
    print("=" * 100)
    print(nombre)
    print("=" * 100)
    print(planes[nombre]["formatted"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Qué muestran los planes: local frente a Databricks (1.ª ejecución, 19 nov)
# MAGIC
# MAGIC | | Local (Spark 4.2.0, Entrega 1) | Databricks serverless (Spark 4.2.0 + Photon) |
# MAGIC |---|---|---|
# MAGIC | Tiempo V1 / V2 / V3 | 4,6 s / 2,0 s / 1,1 s | 0,80 s / 0,70 s / 0,67 s (otra ejecución: 0,89 / 0,71 / 0,67) |
# MAGIC | Shuffles + broadcasts | V1: 2 + 0 · V2: 1 + 1 · V3: 1 + 1 | V1: 2 + 0 · V2: 2 + 1 · V3: 2 + 1 (el 2.º shuffle junta el top 10 en 1 partición) |
# MAGIC | Motor | Spark (`*` = *whole-stage codegen*) | **Photon** (`Photon…`) salvo el `SortMergeJoin`, que Photon no hace: vuelve al motor de Spark (`ColumnarToRow`) |
# MAGIC | V1: qué viaja en el shuffle de las reseñas | las **21,6 M filas** (`Exchange` de 494,7 MiB) | **agregado parcial antes del join** (`HashAggregate` debajo del `Exchange`): ~315 grupos por tarea |
# MAGIC | V2: agregación | después del join | Photon **adelanta una agregación parcial (`PhotonGroupingAgg` con `partial_count`) por debajo del join**: V2 se parece a V3 |
# MAGIC | Broadcast | `BroadcastExchange` | `PhotonShuffleExchangeSink` + `PhotonShuffleMapStage` con `EXECUTOR_BROADCAST` |
# MAGIC | Particiones de shuffle | 32 (fijado por nosotros) | `auto` (486 en el plan) |
# MAGIC | Top 10 final | `TakeOrderedAndProject` | `PhotonTopK` parcial + shuffle a 1 partición + `PhotonTopK` final |
# MAGIC | Plan mostrado | **final** (AQE resuelto, `isFinalPlan=true`, leído con `_jvm`) | **inicial** (con Spark Connect, `explain` vuelve a planificar; el plan final de AQE se ve en el *Query Profile* de la interfaz) |
# MAGIC
# MAGIC Conclusión: el optimizador de Databricks aplica por su cuenta una optimización que en local hicimos
# MAGIC **a mano** en V3 («agregar antes del join»). Por eso la diferencia entre V1, V2 y V3 es mucho menor
# MAGIC que en local.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Modelo de ML: ¿la reseña recomienda el juego?
# MAGIC
# MAGIC Mismo modelo que `src/ml/modelo_recomendacion.py` (decisiones 49-51 de la Entrega 1):
# MAGIC `VectorAssembler → StandardScaler → LogisticRegression`, 80 % entrenamiento / 20 % prueba,
# MAGIC semilla 42, evaluado con AUC (un modelo que dijera siempre «sí» acertaría el 87,4 %).
# MAGIC
# MAGIC Variables (todas conocidas al escribir la reseña, para no hacer trampa):
# MAGIC - de la reseña: log(1 + horas al reseñar), si faltan las horas, log(1 + longitud del texto),
# MAGIC   compra en Steam, recibido gratis, acceso anticipado, y los 10 idiomas más frecuentes;
# MAGIC - del catálogo: un sí/no por cada género presente en al menos el 10 % de los juegos.
# MAGIC
# MAGIC **Qué cambia en Databricks:**
# MAGIC - En local, el executor de 1 GB se quedó sin memoria 2 veces y hubo que leer el Parquet en trozos de
# MAGIC   32 MB (`maxPartitionBytes`). Aquí no se toca nada.
# MAGIC - Sin `persist()` (no disponible en serverless): las predicciones se calculan al evaluar.
# MAGIC - El modelo se entrena con Spark ML a través de Spark Connect.
# MAGIC - Resultado esperado: muy parecido, **no idéntico**. `randomSplit` reparte por particiones, y aquí los
# MAGIC   datos están en otros ficheros (Delta) y hay 7 reseñas más; los géneros salen del catálogo de noviembre.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler

PARAM_ML = {"idiomas_top": 10, "min_pct_juegos_genero": 10, "iteraciones_max": 50}

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


tiempos_ml = {}
inicio_ml = time.perf_counter()

# ---------------- 1) Catálogo: géneros frecuentes -----------------------------
t = time.perf_counter()
n_juegos = juegos_tabla.count()
conteo = (juegos_tabla.select(F.explode("generos").alias("genero"))
          .groupBy("genero").count().collect())                 # pocas filas: al driver
frecuentes = [(r["genero"], r["count"])
              for r in sorted(conteo, key=lambda r: (-r["count"], r["genero"]))
              if 100.0 * r["count"] / n_juegos >= PARAM_ML["min_pct_juegos_genero"]]
columnas_genero = {"genero_" + nombre_columna(g): g for g, _ in frecuentes}
print("Géneros en ≥ 10 % de los juegos:", ", ".join(f"{g} ({c})" for g, c in frecuentes))

lista_generos = F.coalesce(F.col("generos"), F.array().cast("array<string>"))
vars_juego = juegos_tabla.select(
    "app_id", *[F.array_contains(lista_generos, g).cast("double").alias(c)
                for c, g in columnas_genero.items()])

# ---------------- 2) Reseñas: solo las columnas necesarias --------------------
horas_ml = F.col("author_playtime_at_review") / 60.0               # minutos -> horas
datos_ml = (
    resenas.select(
        F.col("recommended").cast("double").alias("label"),
        "app_id",
        F.coalesce(F.col("language"), F.lit("desconocido")).alias("idioma"),
        F.log1p(F.greatest(F.coalesce(horas_ml, F.lit(0.0)), F.lit(0.0))).alias("log_horas_al_resenar"),
        F.col("author_playtime_at_review").isNull().cast("double").alias("sin_horas"),
        F.log1p(F.coalesce(F.col("review_length"), F.lit(0)).cast("double")).alias("log_longitud_texto"),
        a_numero("steam_purchase").alias("compra_steam"),
        a_numero("received_for_free").alias("recibido_gratis"),
        a_numero("written_during_early_access").alias("acceso_anticipado"))
    .join(F.broadcast(vars_juego), "app_id", "left")              # catálogo pequeño: broadcast
    .fillna(0.0, subset=list(columnas_genero))
)

# ---------------- 3) Separar entrenamiento / prueba ---------------------------
entrenamiento, prueba = datos_ml.randomSplit(
    [1.0 - PARAM["proporcion_test"], PARAM["proporcion_test"]], seed=PARAM["semilla"])

# Idiomas más frecuentes, contados SOLO en entrenamiento (la prueba no influye)
por_idioma = sorted(((r["idioma"], r["count"]) for r in
                     entrenamiento.groupBy("idioma").count().collect()),
                    key=lambda x: (-x[1], x[0]))
filas_entrenamiento = sum(n for _, n in por_idioma)
top_idiomas = [i for i, _ in por_idioma[:PARAM_ML["idiomas_top"]]]
columnas_idioma = {"idioma_" + nombre_columna(i): i for i in top_idiomas}


def con_idiomas(df):
    return df.select("*", *[(F.col("idioma") == i).cast("double").alias(c)
                            for c, i in columnas_idioma.items()])


entrenamiento, prueba = con_idiomas(entrenamiento), con_idiomas(prueba)
variables = list(VARIABLES_RESENA) + list(columnas_genero) + list(columnas_idioma)
tiempos_ml["preparar_s"] = round(time.perf_counter() - t, 1)
print(f"{len(variables)} variables · idiomas con columna propia: {', '.join(top_idiomas)}")

# ---------------- 4) Entrenar -------------------------------------------------
t = time.perf_counter()
modelo = Pipeline(stages=[
    VectorAssembler(inputCols=variables, outputCol="variables_crudas"),
    StandardScaler(inputCol="variables_crudas", outputCol="features", withMean=False, withStd=True),
    LogisticRegression(featuresCol="features", labelCol="label", maxIter=PARAM_ML["iteraciones_max"]),
]).fit(entrenamiento)
tiempos_ml["entrenar_s"] = round(time.perf_counter() - t, 1)
regresion = modelo.stages[-1]
try:
    iteraciones = regresion.summary.totalIterations
except Exception as error:                     # el resumen puede no estar disponible por Spark Connect
    iteraciones = f"(no disponible: {type(error).__name__})"
print(f"Entrenado en {tiempos_ml['entrenar_s']} s ({iteraciones} iteraciones)")

# COMMAND ----------

# ---------------- 5) Evaluar con los datos de prueba --------------------------
t = time.perf_counter()
predicciones = modelo.transform(prueba).select("label", "rawPrediction", "prediction")
evaluador = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
auc_roc = evaluador.setMetricName("areaUnderROC").evaluate(predicciones)
auc_pr = evaluador.setMetricName("areaUnderPR").evaluate(predicciones)
r_ml = predicciones.agg(
    F.count(F.lit(1)).alias("filas"),
    F.sum("label").alias("positivas"),
    F.sum((F.col("prediction") == F.col("label")).cast("int")).alias("aciertos"),
    F.sum((F.col("prediction") == 0).cast("int")).alias("predice_no")).first()
tiempos_ml["evaluar_s"] = round(time.perf_counter() - t, 1)
tiempos_ml["total_s"] = round(time.perf_counter() - inicio_ml, 1)

acierto = r_ml["aciertos"] / r_ml["filas"]
siempre_si = r_ml["positivas"] / r_ml["filas"]
coeficientes = sorted(zip(variables, regresion.coefficients.toArray().tolist()), key=lambda x: -abs(x[1]))

print(f"Filas: entrenamiento {filas_entrenamiento:,} · prueba {r_ml['filas']:,}  "
      "(local: 17.288.197 · 4.324.240)")
print(f"AUC ROC : {auc_roc:.4f}  (local 0,7451)")
print(f"AUC PR  : {auc_pr:.4f}  (local 0,9469)")
print(f"Acierto : {100 * acierto:.2f} % frente a «siempre sí» {100 * siempre_si:.2f} %  (local 87,41 % / 87,43 %)")
print(f"Predice NO: {r_ml['predice_no']:,}  (local 24.111)")
print(f"Tiempos: {tiempos_ml}  (local: total 281,3 s)")
display(pd.DataFrame(coeficientes + [("(intercepto)", regresion.intercept)],
                     columns=["variable", "coeficiente"]))

# --- Informe (equivalente a /resultados/ml/regresion_logistica.json) ---------
informe_ml = {
    "modelo": "LogisticRegression (VectorAssembler + StandardScaler)",
    "objetivo": "recommended", "semilla": PARAM["semilla"], "proporcion_test": PARAM["proporcion_test"],
    "filas": {"entrenamiento": filas_entrenamiento, "prueba": r_ml["filas"]},
    "metricas_prueba": {"auc_roc": round(auc_roc, 4), "auc_pr": round(auc_pr, 4),
                        "acierto_umbral_0_5": round(acierto, 4),
                        "acierto_diciendo_siempre_si": round(siempre_si, 4),
                        "predice_no": r_ml["predice_no"]},
    "iteraciones": iteraciones,
    "coeficientes": {n: round(v, 4) for n, v in coeficientes},
    "intercepto": round(regresion.intercept, 4),
    "tiempos": tiempos_ml,
}
dbutils.fs.put(f"{CARPETA_RESULTADOS}/ml/regresion_logistica.json",
               json.dumps(informe_ml, indent=2, ensure_ascii=False, default=str), overwrite=True)
print("Informe guardado en", f"{CARPETA_RESULTADOS}/ml/regresion_logistica.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Almacenamiento: replicación (HDFS) frente a versionado (Delta Lake) · pregunta 1
# MAGIC
# MAGIC **En local** éramos los administradores del almacenamiento: elegimos replicación 2 (2 DataNodes),
# MAGIC y con `hdfs fsck /raw/steam_reviews.csv -files -blocks -locations` veíamos los **61 bloques de
# MAGIC 128 MB**, cada uno con `Live_repl=2` y las direcciones de los dos DataNodes que lo guardaban. Si un
# MAGIC DataNode caía, nosotros teníamos que verlo (bloques sub-replicados) y arreglarlo.
# MAGIC
# MAGIC **En Databricks** no hay `fsck`: la tabla vive en almacenamiento de objetos del proveedor
# MAGIC (gestionado por Unity Catalog) y la durabilidad (copias, discos, zonas) es responsabilidad suya.
# MAGIC Lo que sí vemos es otra cosa distinta: el **historial de versiones** de Delta.
# MAGIC
# MAGIC Esta sección hace una prueba controlada sobre `workspace.steam.reviews`:
# MAGIC 1. mirar el historial y la ubicación física de la tabla;
# MAGIC 2. simular un **error humano** (borrar por equivocación las reseñas de 2021);
# MAGIC 3. leer la versión anterior (**time travel**, `VERSION AS OF`) y comprobar que los datos siguen ahí;
# MAGIC 4. deshacer el error con `RESTORE`;
# MAGIC 5. hacer una **copia de seguridad** de verdad (`DEEP CLONE`) de una tabla pequeña, para compararla.
# MAGIC
# MAGIC Las celdas son **independientes** del resto del notebook (definen lo que necesitan) y dejan la tabla
# MAGIC con los mismos datos que tenía.

# COMMAND ----------

from pyspark.sql import functions as F

TABLA = "workspace.steam.reviews"

# --- 10.1 Dónde están los datos y qué versiones hay -------------------------------
detalle = spark.sql(f"DESCRIBE DETAIL {TABLA}").first()
print("Formato      :", detalle["format"])
# En Free Edition la ubicación física sale VACÍA: el proveedor ni siquiera nos dice en
# qué almacenamiento de objetos están los ficheros (en local, fsck daba hasta la IP de
# cada DataNode). Solo vemos el identificador de la tabla en Unity Catalog.
print("Ubicación    :", detalle["location"] or "(oculta por la plataforma)")
print("Id de tabla  :", detalle["id"])
print("Ficheros     :", detalle["numFiles"], "·", f"{detalle['sizeInBytes'] / 1024**2:.1f} MiB")
print("Particiones  :", detalle["partitionColumns"])
print("Propiedades  :", detalle["properties"])

historial = spark.sql(f"DESCRIBE HISTORY {TABLA}")
display(historial.select("version", "timestamp", "operation", "operationMetrics.numOutputRows",
                         "operationMetrics.numFiles"))

# COMMAND ----------

# --- 10.2 Simular un error humano: borrar las reseñas de 2021 ----------------------
version_buena = historial.agg(F.max("version")).first()[0]   # versión antes del "error"
filas_antes = spark.table(TABLA).count()
de_2021 = spark.table(TABLA).filter("year = 2021").count()

spark.sql(f"DELETE FROM {TABLA} WHERE year = 2021")          # ← el «error»

filas_despues = spark.table(TABLA).count()
print(f"Versión buena (antes del error): {version_buena}")
print(f"Filas antes: {filas_antes:,} · reseñas de 2021: {de_2021:,} · filas después del DELETE: {filas_despues:,}")
assert filas_antes - filas_despues == de_2021

# COMMAND ----------

# --- 10.3 Time travel: leer la tabla tal como estaba ------------------------------
# La versión anterior NO se ha perdido: Delta no borra los ficheros al hacer DELETE,
# solo apunta en su registro (_delta_log) que ya no forman parte de la versión actual.
version_error = spark.sql(f"DESCRIBE HISTORY {TABLA}").agg(F.max("version")).first()[0]
pasado = spark.sql(f"SELECT count(*) AS filas FROM {TABLA} VERSION AS OF {version_buena}").first()["filas"]
pasado_2021 = spark.read.option("versionAsOf", version_buena).table(TABLA).filter("year = 2021").count()
print(f"Versión actual (con el error): {version_error} → {filas_despues:,} filas")
print(f"VERSION AS OF {version_buena}: {pasado:,} filas · de 2021: {pasado_2021:,}")

# COMMAND ----------

# --- 10.4 Deshacer el error con RESTORE -------------------------------------------
# RESTORE no copia datos: crea una versión NUEVA que vuelve a apuntar a los ficheros
# de la versión buena. El error sigue en el historial (auditoría).
display(spark.sql(f"RESTORE TABLE {TABLA} TO VERSION AS OF {version_buena}"))
filas_restauradas = spark.table(TABLA).count()
print(f"Filas tras RESTORE: {filas_restauradas:,}  (antes del error: {filas_antes:,})")
assert filas_restauradas == filas_antes, "RESTORE no ha recuperado todas las filas"

display(spark.sql(f"DESCRIBE HISTORY {TABLA}")
        .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
        .limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 Copia de seguridad (`DEEP CLONE`) frente a versionado
# MAGIC El historial de Delta vive **dentro** de la propia tabla: si se borra la tabla (`DROP TABLE`), o si
# MAGIC `VACUUM` elimina los ficheros antiguos (por defecto, los de más de 7 días), las versiones antiguas
# MAGIC desaparecen con ella. Una copia de seguridad es una copia **independiente**. `DEEP CLONE` copia
# MAGIC datos y metadatos a otra tabla; se hace con el catálogo de juegos (315 filas) para no duplicar
# MAGIC los 300 MB de reseñas en la cuenta gratuita.

# COMMAND ----------

ORIGEN, COPIA = "workspace.steam.games", "workspace.steam.games_backup"
spark.sql(f"CREATE OR REPLACE TABLE {COPIA} DEEP CLONE {ORIGEN}")

d_origen = spark.sql(f"DESCRIBE DETAIL {ORIGEN}").first()
d_copia = spark.sql(f"DESCRIBE DETAIL {COPIA}").first()
# La ubicación física está oculta en Free Edition, así que se comparan los
# identificadores de tabla: si son distintos, son dos tablas con sus propios ficheros.
print("Origen:", d_origen["id"], "·", d_origen["numFiles"], "ficheros ·", d_origen["sizeInBytes"], "bytes")
print("Copia :", d_copia["id"], "·", d_copia["numFiles"], "ficheros ·", d_copia["sizeInBytes"], "bytes")
print("¿Tabla independiente?", "SÍ" if d_origen["id"] != d_copia["id"] else "NO")
print("Filas origen / copia:", spark.table(ORIGEN).count(), "/", spark.table(COPIA).count())
display(spark.sql(f"DESCRIBE HISTORY {COPIA}").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.6 Resumen para la pregunta 1
# MAGIC
# MAGIC | | Replicación (HDFS, Entrega 1) | Copia de seguridad (`DEEP CLONE`) | Versionado (Delta Lake) |
# MAGIC |---|---|---|---|
# MAGIC | Qué guarda | La **misma** versión de cada bloque en varios discos | Una copia **independiente** de la tabla en otro sitio | Todas las **versiones** de la tabla (registro `_delta_log`) |
# MAGIC | Protege de | Fallo de un disco o un nodo | Pérdida o borrado de la tabla original | Errores lógicos (DELETE/UPDATE/overwrite equivocados) |
# MAGIC | No protege de | Un `DELETE` equivocado (se replica también el error) | Cambios hechos después de la copia | `DROP TABLE`, `VACUUM`, pérdida del almacenamiento |
# MAGIC | Quién lo gestiona | Nosotros: `dfs.replication=2`, `setrep`, `hdfs fsck` | Nosotros (decidir cuándo y dónde copiar) | Delta, automático en cada escritura |
# MAGIC | En Databricks | Lo hace el proveedor de la nube (no visible) | `DEEP CLONE` | `DESCRIBE HISTORY`, `VERSION AS OF`, `RESTORE` |
