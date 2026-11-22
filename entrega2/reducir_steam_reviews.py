#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reducir_steam_reviews.py
------------------------
Genera un subconjunto (<= 2 GB) del dataset "Steam Reviews Dataset 2021"
(Kaggle: najzeko/steam-reviews-2021, fichero steam_reviews.csv, ~8,17 GB,
21.747.371 reseñas de 315 juegos) para poder subirlo a Databricks Free Edition.

Pensado para el proyecto del equipo (repo Marc456-jpg/BigDataTeam).

MODO "proyecto" (por defecto, el que se usa para la entrega):
  Se conservan TODAS las reseñas (21,7 M) y solo las columnas que necesita el
  pipeline de la Entrega 1:
    - Q1 (% positivas por juego, >= 1.000 reseñas): app_id, recommended
    - Q2 (horas al reseñar, recomienda vs. no): recommended,
      author.playtime_at_review
    - Q3 (top 5 juegos por idioma según votos útiles en 2020): language,
      app_id, votes_helpful, timestamp_created
    - ETL (fechas, duplicados, informe de calidad): review_id,
      timestamp_created, timestamp_updated, votes_funny
    - ML (regresión logística): recommended, language, author.playtime_at_review,
      steam_purchase, received_for_free, written_during_early_access
      y la LONGITUD del texto -> el texto se sustituye por review_length.
  El texto de la reseña es la mayor parte de los 8 GB y ninguna pregunta lo
  usa salvo su longitud. Así no se pierde ninguna fila y los resultados en
  Databricks se pueden comparar directamente con los del clúster local.

MODO "completo": todas las columnas (con texto) + muestreo aleatorio uniforme
  (semilla fija) hasta el tamaño máximo.

Funcionamiento (lectura por bloques, nunca carga el CSV entero en RAM):
  1) Filtros opcionales (idiomas, juegos, fechas...).
  2) Pasada 1: mide cuánto ocuparía la salida.
  3) Si supera el máximo, calcula la fracción de muestreo y avisa.
  4) Pasada 2: escribe la salida.
  5) Guarda un resumen (*_resumen.txt) para el informe.

Uso: ejecutar con ▶ en VS Code (usa RUTA_ENTRADA / RUTA_SALIDA de abajo) o
    python reducir_steam_reviews.py [entrada.csv|zip] [salida.csv] [opciones]

Ejemplos:
    python reducir_steam_reviews.py --solo-medir
    python reducir_steam_reviews.py --modo completo --idiomas english spanish

Requisitos: Python 3.8+ y pandas.
"""

import argparse
import csv
import io
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# ===========================================================================
# CONFIGURACIÓN RÁPIDA (para ejecutar con el botón ▶ / F5 de VS Code)
# Si ejecutas el script sin argumentos, se usan estas rutas.
# Usa r"..." en Windows para que las barras \ no den problemas.
# ===========================================================================
# Origen de los datos:
#   USAR_KAGGLEHUB = True  -> descarga el dataset con kagglehub (la primera vez
#                             tarda; después usa la copia en caché) y lo reduce.
#   USAR_KAGGLEHUB = False -> usa el CSV de RUTA_ENTRADA.
USAR_KAGGLEHUB = True
KAGGLE_DATASET = "najzeko/steam-reviews-2021"
RUTA_ENTRADA = r"C:\steam-bigdata\data\steam_reviews.csv"

# Fichero reducido: por defecto, en la misma carpeta que este script.
RUTA_SALIDA = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "steam_reviews_databricks.csv")

# Modo por defecto:
#   "proyecto" -> TODAS las filas, solo las columnas que usa el pipeline de la
#                 Entrega 1 (Q1, Q2, Q3, ETL y modelo ML) y el texto de la
#                 reseña sustituido por su longitud (review_length).
#   "completo" -> todas las columnas (con texto) + muestreo hasta el límite.
MODO_POR_DEFECTO = "proyecto"

# Columnas del modo "proyecto" y para qué se usa cada una:
COLUMNAS_PROYECTO = [
    "app_id",                      # Q1, Q3, ML: clave del juego (join con catálogo)
    "review_id",                   # ETL: detección de reseñas duplicadas
    "language",                    # Q3 (partición de la ventana) y ML
    "timestamp_created",           # ETL: year/month, filtro de fechas; Q3 (2020)
    "timestamp_updated",           # ETL: en duplicados se queda la más reciente
    "recommended",                 # Q1 (% positivas), Q2, ML (etiqueta)
    "votes_helpful",               # Q3 (suma de votos útiles)
    "votes_funny",                 # ETL: informe de calidad (contador desbordado)
    "steam_purchase",              # ML: compra en Steam
    "received_for_free",           # ML: recibido gratis
    "written_during_early_access", # ML: acceso anticipado
    "author.playtime_at_review",   # Q2 (horas al reseñar) y ML
    "review_length",               # ML: longitud del texto (= length(review))
]
# Se deja fuera weighted_vote_score: solo alimentaba un aviso del informe de
# calidad («fuera de [0,1]»), que en la Entrega 1 dio 0 filas, y sus decimales
# largos (p. ej. 0.523809552192688) ocupan ~100-150 MB que no caben con margen.
# Si RUTA_ENTRADA no existe, el script busca automáticamente
# steam_reviews.csv (o un .zip que lo contenga) en estas carpetas y subcarpetas.
CARPETAS_BUSQUEDA = [
    r"C:\steam-bigdata",
    os.path.dirname(os.path.abspath(__file__)),
    os.path.join(os.path.expanduser("~"), "Downloads"),
    os.path.join(os.path.expanduser("~"), "Descargas"),
    os.path.join(os.path.expanduser("~"), "Desktop"),
    os.path.join(os.path.expanduser("~"), "Documents"),
]
NOMBRE_CSV = "steam_reviews.csv"

# ---------------------------------------------------------------------------
# Columnas del CSV original (la primera columna no tiene nombre: es el índice
# que se guardó con pandas al crear el dataset).
# ---------------------------------------------------------------------------
COLUMNAS_ORIGINALES = [
    "", "app_id", "app_name", "review_id", "language", "review",
    "timestamp_created", "timestamp_updated", "recommended", "votes_helpful",
    "votes_funny", "weighted_vote_score", "comment_count", "steam_purchase",
    "received_for_free", "written_during_early_access", "author.steamid",
    "author.num_games_owned", "author.num_reviews", "author.playtime_forever",
    "author.playtime_last_two_weeks", "author.playtime_at_review",
    "author.last_played",
]

# Columnas que se descartan por defecto (no aportan al análisis):
#   - la columna índice sin nombre ("Unnamed: 0")
COLUMNAS_DESCARTAR_POR_DEFECTO = ["Unnamed: 0"]

BYTES_POR_GB = 1_000_000_000  # GB decimal (como Kaggle y Databricks)


# ---------------------------------------------------------------------------
# Argumentos
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Reduce el CSV de Steam Reviews 2021 a un tamaño máximo "
                    "(por defecto 2 GB) aplicando filtros y muestreo uniforme.")
    p.add_argument("entrada", nargs="?", default=None,
                   help="Ruta a steam_reviews.csv (original). "
                        "Si no se indica: kagglehub o RUTA_ENTRADA.")
    p.add_argument("salida", nargs="?", default=RUTA_SALIDA,
                   help="Ruta del CSV reducido a generar. "
                        "Por defecto RUTA_SALIDA.")

    # Tamaño
    p.add_argument("--max-gb", type=float, default=1.95,
                   help="Tamaño máximo del fichero de salida en GB (decimal). "
                        "Por defecto 1.95 para dejar margen bajo los 2 GB.")

    # Modo
    p.add_argument("--modo", choices=["proyecto", "completo"],
                   default=MODO_POR_DEFECTO,
                   help="proyecto: todas las filas y solo las columnas del "
                        "pipeline (texto -> review_length). completo: todas "
                        "las columnas y muestreo hasta el límite.")

    # Columnas
    p.add_argument("--columnas", nargs="+", default=None,
                   help="Lista de columnas a conservar (por defecto todas "
                        "menos el índice sin nombre).")
    p.add_argument("--sin-texto", action="store_true",
                   help="Elimina la columna 'review' (el texto). Reduce mucho "
                        "el tamaño; úsalo solo si no hacéis análisis de texto.")
    p.add_argument("--max-caracteres-review", type=int, default=None,
                   help="Trunca el texto de cada reseña a N caracteres.")

    # Filtros de filas
    p.add_argument("--idiomas", nargs="+", default=None,
                   help="Idiomas a conservar (ej.: english spanish schinese).")
    p.add_argument("--juegos", nargs="+", default=None,
                   help="app_id o app_name exactos de los juegos a conservar.")
    p.add_argument("--top-juegos", type=int, default=None,
                   help="Conserva solo los N juegos con más reseñas "
                        "(se calcula en una pasada previa).")
    p.add_argument("--desde", default=None,
                   help="Fecha mínima de creación de la reseña (YYYY-MM-DD).")
    p.add_argument("--hasta", default=None,
                   help="Fecha máxima de creación de la reseña (YYYY-MM-DD).")
    p.add_argument("--solo-compras-steam", action="store_true",
                   help="Solo reseñas con steam_purchase = True.")
    p.add_argument("--excluir-gratis", action="store_true",
                   help="Excluye reseñas con received_for_free = True.")
    p.add_argument("--excluir-early-access", action="store_true",
                   help="Excluye reseñas escritas en Early Access.")
    p.add_argument("--min-caracteres", type=int, default=None,
                   help="Excluye reseñas con texto de menos de N caracteres.")
    p.add_argument("--min-playtime-horas", type=float, default=None,
                   help="Excluye reseñas de autores con menos de N horas "
                        "jugadas en el momento de la reseña.")

    # Muestreo
    p.add_argument("--min-por-juego", type=int, default=0,
                   help="Garantiza al menos N reseñas por juego (si existen), "
                        "aunque el muestreo uniforme le asigne menos.")
    p.add_argument("--semilla", type=int, default=42,
                   help="Semilla aleatoria (reproducibilidad).")

    # Ejecución
    p.add_argument("--chunksize", type=int, default=250_000,
                   help="Filas por bloque de lectura (bájalo si falta RAM).")
    p.add_argument("--solo-medir", action="store_true",
                   help="Solo calcula el tamaño tras filtrar; no escribe.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Lectura por bloques
# ---------------------------------------------------------------------------
def leer_bloques(ruta, chunksize, usecols=None):
    """Lee el CSV en bloques como texto (dtype=str) para no alterar el
    formato original de los valores al reescribirlos."""
    return pd.read_csv(
        ruta,
        chunksize=chunksize,
        dtype=str,
        keep_default_na=False,   # celdas vacías se quedan como "" (no NaN)
        na_filter=False,
        usecols=usecols,
        encoding="utf-8-sig",      # el CSV original empieza con BOM
        encoding_errors="replace",
        on_bad_lines="skip",
        engine="c",
        low_memory=False,
    )


def zip_contiene_csv(ruta_zip):
    import zipfile
    try:
        with zipfile.ZipFile(ruta_zip) as z:
            nombres = [n for n in z.namelist() if n.lower().endswith(".csv")]
            return len(nombres) == 1 and \
                os.path.basename(nombres[0]).lower() == NOMBRE_CSV
    except Exception:
        return False


def descargar_con_kagglehub():
    """Descarga el dataset con kagglehub y devuelve la ruta del CSV."""
    try:
        import kagglehub
    except ImportError:
        sys.exit("Falta la librería kagglehub. Instálala con:\n"
                 "    pip install kagglehub")
    print(f"Descargando '{KAGGLE_DATASET}' con kagglehub "
          "(la primera vez son ~3 GB comprimidos; luego usa la caché)...")
    carpeta = kagglehub.dataset_download(KAGGLE_DATASET)
    print("Path to dataset files:", carpeta)
    if os.path.isfile(carpeta):
        return carpeta
    for raiz, _, ficheros in os.walk(carpeta):
        for f in ficheros:
            if f.lower() == NOMBRE_CSV:
                return os.path.join(raiz, f)
    for raiz, _, ficheros in os.walk(carpeta):        # por si viene zip
        for f in ficheros:
            if f.lower().endswith(".zip") and \
                    zip_contiene_csv(os.path.join(raiz, f)):
                return os.path.join(raiz, f)
    sys.exit(f"kagglehub descargó en {carpeta}, pero no se encontró "
             f"'{NOMBRE_CSV}' dentro.")


def buscar_entrada():
    """Busca steam_reviews.csv (o un .zip de Kaggle que lo contenga)."""
    print(f"Buscando '{NOMBRE_CSV}' o un .zip que lo contenga en:")
    zips = []
    for base in CARPETAS_BUSQUEDA:
        if not os.path.isdir(base):
            continue
        print(f"  - {base}")
        base_nivel = base.rstrip(os.sep).count(os.sep)
        for raiz, dirs, ficheros in os.walk(base):
            if raiz.count(os.sep) - base_nivel >= 3:   # máx. 3 niveles
                dirs[:] = []
            for f in ficheros:
                ruta = os.path.join(raiz, f)
                if f.lower() == NOMBRE_CSV:
                    return ruta
                if f.lower().endswith(".zip") and \
                        os.path.getsize(ruta) > 500_000_000:
                    zips.append(ruta)
    for z in zips:
        if zip_contiene_csv(z):
            return z
    return None


def a_num(serie):
    return pd.to_numeric(serie, errors="coerce")


def es_true(serie):
    return serie.str.strip().str.lower().isin(["true", "1", "1.0"])


def fecha_a_epoch(texto, fin_de_dia=False):
    d = datetime.strptime(texto, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    ts = int(d.timestamp())
    return ts + 86399 if fin_de_dia else ts


# ---------------------------------------------------------------------------
# Filtros
# ---------------------------------------------------------------------------
class Filtros:
    def __init__(self, args, juegos_permitidos=None):
        self.args = args
        self.idiomas = set(i.lower() for i in args.idiomas) if args.idiomas else None
        self.juegos = set(args.juegos) if args.juegos else None
        self.juegos_top = juegos_permitidos  # set de app_id (o None)
        self.ts_desde = fecha_a_epoch(args.desde) if args.desde else None
        self.ts_hasta = fecha_a_epoch(args.hasta, fin_de_dia=True) if args.hasta else None

    def aplicar(self, df):
        a = self.args
        m = pd.Series(True, index=df.index)

        if self.idiomas is not None:
            m &= df["language"].str.lower().isin(self.idiomas)
        if self.juegos is not None:
            m &= df["app_id"].isin(self.juegos) | df["app_name"].isin(self.juegos)
        if self.juegos_top is not None:
            m &= df["app_id"].isin(self.juegos_top)
        if self.ts_desde is not None or self.ts_hasta is not None:
            ts = a_num(df["timestamp_created"])
            if self.ts_desde is not None:
                m &= ts >= self.ts_desde
            if self.ts_hasta is not None:
                m &= ts <= self.ts_hasta
        if a.solo_compras_steam:
            m &= es_true(df["steam_purchase"])
        if a.excluir_gratis:
            m &= ~es_true(df["received_for_free"])
        if a.excluir_early_access:
            m &= ~es_true(df["written_during_early_access"])
        if a.min_caracteres is not None:
            m &= df["review"].str.len() >= a.min_caracteres
        if a.min_playtime_horas is not None:
            # playtime viene en minutos
            m &= a_num(df["author.playtime_at_review"]) >= a.min_playtime_horas * 60

        return df[m]


def preparar_columnas(df, columnas_salida, args):
    if "review_length" in columnas_salida:
        # Igual que F.length("review") en Spark: nº de caracteres; reseña
        # vacía -> celda vacía (Spark la leerá como null).
        df = df.copy()
        largo = df["review"].str.len()
        df["review_length"] = largo.astype(str).where(largo > 0, "")
    if args.max_caracteres_review and "review" in columnas_salida:
        df = df.copy()
        df["review"] = df["review"].str.slice(0, args.max_caracteres_review)
    return df[columnas_salida]


def a_csv_bytes(df, cabecera):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=cabecera, quoting=csv.QUOTE_MINIMAL,
              lineterminator="\n")
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# Utilidades de log
# ---------------------------------------------------------------------------
def fmt_gb(n_bytes):
    return f"{n_bytes / BYTES_POR_GB:,.3f} GB"


def progreso(fase, filas_leidas, t0):
    dt = time.time() - t0
    vel = filas_leidas / dt if dt > 0 else 0
    print(f"\r  [{fase}] {filas_leidas:,} filas leídas "
          f"({vel:,.0f} filas/s, {dt:,.0f} s)", end="", flush=True)


# ---------------------------------------------------------------------------
# Principal
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    if args.entrada is None:
        if USAR_KAGGLEHUB:
            args.entrada = descargar_con_kagglehub()
        else:
            args.entrada = RUTA_ENTRADA
    if not os.path.exists(args.entrada):
        print(f"No existe: {args.entrada}")
        encontrado = buscar_entrada()
        if encontrado is None:
            sys.exit(
                f"\nNo se ha encontrado '{NOMBRE_CSV}' ni un .zip que lo "
                "contenga.\nDescárgalo de Kaggle (najzeko/steam-reviews-2021) "
                "y pon su ruta en RUTA_ENTRADA, al principio del script.\n"
                "Truco: en el Explorador de Windows, clic derecho sobre el "
                "fichero -> 'Copiar como ruta' y pégala entre r\"...\".")
        print(f"Encontrado: {encontrado}\n")
        args.entrada = encontrado
    if args.entrada.lower().endswith(".zip"):
        print("Se leerá directamente del .zip (no hace falta descomprimir).")

    objetivo = int(args.max_gb * BYTES_POR_GB)
    rng = np.random.default_rng(args.semilla)

    # --- Columnas de salida -------------------------------------------------
    cabecera = pd.read_csv(args.entrada, nrows=0,
                           encoding="utf-8-sig").columns.tolist()
    if not args.columnas and args.modo == "proyecto":
        args.columnas = list(COLUMNAS_PROYECTO)
    if args.columnas:
        faltan = [c for c in args.columnas
                  if c not in cabecera and c != "review_length"]
        if faltan:
            sys.exit(f"Columnas inexistentes: {faltan}\nDisponibles: {cabecera}")
        columnas_salida = list(args.columnas)
    else:
        columnas_salida = [c for c in cabecera
                           if c not in COLUMNAS_DESCARTAR_POR_DEFECTO]
    if args.sin_texto and "review" in columnas_salida:
        columnas_salida.remove("review")

    # Columnas necesarias para filtrar aunque no se escriban
    necesarias_filtro = {"app_id", "app_name", "language", "timestamp_created",
                         "steam_purchase", "received_for_free",
                         "written_during_early_access",
                         "author.playtime_at_review"}
    if args.min_caracteres is not None or "review_length" in columnas_salida:
        necesarias_filtro.add("review")
    usecols = [c for c in cabecera
               if c in set(columnas_salida) | necesarias_filtro]

    print("=" * 70)
    print("Reducción de Steam Reviews 2021")
    print("=" * 70)
    print(f"Entrada : {args.entrada} ({fmt_gb(os.path.getsize(args.entrada))})")
    print(f"Salida  : {args.salida}")
    print(f"Objetivo: <= {fmt_gb(objetivo)}")
    print(f"Columnas de salida ({len(columnas_salida)}): {columnas_salida}")

    # --- Pasada 0 (opcional): top-N juegos ----------------------------------
    juegos_permitidos = None
    if args.top_juegos:
        print(f"\nPasada 0: contando reseñas por juego (top {args.top_juegos})...")
        cont = Counter()
        t0, leidas = time.time(), 0
        for bloque in leer_bloques(args.entrada, args.chunksize,
                                   usecols=["app_id"]):
            cont.update(bloque["app_id"].tolist())
            leidas += len(bloque)
            progreso("conteo", leidas, t0)
        juegos_permitidos = {j for j, _ in cont.most_common(args.top_juegos)}
        print()

    filtros = Filtros(args, juegos_permitidos)

    # --- Pasada 1: medir tamaño tras filtrar --------------------------------
    print("\nPasada 1: aplicando filtros y midiendo tamaño...")
    t0 = time.time()
    leidas = filtradas = bytes_filtrados = 0
    filas_por_juego = Counter()
    primera = True
    for bloque in leer_bloques(args.entrada, args.chunksize, usecols=usecols):
        leidas += len(bloque)
        f = filtros.aplicar(bloque)
        if len(f):
            filtradas += len(f)
            filas_por_juego.update(f["app_id"].tolist())
            bytes_filtrados += len(a_csv_bytes(
                preparar_columnas(f, columnas_salida, args), primera))
            primera = False
        progreso("medición", leidas, t0)
    print()
    print(f"  Filas leídas      : {leidas:,}")
    print(f"  Filas tras filtros: {filtradas:,} "
          f"({100 * filtradas / max(leidas, 1):.1f} %)")
    print(f"  Tamaño estimado   : {fmt_gb(bytes_filtrados)}")

    if filtradas == 0:
        sys.exit("Ningún registro cumple los filtros. Revisa los parámetros.")

    fraccion = min(1.0, objetivo / bytes_filtrados)
    # Pequeño margen de seguridad por la variabilidad del muestreo
    if fraccion < 1.0:
        fraccion *= 0.995
        if args.modo == "proyecto":
            print("  AVISO: en modo proyecto no caben todas las filas; se "
                  "muestreará. Para evitarlo, quita columnas, p. ej.:\n"
                  "    --columnas app_id review_id language timestamp_created "
                  "timestamp_updated recommended votes_helpful "
                  "author.playtime_at_review review_length")
    print(f"  Fracción de muestreo: {fraccion:.4f} "
          f"(~{int(filtradas * fraccion):,} filas)")

    # Probabilidad por juego para garantizar el mínimo por juego
    prob_juego = {}
    if args.min_por_juego > 0 and fraccion < 1.0:
        for juego, n in filas_por_juego.items():
            minimo = min(n, args.min_por_juego)
            prob_juego[juego] = max(fraccion, minimo / n)

    if args.solo_medir:
        print("\n--solo-medir activado: no se escribe el fichero.")
        return

    # --- Pasada 2: muestrear y escribir --------------------------------------
    print("\nPasada 2: muestreando y escribiendo...")
    t0 = time.time()
    leidas = escritas = bytes_escritos = 0
    por_juego, por_idioma, por_anio, por_rec = Counter(), Counter(), Counter(), Counter()
    primera = True
    tmp = args.salida + ".tmp"
    with open(tmp, "wb") as out:
        for bloque in leer_bloques(args.entrada, args.chunksize, usecols=usecols):
            leidas += len(bloque)
            f = filtros.aplicar(bloque)
            if len(f) and fraccion < 1.0:
                if prob_juego:
                    p = f["app_id"].map(prob_juego).fillna(fraccion).to_numpy()
                else:
                    p = fraccion
                f = f[rng.random(len(f)) < p]
            if len(f):
                datos = a_csv_bytes(preparar_columnas(f, columnas_salida, args),
                                    primera)
                # Corte de seguridad: nunca superar el objetivo
                if bytes_escritos + len(datos) > objetivo:
                    print("\n  Aviso: se alcanzó el límite de tamaño; "
                          "se detiene la escritura.")
                    break
                out.write(datos)
                primera = False
                bytes_escritos += len(datos)
                escritas += len(f)
                por_juego.update(f["app_name"].tolist())
                por_idioma.update(f["language"].tolist())
                anios = pd.to_datetime(a_num(f["timestamp_created"]),
                                       unit="s", errors="coerce").dt.year
                por_anio.update(anios.dropna().astype(int).tolist())
                por_rec.update(f["recommended"].tolist()
                               if "recommended" in f.columns else [])
            progreso("escritura", leidas, t0)
    os.replace(tmp, args.salida)
    print()

    tam_final = os.path.getsize(args.salida)

    # --- Resumen -------------------------------------------------------------
    lineas = []
    lineas.append("RESUMEN DE LA REDUCCIÓN")
    lineas.append(f"Fecha: {datetime.now():%Y-%m-%d %H:%M}")
    lineas.append(f"Parámetros: {vars(args)}")
    lineas.append(f"Filas originales      : {leidas:,}")
    lineas.append(f"Filas tras filtros    : {filtradas:,}")
    lineas.append(f"Fracción de muestreo  : {fraccion:.4f}")
    lineas.append(f"Filas en la salida    : {escritas:,}")
    lineas.append(f"Tamaño final          : {fmt_gb(tam_final)} ({tam_final:,} bytes)")
    lineas.append(f"Juegos distintos      : {len(por_juego)}")
    lineas.append(f"Idiomas distintos     : {len(por_idioma)}")
    lineas.append("\nReseñas por año:")
    for a, n in sorted(por_anio.items()):
        lineas.append(f"  {a}: {n:,}")
    lineas.append("\nTop 15 idiomas:")
    for i, n in por_idioma.most_common(15):
        lineas.append(f"  {i:<15} {n:>12,}  ({100 * n / escritas:5.2f} %)")
    lineas.append("\nTop 20 juegos:")
    for j, n in por_juego.most_common(20):
        lineas.append(f"  {j[:45]:<45} {n:>10,}")
    if por_rec:
        lineas.append("\nRecomendado:")
        for r, n in por_rec.most_common():
            lineas.append(f"  {r:<6} {n:>12,}  ({100 * n / escritas:5.2f} %)")

    resumen = "\n".join(lineas)
    print("\n" + resumen)
    ruta_resumen = os.path.splitext(args.salida)[0] + "_resumen.txt"
    with open(ruta_resumen, "w", encoding="utf-8") as fh:
        fh.write(resumen + "\n")
    print(f"\nResumen guardado en: {ruta_resumen}")

    if tam_final > 2 * BYTES_POR_GB:
        print("ATENCIÓN: el fichero supera 2 GB. Baja --max-gb.")
    else:
        print("OK: el fichero está por debajo de 2 GB.")


if __name__ == "__main__":
    main()
