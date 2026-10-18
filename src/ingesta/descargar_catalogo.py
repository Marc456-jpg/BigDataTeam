#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
descargar_catalogo.py · Descarga el catálogo de los juegos de nuestras reseñas
desde la API pública de la tienda de Steam (tabla auxiliar del proyecto).

Por qué existe: la inscripción pide una tabla auxiliar con la que hacer un join.
La nuestra es el catálogo de juegos (nombre, géneros, desarrolladores, precio,
fecha de salida), unido a las reseñas por «app_id». La pregunta Q1 y el ML la
necesitan. Se usa la API de la tienda de Steam, como se dijo en la inscripción
(decisión 48):
  - es pública: no necesita cuenta ni clave;
  - solo se piden NUESTROS juegos (los 315 app_id distintos de las reseñas).

Qué hace, en orden:
  1. Si el archivo crudo ya existe en HDFS, NO descarga nada (idempotente;
     --forzar para repetir).
  2. Lee de la zona curada la lista de app_id distintos (Spark, segundos).
  3. Pide a la API los datos de cada juego, uno a uno, con una pausa entre
     peticiones (Steam admite unas 200 cada 5 minutos) y reintentos si Steam
     responde «demasiadas peticiones» o falla la red.
  4. Guarda las respuestas TAL CUAL (zona raw) en HDFS, una línea JSON por
     juego: {"app_id": ..., "descargado_utc": ..., "respuesta": {...}}.
     La limpieza a Parquet es otro paso (mini-paso 3.8).

Idioma inglés y país EE. UU. fijos (config/pipeline.yaml), para que los datos
no cambien según dónde se ejecute. Limitación conocida: los precios y géneros
son los de HOY, no los de la fecha de las reseñas (2010-2021).

Uso (desde PowerShell, en C:\\steam-bigdata; tarda ≈ 8-10 min):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/ingesta/descargar_catalogo.py

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"


def pedir(url, reintentos, pausa):
    """Pide una URL y devuelve el JSON. Si Steam responde 429 (demasiadas
    peticiones), un error 5xx, un fallo de red o un cuerpo vacío («null»),
    espera cada vez el doble y lo vuelve a intentar. Si se agotan los
    reintentos, devuelve None (el juego queda registrado como no descargado)."""
    espera = pausa
    for intento in range(reintentos + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                datos = json.loads(r.read().decode("utf-8"))
            if datos is not None:
                return datos
            motivo = "respuesta vacia"
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                return None                      # error definitivo (p. ej. 404)
            motivo = "HTTP {}".format(e.code)
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            motivo = type(e).__name__
        if intento < reintentos:
            espera *= 2
            print("           reintento {} en {:.0f} s ({})".format(intento + 1, espera, motivo))
            time.sleep(espera)
    return None


def main():
    parser = argparse.ArgumentParser(description="Descarga el catalogo de juegos de la API de Steam")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--master", default=None, help="por defecto, el del archivo de configuracion")
    parser.add_argument("--forzar", action="store_true", help="descargar aunque ya exista")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    cfg_spark, cfg_cat = cfg["spark"], cfg["catalogo"]

    spark = (
        SparkSession.builder
        .appName("descargar_catalogo")
        .master(args.master or cfg_spark["master"])
        .config("spark.driver.memory", cfg_spark["memoria_driver"])
        .config("spark.executor.memory", cfg_spark["memoria_executor"])
        .config("spark.hadoop.dfs.replication", str(cfg_spark["replicacion_hdfs"]))
        .getOrCreate()
    )
    try:
        Path_ = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
        fs = Path_(ns + "/").getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        ruta_raw = Path_(ns + cfg_cat["raw"])

        # 1) Idempotencia: si ya está descargado, no se repite.
        if fs.exists(ruta_raw) and not args.forzar:
            print("[CATALOGO] Ya existe {}{}. No se descarga nada (usa --forzar para repetir)."
                  .format(ns, cfg_cat["raw"]))
            return 0

        # 2) Lista de juegos: los app_id distintos de las reseñas limpias.
        ids = sorted(r["app_id"] for r in
                     spark.read.parquet(ns + cfg["etl"]["salida"]).select("app_id").distinct().collect())
        print("[CATALOGO] Juegos a descargar: {} (pausa {} s entre peticiones)"
              .format(len(ids), cfg_cat["pausa_s"]))

        # 3) Una petición por juego.
        lineas, correctos = [], 0
        inicio = time.perf_counter()
        for n, app_id in enumerate(ids, start=1):
            url = "{}?{}".format(cfg_cat["url_api"], urllib.parse.urlencode(
                {"appids": app_id, "l": cfg_cat["idioma"], "cc": cfg_cat["pais"]}))
            datos = pedir(url, int(cfg_cat["reintentos"]), float(cfg_cat["pausa_s"]))
            respuesta = datos.get(str(app_id)) if isinstance(datos, dict) else None
            if respuesta and respuesta.get("success"):
                correctos += 1
            lineas.append(json.dumps({
                "app_id": app_id,
                "descargado_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "respuesta": respuesta,                 # None si no se pudo descargar
            }, ensure_ascii=False))
            if n % 25 == 0 or n == len(ids):
                print("[CATALOGO] {}/{} ({} con datos) - {:.1f} min".format(
                    n, len(ids), correctos, (time.perf_counter() - inicio) / 60))
            time.sleep(float(cfg_cat["pausa_s"]))       # respetar el límite de Steam

        # 4) Guardar las respuestas crudas en HDFS (una línea JSON por juego).
        salida = fs.create(ruta_raw, True)              # True = reemplazar
        salida.write(bytearray(("\n".join(lineas) + "\n").encode("utf-8")))
        salida.close()
        print("[CATALOGO] Sin datos (retirados de la tienda o fallo): {}".format(len(ids) - correctos))
        print("[OK] Catalogo crudo guardado en {}{}".format(ns, cfg_cat["raw"]))
        return 0
    finally:
        spark.stop()                                # libera el clúster (siempre)


if __name__ == "__main__":
    sys.exit(main())
