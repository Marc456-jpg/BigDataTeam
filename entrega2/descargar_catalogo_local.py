#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
descargar_catalogo_local.py · Catálogo de juegos de la API de Steam, para Databricks.

Por qué existe (Entrega 2): en la Entrega 1 el catálogo lo descargaba
src/ingesta/descargar_catalogo.py y lo guardaba DIRECTAMENTE en HDFS
(/raw/games/appdetails.jsonl), dentro del clúster Docker de otro ordenador.
Databricks Free Edition no tiene salida a internet, así que no puede llamar a
la API de Steam: hay que descargarlo en el PC y subirlo a mano al volume
/Volumes/workspace/steam/raw/ (igual que el CSV de las reseñas).

Es la MISMA lógica que el script original (misma API, mismo idioma y país,
misma pausa y reintentos, mismo formato de salida: una línea JSON por juego
con {"app_id", "descargado_utc", "respuesta"}), con dos cambios:
  1. La lista de juegos sale del CSV reducido (columna app_id), no de HDFS.
  2. El resultado se escribe en un fichero local (appdetails.jsonl).

Limitación (igual que en la Entrega 1): precios, nombres y géneros son los
de HOY, no los de la fecha de las reseñas; pueden diferir algo de los que
se descargaron en octubre para la Entrega 1.

Uso (PowerShell, en la carpeta del proyecto; tarda ≈ 8-10 min):
    python descargar_catalogo_local.py
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

import pandas as pd

CARPETA = Path(__file__).resolve().parent

# Mismos valores que la sección «catalogo» de config/pipeline.yaml
URL_API = "https://store.steampowered.com/api/appdetails"
IDIOMA = "english"      # idioma fijo: los datos no dependen de dónde se ejecute
PAIS = "us"             # precios en dólares
PAUSA_S = 1.5           # Steam admite unas 200 peticiones cada 5 minutos
REINTENTOS = 4


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
            print("           reintento {} en {:.0f} s ({})".format(intento + 1, espera, motivo), flush=True)
            time.sleep(espera)
    return None


def main():
    parser = argparse.ArgumentParser(description="Descarga el catalogo de juegos de la API de Steam")
    parser.add_argument("--csv", default=str(CARPETA / "steam_reviews_databricks.csv"),
                        help="CSV reducido de donde se sacan los app_id")
    parser.add_argument("--salida", default=str(CARPETA / "appdetails.jsonl"))
    parser.add_argument("--forzar", action="store_true", help="descargar aunque ya exista")
    args = parser.parse_args()

    salida = Path(args.salida)
    # 1) Idempotencia: si ya está descargado, no se repite.
    if salida.exists() and not args.forzar:
        print("[CATALOGO] Ya existe {}. No se descarga nada (usa --forzar).".format(salida))
        return 0

    # 2) Lista de juegos: los app_id distintos del CSV reducido. Se lee solo esa
    #    columna, en bloques, para no cargar 1,8 GB en memoria.
    ids = set()
    for bloque in pd.read_csv(args.csv, usecols=["app_id"], dtype={"app_id": "int64"},
                              chunksize=2_000_000):
        ids.update(bloque["app_id"].unique().tolist())
    ids = sorted(ids)
    print("[CATALOGO] Juegos a descargar: {} (pausa {} s entre peticiones)".format(len(ids), PAUSA_S), flush=True)

    # 3) Una petición por juego.
    lineas, correctos = [], 0
    inicio = time.perf_counter()
    for n, app_id in enumerate(ids, start=1):
        url = "{}?{}".format(URL_API, urllib.parse.urlencode(
            {"appids": app_id, "l": IDIOMA, "cc": PAIS}))
        datos = pedir(url, REINTENTOS, PAUSA_S)
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
                n, len(ids), correctos, (time.perf_counter() - inicio) / 60), flush=True)
        time.sleep(PAUSA_S)                         # respetar el límite de Steam

    # 4) Guardar las respuestas crudas (una línea JSON por juego), en UTF-8.
    salida.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print("[CATALOGO] Sin datos (retirados de la tienda o fallo): {}".format(len(ids) - correctos))
    print("[OK] Catalogo crudo guardado en {} ({:,} bytes)".format(salida, salida.stat().st_size))
    return 0


if __name__ == "__main__":
    sys.exit(main())
