#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
descargar_dataset.py · Descarga el dataset «Steam Reviews 2021» de Kaggle.

Por qué existe: las bases del proyecto exigen que el repositorio incluya un
script que reproduzca la obtención de los datos, y que cualquier cuenta o clave
necesaria esté documentada. El CSV (8 GB) no se sube a GitHub: cada persona del
equipo lo descarga con este script.

Qué hace, en orden:
  1. Lee la configuración de config/pipeline.yaml (dataset, carpetas, tamaño).
  2. Si el CSV ya está en data/ con el tamaño exacto, NO descarga nada
     (re-ejecución idempotente: correrlo dos veces no repite 8 GB).
  3. Busca las credenciales de Kaggle (ver «Credenciales» abajo).
  4. Instala la librería oficial de Kaggle si falta (versión fijada).
  5. Descarga el zip del dataset, lo descomprime en data/ y borra el zip.
  6. Comprueba que el CSV tiene exactamente el tamaño esperado.

Uso (desde PowerShell, en la carpeta del proyecto C:\\steam-bigdata):
    docker exec jupyter python /home/jovyan/work/scripts/descargar_dataset.py
    docker exec jupyter python /home/jovyan/work/scripts/descargar_dataset.py --forzar

Se ejecuta dentro del contenedor «jupyter» porque ya tiene Python e internet,
y la carpeta del proyecto está montada en /home/jovyan/work: lo que se descarga
ahí aparece en C:\\steam-bigdata\\data del PC.

Credenciales de Kaggle (hace falta una cuenta gratuita en kaggle.com):
  - En https://www.kaggle.com/settings/api se genera un token («API Token»).
  - Se guarda en UNA de estas formas (de más a menos recomendada):
      a) archivo secrets/access_token  con el token en una sola línea;
      b) archivo secrets/kaggle.json   (formato antiguo: {"username": ..., "key": ...});
      c) variables de entorno KAGGLE_API_TOKEN, o KAGGLE_USERNAME + KAGGLE_KEY.
  - La carpeta secrets/ está en .gitignore: las claves nunca llegan a GitHub.

Nota sobre los mensajes: se escriben sin tildes ni eñes a propósito, porque
PowerShell, por defecto, muestra mal los caracteres especiales que llegan desde
un contenedor Linux. Los comentarios del código sí llevan tildes.
"""

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Raíz del repositorio: este archivo está en <raíz>/scripts/, así que la raíz es
# la carpeta «abuela». Así el script funciona igual dentro de Docker
# (/home/jovyan/work) que en un PC (C:\steam-bigdata).
RAIZ = Path(__file__).resolve().parent.parent

# Configuración por defecto del pipeline (se puede cambiar con --config).
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"

# Versión fijada de la librería oficial de Kaggle, para que todo el equipo use
# exactamente la misma y el resultado sea reproducible.
VERSION_KAGGLE = "2.2.4"

# Espacio libre mínimo para descargar: el zip comprimido más el CSV
# descomprimido (8,2 GB) caben con margen en 12 GiB.
ESPACIO_MINIMO_BYTES = 12 * 1024**3


def gib(n_bytes):
    """Convierte bytes a un texto legible en GiB (1 GiB = 1024^3 bytes)."""
    return "{:.2f} GiB".format(n_bytes / 1024**3)


def leer_config(ruta_config):
    """Lee el archivo YAML de configuración y devuelve un diccionario."""
    import yaml  # PyYAML viene instalado en la imagen de Jupyter

    with open(ruta_config, encoding="utf-8") as f:
        return yaml.safe_load(f)


def ya_descargado(ruta_csv, tamano_esperado):
    """True si el CSV existe y su tamaño coincide exactamente con el esperado."""
    return ruta_csv.is_file() and ruta_csv.stat().st_size == tamano_esperado


def preparar_credenciales(carpeta_secretos):
    """
    Busca las credenciales de Kaggle y las deja listas para la librería oficial.

    Devuelve un texto que dice de dónde salieron (para mostrarlo), o None si no
    hay ninguna. Importante: hay que llamarla ANTES de importar la librería de
    Kaggle, porque esta intenta autenticarse nada más importarse.
    """
    # c) Variables de entorno ya puestas por quien ejecuta el script.
    if os.environ.get("KAGGLE_API_TOKEN"):
        return "variable de entorno KAGGLE_API_TOKEN"
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return "variables de entorno KAGGLE_USERNAME y KAGGLE_KEY"

    # a) Token moderno guardado en secrets/access_token.
    archivo_token = carpeta_secretos / "access_token"
    if archivo_token.is_file():
        token = archivo_token.read_text(encoding="utf-8").strip()
        if token:
            os.environ["KAGGLE_API_TOKEN"] = token
            return "archivo secrets/access_token"

    # b) Formato antiguo: secrets/kaggle.json. La librería lo busca en la carpeta
    #    que indique KAGGLE_CONFIG_DIR.
    if (carpeta_secretos / "kaggle.json").is_file():
        os.environ["KAGGLE_CONFIG_DIR"] = str(carpeta_secretos)
        return "archivo secrets/kaggle.json"

    return None


def asegurar_libreria_kaggle():
    """Instala la librería oficial de Kaggle (versión fijada) si no está."""
    if importlib.util.find_spec("kaggle") is not None:
        return
    print("[2/5] Instalando la libreria oficial de Kaggle (kaggle=={}) ...".format(VERSION_KAGGLE))
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "kaggle=={}".format(VERSION_KAGGLE)]
    )


def main():
    # --- Argumentos de la línea de comandos ---------------------------------
    parser = argparse.ArgumentParser(description="Descarga el dataset Steam Reviews 2021 de Kaggle.")
    parser.add_argument("--forzar", action="store_true",
                        help="descargar aunque el CSV ya exista con el tamano correcto")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO),
                        help="ruta del archivo de configuracion YAML")
    args = parser.parse_args()

    # --- 1) Configuración ----------------------------------------------------
    config = leer_config(args.config)
    kaggle_id = config["dataset"]["kaggle_id"]
    nombre_csv = config["dataset"]["archivo"]
    tamano_esperado = int(config["dataset"]["tamano_bytes"])
    carpeta_datos = RAIZ / config["local"]["carpeta_datos"]
    carpeta_secretos = RAIZ / config["local"]["carpeta_secretos"]
    ruta_csv = carpeta_datos / nombre_csv

    print("Dataset : {}".format(kaggle_id))
    print("Destino : {}".format(ruta_csv))

    # --- 2) ¿Ya está descargado? (idempotencia) ------------------------------
    if ya_descargado(ruta_csv, tamano_esperado) and not args.forzar:
        print("[OK] El CSV ya existe con el tamano exacto ({} bytes = {}). "
              "No se descarga nada.".format(tamano_esperado, gib(tamano_esperado)))
        return 0
    if args.forzar:
        print("[AVISO] Opcion --forzar: se descarga de nuevo aunque ya exista.")
    elif ruta_csv.is_file():
        print("[AVISO] Hay un CSV con tamano distinto ({} bytes): se volvera a descargar."
              .format(ruta_csv.stat().st_size))

    # Antes de empezar, comprobamos que cabe en el disco.
    carpeta_datos.mkdir(parents=True, exist_ok=True)
    libre = shutil.disk_usage(carpeta_datos).free
    if libre < ESPACIO_MINIMO_BYTES:
        print("[ERROR] Espacio libre insuficiente: {} (hacen falta unos {}).".format(
            gib(libre), gib(ESPACIO_MINIMO_BYTES)))
        return 3

    # --- 3) Credenciales -----------------------------------------------------
    origen = preparar_credenciales(carpeta_secretos)
    if origen is None:
        print("[ERROR] No se han encontrado credenciales de Kaggle.")
        print("  1. Entra en https://www.kaggle.com/settings/api (con tu cuenta de Kaggle).")
        print("  2. Genera un token en el apartado 'API'.")
        print("  3. Guardalo en una sola linea en el archivo: {}".format(carpeta_secretos / "access_token"))
        print("     (la carpeta secrets/ esta en .gitignore: nunca se sube a GitHub)")
        return 2
    print("[1/5] Credenciales: {}".format(origen))

    # --- 4) Librería oficial de Kaggle --------------------------------------
    asegurar_libreria_kaggle()
    # Se importa aquí, después de preparar las credenciales (ver arriba).
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    try:
        api.authenticate()
    except (SystemExit, Exception) as error:
        # La librería termina el programa (SystemExit) si las credenciales no
        # valen, o lanza un error si no puede conectar con Kaggle (sin internet).
        motivo = "credenciales rechazadas" if isinstance(error, SystemExit) else error
        print("[ERROR] No se ha podido autenticar en Kaggle con {}: {}".format(origen, motivo))
        return 2

    # --- 5) Descarga + descompresión ----------------------------------------
    print("[3/5] Descargando y descomprimiendo (puede tardar bastante) ...")
    inicio = time.time()
    api.dataset_download_files(kaggle_id, path=str(carpeta_datos), force=args.forzar,
                               quiet=False, unzip=True)
    minutos = (time.time() - inicio) / 60

    # --- 6) Comprobación final ----------------------------------------------
    print("[4/5] Comprobando el tamano del CSV ...")
    if not ruta_csv.is_file():
        print("[ERROR] No aparece {} despues de descomprimir.".format(ruta_csv))
        return 4
    tamano_real = ruta_csv.stat().st_size
    if tamano_real != tamano_esperado:
        print("[ERROR] Tamano inesperado: {} bytes (se esperaban {}).".format(
            tamano_real, tamano_esperado))
        return 4
    print("[5/5] [OK] Descarga completa: {} bytes ({}) en {:.1f} min.".format(
        tamano_real, gib(tamano_real), minutos))
    return 0


if __name__ == "__main__":
    sys.exit(main())
