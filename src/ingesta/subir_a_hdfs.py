#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
subir_a_hdfs.py · Sube el CSV de reseñas a la zona raw de HDFS, de forma
idempotente (decisión 40 del documento explicativo).

Por qué existe: el apartado E pide que las re-ejecuciones sean idempotentes
(repetir no duplica ni estropea nada). La subida del CSV (8,2 GB) tarda
≈ 15 min, porque el archivo llega por la carpeta compartida entre Windows y
Docker. Antes se hacía a mano con «hdfs dfs -put»; este script la sustituye.

Qué hace, en orden:
  1. Lee config/pipeline.yaml: dónde está el CSV en el PC (data/), su tamaño
     exacto y dónde va en HDFS (/raw/steam_reviews.csv).
  2. Si en HDFS ya está con el tamaño exacto, NO sube nada y termina.
     (Así funciona aunque luego se borre la copia local del CSV.)
  3. Si no, comprueba que el CSV local existe y tiene el tamaño exacto.
  4. Lo sube a un nombre provisional («…._subiendo») y, al terminar, lo
     renombra al definitivo. Si la subida se corta a medias, nunca queda un
     archivo incompleto con el nombre bueno que engañe a la siguiente ejecución.
  5. Pone 2 copias (setReplication explícito, decisión 45) y comprueba el
     tamaño y las copias finales.
  Con --forzar vuelve a subirlo aunque ya esté.

Se ejecuta en el contenedor «jupyter», que ve la carpeta del proyecto en
/home/jovyan/work. Usa la API de ficheros de Hadoop que trae PySpark; arranca
Spark en modo local (1 núcleo) solo para tener esa API, sin ocupar el clúster.

Uso (desde PowerShell, en C:\\steam-bigdata):
    docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/ingesta/subir_a_hdfs.py

Devuelve 0 si el CSV queda bien en HDFS y 1 si no.
Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import sys
import threading
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

RAIZ = Path(__file__).resolve().parents[2]
CONFIG_POR_DEFECTO = RAIZ / "config" / "pipeline.yaml"
MB = 1024 * 1024


def main():
    parser = argparse.ArgumentParser(description="Sube el CSV a /raw de HDFS (idempotente)")
    parser.add_argument("--config", default=str(CONFIG_POR_DEFECTO))
    parser.add_argument("--forzar", action="store_true", help="subirlo aunque ya este en HDFS")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ns = cfg["hdfs"]["namenode"].rstrip("/")
    tamano = int(cfg["dataset"]["tamano_bytes"])
    copias = int(cfg["spark"]["replicacion_hdfs"])
    local = RAIZ / cfg["local"]["carpeta_datos"] / cfg["dataset"]["archivo"]
    destino = ns + cfg["etl"]["csv_raw"]
    provisional = destino + "._subiendo"

    # Spark en modo local, solo para usar la API de ficheros de Hadoop.
    spark = (SparkSession.builder.appName("subir_a_hdfs").master("local[1]")
             .config("spark.hadoop.dfs.replication", str(copias))
             .getOrCreate())
    try:
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        Path_ = jvm.org.apache.hadoop.fs.Path
        fs = Path_(ns + "/").getFileSystem(conf)
        p_destino, p_provisional = Path_(destino), Path_(provisional)

        def tamano_en_hdfs(p):
            """Bytes del archivo en HDFS, o None si no existe."""
            return fs.getFileStatus(p).getLen() if fs.exists(p) else None

        # ---- 1) ¿Ya está en HDFS con el tamaño exacto? ----------------------
        actual = tamano_en_hdfs(p_destino)
        print("[INGESTA] Destino : {}".format(destino))
        print("[INGESTA] En HDFS : {}".format(
            "no existe" if actual is None else "{} bytes".format(actual)))
        if actual == tamano and not args.forzar:
            print("[OK] El CSV ya esta en HDFS con el tamano exacto ({} bytes). "
                  "No se sube nada.".format(tamano))
            return 0

        # ---- 2) Comprobar el CSV local ----------------------------------
        print("[INGESTA] Origen  : {}".format(local))
        if not local.is_file():
            print("[ERROR] No existe el CSV local. Ejecuta antes scripts/descargar_dataset.py")
            return 1
        if local.stat().st_size != tamano:
            print("[ERROR] El CSV local mide {} bytes y deberia medir {}. "
                  "Vuelve a descargarlo con scripts/descargar_dataset.py --forzar".format(
                      local.stat().st_size, tamano))
            return 1

        # ---- 3) Subir con nombre provisional y renombrar al final ---------
        if fs.exists(p_provisional):
            fs.delete(p_provisional, False)          # restos de una subida cortada
        fs.mkdirs(p_destino.getParent())
        print("[INGESTA] Subiendo {:.0f} MB. Por la carpeta compartida de Windows "
              "tarda ~15 min; cada minuto se muestra lo subido.".format(tamano / MB))

        terminado = threading.Event()
        inicio = time.perf_counter()

        def avisar_progreso():
            # Cada 60 s: cuántos MB tiene ya el archivo provisional en HDFS
            # (HDFS informa del tamaño por bloques completados de 128 MB).
            while not terminado.wait(60):
                try:
                    subido = tamano_en_hdfs(p_provisional) or 0
                except Exception:
                    subido = 0
                print("[INGESTA]   {:>4.0f} s: {:>6.0f} MB de {:.0f} MB".format(
                    time.perf_counter() - inicio, subido / MB, tamano / MB))
                sys.stdout.flush()

        hilo = threading.Thread(target=avisar_progreso, daemon=True)
        hilo.start()
        try:
            # (borrar origen = False, sobrescribir = True, origen, destino)
            fs.copyFromLocalFile(False, True, Path_("file://" + str(local)), p_provisional)
        finally:
            terminado.set()
        segundos = time.perf_counter() - inicio

        if fs.exists(p_destino):
            fs.delete(p_destino, False)              # versión vieja o incompleta
        if not fs.rename(p_provisional, p_destino):
            print("[ERROR] No se pudo renombrar {} a {}".format(provisional, destino))
            return 1
        fs.setReplication(p_destino, copias)         # decisión 45: copias explícitas

        # ---- 4) Comprobación final ---------------------------------------
        final = fs.getFileStatus(p_destino)
        print("[INGESTA] Subido en {:.0f} s ({:.1f} MB/s)".format(segundos, tamano / MB / segundos))
        print("[INGESTA] Tamano en HDFS: {} bytes; copias: {}".format(
            final.getLen(), final.getReplication()))
        if final.getLen() != tamano:
            print("[ERROR] El tamano en HDFS no coincide con el esperado ({})".format(tamano))
            return 1
        print("[OK] CSV subido a {}".format(destino))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
