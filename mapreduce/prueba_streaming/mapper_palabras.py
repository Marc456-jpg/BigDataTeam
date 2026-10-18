#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mapper de prueba para Hadoop Streaming (mini-paso 1.7c).

Qué hace: lee texto línea a línea y, por cada palabra, escribe "palabra<TAB>1".
Es la fase MAP del WordCount del Tema 2, pero escrita en Python y ejecutada por
Hadoop Streaming en el nodemanager (no en Jupyter ni en Spark).

Cómo lo usa Hadoop Streaming:
  - Hadoop le pasa por la entrada estándar (stdin) las líneas de su trozo
    ("split") del archivo de HDFS.
  - Todo lo que este programa escribe en la salida estándar (stdout) son pares
    clave<TAB>valor. Hadoop los agrupa por clave (shuffle) y se los pasa
    ordenados al reducer.

IMPORTANTE (compatibilidad): el nodemanager tiene Python 3.5.3, así que aquí
NO se usan f-strings ni nada posterior a Python 3.5.
"""
import re
import sys

# Una "palabra" es una secuencia de letras, dígitos o "_" de CUALQUIER idioma
# (con re.UNICODE, \w reconoce también ñ, á, кириллица, 中文...).
PATRON_PALABRA = re.compile(r"\w+", re.UNICODE)


def main():
    # Escribimos BYTES en la salida (sys.stdout.buffer) en vez de texto.
    salida = sys.stdout.buffer

    # Leemos BYTES de la entrada (sys.stdin.buffer) y los decodificamos como
    # UTF-8 nosotros mismos. Motivo: Python 3.5 usa la codificación del
    # "locale" del sistema, y dentro de los contenedores de YARN no hay locale
    # definido, así que sería ASCII y fallaría con tildes, ñ, cirílico o chino.
    # errors="replace": si llegara un byte inválido, se cambia por "?" en vez
    # de detener todo el trabajo.
    for linea_bytes in sys.stdin.buffer:
        linea = linea_bytes.decode("utf-8", errors="replace")

        # Pasamos a minúsculas para que "Juego", "JUEGO" y "juego" cuenten
        # como la misma palabra (el WordCount de Hadoop no lo hace).
        for palabra in PATRON_PALABRA.findall(linea.lower()):
            # Par clave<TAB>valor: la palabra y un 1.
            salida.write((palabra + "\t1\n").encode("utf-8"))


if __name__ == "__main__":
    main()
