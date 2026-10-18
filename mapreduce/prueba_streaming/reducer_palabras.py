#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reducer de prueba para Hadoop Streaming (mini-paso 1.7c).

Qué hace: suma los 1 de cada palabra y escribe "palabra<TAB>total".
Es la fase REDUCE del WordCount del Tema 2.

Cómo lo usa Hadoop Streaming:
  - Recibe por stdin las líneas "palabra<TAB>1" que escribieron los mappers,
    YA ORDENADAS por palabra (eso lo hace el shuffle de Hadoop).
  - Gracias a ese orden, todas las líneas de una misma palabra llegan seguidas:
    basta con ir sumando y, cuando cambia la palabra, escribir el total.

IMPORTANTE (compatibilidad): el nodemanager tiene Python 3.5.3, así que aquí
NO se usan f-strings ni nada posterior a Python 3.5.
"""
import sys


def escribir(salida, palabra, total):
    """Escribe una línea "palabra<TAB>total" en la salida, en UTF-8."""
    salida.write((palabra + "\t" + str(total) + "\n").encode("utf-8"))


def main():
    salida = sys.stdout.buffer

    palabra_actual = None   # palabra que estamos sumando ahora mismo
    total_actual = 0        # cuántas veces la llevamos vista

    # Igual que en el mapper: leemos bytes y decodificamos UTF-8 a mano,
    # porque en los contenedores de YARN Python 3.5 usaría ASCII.
    for linea_bytes in sys.stdin.buffer:
        linea = linea_bytes.decode("utf-8", errors="replace").rstrip("\n")
        if not linea:
            continue  # línea vacía: no hay nada que sumar

        # Separamos clave y valor por el PRIMER tabulador.
        palabra, _, valor = linea.partition("\t")
        try:
            numero = int(valor)
        except ValueError:
            continue  # línea mal formada: se ignora en vez de romper el trabajo

        if palabra == palabra_actual:
            # Misma palabra que la línea anterior: seguimos sumando.
            total_actual += numero
        else:
            # Ha cambiado la palabra: la anterior ya está completa.
            if palabra_actual is not None:
                escribir(salida, palabra_actual, total_actual)
            palabra_actual = palabra
            total_actual = numero

    # La última palabra no tiene "siguiente" que la cierre: la escribimos aquí.
    if palabra_actual is not None:
        escribir(salida, palabra_actual, total_actual)


if __name__ == "__main__":
    main()
