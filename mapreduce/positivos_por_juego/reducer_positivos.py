#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reducer de Hadoop Streaming: porcentaje de reseñas positivas por juego (apartado C).

Entrada (lo que escribieron los mappers, YA AGRUPADO Y ORDENADO por app_id
gracias al shuffle de Hadoop):
    app_id<TAB>1   o   app_id<TAB>0

Salida (una línea por juego):
    app_id<TAB>reseñas<TAB>positivas<TAB>porcentaje_positivas
    p. ej. "730<TAB>1000<TAB>873<TAB>87.30"

Como las líneas de un mismo juego llegan seguidas, basta con ir contando y,
cuando cambia el app_id, escribir el resultado del juego anterior.

IMPORTANTE (compatibilidad): el nodemanager tiene Python 3.5.3, así que aquí
NO se usan f-strings ni nada posterior a Python 3.5.
"""
import sys


def escribir(salida, app_id, resenas, positivas):
    """Escribe la línea de un juego, con el porcentaje redondeado a 2 decimales."""
    porcentaje = "{:.2f}".format(100.0 * positivas / resenas)
    linea = "\t".join([app_id, str(resenas), str(positivas), porcentaje]) + "\n"
    salida.write(linea.encode("utf-8"))


def main():
    salida = sys.stdout.buffer
    app_actual = None      # juego que estamos contando ahora mismo
    resenas = 0            # reseñas vistas de ese juego
    positivas = 0          # de ellas, cuántas lo recomiendan

    for linea_bytes in sys.stdin.buffer:
        linea = linea_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
        app_id, _, valor = linea.partition("\t")
        if valor not in ("0", "1"):
            continue                      # línea mal formada: se ignora

        if app_id != app_actual:
            # Ha cambiado el juego: el anterior ya está completo.
            if app_actual is not None:
                escribir(salida, app_actual, resenas, positivas)
            app_actual, resenas, positivas = app_id, 0, 0
        resenas += 1
        positivas += int(valor)

    # El último juego no tiene "siguiente" que lo cierre: se escribe aquí.
    if app_actual is not None:
        escribir(salida, app_actual, resenas, positivas)


if __name__ == "__main__":
    main()
