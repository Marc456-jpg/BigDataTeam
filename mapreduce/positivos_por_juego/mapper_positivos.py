#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mapper de Hadoop Streaming: porcentaje de reseñas positivas por juego (apartado C).

Entrada (una línea por reseña, la crea src/procesamiento/exportar_entrada_mapreduce.py):
    app_id<TAB>recommended          p. ej. "730<TAB>true"

Salida (un par clave<TAB>valor por reseña):
    app_id<TAB>1   si la reseña recomienda el juego
    app_id<TAB>0   si no lo recomienda

Qué pasa después (lo hace Hadoop, no este programa): el SHUFFLE agrupa todas
las líneas con el mismo app_id, las ordena y se las entrega juntas al reducer,
que cuenta cuántas hay (reseñas) y suma los 1 (positivas).

IMPORTANTE (compatibilidad): el nodemanager tiene Python 3.5.3, así que aquí
NO se usan f-strings ni nada posterior a Python 3.5.
"""
import sys

# Texto de la columna recommended → valor que se emite.
VALOR = {"true": "1", "false": "0"}


def main():
    salida = sys.stdout.buffer
    # Leemos BYTES y decodificamos UTF-8 a mano: dentro de los contenedores de
    # YARN no hay "locale" y Python 3.5 usaría ASCII (ver mapper de la 1.7c).
    for linea_bytes in sys.stdin.buffer:
        campos = linea_bytes.decode("utf-8", errors="replace").rstrip("\r\n").split("\t")
        if len(campos) != 2:
            continue                      # línea mal formada: se ignora
        app_id, recomendada = campos
        valor = VALOR.get(recomendada.strip().lower())
        if not app_id.strip().isdigit() or valor is None:
            continue                      # id no numérico o valor raro: se ignora
        salida.write((app_id.strip() + "\t" + valor + "\n").encode("utf-8"))


if __name__ == "__main__":
    main()
