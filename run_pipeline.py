#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_pipeline.py · Punto de entrada ÚNICO del proyecto (apartado E, decisión 54).

Ejecuta todo el flujo, en orden y parando en el primer paso que falle:

    ingesta → HDFS → procesamiento en Spark → resultados
    (y, en el mismo flujo, el MapReduce y su versión RDD, que se comparan solos)

Por qué es un programa de Python que se ejecuta en el PC y no dentro de Docker:
el profesor pidió (19 oct) que el MapReduce vaya en el mismo pipeline que Spark
y que la comparación de eficiencia sea automática, no a mano: «ejecutar comandos
de terminal desde Python, guardar lo ejecutado y comparar». Los pasos viven en
contenedores distintos (Spark en «jupyter», Hadoop en «namenode», YARN en un
perfil aparte), y solo desde fuera de los contenedores se pueden lanzar todos.
Este programa hace exactamente lo que haríamos a mano en PowerShell (los mismos
«docker exec»), pero sin olvidarse de nada, midiendo el tiempo de cada paso y
guardándolo todo.

Solo usa la biblioteca estándar de Python (no hay que instalar nada en el PC).
Todas las rutas y parámetros (de los datos y del entorno: contenedores, carpeta
del proyecto, spark-submit, JAR de Streaming...) se leen de config/pipeline.yaml
(decisión 66). En el código solo quedan dos datos imprescindibles para LEER esa
configuración: dónde está el archivo (config/pipeline.yaml) y qué contenedor
sabe traducir YAML (jupyter, que tiene la librería; el PC no la necesita).

Cada paso es idempotente (decisión 40 y siguientes): repetir el pipeline no
duplica nada. Los pasos de descarga y subida se saltan solos si ya están hechos.

Qué guarda (evidencias, carpeta evidence/ejecuciones/AAAAMMDD_HHMMSS/):
  - pipeline.log  : todo lo que se ha visto en pantalla;
  - resumen.json  : cada paso con su comando, segundos y resultado.
Y en HDFS, /resultados/comparativa/mapreduce_vs_rdd.json con la comparación
automática MapReduce frente a RDD (el último paso lo copia a evidence/resultados).

Uso (PowerShell normal, en C:\\steam-bigdata, con el clúster encendido):
    python run_pipeline.py                 # todo el flujo
    python run_pipeline.py --lista         # ver los pasos y sus números
    python run_pipeline.py --desde 5       # empezar en el paso 5 (sin repetir el ETL)
    python run_pipeline.py --solo 11,12,13 # solo esos pasos

Los mensajes van sin tildes a propósito (PowerShell las muestra mal).
"""

import argparse
import datetime
import json
import re
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
# Lo único fijo: dónde está la configuración y quién la traduce (ver arriba).
ARCHIVO_CONFIG = RAIZ / "config" / "pipeline.yaml"
CONTENEDOR_YAML = "jupyter"


def en_proyecto(cfg, ruta):
    """Ruta de un archivo del proyecto vista desde dentro del contenedor jupyter."""
    return cfg["entorno"]["carpeta_proyecto"].rstrip("/") + "/" + ruta


def comando_spark(cfg, script, *extra):
    """Comando para lanzar un script de Spark del proyecto en el clúster."""
    ent = cfg["entorno"]
    return (["docker", "exec", "-u", ent["usuario_spark"], ent["contenedor_spark"],
             ent["spark_submit"], "--driver-memory", str(cfg["spark"]["memoria_driver"]),
             "--conf", "spark.log.level=" + ent["nivel_log_spark"],
             en_proyecto(cfg, script)] + list(extra))


def hdfs(cfg, *argumentos):
    """Comando que se ejecuta en el contenedor de HDFS (hdfs dfs ..., hadoop jar ...)."""
    return ["docker", "exec", cfg["entorno"]["contenedor_hdfs"]] + list(argumentos)


class Registro:
    """Escribe a la vez en pantalla y en el archivo pipeline.log."""

    def __init__(self, carpeta):
        carpeta.mkdir(parents=True, exist_ok=True)
        self.archivo = open(carpeta / "pipeline.log", "w", encoding="utf-8")

    def __call__(self, texto=""):
        print(texto, flush=True)
        self.archivo.write(texto + "\n")
        self.archivo.flush()

    def cerrar(self):
        self.archivo.close()


def ejecutar(cmd, log, entrada=None, mostrar=True):
    """Lanza un comando, muestra su salida línea a línea (y la guarda en el
    registro) y devuelve (código de salida, salida completa como texto)."""
    if mostrar:
        log("$ " + " ".join(cmd))
    proceso = subprocess.Popen(
        cmd, cwd=RAIZ, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE if entrada is not None else subprocess.DEVNULL,
        text=True, encoding="utf-8", errors="replace")
    if entrada is not None:
        proceso.stdin.write(entrada)
        proceso.stdin.close()
    lineas = []
    for linea in proceso.stdout:
        linea = linea.rstrip("\r\n")
        lineas.append(linea)
        if mostrar:
            log("  " + linea)
    return proceso.wait(), "\n".join(lineas)


def leer_config(log):
    """Lee config/pipeline.yaml en el PC y se lo pasa al contenedor jupyter, que
    lo traduce a JSON (así el PC no necesita la librería de YAML)."""
    texto = ARCHIVO_CONFIG.read_text(encoding="utf-8")
    codigo = "import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin.read())))"
    rc, salida = ejecutar(["docker", "exec", "-i", CONTENEDOR_YAML, "python", "-c", codigo],
                          log, entrada=texto, mostrar=False)
    if rc != 0:
        log(salida)
        raise SystemExit("[ERROR] No se pudo leer la configuracion. ¿Esta el cluster encendido "
                         "(docker compose up -d)?")
    return json.loads(salida.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# Pasos especiales (los demás son un único «spark-submit»)
# ---------------------------------------------------------------------------

def paso_descargar(ctx):
    """Descarga el CSV de Kaggle si hace falta. Si ya está en HDFS con el tamaño
    exacto, no hace falta ni el CSV local (se puede borrar para ahorrar 8 GB)."""
    cfg, log = ctx["cfg"], ctx["log"]
    rc, salida = ejecutar(hdfs(cfg, "hdfs", "dfs", "-stat", "%b", cfg["etl"]["csv_raw"]),
                          log, mostrar=False)
    ultima = salida.strip().splitlines()[-1] if salida.strip() else ""
    if rc == 0 and ultima == str(cfg["dataset"]["tamano_bytes"]):
        log("[PIPELINE] El CSV ya esta en HDFS con el tamano exacto: no hace falta descargarlo.")
        return 0
    return ejecutar(["docker", "exec", cfg["entorno"]["contenedor_spark"], "python",
                     en_proyecto(cfg, "scripts/descargar_dataset.py")], log)[0]


def esperar_sanos(contenedores, segundos, log):
    """Espera a que los contenedores estén «healthy» (como mucho «segundos»)."""
    limite = time.time() + segundos
    while time.time() < limite:
        rc, salida = ejecutar(["docker", "inspect", "-f", "{{.State.Health.Status}}"] + contenedores,
                              log, mostrar=False)
        estados = salida.split()
        if rc == 0 and estados and all(e == "healthy" for e in estados):
            log("[PIPELINE] YARN listo: " + ", ".join(contenedores) + " healthy")
            return True
        time.sleep(5)
    log("[ERROR] YARN no esta listo tras {} s".format(segundos))
    return False


def segundos_del_job(salida):
    """Duración del job de MapReduce según su propio registro: desde «Running
    job» hasta «completed successfully» (sin contar el arranque del cliente)."""
    marca = r"^\s*(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3}) .*"
    marcas = []
    for frase in (r"Running job:", r"completed successfully"):
        m = re.search(marca + frase, salida, re.MULTILINE)
        if not m:
            return None
        t = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        marcas.append(t.timestamp() + int(m.group(2)) / 1000)
    return round(marcas[1] - marcas[0], 1)


def paso_mapreduce(ctx):
    """Job de Hadoop Streaming en YARN (apartado C). Enciende YARN solo para
    este paso y lo apaga al terminar, pase lo que pase, para dejar memoria."""
    cfg, log = ctx["cfg"], ctx["log"]
    cfg_mr, cfg_pl, ent = cfg["mapreduce"], cfg["pipeline"], cfg["entorno"]
    yarn = list(ent["servicios_yarn"])
    rc = ejecutar(["docker", "compose", "--profile", ent["perfil_yarn"], "up", "-d"] + yarn, log)[0]
    try:
        if rc != 0 or not esperar_sanos(yarn, int(cfg_pl["espera_yarn_s"]), log):
            return 1
        # Los dos scripts se copian al contenedor de HDFS, que es desde donde se lanza.
        carpeta_local = RAIZ / cfg_mr["carpeta_scripts"]
        temporal = ent["carpeta_temporal_hdfs"].rstrip("/")
        remota = temporal + "/" + carpeta_local.name
        if ejecutar(["docker", "cp", str(carpeta_local),
                     ent["contenedor_hdfs"] + ":" + temporal + "/"], log)[0] != 0:
            return 1
        # Idempotencia: Hadoop no escribe en una carpeta que ya existe.
        ejecutar(hdfs(cfg, "hdfs", "dfs", "-rm", "-r", "-f", cfg_mr["salida"]), log)
        inicio = time.perf_counter()
        rc, salida = ejecutar(hdfs(
            cfg, "hadoop", "jar", ent["jar_streaming"],
            "-files", remota + "/" + cfg_mr["mapper"] + "," + remota + "/" + cfg_mr["reducer"],
            "-mapper", cfg_mr["interprete"] + " " + cfg_mr["mapper"],
            "-reducer", cfg_mr["interprete"] + " " + cfg_mr["reducer"],
            "-input", cfg_mr["entrada"], "-output", cfg_mr["salida"]), log)
        total = time.perf_counter() - inicio
        job = re.search(r"Running job: (job_\S+)", salida)
        ctx["mapreduce"] = {"segundos_total": round(total, 1),
                            "segundos_job": segundos_del_job(salida),
                            "job": job.group(1) if job else None}
        return rc
    finally:
        ejecutar(["docker", "compose", "--profile", ent["perfil_yarn"], "stop"] + yarn, log)


def paso_rdd(ctx):
    """El mismo cálculo con RDDs de Spark; el script ya compara con el MapReduce."""
    log = ctx["log"]
    inicio = time.perf_counter()
    rc, salida = ejecutar(comando_spark(ctx["cfg"], "src/procesamiento/positivos_por_juego_rdd.py"), log)
    total = time.perf_counter() - inicio
    calculo = re.search(r"Calculo y escritura en ([\d.]+) s", salida)
    iguales = re.search(r"identicos al MapReduce: (SI|NO)", salida)
    ctx["rdd"] = {"segundos_total": round(total, 1),
                  "segundos_calculo": float(calculo.group(1)) if calculo else None,
                  "identicos_al_mapreduce": iguales.group(1) if iguales else None}
    return rc


def paso_comparativa(ctx):
    """Guarda la comparación automática MapReduce frente a RDD en HDFS."""
    cfg, log = ctx["cfg"], ctx["log"]
    if "mapreduce" not in ctx or "rdd" not in ctx:
        log("[ERROR] La comparativa necesita los pasos de MapReduce y RDD en la misma ejecucion.")
        return 1
    mr, rdd = ctx["mapreduce"], ctx["rdd"]

    def veces(a, b):
        return round(a / b, 1) if a and b else None

    comparativa = {
        "fecha_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "calculo": "porcentaje de resenas positivas por juego (misma entrada para los dos)",
        "mapreduce": mr,
        "rdd": rdd,
        "rdd_veces_mas_rapido": {
            "solo_calculo": veces(mr["segundos_job"], rdd["segundos_calculo"]),
            "con_arranque": veces(mr["segundos_total"], rdd["segundos_total"])},
        "resultados_identicos": rdd["identicos_al_mapreduce"],
        "nota": ("segundos_job: desde 'Running job' hasta 'completed successfully' en el "
                 "registro de Hadoop; segundos_calculo: el que mide el script de RDD; "
                 "segundos_total: el comando entero, con el arranque de la JVM y del cliente."),
    }
    texto = json.dumps(comparativa, indent=2, ensure_ascii=False)
    log(texto)
    ruta = cfg["pipeline"]["comparativa_mr_rdd"]
    ejecutar(hdfs(cfg, "hdfs", "dfs", "-mkdir", "-p", ruta.rsplit("/", 1)[0]), log, mostrar=False)
    rc = ejecutar(["docker", "exec", "-i", cfg["entorno"]["contenedor_hdfs"],
                   "hdfs", "dfs", "-put", "-f", "-", ruta], log, entrada=texto + "\n")[0]
    if rc == 0:
        log("[PIPELINE] Comparativa guardada en " + ruta)
    return rc


def pasos():
    """(número, nombre, qué hace, función o script de Spark del proyecto).

    Los scripts se dan como ruta dentro del repositorio; el comando completo
    (contenedor, spark-submit, memoria...) se construye con la configuración."""
    return [
        (1, "descargar_dataset", "CSV de Kaggle (se salta si ya esta en HDFS o en data/)", paso_descargar),
        (2, "subir_a_hdfs", "CSV -> zona raw de HDFS (se salta si ya esta)", "src/ingesta/subir_a_hdfs.py"),
        (3, "descargar_catalogo", "catalogo de juegos de la API de Steam -> zona raw (se salta si ya esta)",
         "src/ingesta/descargar_catalogo.py"),
        (4, "etl_reviews", "ETL: CSV -> Parquet en la zona curada + cuarentena + informe de calidad (~10 min)",
         "src/procesamiento/etl_reviews.py"),
        (5, "verificar_curada", "controles de calidad sobre la zona curada", "src/procesamiento/verificar_curada.py"),
        (6, "limpiar_catalogo", "catalogo limpio -> zona curada", "src/procesamiento/limpiar_catalogo.py"),
        (7, "comparar_formatos", "CSV frente a Parquet: tamano y tiempo de lectura (3 medidas)",
         "src/procesamiento/comparar_formatos.py"),
        (8, "consultas", "preguntas Q1-Q3 -> zona resultados", "src/analisis/consultas.py"),
        (9, "optimizar_q1", "explain + optimizacion medida de Q1 (planes en evidence/fase5)",
         "src/analisis/optimizar_q1.py"),
        (10, "exportar_entrada_mapreduce", "entrada de texto para el MapReduce",
         "src/procesamiento/exportar_entrada_mapreduce.py"),
        (11, "mapreduce", "job de Hadoop Streaming en YARN (enciende y apaga YARN)", paso_mapreduce),
        (12, "rdd", "mismo calculo con RDDs de Spark y comparacion de resultados", paso_rdd),
        (13, "comparativa_mapreduce_rdd", "tiempos MapReduce frente a RDD -> zona resultados",
         paso_comparativa),
        (14, "ml", "LogisticRegression con todas las resenas (AUC)", "src/ml/modelo_recomendacion.py"),
        (15, "exportar_resultados", "copia los resultados pequenos y la cuarentena a evidence/resultados",
         "src/analisis/exportar_resultados.py"),
    ]


def elegir(args, todos):
    numeros = [n for n, *_ in todos]
    if args.solo:
        elegidos = [int(x) for x in args.solo.split(",") if x.strip()]
    else:
        elegidos = [n for n in numeros if args.desde <= n <= (args.hasta or numeros[-1])]
    desconocidos = sorted(set(elegidos) - set(numeros))
    if desconocidos:
        raise SystemExit("[ERROR] Pasos que no existen: {}".format(desconocidos))
    return [p for p in todos if p[0] in elegidos]


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass
    parser = argparse.ArgumentParser(description="Punto de entrada unico del pipeline Steam Reviews")
    parser.add_argument("--lista", action="store_true", help="muestra los pasos y termina")
    parser.add_argument("--desde", type=int, default=1, help="primer paso a ejecutar")
    parser.add_argument("--hasta", type=int, default=None, help="ultimo paso a ejecutar")
    parser.add_argument("--solo", default=None, help="solo estos pasos, separados por comas (p. ej. 11,12,13)")
    args = parser.parse_args()

    todos = pasos()
    if args.lista:
        for n, nombre, que, _ in todos:
            print("{:>3}. {:<28} {}".format(n, nombre, que))
        return 0
    elegidos = elegir(args, todos)

    marca = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    carpeta = RAIZ / "evidence" / "ejecuciones" / marca
    log = Registro(carpeta)
    ctx = {"log": log}
    resumen = {"inicio": marca, "pasos_pedidos": [n for n, *_ in elegidos], "pasos": []}
    inicio_total = time.perf_counter()
    rc_final = 0
    try:
        ctx["cfg"] = leer_config(log)
        log("[PIPELINE] Pasos: " + ", ".join("{}.{}".format(n, nombre) for n, nombre, *_ in elegidos))
        for n, nombre, que, accion in elegidos:
            log("")
            log("=" * 78)
            log("[PIPELINE] Paso {} · {} · {}".format(n, nombre, que))
            log("=" * 78)
            comando = None if callable(accion) else comando_spark(ctx["cfg"], accion)
            inicio = time.perf_counter()
            rc = accion(ctx) if callable(accion) else ejecutar(comando, log)[0]
            segundos = round(time.perf_counter() - inicio, 1)
            resumen["pasos"].append({"paso": n, "nombre": nombre, "segundos": segundos,
                                     "resultado": "OK" if rc == 0 else "FALLO (codigo {})".format(rc),
                                     "comando": None if comando is None else " ".join(comando)})
            log("[PIPELINE] Paso {} {} en {:.1f} s".format(n, "OK" if rc == 0 else "FALLO", segundos))
            if rc != 0:
                log("[PIPELINE] Se detiene el pipeline en el paso {}. Arreglalo y vuelve a lanzarlo "
                    "con --desde {}".format(n, n))
                rc_final = 1
                break
    except KeyboardInterrupt:
        log("[PIPELINE] Interrumpido a mano (Ctrl+C)")
        rc_final = 1
    except Exception as error:                      # error inesperado del propio pipeline
        import traceback
        log(traceback.format_exc())
        log("[ERROR] Error inesperado: {}".format(error))
        rc_final = 1
    finally:
        resumen["segundos_total"] = round(time.perf_counter() - inicio_total, 1)
        if "mapreduce" in ctx:
            resumen["mapreduce"] = ctx["mapreduce"]
        if "rdd" in ctx:
            resumen["rdd"] = ctx["rdd"]
        (carpeta / "resumen.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False),
                                             encoding="utf-8")
        log("")
        log("[PIPELINE] Resumen:")
        for p in resumen["pasos"]:
            log("  {:>3}. {:<28} {:>8.1f} s  {}".format(p["paso"], p["nombre"], p["segundos"], p["resultado"]))
        log("[PIPELINE] Total: {:.1f} min. Registro en evidence/ejecuciones/{}".format(
            resumen["segundos_total"] / 60, marca))
        log("[OK] Pipeline completo" if rc_final == 0 else "[FALLO] El pipeline no ha terminado")
        log.cerrar()
    return rc_final


if __name__ == "__main__":
    sys.exit(main())
