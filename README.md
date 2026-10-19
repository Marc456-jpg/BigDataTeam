# Steam Reviews 2021 · Arquitectura Big Data local (Entrega 1)

Proyecto de la asignatura **Big Data & Cloud Computing** (Tecnun, Universidad de Navarra, curso 26-27).

## Equipo

| Integrante | Usuario de GitHub |
|---|---|
| Manuel Fernández Pérez-Ricarte | ManuelFernandezPR |
| Marc Pich-Aguilera Béjar | (lo añade Marc) |
| Nora Cepas Zapico | (lo añade Nora) |

**Documento de la entrega:** `docs/Memoria_Entrega1_SteamReviews.pdf` (arquitectura, evidencias de A a F, resultados, limitaciones y registro de decisiones).

Una arquitectura Big Data completa que corre en un solo PC con Docker: **HDFS + YARN + Spark + Jupyter**, desde la ingesta del dataset hasta un modelo de Machine Learning, con un **único punto de entrada** (`python run_pipeline.py`).

**Caso de uso.** Un publisher de videojuegos quiere vigilar la opinión sobre sus juegos a partir de las reseñas de Steam. Por ejemplo, saber qué juegos valora mejor la comunidad, cómo cambia la opinión según las horas jugadas, qué reseñas consideran útiles los jugadores de cada idioma, y si se puede predecir si una reseña será positiva.

**Dataset.** [Steam Reviews Dataset 2021](https://www.kaggle.com/datasets/najzeko/steam-reviews-2021) (Kaggle, `najzeko/steam-reviews-2021`, licencia GPL-2):

- un único CSV de **8.171.787.229 bytes (8,17 GB)**;
- **21.747.371 reseñas** de **315 juegos**, entre noviembre de 2010 y el 23 de enero de 2021.

**Tabla auxiliar.** Un catálogo de los 315 juegos (nombre, géneros, desarrollador, precio), descargado de la API pública de la tienda de Steam. Se une a las reseñas por `app_id`.

---

## Índice

1. [Qué hay en el repositorio](#1-qué-hay-en-el-repositorio)
2. [Dónde está cada parte del enunciado](#2-dónde-está-cada-parte-del-enunciado)
3. [Arquitectura](#3-arquitectura)
4. [Requisitos](#4-requisitos)
5. [Instalación (desde cero)](#5-instalación-desde-cero)
6. [Obtener los datos](#6-obtener-los-datos)
7. [Ejecutar el pipeline completo](#7-ejecutar-el-pipeline-completo)
8. [Ejecutar un paso suelto](#8-ejecutar-un-paso-suelto)
9. [Configuración](#9-configuración-configpipelineyaml)
10. [Organización de HDFS](#10-organización-de-hdfs)
11. [Resultados](#11-resultados)
12. [Evidencias](#12-evidencias)
13. [Webs de control](#13-webs-de-control)
14. [Apagar, arrancar y no perder datos](#14-apagar-arrancar-y-no-perder-datos)
15. [Problemas conocidos y soluciones](#15-problemas-conocidos-y-soluciones)
16. [Decisiones de diseño principales](#16-decisiones-de-diseño-principales)
17. [Limitaciones](#17-limitaciones)

---

## 1. Qué hay en el repositorio

```
steam-bigdata/
├── README.md                    ← este archivo
├── docker-compose.yml           ← el entorno completo; se levanta con un solo comando
├── run_pipeline.py              ← PUNTO DE ENTRADA ÚNICO del pipeline (15 pasos)
├── docs/
│   └── Memoria_Entrega1_SteamReviews.pdf   ← el documento de la entrega
├── config/
│   └── pipeline.yaml            ← todas las rutas y parámetros (el código no lleva ninguno escrito)
├── docker/
│   ├── hadoop.env               ← configuración de Hadoop y YARN adaptada a ≈ 8 GB de RAM
│   ├── run-namenode.sh, run-datanode.sh, run-resourcemanager.sh, run-nodemanager.sh
│   │                            ← arranque de Hadoop con «exec» (apagado ordenado)
│   ├── nodemanager/Dockerfile   ← imagen del NodeManager + Python 3 (para Hadoop Streaming)
│   └── docker-compose.yml       ← copia del compose del profesor (solo como referencia; no se usa)
├── scripts/
│   └── descargar_dataset.py     ← descarga reproducible del CSV desde Kaggle
├── src/
│   ├── ingesta/                 ← subir_a_hdfs.py, descargar_catalogo.py
│   ├── procesamiento/           ← etl_reviews.py, verificar_curada.py, limpiar_catalogo.py,
│   │                              comparar_formatos.py, exportar_entrada_mapreduce.py,
│   │                              positivos_por_juego_rdd.py
│   ├── analisis/                ← consultas.py (Q1-Q3), optimizar_q1.py, exportar_resultados.py
│   └── ml/                      ← modelo_recomendacion.py
├── mapreduce/
│   ├── positivos_por_juego/     ← mapper y reducer de Hadoop Streaming (apartado C)
│   └── prueba_streaming/        ← prueba de Hadoop Streaming con UTF-8 (fase 1)
├── notebooks/
│   └── 00_prueba_humo.ipynb     ← prueba de que todas las piezas se hablan (con sus resultados)
├── evidence/                    ← capturas, planes de ejecución, métricas y resultados (sección 12)
├── data/                        ← el CSV (NO está en GitHub: .gitignore)
└── secrets/                     ← credenciales de Kaggle (NO está en GitHub: .gitignore)
```

`data/` y `secrets/` no se suben nunca a GitHub. El CSV pesa 8 GB y se descarga con el script (sección 6), y las claves son personales.

El archivo `.gitattributes` obliga a guardar todos los archivos de texto con finales de línea de Linux (LF), porque se ejecutan dentro de contenedores Linux.

---

## 2. Dónde está cada parte del enunciado

| Apartado | Qué se pide | Dónde está |
|---|---|---|
| **A** | Entorno Docker con un solo comando | `docker-compose.yml` → `docker compose up -d` (sección 5) |
| **B** | Ingesta en zonas raw / curada / resultados | `src/ingesta/subir_a_hdfs.py`, sección 10 |
| **B** | Parquet particionado | `src/procesamiento/etl_reviews.py` → `/curada/reviews`, particionado por `year` |
| **B** | `hdfs fsck` con bloques y réplicas | `evidence/fase2/2.2_fsck_raw_steam_reviews.txt` |
| **B** | Estructura de las zonas (`hdfs dfs -ls -R`) | `evidence/fase2/2.5_hdfs_ls_R_zonas.txt` |
| **B** | Comparación CSV frente a Parquet | `src/procesamiento/comparar_formatos.py` → `evidence/resultados/comparativa_csv_vs_parquet.json` |
| **C** | Job de MapReduce (Hadoop Streaming, Python) | `mapreduce/positivos_por_juego/` (paso 11 del pipeline) |
| **C** | El mismo cálculo con RDDs y la comparación | `src/procesamiento/positivos_por_juego_rdd.py` (paso 12) y comparación automática (paso 13) |
| **D** | ETL con DataFrames y esquema explícito | `src/procesamiento/etl_reviews.py` (esquema de 23 columnas) |
| **D** | 3 preguntas (agrupación, join, ventana) | `src/analisis/consultas.py` → `evidence/resultados/q1…q3` |
| **D** | `explain()`, Spark UI y optimización medida | `src/analisis/optimizar_q1.py`, `evidence/fase5/` |
| **E** | Punto de entrada único | `run_pipeline.py` |
| **E** | Configuración separada del código | `config/pipeline.yaml` |
| **E** | Re-ejecuciones idempotentes | Todos los pasos (sección 7) |
| **E** | Calidad de datos | Informe del ETL, `verificar_curada.py` y zona `/quarantine` |
| **E** | Línea base de MLlib | `src/ml/modelo_recomendacion.py` (LogisticRegression, AUC) |
| **F** | Desafío 2: tolerancia a fallos | `evidence/fase7/` (sección 11.7) |

---

## 3. Arquitectura

```
                         Tu navegador (solo en este PC)
           localhost:8888 (Jupyter)      localhost:4040 (Spark UI de cada programa)
                                   │
   PowerShell ── python run_pipeline.py ── docker exec ──┐
                                   ▼                     │
                     ┌──────────────────────────┐        │
                     │ jupyter = driver de Spark│◄───────┘  (también: hdfs y hadoop jar en el namenode)
                     └────────────┬─────────────┘
                                  │ spark://spark-master:7077
                     ┌────────────▼─────────────┐      ┌───────────────────────────┐
                     │ spark-master (8080)      │─────►│ spark-worker (8081)       │
                     └──────────────────────────┘      │ 4 núcleos, 2 GB executors │
                                                       └────────────┬──────────────┘
                                                                    │ lee / escribe bloques
            ┌──────────────────────┐   índice   ┌───────────────────▼─────────────────┐
            │ namenode (9870, 9000)│◄──────────►│ datanode1   datanode2  (replicación 2)│
            └──────────────────────┘            └─────────────────────────────────────┘
            Perfil «yarn» (solo para el MapReduce): resourcemanager (8088) + nodemanager (con Python 3)
```

| Contenedor | Imagen | Tope de memoria | Para qué |
|---|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 1 GB (Java 512 MB) | Índice de HDFS: sabe dónde está cada bloque |
| `datanode1`, `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | 768 MB (Java 384 MB) | Guardan los bloques; replicación 2 |
| `spark-master` | `quay.io/jupyter/pyspark-notebook@sha256:7cad5357…` | 768 MB | Reparte el trabajo de Spark |
| `spark-worker` | La misma imagen | 2.816 MB | Ejecuta las tareas: 4 núcleos y 2 GB para executors (cada programa usa un executor de 1,5 GB) |
| `jupyter` | La misma imagen | 2 GB | Entorno de desarrollo y driver de Spark |
| `resourcemanager` *(perfil yarn)* | `bde2020/hadoop-resourcemanager:…` | 1 GB | Reparte memoria y CPU a MapReduce |
| `nodemanager` *(perfil yarn)* | `steam-bigdata/hadoop-nodemanager-python:1.0` (se construye solo) | 2.816 MB | Ejecuta el mapper y el reducer en Python 3.5 |

**Versiones:** Spark 4.2.0, Python 3.13.15 y Java 21 en `jupyter`, `spark-master` y `spark-worker` (la misma imagen, fijada por su digest), y Hadoop 3.2.1 con Java 8.

**Por qué YARN va en un perfil aparte.** Con YARN siempre encendido, la memoria llegaría a ≈ 7,4 de 7,57 GiB mientras Spark trabaja. Por eso `docker compose up -d` levanta HDFS + Spark + Jupyter, y YARN solo se enciende durante el job de MapReduce. Lo hace el propio pipeline, que lo apaga después pase lo que pase.

---

## 4. Requisitos

| Qué | Mínimo | Comprobado en |
|---|---|---|
| Sistema | Windows 10/11 con **Docker Desktop** (WSL2) y Docker Compose v2 | Windows, PC de Manu |
| RAM para Docker | **8 GB** (Docker Desktop da la mitad de la RAM del PC: hace falta un PC de 16 GB) | 7,57 GiB para Docker; pico medido de 3,81 GiB en uso |
| Disco libre en C: | **≥ 50 GB recomendados** (ver nota) | — |
| Git | Cualquier versión reciente | Git 2.55 |
| Python en el PC | **3.8 o posterior**, solo para `run_pipeline.py`; usa solo la biblioteca estándar, así que no hay que instalar nada más | Python 3.14.7 |
| Cuenta de Kaggle | Gratuita; solo para descargar el CSV | — |
| Internet | Para descargar las imágenes, el dataset y el catálogo | — |

**Nota sobre el disco.** Las imágenes de Docker ocupan ≈ 9 GB, y el CSV ocupa 8,2 GB en `data/`.

Dentro de HDFS, con 2 copias:

- el CSV ocupa 16,3 GB;
- el Parquet, 8,5 GB;
- los temporales del ETL, varios GB más.

Docker guarda todo en un disco virtual (`docker_data.vhdx`) que **crece y no encoge**. En nuestro PC llegó a **114 GB** con solo 33 GB de datos dentro. **Cada ejecución del ETL lo hizo crecer ≈ 25-38 GB**: 25 GB en un intento fallido (49 min) y 38 GB en la ejecución buena, con el disco recién compactado. Si C: se llena en mitad de una escritura, HDFS puede quedar dañado. Cómo devolver ese espacio a Windows: sección 15.1.

Probado solo en Windows. En Linux o macOS debería funcionar con `python3` en lugar de `python`, pero no lo hemos comprobado. Las imágenes de Hadoop son amd64.

---

## 5. Instalación (desde cero)

Todo se hace en **PowerShell normal** (no como administrador, y no en CMD).

**1. Clonar el repositorio** en una carpeta fuera de `System32` y de OneDrive. Recomendado: `C:\steam-bigdata`.

```powershell
cd C:\
git clone https://github.com/Marc456-jpg/BigDataTeam.git steam-bigdata
cd C:\steam-bigdata
```

**2. Abrir Docker Desktop** y esperar a que diga *Engine running*.

**3. Levantar el entorno con un solo comando:**

```powershell
docker compose up -d
```

- La primera vez descarga las imágenes (varios GB), así que tarda.
- Levanta **6 contenedores**: namenode, datanode1, datanode2, spark-master, spark-worker y jupyter.
- YARN **no** arranca aquí: lo enciende el pipeline cuando lo necesita.

**4. Comprobar que está todo sano** (≈ 1-2 min después):

```powershell
docker compose ps
docker exec namenode hdfs dfsadmin -safemode get
docker exec namenode hdfs fsck / | Select-String "Status|Under-replicated"
```

Lo esperado:

- 6 servicios `Up … (healthy)`;
- `Safe mode is OFF`;
- `Status: HEALTHY`;
- `Under-replicated blocks: 0`.

**5. (Opcional) Abrir Jupyter:** <http://localhost:8888/lab?token=steam>. La carpeta del proyecto aparece como `work/`.

---

## 6. Obtener los datos

**No hace falta hacerlo a mano.** El paso 1 del pipeline descarga el CSV si no está ni en HDFS ni en `data/`. Solo hacen falta las credenciales de Kaggle.

**1. Crear un token de Kaggle.** En <https://www.kaggle.com/settings>, sección *API*, crea un token. Guárdalo de **una** de estas formas:

| Forma | Dónde |
|---|---|
| a) Token en una sola línea (recomendada) | `C:\steam-bigdata\secrets\access_token` |
| b) Archivo `kaggle.json` descargado (formato antiguo) | `C:\steam-bigdata\secrets\kaggle.json` |
| c) Variables de entorno | `KAGGLE_API_TOKEN`, o `KAGGLE_USERNAME` + `KAGGLE_KEY` |

Por ejemplo, para la forma a):

```powershell
New-Item -ItemType Directory -Force secrets | Out-Null
Set-Content secrets\access_token "PEGA_AQUI_TU_TOKEN"
```

La carpeta `secrets/` está en `.gitignore`: **las claves nunca llegan a GitHub.**

**2. Descargar** (opcional, porque `run_pipeline.py` lo hace solo):

```powershell
docker exec jupyter python /home/jovyan/work/scripts/descargar_dataset.py
```

El script, en orden:

1. Mira si `data/steam_reviews.csv` ya existe con el tamaño exacto (8.171.787.229 bytes). Si es así, no hace nada.
2. Comprueba que hay ≥ 12 GiB libres.
3. Instala la librería oficial `kaggle==2.2.4`.
4. Descarga el zip (2,97 GB), lo descomprime en `data/` y borra el zip.
5. Comprueba el tamaño exacto del CSV.

Con `--forzar` repite la descarga.

> **Transparencia.** La descarga se probó con una imitación de Kaggle. En nuestro PC el CSV se descargó desde la web de Kaggle, así que la descarga real con token no se ha ejecutado de principio a fin. **Alternativa manual:** descargar el zip desde la página del dataset, descomprimirlo y dejar `steam_reviews.csv` en `C:\steam-bigdata\data\`.

**El catálogo de juegos** (tabla auxiliar) no necesita cuenta ni clave. Lo descarga el paso 3 desde la API pública `store.steampowered.com/api/appdetails`, con:

- idioma inglés y país EE. UU. fijos;
- 1,5 s entre peticiones;
- hasta 4 reintentos;
- ≈ 10-11 min de duración.

---

## 7. Ejecutar el pipeline completo

Con el entorno levantado (sección 5), en PowerShell, en `C:\steam-bigdata`:

```powershell
python run_pipeline.py
```

`run_pipeline.py` se ejecuta en el PC, no dentro de un contenedor.

- Lanza cada paso con `docker exec`: los scripts de Spark en `jupyter`, y HDFS y Hadoop en `namenode`.
- Lee los parámetros de `config/pipeline.yaml`.
- Se detiene en el primer paso que falle y dice cómo seguir.

Va fuera de los contenedores porque tiene que manejar a la vez Spark, Hadoop y el encendido de YARN, y eso solo se puede hacer desde fuera.

| Paso | Nombre | Qué hace | Tiempo medido¹ |
|---|---|---|---|
| 1 | `descargar_dataset` | CSV de Kaggle; se salta si ya está en HDFS o en `data/` | 1,4 s |
| 2 | `subir_a_hdfs` | CSV → `/raw`; se salta si ya está con el tamaño exacto | 15,6 s (subirlo de verdad: ≈ 15 min) |
| 3 | `descargar_catalogo` | Catálogo de Steam → `/raw/games`; se salta si ya está | 10,4 s (descargarlo de verdad: ≈ 10,6 min) |
| 4 | `etl_reviews` | ETL: CSV → Parquet en `/curada/reviews` + cuarentena + informe de calidad | 580,0 s (9,7 min) |
| 5 | `verificar_curada` | Vuelve a leer lo escrito y comprueba 4 reglas (devuelve 0 o 1) | 36,7 s |
| 6 | `limpiar_catalogo` | Catálogo limpio → `/curada/games` | 17,6 s |
| 7 | `comparar_formatos` | CSV frente a Parquet: tamaño y tiempo de lectura (mediana de 3 medidas) | 70,3 s (con 3 medidas: 151,2 s²) |
| 8 | `consultas` | Q1-Q3 → `/resultados/q1…q3` | 64,2 s |
| 9 | `optimizar_q1` | `explain()` + 3 versiones de Q1 medidas; planes en `evidence/fase5/` | 37,3 s |
| 10 | `exportar_entrada_mapreduce` | Texto `app_id<TAB>recommended` para Hadoop Streaming | 23,2 s |
| 11 | `mapreduce` | Enciende YARN → espera a que esté sano → `hadoop jar` (Streaming) → **apaga YARN siempre** | 105,6 s |
| 12 | `rdd` | El mismo cálculo con RDDs; comprueba que el resultado es idéntico | 20,5 s |
| 13 | `comparativa_mapreduce_rdd` | Compara tiempos y resultados → `/resultados/comparativa/mapreduce_vs_rdd.json` | 3,1 s |
| 14 | `ml` | LogisticRegression con las 21,6 M de reseñas (AUC) | 226,0 s |
| 15 | `exportar_resultados` | Copia los resultados pequeños de HDFS a `evidence/resultados/` | 7,4 s |
| | **Total** | | **≈ 20,4 min (19,9 min de los pasos 4-15 + 27 s de los pasos 1-3)** |

¹ Ejecución final del 19 oct 2026, con los datos ya en HDFS. Los pasos 4-15 son de `evidence/ejecuciones/20261019_183937/` (`--desde 4`). Los pasos 1-3, que solo comprueban y se saltan, son de la ejecución anterior del mismo día (`20261019_163933/`). Desde un PC vacío hay que sumar la descarga (depende de la conexión), la subida a HDFS (≈ 15 min) y el catálogo (≈ 10,6 min).

² Desde el 19 oct por la noche el paso 7 mide cada consulta 3 veces y da la mediana (`evidence/ejecuciones/20261020_011845/`). Tarda ≈ 1,3 min más, así que el total queda en ≈ 21,8 min.

**Opciones:**

```powershell
python run_pipeline.py --lista           # ver los pasos con su número
python run_pipeline.py --desde 5         # empezar en el paso 5 (por ejemplo, sin repetir el ETL)
python run_pipeline.py --hasta 9         # parar después del paso 9
python run_pipeline.py --solo 11,12,13   # solo esos pasos (la comparativa necesita el 11 y el 12)
```

**Qué guarda cada ejecución:**

- `evidence/ejecuciones/AAAAMMDD_HHMMSS/pipeline.log`: todo lo que salió en pantalla.
- `evidence/ejecuciones/AAAAMMDD_HHMMSS/resumen.json`: cada paso con su comando, sus segundos y su resultado.
- La última línea es `[OK] Pipeline completo` o `[FALLO] El pipeline no ha terminado`.

**Idempotencia.** Ejecutarlo varias veces da el mismo resultado y no duplica nada:

- las descargas y la subida se saltan si ya están hechas;
- todas las escrituras de Spark reemplazan lo anterior (`overwrite`);
- el MapReduce borra su carpeta de salida antes de empezar.

**Comprobado el 19 oct**, rehaciendo todo desde el CSV y comparando con el commit anterior (`git status`):

- Q1, Q2, Q3, el resultado del MapReduce y el plan V1 salieron **idénticos byte a byte**.
- El resultado del RDD tiene **las mismas 315 líneas en otro orden**: `reduceByKey` no ordena la salida. Comprobado ordenando las dos: coinciden con el MapReduce.
- En los planes V2 y V3 solo cambia la estimación de filas de la agregación parcial (`Statistics(… rowCount=…)`), porque el Parquet nuevo tiene otro tamaño y se lee en otros trozos.
- Los informes JSON cambian en los tiempos medidos y, el del ETL, también en los campos nuevos de la cuarentena.

**Comprobado otra vez la noche del 19 al 20 oct**, con el código final (todo el entorno en `config/pipeline.yaml`): pasos 5-15 (`20261019_233334/`) y paso 7 (`20261020_011845/`). Según `git diff --stat`, solo cambiaron los 3 JSON que guardan tiempos (`comparativa_*` y `ml_regresion_logistica.json`); Q1-Q3, el MapReduce, el RDD, el informe de calidad y los 3 planes de `evidence/fase5/` salieron idénticos byte a byte (`evidence/fase8/8.1_12_git_status_diff_reejecucion_20oct.png`).

**Mientras corre:**

- no suspendas el PC: Docker se congela;
- no abras notebooks con Spark: ocuparían los 4 núcleos del worker y el pipeline se quedaría esperando;
- no cierres la ventana.

Para pararlo a mano, pulsa `Ctrl+C`. Los datos de `/raw` no se tocan nunca.

---

## 8. Ejecutar un paso suelto

Todos los scripts de Spark se lanzan igual, desde PowerShell, con `spark-submit` dentro del contenedor `jupyter`:

```powershell
docker exec -u jovyan jupyter /usr/local/spark/bin/spark-submit --driver-memory 1g --conf spark.log.level=WARN /home/jovyan/work/src/<carpeta>/<script>.py [opciones]
```

- `python script.py` a secas **no funciona**: da `No module named 'pyspark'`. En esta imagen, PySpark solo viene preparado en los notebooks, y `spark-submit` lo prepara él mismo.
- `--conf spark.log.level=WARN` quita las ≈ 1.300 líneas de información interna de Spark.

| Script | Opciones útiles |
|---|---|
| `src/ingesta/subir_a_hdfs.py` | `--forzar`: vuelve a subir el CSV aunque ya esté |
| `src/ingesta/descargar_catalogo.py` | `--forzar`: vuelve a descargar el catálogo |
| `src/ml/modelo_recomendacion.py` | `--fraccion 0.1`: entrena con el 10 % (≈ 1 min) |
| `src/analisis/optimizar_q1.py` | `--pausa 300`: deja la Spark UI (localhost:4040) abierta 300 s al final |
| Todos los de Spark | `--config`: otra configuración |

**El MapReduce a mano.** El paso 11 hace exactamente esto:

```powershell
docker compose --profile yarn up -d resourcemanager nodemanager
docker cp mapreduce\positivos_por_juego namenode:/tmp/
docker exec namenode hdfs dfs -rm -r -f /resultados/mapreduce/positivos_por_juego
docker exec namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -files /tmp/positivos_por_juego/mapper_positivos.py,/tmp/positivos_por_juego/reducer_positivos.py -mapper "python3 mapper_positivos.py" -reducer "python3 reducer_positivos.py" -input /curada/mapreduce_entrada -output /resultados/mapreduce/positivos_por_juego
docker compose --profile yarn stop resourcemanager nodemanager
```

- Antes de lanzar `hadoop jar`, espera a que `docker compose --profile yarn ps` muestre los dos contenedores de YARN como `(healthy)`.
- La primera vez que se enciende el perfil `yarn`, Docker construye la imagen del NodeManager con Python 3. Hace falta internet, porque usa `archive.debian.org`.

**Ver datos en HDFS:**

```powershell
docker exec namenode hdfs dfs -ls -R /resultados
docker exec namenode hdfs dfs -du -s -h /raw /curada /resultados /quarantine
docker exec namenode hdfs fsck /raw/steam_reviews.csv -files -blocks -locations
docker exec namenode hdfs dfs -cat /quarantine/reviews/part-*
```

---

## 9. Configuración (`config/pipeline.yaml`)

Ningún script lleva rutas ni parámetros escritos dentro: todo sale de este archivo, también los del entorno Docker (sección `entorno`). En `run_pipeline.py` solo quedan los dos datos imprescindibles para leer la propia configuración: dónde está el archivo y qué contenedor traduce el YAML (`jupyter`, que tiene la librería; así el PC no necesita instalar nada).

| Sección | Qué guarda |
|---|---|
| `dataset` | Id de Kaggle, nombre del CSV y tamaño exacto (sirve para comprobar descargas y subidas) |
| `local` | Carpetas `data/` y `secrets/` |
| `entorno` | Cómo se lanzan los pasos: contenedores (`jupyter`, `namenode`), usuario, carpeta del proyecto dentro de jupyter, `spark-submit`, nivel de log, perfil y servicios de YARN, JAR de Hadoop Streaming y carpeta temporal |
| `hdfs` | NameNode (`hdfs://namenode:9000`) y zonas: `/raw`, `/curada`, `/resultados`, `/quarantine` |
| `spark` | Master, memorias (driver 1g, **executor 1536m**), zona horaria **UTC**, replicación 2, 32 particiones de shuffle |
| `etl` | Rutas, criterio de partición (`auto`, umbral de 128 MB), grupos de filas de Parquet de 16 MB, rango de fechas válido (2003-09-12 … 2021-12-31), informe de calidad y cuarentena |
| `mapreduce` | Entrada (8 archivos de texto), salidas del MapReduce y del RDD, carpeta y nombres del mapper y del reducer, intérprete |
| `catalogo` | URL de la API de Steam, idioma y país, pausa y reintentos |
| `consultas` | Q1: mínimo de 1.000 reseñas y top 10; Q3: año 2020 y top 5; rutas de las respuestas; repeticiones y carpeta de los planes de la optimización |
| `comparar_formatos` | Repeticiones de cada medida CSV/Parquet (3) y ruta del informe |
| `ml` | Fracción (1.0 = todo), 20 % de prueba, semilla 42, 10 idiomas, géneros ≥ 10 % de los juegos, máximo de 50 iteraciones, trozos de 32 MB |
| `exportar` | Carpeta local de resultados (`evidence/resultados`), tamaño máximo por archivo y la lista de qué se copia (incluida la cuarentena) |
| `pipeline` | Espera máxima a YARN (240 s) y ruta de la comparativa MapReduce/RDD |

---

## 10. Organización de HDFS

```
/
├── raw/                          datos originales, nunca se modifican
│   ├── steam_reviews.csv         8,17 GB = 61 bloques de 128 MB × 2 copias
│   └── games/appdetails.jsonl    respuestas crudas de la API de Steam (315 juegos)
├── curada/                       datos limpios
│   ├── reviews/year=2010 … year=2021/   Parquet (snappy), 1 archivo por año, 4.185 MB por copia
│   ├── games/                    catálogo limpio en Parquet
│   └── mapreduce_entrada/        8 archivos de texto app_id<TAB>recommended
├── resultados/                   lo que producen los análisis
│   ├── calidad/etl_reviews.json
│   ├── comparativa/csv_vs_parquet.json, mapreduce_vs_rdd.json
│   ├── q1/, q2/, q3/             CSV
│   ├── mapreduce/, rdd/          positivos_por_juego
│   └── ml/                       informe JSON + modelo guardado
└── quarantine/reviews/           filas inválidas del ETL (JSON, con su motivo)
```

**Por qué se particiona por `year` y no por año + mes.** El ETL mide antes de decidir. La partición mensual típica (mediana) pesaría **24,7 MB**, lejos de los 128 MB de un bloque, y habría creado 123 archivos pequeños. La anual pesa **192,5 MB**: «cientos de MB, no KB», como pide el enunciado.

---

## 11. Resultados

Todos los resultados están en `evidence/resultados/`, copiados de HDFS por el paso 15. El documento de la entrega los explica en detalle.

### 11.1 Calidad de datos (ETL)

```
  21.747.371 filas leídas del CSV
  −        7 corruptas (Spark no pudo interpretarlas)       → /quarantine/reviews
  −        0 sin columna clave · 0 con fecha fuera de rango → /quarantine/reviews
  −  134.927 duplicadas por review_id (se queda la versión más reciente)
  = 21.612.437 filas en /curada/reviews
```

- **Filas en cuarentena:** 7 (las 7 corruptas), exportadas con su motivo a `evidence/resultados/cuarentena_reviews.json`.
- **Executors perdidos en el ETL:** 0, con executors de 1,5 GB. Con 1 GB, el mismo día, se perdieron 6 y el ETL abortó (sección 15).
- **Avisos que se cuentan pero no se borran:**
  - 33.742 reseñas sin texto;
  - 25.682 sin horas de juego;
  - 543 `votes_funny` y 2 `votes_helpful` con el valor imposible 4.294.967.295 (2³²−1).
- **Fechas:** del 19-11-2010 al 23-01-2021.

`verificar_curada.py` lo comprueba de forma independiente:

- filas escritas = informe;
- ninguna `review_id` repetida;
- 0 claves nulas;
- cada fila en la carpeta de su año.

### 11.2 CSV frente a Parquet

| | CSV (`/raw`) | Parquet (`/curada`) | Parquet, solo 2020 |
|---|---|---|---|
| Tamaño (1 copia) | 7.793 MB | 4.185 MB (53,7 %) | — |
| ¿Cuántas recomiendan? (mediana de 3; rango) | 44,8 s (42,8-46,5) | 0,9 s (0,8-2,1) | 0,4 s (0,4-0,5; poda de particiones) |

- Parquet ocupa el 53,7 % y la consulta es **≈ 50 veces** más rápida (44,8 / 0,9 = 49,8).
- **Método:** cada consulta se mide 3 veces, en ronda (CSV, Parquet, Parquet 2020, CSV…), en el mismo clúster y sin otros programas abiertos en el PC; se da la mediana (`evidence/resultados/comparativa_csv_vs_parquet.json`, con las 9 medidas; ejecución `20261020_011845/`).
- La primera lectura de Parquet (2,1 s) es la más lenta, probablemente por el arranque en frío. Con una sola medida, una ejecución anterior dio «15 veces» (3,7 s frente a 54,7 s): por eso ahora se repite.
- La carga del PC también cuenta: con un vídeo reproduciéndose, el CSV tardó un 28 % más (62,5 s de mediana, `20261019_233334/`).

### 11.3 MapReduce frente a RDD (porcentaje de reseñas positivas por juego)

| | Hadoop Streaming en YARN | Spark RDD (`reduceByKey`) |
|---|---|---|
| Cálculo | 61,5 s | 13,3 s |
| Comando entero (con arranque) | 80,1 s | 22,6 s |
| Resultado | 315 juegos | **Idéntico** (comprobado automáticamente) |

Spark es ≈ 4,6 veces más rápido en el cálculo (3,5 contando el arranque; ejecución `20261019_233334`). **Nota honesta:** esa ejecución, de la que salen también los tiempos de la sección 11.5 y del ML, se hizo con un vídeo abierto en el PC. Con el PC libre (19 oct, 18:39, `20261019_183937/`) fueron 56,4 s frente a 11,9 s (4,7 veces): la proporción apenas cambia. El motivo principal:

- `reduceByKey` suma dentro de cada trozo antes de enviar nada (un *combiner*), así que por la red viajan como mucho 2.520 totales parciales;
- nuestro MapReduce, sin combiner, envía 21,6 millones de pares, y además arranca un contenedor de YARN por tarea.

### 11.4 Las tres preguntas (apartado D)

- **Q1 · agrupación + join.** Los 10 juegos con mayor % de reseñas positivas (mínimo 1.000 reseñas), con sus géneros, desarrollador y precio del catálogo.
  - 1.º ULTRAKILL (99,53 %), 2.º Senren＊Banka (99,40 %), 3.º A Short Hike (99,14 %).
  - Los mejor valorados son sobre todo indies; ninguno de los más reseñados (PUBG, GTA V) entra.
- **Q2 · media y mediana.** Horas jugadas al escribir la reseña:
  - quien **no** recomienda llevaba más horas (mediana 37,3 h frente a 30,8 h; media 202,5 h frente a 139,5 h);
  - la media es entre 4,5 y 5,4 veces la mediana, así que la disparan unos pocos jugadores con miles de horas.
- **Q3 · ventana `DENSE_RANK`.** Top 5 de juegos por idioma según votos de «útil» en 2020 (28 idiomas):
  - en inglés lidera Among Us (171.445);
  - en chino simplificado, Wallpaper Engine.
  - Solo se lee la carpeta `year=2020`.

### 11.5 Optimización medida de Q1

| Versión | Join | Shuffles | Tiempo |
|---|---|---|---|
| V1 · join primero | SortMergeJoin | 2 | 4,6 s |
| V2 · join primero, catálogo en broadcast | BroadcastHashJoin | 1 | 2,0 s |
| V3 · agregar antes + broadcast | BroadcastHashJoin | 1 | 1,1 s (**4,3 veces más rápida**) |

Cada versión se mide 2 veces y se da la mejor: la primera medida suele ser más lenta (V1: 7,9 s y 4,6 s; ejecución `20261019_233334`, con un vídeo abierto en el PC). Con el PC libre (19 oct, 18:39) fueron 4,0 → 1,6 → 0,9 s (4,6 veces).

- Las 3 dan el mismo top 10.
- En V3 solo cruzan la red ≈ 7.160 totales parciales (`rowCount=7.16E+3` en el plan; un total por juego y por trozo leído), en vez de 21,6 M de filas.
- Los planes (`explain` formatted) están en `evidence/fase5/5.2_plan_*.txt` y las capturas de la Spark UI en `evidence/fase5/5.2_0*`.
- La pestaña **Stages** (`5.2_07_spark_ui_stages_a/b.png`) mide lo que cruza la red: V1 escribe **20,8 MiB** de shuffle (34 tareas) que lee la etapa del join (5 tareas); V2, **379,8 KiB**; V3, **188,9 KiB** (56 y 113 veces menos). Cada lectura del Parquet son solo 8,4 MiB, porque Spark lee únicamente las columnas que usa. Las 9 etapas «skipped» son shuffles ya calculados que Spark reutiliza al repetir la medida.

### 11.6 Machine Learning (línea base, apartado E)

**Modelo.** `LogisticRegression` que predice `recommended`. Se entrena con 80 % / 20 % de las **21.612.437** reseñas, con semilla 42.

**Variables (23).** Solo información que existía al escribir la reseña:

- horas jugadas al reseñar (log) y si no constan;
- longitud del texto (log);
- compra en Steam, recibido gratis y acceso anticipado;
- 10 idiomas;
- 7 géneros del catálogo.

**Variables excluidas:**

- las que ocurren después de la reseña (horas totales, votos, comentarios), para no filtrar información del futuro;
- el precio y «gratis» del catálogo, porque son los de hoy y no los de la fecha de la reseña.

| Métrica | Valor |
|---|---|
| AUC ROC (prueba) | **0,7451** (0,5 = azar) |
| AUC PR | 0,9469 |
| Acierto con umbral 0,5 | 87,41 % (decir siempre «sí»: 87,43 %) |

Se mide con AUC porque el 87,4 % de las reseñas son positivas: un modelo que dijera siempre «sí» ya acertaría el 87 %.

Las variables que más pesan son la longitud del texto (−0,50: las reseñas largas suelen ser negativas), Indie (+0,48) y el chino simplificado (−0,39). Son correlaciones, no causas.

### 11.7 Desafío 2: tolerancia a fallos

Desafío elegido: **tolerancia a fallos**, con los 2 DataNodes del entorno (replicación 2). El trabajo de prueba es `python run_pipeline.py --solo 5` (`verificar_curada`):

- solo lee, así que no toca los resultados;
- acaba con «TODO CUADRA», lo que prueba que el fallo no alteró los datos.

Evidencias en `evidence/fase7/`: `7.1_00`-`7.1_16` para el escenario 1 y `7.2_00`-`7.2_05` para el escenario 2.

**Escenario 1: se apaga un DataNode en mitad del trabajo (19 oct).**

| Hora (UTC) | Qué pasó |
|---|---|
| 18:13:10 | Arranca el trabajo. A los 2 s, `docker compose stop datanode2` |
| 18:13:33-18:14:58 | Cada tarea que intenta leer de datanode2 recibe `NoRouteToHostException` y lee la otra copia, la de datanode1. Hay 150 avisos `WARN` en el log del executor y ningún error |
| 18:14:21 | El NameNode aún cuenta 2 DataNodes vivos (`dfsadmin`) y 0 bloques sub-replicados |
| 18:14:58 | **TODO CUADRA** (21.612.437 filas), en 112,5 s en vez de ≈ 37 s: el tiempo extra son los intentos de leer del nodo apagado |
| 18:24:07 | `lost heartbeat from 172.18.0.3:9866`: datanode2 se da por muerto. `fsck` sigue HEALTHY, con **178 de 178 bloques sub-replicados** y 0 perdidos. No se copia nada: con un solo nodo vivo, no hay dónde poner la segunda copia |
| 18:37 | `docker compose start datanode2`. En menos de 1 min hay 2 vivos y 0 sub-replicados, **sin copiar nada**: al volver, el nodo presenta al NameNode la lista de sus bloques (*block report*) |

**Hallazgo: la detección tardó 655 s, no los ≈ 14-20 s previstos.**

- La línea `HDFS_CONF_dfs_namenode_heartbeat_recheck-interval=2000`, que viene igual del compose del profesor, **no se aplica**: las imágenes bde2020 exigen escribir el guion como `___`.
- `hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval` devuelve `300000`, el valor por defecto.
- Con 300 s, un nodo se da por muerto tras 2 × 300 s + 10 × 1 s = 610 s, y esa revisión se hace cada 300 s: entre ≈ 10 y ≈ 15 min. Se midieron 655 s.
- Se decidió no recrear el NameNode la víspera de la entrega y documentarlo (el comentario del `docker-compose.yml` lo explica). Para corregirlo bastaría con escribir `recheck___interval` y ejecutar `docker compose up -d namenode`.

**Escenario 2: se mata el executor de Spark en mitad del trabajo (19 oct).** Un comando espera a que aparezca el executor, deja pasar 8 s y lo mata con `pkill -f CoarseGrainedExecutorBackend`.

| Hora (UTC) | Qué pasó |
|---|---|
| 20:33:42 | Arranca el executor 0 |
| 20:33:50 | Se mata. El driver anota `Lost executor 0 … Command exited with code 143` (SIGTERM) y pierde las 4 tareas que corrían (8.0-11.0 de la etapa 3.0) |
| 20:33:50 → | Spark lanza el executor 1, que repite **las 12 tareas que el 0 había hecho en la etapa 3.0**: las 4 que estaban a medias y **las 8 que ya habían terminado** (0.1 … 11.1 en su log) |
| Final | **TODO CUADRA**, mismas cifras, en 30,2 s (26 s según la web 8080). En la web, el executor 0 figura como EXITED y el 1 como KILLED: el 0 salió solo con el código 143, y al 1 lo cerró el master al terminar |

**Por qué se repiten también las 8 tareas ya terminadas.** Su resultado intermedio (lo que la etapa 3.0 deja preparado para el *shuffle*) estaba guardado en el executor muerto y se perdió con él. Spark no lo copia a ningún sitio: guarda la «receta» (el linaje). Por eso volvió a leer esas partes del Parquet en HDFS y rehízo el cálculo.


**Evidencia previa de Spark recuperándose solo:**

- el 18 oct, 4 executors perdidos en el ETL, con el resultado verificado (`evidence/fase3/3.1d_*`);
- el 19 oct, 2 executors perdidos en el ML con 128 MB por trozo (`evidence/fase6/6.3_01_*`);
- el 19 oct, 2 executors perdidos en la pasada 1 del ETL, que salió bien al tercer intento (`evidence/ejecuciones/20261019_163933/`). En ese mismo registro, Spark aborta cuando la misma etapa falla 4 veces.

---

## 12. Evidencias

| Carpeta | Qué contiene |
|---|---|
| `evidence/fase1/` | Entorno: apagado limpio, YARN, Jupyter, prueba de humo, WordCount, Streaming con UTF-8, arranque desde cero con un solo comando |
| `evidence/fase2/` | Ingesta: espacio en disco, subida a HDFS, `fsck` del CSV (61 bloques × 2 copias), estructura de las zonas (`hdfs dfs -ls -R`), catálogo de Steam |
| `evidence/fase3/` | ETL (los 3 intentos, con sus fallos y arreglos), verificación, CSV frente a Parquet, catálogo limpio |
| `evidence/fase4/` | MapReduce y RDD con resultados idénticos |
| `evidence/fase5/` | Q1-Q3, optimización de Q1: planes de ejecución y capturas de la Spark UI |
| `evidence/fase6/` | ML: muestra del 10 %, modelo sin precio, ejecución completa (con y sin executors perdidos) |
| `evidence/fase7/` | Medidas de memoria y disco para decidir el Desafío 2, y el propio Desafío 2 |
| `evidence/fase8/` | Ejecución final: el disco C: durante los dos intentos, el diagnóstico tras el fallo, la compactación del disco de Docker (114,2 → 35,5 GB), el arranque sano, el executor de 1536 MiB en la web de Spark y el `git status` tras las reejecuciones (idempotencia: `8.1_11` y `8.1_12`) |
| `evidence/ejecuciones/` | Registro (`pipeline.log`) y resumen (`resumen.json`) de cada ejecución del pipeline |
| `evidence/resultados/` | Los resultados finales, copiados de HDFS |
| `evidence/csv_primeras_lineas.txt` | Las 20 primeras líneas del CSV: reseñas multilínea y comillas dobladas |

---

## 13. Webs de control

| Dirección | Qué es | Cuándo está |
|---|---|---|
| <http://localhost:9870> | NameNode: estado de HDFS, DataNodes, explorador de archivos | Siempre |
| <http://localhost:8080> | Spark master: workers y aplicaciones | Siempre |
| <http://localhost:8081> | Spark worker | Siempre |
| <http://localhost:8888/lab?token=steam> | JupyterLab | Siempre |
| <http://localhost:4040> | Spark UI del programa que esté corriendo (Jobs, Stages, SQL, Executors) | Mientras corre un programa de Spark |
| <http://localhost:8088> | YARN: aplicaciones de MapReduce | Solo con el perfil `yarn` encendido |

Algunas webs (y todos los registros) muestran la hora en **UTC**; otras, en hora local. En octubre, Madrid va 2 horas por delante.

---

## 14. Apagar, arrancar y no perder datos

Los datos de HDFS viven en 3 volúmenes de Docker: `steam-bigdata_hadoop_namenode`, `…_datanode1` y `…_datanode2`.

| Comando | Qué hace | ¿Se pierde HDFS? |
|---|---|---|
| `docker compose stop` | Apaga los contenedores, que siguen existiendo | No |
| `docker compose up -d` | Crea lo que falte y lo arranca | No |
| `docker compose --profile yarn down` | Borra contenedores y red, también los de YARN | **No**: los volúmenes se conservan |
| `docker compose down -v` | Borra también los volúmenes | **SÍ, todo HDFS** |

**Nunca** uses `docker compose down -v`, `docker volume prune` ni `docker system prune --volumes`. En Docker Desktop, tampoco «Clean / Purge data» ni «Reset to factory defaults».

**Cierre seguro:**

1. `docker compose stop`;
2. salir de Docker Desktop;
3. apagar el PC con normalidad.

No apagues el PC a la fuerza con los contenedores en marcha.

**Al volver:**

1. abrir Docker Desktop;
2. `docker compose up -d`;
3. hacer las comprobaciones del paso 4 de la sección 5.

---

## 15. Problemas conocidos y soluciones

| Síntoma | Causa | Solución |
|---|---|---|
| `No module named 'pyspark'` | Se lanzó con `python script.py` | Usar `spark-submit` (sección 8) o `run_pipeline.py` |
| Un programa de Spark se queda esperando (`WAITING` en la web 8080) | Otra aplicación (por ejemplo, un notebook) ocupa los 4 núcleos | Al terminar un notebook: `spark.stop()` y *Kernel → Shut Down All Kernels* |
| `[ERROR] No se pudo leer la configuracion` al lanzar el pipeline | El clúster está apagado | `docker compose up -d` y esperar a que esté sano |
| `[ERROR] YARN no esta listo tras 240 s` | YARN tarda en arrancar (o la primera vez construye su imagen) | Repetir con `python run_pipeline.py --desde 11`; subir `pipeline.espera_yarn_s` si hace falta |
| `Truncated the string representation of a plan…` | Aviso de Spark: recorta un texto de depuración | Inofensivo |
| `netlib-blas … libblas.so.3: cannot open shared object file` | No hay librería nativa de álgebra; Spark usa su versión en Java | Inofensivo: mismo resultado, algo más lento |
| `WARNING: Using incubator modules: jdk.incubator.vector` / `NativeCodeLoader` | Avisos de Java 21 y de Hadoop | Inofensivos |
| `AHSProxy: Connecting to Application History server at historyserver:10200` | La configuración heredada nombra un historyserver que no montamos | Inofensivo |
| `Killed map tasks=1` en el MapReduce | Probablemente ejecución especulativa (una copia de una tarea lenta, cancelada) | Inofensivo si el job acaba `completed successfully` |
| Tildes rotas en PowerShell | PowerShell muestra mal los caracteres que llegan de un contenedor Linux | Los mensajes de los scripts van sin tildes a propósito |
| `CRLF will be replaced by LF` al hacer `git add` | `run_pipeline.py` (Python de Windows) escribe sus registros con CRLF | Inofensivo: `.gitattributes` los guarda con LF |
| Tras suspender el PC, el master de Spark da por muerto al worker | Faltan latidos durante la suspensión | Se recupera solo en segundos |
| Un DataNode parado sigue apareciendo como vivo (`Live`) durante ≈ 10-15 min | El NameNode usa el plazo por defecto (300 s de revisión): la línea `recheck-interval` del compose no se aplica (sección 11.7) | Esperar ≈ 15 min antes de mirar `dfsadmin`, o escribir `recheck___interval` y recrear el NameNode |
| El disco C: se queda sin espacio | El disco virtual de Docker crece y no encoge | Liberar espacio antes de ejecutar el ETL; vigilar C: durante la ejecución |
| `OutOfMemoryError` / executors perdidos (`exited with code 52`) | El executor ejecuta 4 tareas a la vez, y leer o escribir Parquet con el texto de las reseñas usa memoria que Spark no controla | Con 1 GB fallaba (19 oct: 6 executors perdidos y el ETL abortado). Ahora el executor tiene **1536m** (`spark.memoria_executor`); además, Parquet con grupos de 16 MB y ML con trozos de 32 MB. Spark repite solo las tareas perdidas, y aborta si la misma etapa falla 4 veces seguidas |
| C: se llena durante el ETL | El disco virtual de Docker crece con cada escritura y no encoge (≈ 25-38 GB por cada ETL) | Tener ≥ 40 GB libres antes del ETL, vigilar C: durante la ejecución y compactar el disco después (sección 15.1) |
| Hay que parar un programa de Spark a mitad | `Ctrl+C` corta `docker exec` en el PC, pero el programa puede seguir dentro del contenedor | `docker exec jupyter pkill -f <script>.py`: el pipeline lo registra como fallo y se para limpio |

### 15.1 Devolver a Windows el espacio del disco de Docker (compactar)

Hazlo cuando `docker_data.vhdx` mida mucho más que lo que se usa dentro. Para comparar:

- su tamaño, con `Get-ChildItem "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx"`;
- lo que se usa dentro, con `docker exec namenode df -h /hadoop/dfs/name`.

**No borra ningún dato.** Así lo hicimos el 19 oct: el archivo pasó de 114,2 a 35,5 GB, y `fsck` siguió HEALTHY.

```powershell
wsl -d docker-desktop -u root -- fstrim -av    # 1. marcar los bloques vacíos (con Docker encendido)
docker compose stop                            # 2. cierre seguro de los contenedores
#   3. salir de Docker Desktop (icono de la ballena → Quit Docker Desktop)
wsl --shutdown                                 # 4. apagar WSL para liberar el archivo
```

5. Abre **PowerShell como administrador** y escribe:

   ```
   diskpart
   select vdisk file="C:\Users\<usuario>\AppData\Local\Docker\wsl\disk\docker_data.vhdx"
   attach vdisk readonly
   compact vdisk
   detach vdisk
   exit
   ```

   Si Windows ofrece «formatear» el disco, pulsa **Cancelar**.
6. Abre Docker Desktop, ejecuta `docker compose up -d` y comprueba la salud (sección 5, paso 4).

---

## 16. Decisiones de diseño principales

El documento de la entrega justifica cada una con sus medidas.

| Decisión | Por qué |
|---|---|
| Un solo `docker-compose.yml` con HDFS, Spark, Jupyter y YARN | El enunciado pide un solo comando y Spark con master y worker separados |
| **YARN en un perfil** que solo se enciende para el MapReduce | La memoria: con YARN siempre encendido se llegaría a ≈ 7,4 de 7,57 GiB |
| **2 DataNodes, replicación 2** | Bloques repartidos entre nodos (B) y tolerancia a fallos (F). Un 3.º compartiría el mismo PC y el mismo disco: no añade tolerancia real |
| Misma imagen de Spark (fijada por digest) en driver y workers | Evita `PYTHON_VERSION_MISMATCH` y que una actualización cambie la versión entre PCs |
| Topes de memoria por contenedor y de Java (`HADOOP_HEAPSIZE_MAX`); `hadoop.env` adaptado | Java 8 no lee el tope del contenedor; el `hadoop.env` original pedía 16 GB |
| Arranque de Hadoop con `exec` y latido de 1 s en los DataNodes | Apagado ordenado (código 143, no 137); todos laten al mismo ritmo. La detección rápida de nodos caídos (≈ 14-20 s) no funciona: su ajuste no se aplica y son ≈ 10-15 min (sección 11.7) |
| CSV leído con `multiLine` y `escape='"'` y esquema explícito de 23 columnas | Hay reseñas con saltos de línea y comillas dentro; adivinar tipos obligaría a leer 8 GB más |
| ETL en dos pasadas (CSV → Parquet de paso → curada) | El CSV multilínea solo se puede leer con 1 tarea: se paga una vez y lo demás va en paralelo |
| Fechas en UTC | El año (la partición) no depende del PC |
| Partición por `year` elegida midiendo | Mediana por mes de 24,7 MB (archivos pequeños) frente a 192,5 MB por año (`evidence/resultados/calidad_etl_reviews.json`) |
| Duplicados: se queda la versión más reciente, con la lista de ids repetidos en broadcast | La versión anterior ordenaba las 21,7 M de filas con su texto y mató 4 executors |
| Zona `/quarantine` para las filas inválidas | Se pueden revisar, no solo contar (aprobada por el profesor) |
| Entrada del MapReduce exportada desde la zona curada | Hadoop Streaming lee línea a línea; MapReduce y RDD leen lo mismo, así que la comparación es justa |
| Catálogo de la API de Steam; en el ML, géneros sí y precio no | El precio de hoy (2026) no es el de la fecha de la reseña |
| ML solo con variables previas a la reseña, medido con AUC | Evitar la fuga de datos; clases desequilibradas (87,4 % positivas) |
| Orquestador en Python en el PC (`run_pipeline.py`) | El MapReduce y Spark en el mismo pipeline, con la comparación automática que pidió el profesor |
| Executor de 1536 MB, no de 1 GB | Con 1 GB y 4 tareas a la vez, el ETL perdía executors al leer y escribir Parquet. El worker ya ofrecía 2 GB y solo se usaba 1 |

---

## 17. Limitaciones

- **Clúster simulado en un solo PC.** Todos los contenedores comparten CPU, memoria y disco.
  - No hay localidad de datos: las tareas de MapReduce leen siempre «del vecino» (*rack-local*).
  - La replicación no protege de un fallo del disco físico.
  - La capacidad que muestra HDFS (≈ 1,97 TB) es la del disco virtual contada dos veces, no la real.
- **El CSV multilínea no se puede repartir.** La primera pasada del ETL la hace una sola tarea (≈ 10 MB/s).
- **El catálogo es de hoy.** Nombres, géneros y precios son los actuales de la tienda (por ejemplo, «Grand Theft Auto V Legacy»), no los de cada reseña. Por eso el ML no usa el precio.
- **Memoria y disco justos.** Con ≈ 8 GB para Docker, un executor de 1,5 GB ejecuta 4 tareas a la vez. Con 1 GB, los reintentos por falta de memoria triplicaban el tiempo del ETL. Además, el disco virtual de Docker crece ≈ 25-38 GB con cada ETL y hay que compactarlo a mano.
- **Detección lenta de DataNodes caídos.** ≈ 10-15 min (el valor por defecto de Hadoop), no los ≈ 14-20 s previstos (sección 11.7). Mientras tanto, los programas leen de la otra copia, pero más despacio.
- **Pocas repeticiones.** CSV frente a Parquet se mide 3 veces (mediana) y cada versión de Q1, 2 veces (la mejor). El resto (MapReduce, RDD, ML y los pasos del pipeline) son de una ejecución; entre dos ejecuciones variaron ≈ 10 % (MapReduce: 56,4 s y 61,5 s, esta última con un vídeo abierto en el PC), aunque las proporciones entre versiones apenas cambian. La carga del PC influye: con el vídeo, el CSV del paso 7 tardó un 28 % más.
- **MapReduce sin combiner.** Añadir `-combiner` acercaría su shuffle al de Spark; no se ha probado.
- **Python 3.5 en el NodeManager.** La imagen de Hadoop usa Debian 9, así que el mapper y el reducer no usan f-strings.
- **Puertos.** Jupyter y la Spark UI solo escuchan en `127.0.0.1`, pero el NameNode (9870, 9000) y las webs de Spark (8080, 8081) escuchan en todas las interfaces: se ven desde la red local si el firewall lo permite. No uses el clúster en redes públicas.
- **El modelo es una línea base.** Con el umbral 0,5 apenas mejora a decir siempre «sí»; el AUC muestra que ordena bien las reseñas. No usa el texto (NLP): sería muy costoso con ≈ 8 GB de RAM.
