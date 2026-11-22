# Entrega 2 · De local a la nube (Databricks Free Edition)

El pipeline de la Entrega 1 (clúster Docker: HDFS + YARN + Spark 4.2.0) adaptado a
**Databricks Free Edition**, con las mismas 3 preguntas de negocio y resultados equivalentes.

## Ficheros

| Fichero | Dónde se ejecuta | Qué hace |
|---|---|---|
| `reducir_steam_reviews.py` | En el PC | **Script de selección de datos.** Descarga el dataset con `kagglehub` y genera `steam_reviews_databricks.csv`: todas las filas y solo las 13 columnas que usa el pipeline. El texto de la reseña se sustituye por su longitud (`review_length`) |
| `descargar_catalogo_local.py` | En el PC | Descarga el catálogo de los 315 juegos de la API de Steam (`appdetails.jsonl`). Misma lógica que `src/ingesta/descargar_catalogo.py`, pero escribe en local: Databricks no tiene internet |
| `notebooks/steam_reviews_databricks.py` | En Databricks | **El notebook** (formato «Databricks source»). Se importa con Workspace → ⋮ → Import |

Los datos **no se suben** a GitHub: el CSV reducido pesa 1,79 GB y los dos ficheros se regeneran con los scripts.

## Cómo reproducirlo

1. En el PC: `python reducir_steam_reviews.py` (unos 11 min) y `python descargar_catalogo_local.py` (unos 10 min).
2. En Databricks: crear el esquema `workspace.steam` y los volumes `raw` y `resultados` (lo hace también la sección 1 del notebook).
3. Subir `steam_reviews_databricks.csv` y `appdetails.jsonl` al volume `raw` (Catalog → volume → Upload) y comprobar el tamaño del CSV: 1.786.432.475 bytes.
4. Importar el notebook y ejecutar *Run all*.

## Datos de la selección (resumen del script)

- 21.747.371 filas (sin muestreo), 13 columnas, 1,786 GB (1.786.432.475 bytes).
- 315 juegos y 28 idiomas; 2020 es el año con más reseñas (7.882.424).
- Criterio: quitar columnas y no filas, para que Q1-Q3 no cambien.

## Resultados frente a la Entrega 1

| | Local (Entrega 1) | Databricks |
|---|---|---|
| Filas limpias | 21.612.437 | 21.612.444 (+7: en local eran corruptas por columnas `author_num_*` que ya no se usan) |
| ETL | 9,7 min | 0,74 min |
| Q1 | ULTRAKILL 99,53 % · Senren＊Banka 99,40 % · A Short Hike 99,14 % | Idéntica |
| Q1 V1 / V2 / V3 | 4,6 s / 2,0 s / 1,1 s | 0,80 s / 0,70 s / 0,67 s |
| ML (AUC ROC) | 0,7451 | 0,7453 |
