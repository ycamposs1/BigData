"""Funciones compartidas por los jobs. Solo construyen expresiones de Spark (sin UDF de Python),
así que se ejecutan en el driver y no hace falta enviarlas a los ejecutores con --py-files."""
import csv
import time
from contextlib import contextmanager
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

import config as C


def crear_spark(nombre):
    """Sesión de Spark. El master (yarn o local[*]) lo decide spark-submit, no el código."""
    spark = (SparkSession.builder.appName(nombre)
             # Los timestamps se guardan como "hora de pared" de Lima. Fijar la zona de la sesión
             # en UTC evita que la zona horaria de la VM introduzca desfases al convertir.
             .config("spark.sql.session.timeZone", "UTC")
             # Valores no numéricos -> nulo (y se cuentan en el reporte), en vez de abortar el job.
             .config("spark.sql.ansi.enabled", "false")
             # Reescribe solo las particiones (años) que el job produce: corridas idempotentes.
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clave_estacion(col):
    """'Santa Anita ' -> 'SANTA_ANITA'. La misma función se aplica a todas las fuentes,
    porque una clave escrita distinto hace que la unión pierda filas en silencio."""
    c = F.upper(F.trim(col))
    c = F.translate(c, "ÁÉÍÓÚÜÑ", "AEIOUUN")
    c = F.regexp_replace(c, r"[^A-Z0-9]+", "_")
    return F.regexp_replace(c, r"^_+|_+$", "")


def a_double(col):
    """Texto -> double. Acepta coma decimal. Vacíos y textos no numéricos quedan como nulo."""
    return F.regexp_replace(F.trim(col.cast("string")), ",", ".").cast("double")


def fuera_de_periodo(col_fecha_hora):
    return (col_fecha_hora.isNull()
            | (col_fecha_hora < F.lit(C.FECHA_MIN).cast("timestamp"))
            | (col_fecha_hora > F.lit(C.FECHA_MAX).cast("timestamp")))


def escribir_parquet(df, ruta, orden, particion="anio"):
    """Parquet + Snappy, una carpeta por año y filas ordenadas por estación dentro de cada archivo,
    para que los filtros por estación salten bloques usando las estadísticas de Parquet."""
    (df.repartition(particion)
       .sortWithinPartitions(*orden)
       .write.mode("overwrite")
       .partitionBy(particion)
       .parquet(ruta))
    print(f"   escrito: {ruta}")


def _a_int(x):
    return int(x) if x is not None else 0


class ReporteCalidad:
    """Acumula cuántos registros afecta cada regla y lo guarda en gold y en evidencias/."""

    def __init__(self, job):
        self.job = job
        self.filas = []

    def agregar(self, fuente, regla, descripcion, afectados, total=None):
        afectados, total = _a_int(afectados), (_a_int(total) if total is not None else None)
        pct = round(100.0 * afectados / total, 3) if total else None
        self.filas.append((fuente, regla, descripcion, afectados, total, pct))
        print(f"   [{regla}] {fuente}: {descripcion} -> {afectados}" + (f" de {total} ({pct} %)" if total else ""))

    def guardar(self, spark):
        C.EVIDENCIAS.mkdir(parents=True, exist_ok=True)
        local = C.EVIDENCIAS / f"reporte_calidad_{self.job}.csv"
        with open(local, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["fuente", "regla", "descripcion", "afectados", "total", "pct"])
            w.writerows(self.filas)
        esquema = "fuente string, regla string, descripcion string, afectados long, total long, pct double"
        (spark.createDataFrame(self.filas, esquema).coalesce(1)
              .write.mode("overwrite").option("header", True)
              .csv(f"{C.GOLD}/reporte_calidad/{self.job}"))
        print(f"   reporte de calidad: {local}")


def guardar_csv_local(df_spark, nombre):
    """Para tablas pequeñas (reportes): las baja al driver y las deja en evidencias/."""
    C.EVIDENCIAS.mkdir(parents=True, exist_ok=True)
    ruta = C.EVIDENCIAS / nombre
    filas = df_spark.collect()
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(df_spark.columns)
        w.writerows([list(r) for r in filas])
    print(f"   {ruta}")


@contextmanager
def cronometro(etapa):
    """Mide el tiempo de pared de una etapa y lo agrega a evidencias/tiempos.csv (insumo de 3.6)."""
    C.EVIDENCIAS.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    yield
    seg = round(time.perf_counter() - t0, 2)
    ruta = C.EVIDENCIAS / "tiempos.csv"
    nuevo = not ruta.exists()
    with open(ruta, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if nuevo:
            w.writerow(["fecha", "etapa", "segundos"])
        w.writerow([datetime.now().isoformat(timespec="seconds"), etapa, seg])
    print(f"== {etapa}: {seg} s")
