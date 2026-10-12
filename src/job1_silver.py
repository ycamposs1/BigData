"""Job 1: bronze -> silver.

Lee la fuente A (datos abiertos SENAMHI), la fuente C (NASA POWER) y, cuando esté implementada,
la fuente B (repositorio SENAMHI); las lleva a un esquema horario canónico en hora local de Lima,
aplica las reglas de calidad y escribe en silver:
  silver/estaciones            dimensión de estaciones (coordenadas, distrito, ubigeo)
  silver/calidad_aire_horaria  A (+B) unificadas, todas las estaciones
  silver/meteo_horaria         NASA POWER normalizado
  silver/aire_meteo_horaria    unión aire x meteo por estacion + fecha_hora

Ejecutar:  spark-submit --master yarn src/job1_silver.py
"""
import sys

from pyspark.sql import Window, functions as F

import config as C
from comun import (ReporteCalidad, a_double, clave_estacion, crear_spark, cronometro,
                   escribir_parquet, fuera_de_periodo)

CONT_A = ["pm10", "pm2_5", "no2"]
CONT_B = ["so2", "o3", "co"]               # solo existen en B (2010-2020)
METEO = [p.lower() for p in C.NASA_PARAMS]


def a_hora_lima(ts_utc):
    """UTC -> hora local de Lima (UTC-5 fijo: Lima no usa horario de verano)."""
    return ts_utc - F.expr(f"INTERVAL {C.OFFSET_UTC_H} HOURS")


def deduplicar(df, orden, rep, fuente):
    """R2: una sola fila por estacion + fecha_hora; conserva la primera aparición."""
    w = Window.partitionBy("estacion", "fecha_hora").orderBy(orden)
    df = df.withColumn("_rn", F.row_number().over(w))
    rep.agregar(fuente, "R2", "filas duplicadas por estacion+fecha_hora (descartadas)",
                df.filter("_rn > 1").count())
    return df.filter("_rn = 1").drop("_rn")


def leer_a(spark, rep):
    raw = (spark.read.option("header", True).option("sep", C.A_SEP)
           .option("encoding", C.A_ENCODING)
           .csv(f"{C.BRONZE}/senamhi_datosabiertos/*.csv"))
    raw = raw.toDF(*[c.replace("\ufeff", "").strip().upper() for c in raw.columns])
    faltan = {"ESTACION", "FECHA", "HORA", "PM10", "PM2_5", "NO2"} - set(raw.columns)
    if faltan:
        sys.exit(f"Fuente A: no encuentro las columnas {faltan}. Columnas leídas: {raw.columns}. "
                 "Revisa A_SEP y A_ENCODING en src/config.py")

    fh_utc = F.to_timestamp(F.concat(F.trim("FECHA"), F.lpad(F.trim("HORA"), 6, "0")), "yyyyMMddHHmmss")
    id_origen = F.col("ID").cast("long") if "ID" in raw.columns else F.monotonically_increasing_id()
    df = raw.select(
        id_origen.alias("id_origen"),
        clave_estacion(F.col("ESTACION")).alias("estacion"),
        F.trim("ESTACION").alias("estacion_nombre"),
        a_hora_lima(fh_utc).alias("fecha_hora"),
        *[F.col(c.upper()).alias(f"raw_{c}") for c in CONT_A],
        *[a_double(F.col(c.upper())).alias(c) for c in CONT_A],
        *[F.col(c) for c in ["LATITUD", "LONGITUD", "ALTITUD", "DISTRITO", "UBIGEO"] if c in raw.columns],
    ).persist()

    # Todas las reglas que solo cuentan se calculan en UNA pasada sobre los datos.
    no_numerico = {c: F.col(f"raw_{c}").isNotNull() & (F.trim(F.col(f"raw_{c}")) != "") & F.col(c).isNull()
                   for c in CONT_A}
    r = df.agg(F.count("*").alias("total"),
               F.sum(fuera_de_periodo(F.col("fecha_hora")).cast("int")).alias("r1"),
               *[F.sum(v.cast("int")).alias(f"r0_{c}") for c, v in no_numerico.items()],
               *[F.sum((F.col(c) < 0).cast("int")).alias(f"r3_{c}") for c in CONT_A],
               *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulo_{c}") for c in CONT_A]).first()
    total = r["total"]
    rep.agregar("A", "R1", "fecha-hora inválida o fuera de 2010-01-01..2024-05-31 (filas rechazadas)", r["r1"], total)
    for c in CONT_A:
        rep.agregar("A", "R0", f"{c}: texto no numérico convertido a nulo", r[f"r0_{c}"], total)
        rep.agregar("A", "R3", f"{c}: valor negativo convertido a nulo", r[f"r3_{c}"], total)
        rep.agregar("A", "COMPLETITUD", f"{c}: horas sin dato en la fuente", r[f"nulo_{c}"], total)

    limpio = (df.filter(~fuera_de_periodo(F.col("fecha_hora")))
                .select("id_origen", "estacion", "fecha_hora",
                        *[F.when(F.col(c) < 0, None).otherwise(F.col(c)).alias(c) for c in CONT_A]))
    limpio = deduplicar(limpio, "id_origen", rep, "A").drop("id_origen")

    cols_dim = [c for c in ["LATITUD", "LONGITUD", "ALTITUD", "DISTRITO", "UBIGEO"] if c in df.columns]
    estaciones = df.groupBy("estacion").agg(
        F.first("estacion_nombre", ignorenulls=True).alias("estacion_nombre"),
        *[F.first(a_double(F.col(c)) if c in ("LATITUD", "LONGITUD", "ALTITUD") else F.trim(F.col(c)),
                  ignorenulls=True).alias(c.lower()) for c in cols_dim])
    return limpio.withColumn("fuente", F.lit("A")), estaciones


def leer_b(spark, rep):
    """Fuente B (repositorio SENAMHI, XLSX convertido a CSV en bronze/senamhi_repositorio_csv).

    PENDIENTE: se implementa cuando conozcamos la estructura de las hojas (salida de
    src/00_inspeccionar_excel.py). Debe devolver: estacion, fecha_hora (hora local, B NO se
    desplaza), pm10, pm2_5, no2, so2, o3, co.  Mientras devuelva None, el pipeline corre solo con A.
    """
    return None


def unificar_aire(a, b, rep):
    if b is None:
        print("   Fuente B aún no integrada: calidad_aire_horaria = solo A")
        return a.select("estacion", "fecha_hora", *CONT_A,
                        *[F.lit(None).cast("double").alias(c) for c in CONT_B], "fuente")
    # A tiene prioridad en el solape; B completa lo que A no tiene.
    j = (a.withColumn("_en_a", F.lit(1)).alias("a")
          .join(b.withColumn("_en_b", F.lit(1)).alias("b"), ["estacion", "fecha_hora"], "full_outer"))
    solape = j.filter(F.col("a.pm2_5").isNotNull() & F.col("b.pm2_5").isNotNull())
    n_solape = solape.count()
    n_dif = solape.filter(F.abs(F.col("a.pm2_5") - F.col("b.pm2_5")) > 1).count()
    rep.agregar("A-B", "R9", "horas del solape con |PM2.5 A - PM2.5 B| > 1 µg/m³", n_dif, n_solape)
    return j.select("estacion", "fecha_hora",
                    *[F.coalesce(F.col(f"a.{c}"), F.col(f"b.{c}")).alias(c) for c in CONT_A],
                    *[F.col(f"b.{c}").alias(c) for c in CONT_B],
                    F.when(F.col("_en_a").isNotNull(), "A").otherwise("B").alias("fuente"))


def leer_nasa(spark, rep):
    raw = spark.read.option("header", True).csv(f"{C.BRONZE}/nasa_power_csv/*.csv")
    fh_utc = F.to_timestamp(F.col("fecha_hora_utc"), "yyyyMMddHH")
    df = raw.select(clave_estacion(F.col("estacion")).alias("estacion"),
                    a_hora_lima(fh_utc).alias("fecha_hora"),
                    *[a_double(F.col(p)).alias(p.lower()) for p in C.NASA_PARAMS]).persist()

    # R5 excluye el centinela (-999), que ya se cuenta en R6: así ninguna fila se cuenta dos veces.
    fuera_rango = {"rh2m": (F.col("rh2m") < 0) | (F.col("rh2m") > 100),
                   "ws10m": F.col("ws10m") < 0,
                   "prectotcorr": F.col("prectotcorr") < 0}
    fuera_rango = {v: cond & (F.col(v) != C.NASA_FILL) for v, cond in fuera_rango.items()}
    r = df.agg(F.count("*").alias("total"),
               F.sum(fuera_de_periodo(F.col("fecha_hora")).cast("int")).alias("r1"),
               *[F.sum((F.col(v) == C.NASA_FILL).cast("int")).alias(f"r6_{v}") for v in METEO],
               *[F.sum(cond.cast("int")).alias(f"r5_{v}") for v, cond in fuera_rango.items()]).first()
    total = r["total"]
    rep.agregar("NASA", "R1", "fecha-hora fuera del periodo de estudio (filas rechazadas)", r["r1"], total)
    for v in METEO:
        rep.agregar("NASA", "R6", f"{v}: valor centinela {C.NASA_FILL} convertido a nulo", r[f"r6_{v}"], total)
    for v in fuera_rango:
        rep.agregar("NASA", "R5", f"{v}: fuera de rango físico convertido a nulo", r[f"r5_{v}"], total)

    limpio = df.filter(~fuera_de_periodo(F.col("fecha_hora")))
    for v in METEO:
        limpio = limpio.withColumn(v, F.when(F.col(v) == C.NASA_FILL, None).otherwise(F.col(v)))
    for v, cond in fuera_rango.items():
        limpio = limpio.withColumn(v, F.when(cond, None).otherwise(F.col(v)))
    return deduplicar(limpio, "fecha_hora", rep, "NASA")


def main():
    spark = crear_spark("job1_bronze_a_silver")
    rep = ReporteCalidad("job1")
    with cronometro("job1_silver"):
        print("== Fuente A")
        a, estaciones = leer_a(spark, rep)
        print("== Fuente B")
        b = leer_b(spark, rep)
        aire = unificar_aire(a, b, rep).withColumn("anio", F.year("fecha_hora")).persist()

        rep.agregar("AIRE", "R4", "horas con PM2.5 > PM10 (solo bandera, no se anulan)",
                    aire.filter(F.col("pm2_5") > F.col("pm10")).count(), aire.count())
        aire = aire.withColumn("flag_pm25_mayor_pm10", F.col("pm2_5") > F.col("pm10"))

        print("== Fuente NASA POWER")
        meteo = leer_nasa(spark, rep).withColumn("anio", F.year("fecha_hora")).persist()

        # R7: estaciones de NASA que no existen en las fuentes de aire (error de clave).
        est_meteo = meteo.select("estacion").distinct()
        sin_aire = est_meteo.join(aire.select("estacion").distinct(), "estacion", "left_anti").collect()
        rep.agregar("NASA", "R7", f"estaciones sin correspondencia en aire: {[x.estacion for x in sin_aire]}",
                    len(sin_aire))

        # Unión: solo estaciones con meteorología (semi-join contra 7 nombres -> broadcast).
        aire_meteo = (aire.join(F.broadcast(est_meteo), "estacion", "left_semi")
                          .join(meteo.drop("anio"), ["estacion", "fecha_hora"], "left"))
        r10 = aire_meteo.agg(F.count("*").alias("total"),
                             F.sum(F.col("t2m").isNull().cast("int")).alias("sin_meteo")).first()
        rep.agregar("AIRE x NASA", "R10", "horas de aire sin meteorología tras la unión",
                    r10["sin_meteo"], r10["total"])

        print("== Escritura en silver")
        estaciones.write.mode("overwrite").parquet(f"{C.SILVER}/estaciones")
        escribir_parquet(aire, f"{C.SILVER}/calidad_aire_horaria", ["estacion", "fecha_hora"])
        escribir_parquet(meteo, f"{C.SILVER}/meteo_horaria", ["estacion", "fecha_hora"])
        escribir_parquet(aire_meteo, f"{C.SILVER}/aire_meteo_horaria", ["estacion", "fecha_hora"])
        rep.guardar(spark)
        estaciones.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
