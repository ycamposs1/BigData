"""Job 1: bronze -> silver.

Lee la fuente A (datos abiertos SENAMHI, 2015-2024), la fuente B (repositorio SENAMHI, 2010-2020)
y la fuente C (NASA POWER); las lleva a un esquema horario canónico en hora local de Lima,
aplica las reglas de calidad y escribe en silver:
  silver/estaciones            dimensión de estaciones (código, coordenadas, distrito, ubigeo)
  silver/calidad_aire_horaria  A y B unificadas, todas las estaciones
  silver/meteo_horaria         NASA POWER normalizado
  silver/aire_meteo_horaria    unión aire x meteo por estacion + fecha_hora (estaciones con meteorología)

Ejecutar:  spark-submit --master yarn src/job1_silver.py
"""
import re
import sys
import unicodedata

from pyspark.sql import Window, functions as F

import config as C
from comun import (ReporteCalidad, a_double, clave_estacion, crear_spark, cronometro,
                   escribir_parquet, fuera_de_periodo)

CONT_A = ["pm10", "pm2_5", "no2"]
CONT_B = ["so2", "o3", "co"]                       # solo existen en B
B_COLUMNAS = {"PM_10": "pm10", "PM_2_5": "pm2_5", "SO2": "so2", "NO2": "no2", "O3": "o3", "CO": "co"}
METEO = [p.lower() for p in C.NASA_PARAMS]


def a_hora_lima(ts_utc):
    """UTC -> hora local de Lima (UTC-5 fijo: Lima no usa horario de verano)."""
    return ts_utc - F.expr(f"INTERVAL {C.OFFSET_UTC_H} HOURS")


def normalizar_columna(nombre):
    """'PM 2.5' -> 'PM_2_5', 'AÑO' -> 'ANO'. Spark interpreta el punto de 'PM 2.5' como acceso a un campo."""
    t = unicodedata.normalize("NFKD", nombre.replace("\ufeff", "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9]+", "_", t.strip().upper()).strip("_")


def aplicar_alias(df):
    return df.replace(C.ALIAS_ESTACIONES, subset=["estacion"]) if C.ALIAS_ESTACIONES else df


def deduplicar(df, orden, rep, fuente):
    """R2: una sola fila por estacion + fecha_hora; conserva la primera aparición."""
    w = Window.partitionBy("estacion", "fecha_hora").orderBy(orden)
    df = df.withColumn("_rn", F.row_number().over(w))
    rep.agregar(fuente, "R2", "filas duplicadas por estacion+fecha_hora (descartadas)",
                df.filter("_rn > 1").count())
    return df.filter("_rn = 1").drop("_rn")


def reglas_aire(df, conts, fuente, rep):
    """R1, R0, R3 y completitud en UNA pasada; luego filtra, anula negativos y deduplica.
    df debe traer: orden, estacion, fecha_hora, raw_<c> (texto original) y <c> (double)."""
    no_num = {c: F.col(f"raw_{c}").isNotNull() & (F.trim(F.col(f"raw_{c}")) != "") & F.col(c).isNull()
              for c in conts}
    r = df.agg(F.count("*").alias("total"),
               F.sum(fuera_de_periodo(F.col("fecha_hora")).cast("int")).alias("r1"),
               *[F.sum(v.cast("int")).alias(f"r0_{c}") for c, v in no_num.items()],
               *[F.sum((F.col(c) < 0).cast("int")).alias(f"r3_{c}") for c in conts],
               *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulo_{c}") for c in conts]).first()
    total = r["total"]
    rep.agregar(fuente, "R1", "fecha-hora inválida o fuera de 2010-01-01..2024-05-31 (filas rechazadas)",
                r["r1"], total)
    for c in conts:
        rep.agregar(fuente, "R0", f"{c}: texto no numérico convertido a nulo", r[f"r0_{c}"], total)
        rep.agregar(fuente, "R3", f"{c}: valor negativo convertido a nulo", r[f"r3_{c}"], total)
        rep.agregar(fuente, "COMPLETITUD", f"{c}: horas sin dato en la fuente", r[f"nulo_{c}"], total)
    limpio = (df.filter(~fuera_de_periodo(F.col("fecha_hora")))
                .select("orden", "estacion", "fecha_hora",
                        *[F.when(F.col(c) < 0, None).otherwise(F.col(c)).alias(c) for c in conts]))
    return deduplicar(limpio, "orden", rep, fuente).drop("orden")


def leer_a(spark, rep):
    raw = (spark.read.option("header", True).option("sep", C.A_SEP)
           .option("encoding", C.A_ENCODING)
           .csv(f"{C.BRONZE}/senamhi_datosabiertos/*.csv"))
    raw = raw.toDF(*[normalizar_columna(c) for c in raw.columns])
    faltan = {"ESTACION", "FECHA", "HORA", "PM10", "PM2_5", "NO2"} - set(raw.columns)
    if faltan:
        sys.exit(f"Fuente A: no encuentro las columnas {faltan}. Columnas leídas: {raw.columns}. "
                 "Revisa A_SEP y A_ENCODING en src/config.py")

    fh_utc = F.to_timestamp(F.concat(F.trim("FECHA"), F.lpad(F.trim("HORA"), 6, "0")), "yyyyMMddHHmmss")
    orden = F.col("ID").cast("long") if "ID" in raw.columns else F.monotonically_increasing_id()
    df = aplicar_alias(raw.select(
        orden.alias("orden"),
        clave_estacion(F.col("ESTACION")).alias("estacion"),
        F.trim("ESTACION").alias("estacion_nombre"),
        a_hora_lima(fh_utc).alias("fecha_hora"),
        *[F.col(c.upper()).alias(f"raw_{c}") for c in CONT_A],
        *[a_double(F.col(c.upper())).alias(c) for c in CONT_A],
        *[F.col(c) for c in ["LATITUD", "LONGITUD", "ALTITUD", "DISTRITO", "UBIGEO"] if c in raw.columns],
    )).persist()

    cols_dim = [c for c in ["LATITUD", "LONGITUD", "ALTITUD", "DISTRITO", "UBIGEO"] if c in df.columns]
    texto = lambda c: F.regexp_replace(F.trim(F.col(c)), r"\.0$", "")      # UBIGEO viene como '150113.0'
    estaciones = df.groupBy("estacion").agg(
        F.first("estacion_nombre", ignorenulls=True).alias("estacion_nombre"),
        *[F.first(a_double(F.col(c)) if c in ("LATITUD", "LONGITUD", "ALTITUD") else texto(c),
                  ignorenulls=True).alias(c.lower()) for c in cols_dim])
    limpio = reglas_aire(df, CONT_A, "A", rep)
    return limpio.withColumn("fuente", F.lit("A")), estaciones


def leer_b(spark, rep):
    """Fuente B: una fila por estación y hora, columnas CODIGO ESTACION, ESTACION, ANO, MES, DIA,
    HORA (0-23), PM 10, PM 2.5, SO2, NO2, O3, CO. Ya viene en hora local de Lima: no se desplaza."""
    raw = spark.read.option("header", True).csv(f"{C.BRONZE}/senamhi_repositorio_csv/*.csv")
    raw = raw.toDF(*[normalizar_columna(c) for c in raw.columns])
    faltan = {"ESTACION", "ANO", "MES", "DIA", "HORA", *B_COLUMNAS} - set(raw.columns)
    if faltan:
        sys.exit(f"Fuente B: no encuentro las columnas {faltan}. Columnas leídas: {raw.columns}")

    entero = lambda c: f"CAST(CAST({c} AS DOUBLE) AS INT)"                  # tolera '4' y '4.0'
    df = aplicar_alias(raw.select(
        F.monotonically_increasing_id().alias("orden"),
        clave_estacion(F.col("ESTACION")).alias("estacion"),
        F.trim("ESTACION").alias("estacion_nombre"),
        (F.regexp_replace(F.trim("CODIGO_ESTACION"), r"\.0$", "") if "CODIGO_ESTACION" in raw.columns
         else F.lit(None).cast("string")).alias("codigo_estacion"),
        F.expr(entero("HORA")).alias("hora_origen"),
        F.expr(f"make_timestamp({entero('ANO')}, {entero('MES')}, {entero('DIA')}, {entero('HORA')}, 0, 0)")
         .alias("fecha_hora"),
        *[F.col(o).alias(f"raw_{d}") for o, d in B_COLUMNAS.items()],
        *[a_double(F.col(o)).alias(d) for o, d in B_COLUMNAS.items()],
    )).persist()

    # Convención horaria: si HORA fuera 1..24 (fin del periodo), la hora 24 daría fechas inválidas.
    h = df.agg(F.min("hora_origen").alias("min"), F.max("hora_origen").alias("max")).first()
    print(f"   B: HORA va de {h['min']} a {h['max']} (se espera 0 a 23)")

    estaciones = df.groupBy("estacion").agg(
        F.first("estacion_nombre", ignorenulls=True).alias("estacion_nombre_b"),
        F.first("codigo_estacion", ignorenulls=True).alias("codigo_estacion"))
    limpio = reglas_aire(df.drop("hora_origen", "codigo_estacion", "estacion_nombre"),
                         CONT_A + CONT_B, "B", rep)
    return limpio.withColumn("fuente", F.lit("B")), estaciones


def comparar_estaciones(est_a, est_b, rep):
    ea = {r.estacion for r in est_a.select("estacion").collect()}
    eb = {r.estacion for r in est_b.select("estacion").collect()}
    print(f"   En A y B: {sorted(ea & eb)}")
    print(f"   Solo en A: {sorted(ea - eb)}")
    print(f"   Solo en B: {sorted(eb - ea)}   <- si alguna es la misma estación escrita distinto, "
          "agrégala a ALIAS_ESTACIONES en config.py")
    rep.agregar("A-B", "R7", f"estaciones solo en B, sin coordenadas en A: {sorted(eb - ea)}", len(eb - ea))


def unificar_aire(a, b, rep):
    """A tiene prioridad en el solape; B completa lo que A no tiene (estaciones y años fuera de A)."""
    j = (a.withColumn("_en_a", F.lit(1)).alias("a")
          .join(b.withColumn("_en_b", F.lit(1)).alias("b"), ["estacion", "fecha_hora"], "full_outer"))
    ambos = F.col("_en_a").isNotNull() & F.col("_en_b").isNotNull()
    par25 = ambos & F.col("a.pm2_5").isNotNull() & F.col("b.pm2_5").isNotNull()
    s = j.agg(F.sum(ambos.cast("int")).alias("horas_solape"),
              F.sum(par25.cast("int")).alias("pares_pm25"),
              F.sum((par25 & (F.abs(F.col("a.pm2_5") - F.col("b.pm2_5")) > 1)).cast("int")).alias("dif_pm25"),
              F.corr(F.col("a.pm2_5"), F.col("b.pm2_5")).alias("corr_pm25"),
              F.corr(F.col("a.pm10"), F.col("b.pm10")).alias("corr_pm10")).first()
    rep.agregar("A-B", "R9", "horas del solape con |PM2.5 A - PM2.5 B| > 1 µg/m³", s["dif_pm25"], s["pares_pm25"])
    rep.agregar("A-B", "R9", f"concordancia en el solape: correlación PM2.5 = {s['corr_pm25']}, "
                f"PM10 = {s['corr_pm10']} (A desplazada a hora local)", s["horas_solape"])
    return j.select("estacion", "fecha_hora",
                    *[F.coalesce(F.col(f"a.{c}"), F.col(f"b.{c}")).alias(c) for c in CONT_A],
                    *[F.col(f"b.{c}").alias(c) for c in CONT_B],
                    F.when(F.col("_en_a").isNotNull(), "A").otherwise("B").alias("fuente"))


def leer_nasa(spark, rep):
    raw = spark.read.option("header", True).csv(f"{C.BRONZE}/nasa_power_csv/*.csv")
    fh_utc = F.to_timestamp(F.col("fecha_hora_utc"), "yyyyMMddHH")
    df = aplicar_alias(raw.select(clave_estacion(F.col("estacion")).alias("estacion"),
                                  a_hora_lima(fh_utc).alias("fecha_hora"),
                                  *[a_double(F.col(p)).alias(p.lower()) for p in C.NASA_PARAMS])).persist()

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
        print("== Fuente A (datos abiertos)")
        a, est_a = leer_a(spark, rep)
        print("== Fuente B (repositorio)")
        b, est_b = leer_b(spark, rep)
        comparar_estaciones(est_a, est_b, rep)
        estaciones = (est_a.join(est_b, "estacion", "full_outer")
                      .withColumn("estacion_nombre", F.coalesce("estacion_nombre", "estacion_nombre_b"))
                      .drop("estacion_nombre_b"))

        aire = unificar_aire(a, b, rep).withColumn("anio", F.year("fecha_hora")).persist()
        rep.agregar("AIRE", "R4", "horas con PM2.5 > PM10 (solo bandera, no se anulan)",
                    aire.filter(F.col("pm2_5") > F.col("pm10")).count(), aire.count())
        aire = aire.withColumn("flag_pm25_mayor_pm10", F.col("pm2_5") > F.col("pm10"))

        print("== Fuente C (NASA POWER)")
        meteo = leer_nasa(spark, rep).withColumn("anio", F.year("fecha_hora")).persist()
        est_meteo = meteo.select("estacion").distinct()
        sin_aire = est_meteo.join(aire.select("estacion").distinct(), "estacion", "left_anti").collect()
        rep.agregar("NASA", "R7", f"estaciones de NASA sin correspondencia en aire: {[x.estacion for x in sin_aire]}",
                    len(sin_aire))

        # Unión: solo estaciones con meteorología (semi-join contra los nombres -> broadcast).
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
        estaciones.orderBy("estacion").show(truncate=False)
        aire.groupBy("fuente").count().show()
    spark.stop()


if __name__ == "__main__":
    main()
