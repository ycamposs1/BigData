"""Job 2: silver -> gold.

Agrega por estación y día (hora local de Lima), aplica la regla de promedio válido (R8),
marca la superación del ECA y construye las variables del modelo usando solo información
disponible al cierre del día D para predecir el día D+1.
  gold/features_diarias     una fila por estación y día
  gold/reporte_positivos    días que superan el ECA por estación y año (insumo de 1.4 y 4.1)

Ejecutar:  spark-submit --master yarn src/job2_gold.py
"""
from pyspark.sql import Window, functions as F

import config as C
from comun import ReporteCalidad, crear_spark, cronometro, escribir_parquet, guardar_csv_local


def media_valida(horas, media):
    """R8: el promedio diario solo existe si hay al menos MIN_HORAS_VALIDAS horas medidas."""
    return F.when(F.col(horas) >= C.MIN_HORAS_VALIDAS, F.col(media))


def traer_dia(diario, columnas, desfase, sufijo):
    """Pega a la fila del día D el valor del día D+desfase, buscándolo por FECHA (no por posición):
    si falta un día en la serie, el valor queda nulo en lugar de tomar el de otro día.
    desfase=+1 -> mañana (objetivo); desfase=-1 -> ayer (rezago)."""
    return diario.select("estacion",
                         F.date_add(F.col("fecha"), -desfase).alias("fecha"),
                         *[F.col(c).alias(f"{c}_{sufijo}") for c in columnas])


def main():
    spark = crear_spark("job2_silver_a_gold")
    rep = ReporteCalidad("job2")
    with cronometro("job2_gold"):
        h = spark.read.parquet(f"{C.SILVER}/aire_meteo_horaria")

        diario = (h.withColumn("fecha", F.to_date("fecha_hora"))
                   .groupBy("estacion", "fecha")
                   .agg(F.count("pm2_5").alias("horas_pm25"), F.avg("pm2_5").alias("_pm25"),
                        F.max("pm2_5").alias("pm25_max_horaria"),
                        F.count("pm10").alias("horas_pm10"), F.avg("pm10").alias("_pm10"),
                        F.count("no2").alias("horas_no2"), F.avg("no2").alias("_no2"),
                        F.count("t2m").alias("horas_meteo"),
                        F.avg("t2m").alias("t2m_media"), F.max("t2m").alias("t2m_max"),
                        F.min("t2m").alias("t2m_min"), F.avg("rh2m").alias("rh2m_media"),
                        F.avg("ws10m").alias("ws10m_media"), F.sum("prectotcorr").alias("precip_dia_mm"),
                        F.avg("ps").alias("ps_media"))
                   .select("estacion", "fecha", "horas_pm25", "pm25_max_horaria", "horas_meteo",
                           media_valida("horas_pm25", "_pm25").alias("pm25_media_24h"),
                           media_valida("horas_pm10", "_pm10").alias("pm10_media_24h"),
                           media_valida("horas_no2", "_no2").alias("no2_media_24h"),
                           "t2m_media", "t2m_max", "t2m_min", "rh2m_media", "ws10m_media",
                           "precip_dia_mm", "ps_media")
                   .withColumn("supera_eca", F.when(F.col("pm25_media_24h").isNotNull(),
                                                    (F.col("pm25_media_24h") > C.ECA_PM25_24H).cast("int")))
                   .persist())

        r = diario.agg(F.count("*").alias("dias"),
                       F.sum(F.col("pm25_media_24h").isNull().cast("int")).alias("sin_media")).first()
        rep.agregar("DIARIO", "R8", f"días-estación sin promedio válido de PM2.5 (< {C.MIN_HORAS_VALIDAS} h)",
                    r["sin_media"], r["dias"])

        # Variables: todo lo que se conoce al cierre del día D. El objetivo es el día D+1.
        dias = F.datediff(F.col("fecha"), F.lit("1970-01-01"))
        movil7 = Window.partitionBy("estacion").orderBy(dias).rangeBetween(-6, 0)
        features = (diario
            .join(traer_dia(diario, ["supera_eca"], +1, "manana"), ["estacion", "fecha"], "left")
            .join(traer_dia(diario, ["pm25_media_24h", "supera_eca"], -1, "ayer"), ["estacion", "fecha"], "left")
            .withColumn("pm25_media_7d", F.avg("pm25_media_24h").over(movil7))
            .withColumn("amplitud_termica", F.col("t2m_max") - F.col("t2m_min"))
            .withColumn("mes", F.month("fecha"))
            .withColumn("dia_semana", F.dayofweek("fecha"))          # 1 = domingo
            .withColumn("anio", F.year("fecha"))
            .withColumnRenamed("supera_eca_manana", "y_supera_eca_manana"))
        escribir_parquet(features, f"{C.GOLD}/features_diarias", ["estacion", "fecha"])

        # Reporte de positivos: ¿hay suficientes días que superan el ECA para entrenar y probar?
        positivos = (diario.groupBy("estacion", F.year("fecha").alias("anio"))
                     .agg(F.count("*").alias("dias_con_datos"),
                          F.count("pm25_media_24h").alias("dias_validos"),
                          F.sum("supera_eca").alias("dias_supera_eca"))
                     .withColumn("pct_supera", F.round(100 * F.col("dias_supera_eca") / F.col("dias_validos"), 2))
                     .orderBy("estacion", "anio"))
        positivos.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{C.GOLD}/reporte_positivos")
        guardar_csv_local(positivos, "reporte_positivos.csv")

        # Periodo de prueba y línea base de persistencia ("mañana será igual que hoy").
        prueba = (spark.read.parquet(f"{C.GOLD}/features_diarias")
                  .filter((F.col("anio") >= C.ANIO_INICIO_PRUEBA)
                          & F.col("y_supera_eca_manana").isNotNull() & F.col("supera_eca").isNotNull()))
        persistencia = (prueba.groupBy("estacion")
                        .agg(F.count("*").alias("dias_evaluables"),
                             F.sum("y_supera_eca_manana").alias("positivos_prueba"),
                             F.sum("supera_eca").alias("alertas_persistencia"),
                             F.sum(F.col("supera_eca") * F.col("y_supera_eca_manana")).alias("aciertos_persistencia"))
                        .orderBy("estacion"))
        guardar_csv_local(persistencia, "linea_base_persistencia_prueba.csv")
        rep.guardar(spark)
        positivos.show(200, truncate=False)
        persistencia.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
