"""Configuración única del pipeline.

Todas las rutas y parámetros viven aquí. Cambiar de HDFS a otro almacenamiento
(por ejemplo s3a://bucket/datalake) es cambiar DATALAKE_BASE, no el código de los jobs:
ese es el contrato que se defiende en la prueba de sustitución (sección 2.3).
"""
import os
from pathlib import Path

# --- Rutas locales (dentro del repositorio) ---------------------------------
REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data"
ORIGINALES = DATA / "originales"      # archivos tal como se obtuvieron (no se suben a GitHub)
DERIVADOS = DATA / "derivados"        # conversiones de formato sin limpiar (XLSX->CSV, JSON->CSV)
MUESTRAS = DATA / "muestras"          # recortes pequeños que sí se suben al repositorio
EVIDENCIAS = REPO / "evidencias"      # reportes de calidad, tiempos y logs de corridas

# --- Data lake en HDFS --------------------------------------------------------
# hdfs://localhost:9000 es el fs.defaultFS del core-site.xml del curso.
DATALAKE_BASE = os.environ.get("DATALAKE_BASE", "hdfs://localhost:9000/datalake")
BRONZE = f"{DATALAKE_BASE}/bronze"
SILVER = f"{DATALAKE_BASE}/silver"
GOLD = f"{DATALAKE_BASE}/gold"

# --- Fuente A: datos abiertos SENAMHI (CSV) -----------------------------------
# Verificar ANTES de la primera corrida con:  head -3 data/originales/senamhi_datosabiertos/*.csv
#   y la codificación con:                     file -i data/originales/senamhi_datosabiertos/*.csv
A_SEP = ","
A_ENCODING = "UTF-8"          # si las tildes salen rotas (Ã), usar "ISO-8859-1"
OFFSET_UTC_H = 5              # A y NASA vienen en UTC; Lima es UTC-5 todo el año (sin horario de verano)

# --- Fuente B: repositorio SENAMHI (XLSX -> CSV con src/01_xlsx_a_csv.py) ----------
# B ya viene en hora local de Lima: NO se desplaza.
# Si una estación se escribe distinto en A y en B (el Job 1 imprime la comparación),
# se agrega aquí la equivalencia usando las claves normalizadas, por ejemplo:
#   ALIAS_ESTACIONES = {"S_J_L": "SAN_JUAN_DE_LURIGANCHO"}
ALIAS_ESTACIONES = {}

# --- Fuente C: NASA POWER (API horaria) ---------------------------------------
NASA_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"
NASA_PARAMS = ["T2M", "RH2M", "WS10M", "PRECTOTCORR", "PS"]
NASA_COMMUNITY = "RE"
NASA_ANIOS = range(2010, 2025)
NASA_FIN_2024 = "20240601"    # un día extra en UTC para cubrir las 19:00-23:00 locales del 31/05/2024
NASA_FILL = -999.0            # valor centinela de la API (regla R6)

# --- Periodo de estudio y reglas del negocio -----------------------------------
FECHA_MIN = "2010-01-01 00:00:00"   # hora local de Lima
FECHA_MAX = "2024-05-31 23:59:59"
ECA_PM25_24H = 50.0                 # µg/m³, D.S. N.º 003-2017-MINAM
MIN_HORAS_VALIDAS = 18              # 75 % del día para aceptar un promedio diario (regla R8)
ANIO_INICIO_PRUEBA = 2022           # partición temporal del modelo: entrenamiento < 2022 <= prueba
