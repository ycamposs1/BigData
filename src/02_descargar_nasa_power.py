"""Descarga la meteorología horaria de NASA POWER para cada estación de la fuente A.

- Las coordenadas y la altitud se toman del propio CSV de SENAMHI (no se escriben a mano).
- Se pide la hora en UTC (time-standard=UTC); el job 1 la pasa a hora local de Lima.
- Se envía la altitud de la estación (site-elevation) para que la API corrija la presión (PS).
- La respuesta JSON se guarda SIN cambios (es el original de la ficha de procedencia) y además
  se aplana a CSV para Spark. Si un archivo ya existe no se vuelve a descargar: se puede reanudar.

Uso:  python src/02_descargar_nasa_power.py
Salida: data/originales/nasa_power_json/   data/derivados/nasa_power_csv/   evidencias/resumen_descarga_nasa.csv
"""
import csv
import json
import re
import sys
import time
import unicodedata

import pandas as pd
import requests

import config as C

DIR_JSON = C.ORIGINALES / "nasa_power_json"
DIR_CSV = C.DERIVADOS / "nasa_power_csv"


def slug(texto):
    t = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_").upper()


def estaciones_de_a():
    archivos = sorted((C.ORIGINALES / "senamhi_datosabiertos").glob("*.csv"))
    if not archivos:
        sys.exit("Primero coloca el CSV de datos abiertos en data/originales/senamhi_datosabiertos/")
    norm = lambda c: c.replace("\ufeff", "").strip().upper()
    df = pd.read_csv(archivos[0], sep=C.A_SEP, encoding=C.A_ENCODING, dtype=str,
                     usecols=lambda c: norm(c) in {"ESTACION", "LATITUD", "LONGITUD", "ALTITUD"})
    df.columns = [norm(c) for c in df.columns]
    df["ESTACION"] = df["ESTACION"].str.strip()
    for c in ["LATITUD", "LONGITUD", "ALTITUD"]:
        df[c] = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="coerce")
    est = df.dropna(subset=["LATITUD", "LONGITUD"]).groupby("ESTACION", as_index=False).first()
    if est.empty:
        sys.exit("No hay coordenadas numéricas en A: revisa si vienen en grados-minutos-segundos.")
    print(est.to_string(index=False))
    return est


def descargar(params, intentos=3):
    for i in range(intentos):
        try:
            r = requests.get(C.NASA_URL, params=params, timeout=180)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            print(f"   intento {i + 1} falló: {e}")
            time.sleep(10 * (i + 1))
    return None


def main():
    DIR_JSON.mkdir(parents=True, exist_ok=True)
    DIR_CSV.mkdir(parents=True, exist_ok=True)
    resumen = []
    for _, e in estaciones_de_a().iterrows():
        for anio in C.NASA_ANIOS:
            base = f"{slug(e.ESTACION)}_{anio}"
            f_json, f_csv = DIR_JSON / f"{base}.json", DIR_CSV / f"{base}.csv"
            if not f_json.exists():
                params = {"parameters": ",".join(C.NASA_PARAMS), "community": C.NASA_COMMUNITY,
                          "latitude": e.LATITUD, "longitude": e.LONGITUD,
                          "start": f"{anio}0101", "end": C.NASA_FIN_2024 if anio == 2024 else f"{anio}1231",
                          "format": "JSON", "time-standard": "UTC"}
                if pd.notna(e.ALTITUD):
                    params["site-elevation"] = e.ALTITUD
                texto = descargar(params)
                if texto is None:
                    resumen.append([e.ESTACION, anio, "ERROR", 0])
                    continue
                f_json.write_text(texto, encoding="utf-8")
                time.sleep(1)  # no saturar la API
            serie = json.loads(f_json.read_text(encoding="utf-8"))["properties"]["parameter"]
            horas = sorted(serie[C.NASA_PARAMS[0]].keys())          # claves 'AAAAMMDDHH' en UTC
            with open(f_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["estacion", "fecha_hora_utc", *C.NASA_PARAMS])
                for hh in horas:
                    w.writerow([e.ESTACION, hh, *[serie[p].get(hh) for p in C.NASA_PARAMS]])
            resumen.append([e.ESTACION, anio, "OK", len(horas)])
            print(f"   {base}: {len(horas)} horas")
    C.EVIDENCIAS.mkdir(parents=True, exist_ok=True)
    with open(C.EVIDENCIAS / "resumen_descarga_nasa.csv", "w", newline="", encoding="utf-8") as f:
        csv.writer(f, lineterminator="\n").writerows([["estacion", "anio", "estado", "horas"], *resumen])
    errores = sum(1 for r in resumen if r[2] != "OK")
    print(f"\nDescargas correctas: {len(resumen) - errores} de {len(resumen)}")


if __name__ == "__main__":
    main()
