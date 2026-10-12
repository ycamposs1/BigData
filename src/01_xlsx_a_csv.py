"""Convierte cada hoja de los XLSX de la fuente B a CSV, una sola vez, con TODAS las columnas como texto.

Así la conversión no altera valores al adivinar tipos (fechas, ceros a la izquierda, decimales):
los tipos se asignan después en Spark con un esquema explícito. El XLSX original no se modifica.

Uso:  python src/01_xlsx_a_csv.py
Entrada: data/originales/senamhi_repositorio/*.xlsx
Salida:  data/derivados/senamhi_repositorio_csv/<archivo>__<hoja>.csv
"""
import re
import sys
import unicodedata

import pandas as pd

import config as C

origen = C.ORIGINALES / "senamhi_repositorio"
destino = C.DERIVADOS / "senamhi_repositorio_csv"
destino.mkdir(parents=True, exist_ok=True)


def slug(texto):
    t = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_")


xlsx = sorted(origen.glob("*.xlsx"))
if not xlsx:
    sys.exit(f"No hay .xlsx en {origen}")
for archivo in xlsx:
    hojas = pd.read_excel(archivo, sheet_name=None, dtype=str)
    for nombre, df in hojas.items():
        salida = destino / f"{slug(archivo.stem)}__{slug(nombre)}.csv"
        df.to_csv(salida, index=False, encoding="utf-8")
        print(f"{archivo.name} / hoja '{nombre}': {len(df)} filas, {df.shape[1]} columnas -> {salida.name}")
