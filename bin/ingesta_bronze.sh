#!/usr/bin/env bash
# Ingesta a la zona bronze de HDFS: copia los archivos SIN cambios y registra su huella SHA-256.
# Cada carpeta de data/originales y data/derivados se refleja con el mismo nombre en /datalake/bronze.
# Uso: bash bin/ingesta_bronze.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BRONZE=/datalake/bronze
MANIFIESTO=evidencias/manifiesto_bronze.csv
mkdir -p evidencias

echo "== Manifiesto de integridad"
echo "archivo,bytes,sha256,fecha_ingesta" > "$MANIFIESTO"
find data/originales data/derivados -type f ! -name ".gitkeep" -print0 | sort -z |
  while IFS= read -r -d '' f; do
    echo "\"${f#data/}\",$(stat -c%s "$f"),$(sha256sum "$f" | cut -d' ' -f1),$(date -I)" >> "$MANIFIESTO"
  done
echo "   $(($(wc -l < "$MANIFIESTO") - 1)) archivos registrados en $MANIFIESTO"

echo "== Copia a HDFS"
hdfs dfs -mkdir -p "$BRONZE/_manifiesto"
hdfs dfs -chmod -R u+w "$BRONZE"            # habilita la reingesta (bronze queda en solo lectura al final)
for dir in data/originales/*/ data/derivados/*/; do
  [ -d "$dir" ] || continue
  nombre=$(basename "$dir")
  if [ -z "$(find "$dir" -maxdepth 1 -type f ! -name '.gitkeep' -print -quit)" ]; then
    echo "   (vacía, se omite) $nombre"; continue
  fi
  mapfile -d '' archivos < <(find "$dir" -maxdepth 1 -type f ! -name ".gitkeep" -print0 | sort -z)
  hdfs dfs -mkdir -p "$BRONZE/$nombre"
  hdfs dfs -put -f "${archivos[@]}" "$BRONZE/$nombre/"     # una sola llamada por carpeta
  echo "   $nombre -> $BRONZE/$nombre"
done
hdfs dfs -put -f "$MANIFIESTO" "$BRONZE/_manifiesto/"
hdfs dfs -chmod -R a-w "$BRONZE"

echo "== Verificación"
hdfs dfs -ls "$BRONZE"
hdfs dfs -du -s -h "$BRONZE"
