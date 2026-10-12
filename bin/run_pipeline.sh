#!/usr/bin/env bash
# Corrida completa del pipeline: ingesta -> job 1 (silver) -> job 2 (gold).
# Deja un log con fecha y hora en evidencias/logs/ (evidencia de corrida completa para el informe).
# Uso: bash bin/run_pipeline.sh              (en YARN)
#      MASTER="local[*]" bash bin/run_pipeline.sh   (sin YARN, solo para depurar)
set -euo pipefail
cd "$(dirname "$0")/.."

MASTER="${MASTER:-yarn}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"   # spark-submit lo necesita para YARN
mkdir -p evidencias/logs
LOG="evidencias/logs/corrida_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

echo "== Inicio: $(date -Is) | master: $MASTER"
spark-submit --version 2>&1 | grep -m1 "version" || true
hdfs dfs -mkdir -p /spark-logs

SUBMIT=(spark-submit --master "$MASTER"
        --conf spark.eventLog.enabled=true
        --conf spark.eventLog.dir=hdfs://localhost:9000/spark-logs)

bash bin/ingesta_bronze.sh
"${SUBMIT[@]}" src/job1_silver.py
"${SUBMIT[@]}" src/job2_gold.py

echo "== Tamaños finales"
hdfs dfs -du -h /datalake/silver /datalake/gold
echo "== Fin: $(date -Is) | log: $LOG"
