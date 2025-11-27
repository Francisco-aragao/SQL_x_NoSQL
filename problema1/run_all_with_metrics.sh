#!/bin/bash

RESULTS_DIR="./results"
mkdir -p "$RESULTS_DIR"

ds() {
    local seconds=$1
    sleep "${seconds}s"
    docker stats --no-stream > "$RESULTS_DIR/STATS_RUNNING_T${seconds}.txt" 2>&1
}

echo "Iniciando bancos de dados"
bash ../start_databases.sh

echo "Aguardando 60 segundos para os bancos iniciarem (cassandra pode demorar)"
sleep 60s

docker stats --no-stream > "$RESULTS_DIR/STATS_BEFORE.txt" 2>&1

echo "Preparando tabelas"
python3 prepare_tables.py

echo "Populando dados"
python3 populate_tables.py

echo "Executando queries do problema1..."
python3 queries.py & ds .1 & ds .3
wait

docker stats --no-stream > "$RESULTS_DIR/STATS_AFTER.txt" 2>&1

echo "Execução e coleta de métricas finalizadas."
