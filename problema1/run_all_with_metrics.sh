#!/bin/bash

RESULTS_DIR="./results"
mkdir -p "$RESULTS_DIR"

ds() {
    local seconds=$1
    sleep "${seconds}s"
    docker stats --no-stream > "$RESULTS_DIR/STATS_RUNNING_T${seconds}.txt" 2>&1
}

echo "iniciando bancos de dados"
bash ../start_databases.sh

echo "aguardando 60 segundos para os bancos iniciarem (cassandra demora um pouco)"
sleep 60s

docker stats --no-stream > "$RESULTS_DIR/STATS_BEFORE.txt" 2>&1

echo "preparando tabelas"
python3 prepare_tables.py

echo "populando dados"
python3 populate_tables.py

echo "executando queries do problema1..."
python3 queries.py & ds .1 & ds .3
wait

docker stats --no-stream > "$RESULTS_DIR/STATS_AFTER.txt" 2>&1

echo "fim"
