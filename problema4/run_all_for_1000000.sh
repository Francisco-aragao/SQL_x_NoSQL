#!/bin/bash

# script para rodar todos os benchmarks para 1000000 registros

# parametros
sensores=1000
entradas=1000 # metricas por sensor
registros=$((sensores * entradas)) # entradas totais na tabela
operacoes=10000 # operacoes de consulta/insercao a serem realizadas

# garantir q diretorio atual seja problema4
if dir | grep -q "run_all_for_1000000.sh"; then
    echo "ok"
else
    echo "por favor, execute este script a partir do diretorio problema4"
    exit 1
fi

# funcao para esperar por x segundos e invocar docker stats
# ou seja, coleta metricas durante o uso
# argumentos : segundos, concorrencia
function ws() {
    local seconds=$1
    local concurrency=$2
    sleep "${seconds}s"
    docker stats --no-stream > "./results/STATS_RUNNING_E${registros}_C${concurrency}_T${seconds}.txt" 2>&1
}

# rodar
echo "rodando tudo para ${registros} registros"

echo "iniciando bancos de dados"
bash ../start_databases.sh

echo "aguardando 60 segundos para os bancos iniciarem (cassandra demora)"
sleep 60s

docker stats --no-stream > "./results/STATS_BEFORE_E${registros}.txt" 2>&1

echo "preparando tabelas"
python prepare_tables.py

echo "inserindo ${registros} registros"
python populate_tables.py --sensors ${sensores} --entries ${entradas}

echo "executando consultas"

echo "concurrency 1"
python queries.py --concurrency 1 --operations ${operacoes} --sensors ${sensores} > ./results/OUT_E${registros}_C1.txt & ws 5 1 & ws 10 1 & ws 15 1

echo "concurrency 4"
python queries.py --concurrency 4 --operations ${operacoes} --sensors ${sensores} > ./results/OUT_E${registros}_C4.txt & ws 5 4 & ws 10 4 & ws 15 4

echo "concurrency 8"
python queries.py --concurrency 8 --operations ${operacoes} --sensors ${sensores} > ./results/OUT_E${registros}_C8.txt & ws 5 8 & ws 10 8 & ws 15 8

docker stats --no-stream > "./results/STATS_AFTER_E${registros}.txt" 2>&1