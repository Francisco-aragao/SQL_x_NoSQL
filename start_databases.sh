#!/bin/bash
#
# Script para iniciar os contêineres Docker para o trabalho de BD.
#

echo "parando contêineres antigos"
docker stop dev-postgres dev-mongo dev-cassandra dev-redis > /dev/null 2>&1
docker rm dev-postgres dev-mongo dev-cassandra dev-redis > /dev/null 2>&1

echo "init postgres -port: 5432"
docker run -d \
  --name dev-postgres \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=trabalho_bd \
  -p 5432:5432 \
  postgres:16-alpine

echo "init mongo - port: 27017"
docker run -d \
  --name dev-mongo \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=admin \
  -p 27017:27017 \
  mongo:latest

echo "init cassandra - port: 9042   "
# sem passar flag de memoria cassandra não inicia corretamente
docker run -d \
  --name dev-cassandra \
  -e MAX_HEAP_SIZE=8G \
  -e HEAP_NEWSIZE=1G \
  -p 9042:9042 \
  cassandra:latest

echo "init redis - port: 6379"
docker run -d \
  --name dev-redis \
  -p 6379:6379 \
  redis:7-alpine

echo "Status:"
docker ps --filter "name=dev-*"