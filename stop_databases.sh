#!/bin/bash
#
# Script para parar os contêineres Docker do trabalho de BD.
#

echo "parando containeres"
docker stop dev-postgres dev-mongo dev-cassandra dev-redis > /dev/null 2>&1

echo "removendo containeres"
docker rm dev-postgres dev-mongo dev-cassandra dev-redis > /dev/null 2>&1

echo "status final"
docker ps --filter "name=dev-*"
