#!/bin/bash

set -a
source .env
set +a

CONTAINER_NAME=$(docker ps --format "table {{.Names}}" | grep -E "(webserver|airflow)" | head -n 1)

if [ -z "$CONTAINER_NAME" ]; then
    echo "Erro: Container do Airflow nao encontrado"
    echo "Containers disponiveis:"
    docker ps --format "table {{.Names}}"
    exit 1
fi

echo "Usando container: $CONTAINER_NAME"

echo "Copiando requirements.txt para o container..."
docker cp requirements.txt "$CONTAINER_NAME":/tmp/requirements.txt

docker exec -it "$CONTAINER_NAME" bash -c "
    git config --global credential.helper store
    echo 'https://${git_username}:${git_token}@github.com' > ~/.git-credentials
    
    pip install -r /tmp/requirements.txt
    
    pip install git+https://${git_username}:${git_token}@github.com/wx-middle/libs-middle.git
    
    rm /tmp/requirements.txt
    
    echo 'Bibliotecas atualizadas com sucesso'
"
