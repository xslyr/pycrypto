#!/bin/bash

sudo rm -rf data/postgres
sudo rm -rf data/pgvector/data
rm -r alembic/versions/*

docker compose up -d --force-recreate
sleep 10

bash _create_migration.sh "database_initialization"
echo 
echo "inclua manualmente o comando:"
echo "  op.execute("CREATE EXTENSION IF NOT EXISTS vector")"
