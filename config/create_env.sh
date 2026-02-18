#!/bin/bash

echo "-- Criando arquivos .env --"
mkdir -p config

# ==================== .env ====================
cat > config/.env << 'EOF'
CONTAINER_LANDING=seu_container_landing
CONTAINER_BRONZE=seu_container_bronze
CONTAINER_SILVER=seu_container_silver
STORAGE_ACCOUNT_NAME=seu_storage_account
AZURE_ACCESS_KEY=sua_access_key
EOF

# ==================== postgredb.env ====================
cat > config/postgredb.env << 'EOF'
POSTGRES_DB=seu_banco
POSTGRES_USER=seu_usuario
POSTGRES_PASSWORD=sua_senha
PGADMIN_DEFAULT_EMAIL=seu_email@exemplo.com
PGADMIN_DEFAULT_PASSWORD=sua_senha_pgadmin
EOF

# ==================== airflow.env ====================
cat > config/airflow.env << 'EOF'
AIRFLOW_DB_USER=seu_usuario_airflow
AIRFLOW_DB_PASSWORD=sua_senha_airflow
AIRFLOW_DB_HOST=postgredb
AIRFLOW_DB_PORT=5432
AIRFLOW_DB_NAME=seu_banco_airflow
AIRFLOW_SECRET_KEY=sua_chave_secreta
EOF

echo "-- Arquivos .env criados em config/ --"
echo "⚠️  Lembre-se de preencher os valores antes de rodar o projeto!"