#!/usr/bin/env bash
# Usage : source scripts/reload_env.sh

export AWS_PROFILE="default"
export KUBERNETES_NAMESPACE="${KUBERNETES_NAMESPACE:-$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace 2>/dev/null || echo 'user-l-schwindenhammer-ece')}"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export AWS_S3_ENDPOINT="s3.seaweedfs.adm.adaltas.cloud"

if [ ! -f "$HOME/.aws/credentials" ]; then
  echo "[ERREUR] ~/.aws/credentials est absent. Collez d'abord le bloc Connexion S3 depuis Onyxia."
  return 1 2>/dev/null || exit 1
fi

# 1. Export des clés temporaires S3 dans le shell courant
eval "$(aws configure export-credentials --profile 'default' --format env)"

# 2. Restauration de l'environnement virtuel Python (.venv) via uv
uv sync

# 3. Reconfiguration du secret persistant DuckDB pour S3
duckdb -c "
INSTALL httpfs;
LOAD httpfs;
CREATE OR REPLACE PERSISTENT SECRET s3_onyxia_connection (
    TYPE s3,
    KEY_ID '$AWS_ACCESS_KEY_ID',
    SECRET '$AWS_SECRET_ACCESS_KEY',
    SESSION_TOKEN '$AWS_SESSION_TOKEN',
    ENDPOINT '$AWS_S3_ENDPOINT',
    URL_STYLE 'path'
);
"

# 4. Vérification / alimentation de la couche Bronze dans le bucket de l'utilisateur actif
if ! aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/only5g-dept-75-91.csv" >/dev/null 2>&1; then
  if [ -f "./only5g-dept-75-91.csv" ]; then
    echo "Upload de only5g-dept-75-91.csv vers s3://$LAB_BUCKET_NAME/bronze/..."
    aws s3 --profile 'default' cp "./only5g-dept-75-91.csv" "s3://$LAB_BUCKET_NAME/bronze/only5g-dept-75-91.csv"
  else
    echo "Copie de bronze/only5g-dept-75-91.csv depuis le bucket de référence..."
    aws s3 --profile 'default' cp "s3://user-l-schwindenhammer-ece/bronze/only5g-dept-75-91.csv" "./only5g-dept-75-91.csv" && \
    aws s3 --profile 'default' cp "./only5g-dept-75-91.csv" "s3://$LAB_BUCKET_NAME/bronze/only5g-dept-75-91.csv"
  fi
elif [ ! -f "./only5g-dept-75-91.csv" ]; then
  aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/only5g-dept-75-91.csv" "./only5g-dept-75-91.csv"
fi

echo "=== Environnement prêt | Namespace: $LAB_BUCKET_NAME ==="
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/"
