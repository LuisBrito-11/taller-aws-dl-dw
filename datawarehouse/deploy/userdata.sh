#!/bin/bash
# Bootstrap EC2 (Amazon Linux 2023) del DW: Docker + Compose (desde S3), codigo y variables desde S3 privado.
set -ex
exec > >(tee /var/log/dw-bootstrap.log) 2>&1
BUCKET=__BUCKET__
REGION=us-east-2
dnf install -y docker tar gzip
systemctl enable --now docker
mkdir -p /usr/local/lib/docker/cli-plugins
aws s3 cp "s3://${BUCKET}/deploy/docker-compose" /usr/local/lib/docker/cli-plugins/docker-compose --region "$REGION"
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
mkdir -p /opt/dw && cd /opt/dw
aws s3 cp "s3://${BUCKET}/deploy/dw-app.tgz" . --region "$REGION"
aws s3 cp "s3://${BUCKET}/deploy/dw.env" ./.env --region "$REGION"
tar xzf dw-app.tgz
chown -R 50000:0 /opt/dw/src /opt/dw/dags   # el usuario airflow (uid 50000) escribe dbt_packages/
cd deploy
docker compose --env-file ../.env up -d --build
echo "BOOTSTRAP_DONE"
