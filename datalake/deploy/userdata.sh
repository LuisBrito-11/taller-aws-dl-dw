#!/bin/bash
# Bootstrap de la EC2 (Amazon Linux 2023): Docker + Compose, descarga del codigo desde S3 y arranque de Airflow.
# Nota: la EC2 no alcanza github.com, por eso el plugin de Compose se sirve desde el bucket privado (deploy/docker-compose).
set -ex
exec > >(tee /var/log/dl-bootstrap.log) 2>&1
BUCKET=__BUCKET__
REGION=us-east-2

for u in https://registry-1.docker.io/v2/ https://pypi.org/simple/ https://auth.docker.io/ https://github.com; do
  echo "CONNCHECK $u -> $(curl -s -m 10 -o /dev/null -w '%{http_code}' $u || echo FAIL)"
done

dnf install -y docker tar gzip
systemctl enable --now docker
mkdir -p /usr/local/lib/docker/cli-plugins
aws s3 cp "s3://${BUCKET}/deploy/docker-compose" /usr/local/lib/docker/cli-plugins/docker-compose --region "$REGION"
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
docker compose version

mkdir -p /opt/dl && cd /opt/dl
aws s3 cp "s3://${BUCKET}/deploy/dl-app.tgz" . --region "$REGION"
aws s3 cp "s3://${BUCKET}/deploy/airflow.env" ./.env --region "$REGION"
tar xzf dl-app.tgz
cd deploy
docker compose --env-file ../.env up -d --build
echo "BOOTSTRAP_DONE"
