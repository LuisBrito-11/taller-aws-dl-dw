#!/bin/bash
# Despliegue reproducible del Airflow del Data Lake (ejecutar desde datalake/ con AWS CLI configurado).
# Uso: bash deploy/deploy.sh <SG_ID> <MI_IP_PUBLICA>
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash en Windows: no convertir /dev/xvda a ruta de Windows
mkdir -p .build
REGION=us-east-2
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=de-gr02-user-4-datalake-${ACCOUNT}
SG=$1; MYIP=$2
AMI=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
      --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text --region $REGION)   # SSM parameter store no permitido

# 1) empaquetar codigo y subir a S3 privado
tar czf .build/dl-app.tgz dags src config deploy/Dockerfile deploy/docker-compose.yml
aws s3 cp .build/dl-app.tgz s3://$BUCKET/deploy/dl-app.tgz --region $REGION

# 2) secretos generados al vuelo: nunca en el repo
PASS=$(openssl rand -hex 12); KEY=$(openssl rand -hex 24)
cat > .build/airflow.env <<EOF
DL_BUCKET=$BUCKET
AWS_REGION=$REGION
AIRFLOW_ADMIN_PASSWORD=$PASS
AIRFLOW_SECRET_KEY=$KEY
EOF
aws s3 cp .build/airflow.env s3://$BUCKET/deploy/airflow.env --region $REGION
echo "$PASS" > .build/airflow_admin_password.txt   # solo local (.build esta en .gitignore)

# 3) red: 8080 solo desde mi IP
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 8080 --cidr "$MYIP/32" --region $REGION || true

# 4) lanzar EC2 (t3.medium, IMDSv2 con hop limit 2 para que Docker use el rol de instancia)
sed "s/__BUCKET__/$BUCKET/" deploy/userdata.sh > .build/userdata.sh
aws ec2 run-instances --region $REGION --image-id "$AMI" --instance-type t3.medium \
  --iam-instance-profile Name=de-gr02-user-4-dl-ec2-profile --security-group-ids "$SG" \
  --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3,DeleteOnTermination=true}' \
  --user-data file://.build/userdata.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=de-gr02-user-4-dl-airflow},{Key=owner,Value=de-gr02-user-4}]' \
  --query 'Instances[0].InstanceId' --output text
