#!/bin/bash
# Elimina TODOS los recursos del Data Lake (irreversible). Ejecutar desde datalake/.
set -uo pipefail
export MSYS_NO_PATHCONV=1
R=us-east-2; P=de-gr02-user-4
ACCOUNT=$(aws sts get-caller-identity --query Account --output text); BUCKET=$P-datalake-$ACCOUNT

echo "1) EC2 de Airflow"
IDS=$(aws ec2 describe-instances --region $R --filters "Name=tag:Name,Values=$P-dl-airflow" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query 'Reservations[].Instances[].InstanceId' --output text)
[ -n "$IDS" ] && aws ec2 terminate-instances --region $R --instance-ids $IDS >/dev/null && aws ec2 wait instance-terminated --region $R --instance-ids $IDS

echo "2) Security group"
SG=$(aws ec2 describe-security-groups --region $R --filters Name=group-name,Values=$P-dl-sg --query 'SecurityGroups[0].GroupId' --output text)
[ "$SG" != "None" ] && for i in 1 2 3 4 5 6; do aws ec2 delete-security-group --region $R --group-id $SG 2>/dev/null && break; sleep 20; done

echo "3) Glue Data Catalog y Athena"
aws glue delete-database --region $R --name de_gr02_user_4_news 2>/dev/null   # borra tambien sus tablas
aws athena delete-work-group --region $R --work-group $P-wg --recursive-delete-option 2>/dev/null

echo "4) IAM"
aws iam remove-role-from-instance-profile --instance-profile-name $P-dl-ec2-profile --role-name $P-dl-ec2-role 2>/dev/null
aws iam delete-instance-profile --instance-profile-name $P-dl-ec2-profile 2>/dev/null
aws iam delete-role-policy --role-name $P-dl-ec2-role --policy-name dl-access 2>/dev/null
aws iam delete-role --role-name $P-dl-ec2-role 2>/dev/null

echo "5) Bucket (todas las versiones)"
"${PYTHON:-python}" -c "import boto3; b=boto3.resource('s3').Bucket('$BUCKET'); b.object_versions.delete(); b.delete()"   # PYTHON = interprete con boto3

echo "DL eliminado"
