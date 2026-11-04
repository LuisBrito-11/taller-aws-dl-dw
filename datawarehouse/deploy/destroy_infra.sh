#!/bin/bash
# Elimina TODOS los recursos del DW (irreversible). Ejecutar desde datawarehouse/.
# Uso: bash deploy/destroy_infra.sh [--keep-bucket]
set -uo pipefail
export MSYS_NO_PATHCONV=1
R=us-east-2; P=de-gr02-user-4-dw
ACCOUNT=$(aws sts get-caller-identity --query Account --output text); BUCKET=$P-$ACCOUNT

echo "1) EC2"
IDS=$(aws ec2 describe-instances --region $R --filters "Name=tag:Name,Values=$P-airflow" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query 'Reservations[].Instances[].InstanceId' --output text)
[ -n "$IDS" ] && aws ec2 terminate-instances --region $R --instance-ids $IDS >/dev/null && aws ec2 wait instance-terminated --region $R --instance-ids $IDS

echo "2) Redshift Serverless"
WG_ARN=$(aws redshift-serverless get-workgroup --region $R --workgroup-name $P-wg --query workgroup.workgroupArn --output text 2>/dev/null)
for UL in $(aws redshift-serverless list-usage-limits --region $R --resource-arn "$WG_ARN" --query 'usageLimits[].usageLimitId' --output text 2>/dev/null); do
  aws redshift-serverless delete-usage-limit --region $R --usage-limit-id $UL >/dev/null; done
until ! aws redshift-serverless get-workgroup --region $R --workgroup-name $P-wg >/dev/null 2>&1; do
  # tras borrar el limite de uso el workgroup puede quedar ocupado unos segundos: reintentar el delete
  aws redshift-serverless delete-workgroup --region $R --workgroup-name $P-wg >/dev/null 2>&1; sleep 15; done
aws redshift-serverless delete-namespace --region $R --namespace-name $P-ns >/dev/null 2>&1

echo "3) DocumentDB y RDS"
aws docdb delete-db-instance --region $R --db-instance-identifier $P-docdb-1 >/dev/null 2>&1
aws rds delete-db-instance --region $R --db-instance-identifier $P-mysql --skip-final-snapshot --delete-automated-backups >/dev/null 2>&1
aws docdb wait db-instance-deleted --region $R --db-instance-identifier $P-docdb-1 2>/dev/null
aws docdb delete-db-cluster --region $R --db-cluster-identifier $P-docdb --skip-final-snapshot >/dev/null 2>&1
aws rds wait db-instance-deleted --region $R --db-instance-identifier $P-mysql 2>/dev/null
until ! aws docdb describe-db-clusters --region $R --db-cluster-identifier $P-docdb >/dev/null 2>&1; do sleep 15; done
aws docdb delete-db-subnet-group --region $R --db-subnet-group-name $P-docdb-subnets >/dev/null 2>&1
until ! aws redshift-serverless get-namespace --region $R --namespace-name $P-ns >/dev/null 2>&1; do sleep 15; done

echo "4) Security groups"
for G in $P-db-sg $P-ec2-sg; do
  ID=$(aws ec2 describe-security-groups --region $R --filters Name=group-name,Values=$G --query 'SecurityGroups[0].GroupId' --output text)
  [ "$ID" != "None" ] && for i in 1 2 3 4 5 6; do aws ec2 delete-security-group --region $R --group-id $ID 2>/dev/null && break; sleep 20; done
done

echo "5) IAM"
aws iam remove-role-from-instance-profile --instance-profile-name $P-ec2-profile --role-name $P-ec2-role 2>/dev/null
aws iam delete-instance-profile --instance-profile-name $P-ec2-profile 2>/dev/null
for ROLE in $P-ec2-role $P-redshift-s3-role; do
  for POL in $(aws iam list-role-policies --role-name $ROLE --query PolicyNames --output text 2>/dev/null); do
    aws iam delete-role-policy --role-name $ROLE --policy-name $POL; done
  aws iam delete-role --role-name $ROLE 2>/dev/null
done

if [ "${1:-}" != "--keep-bucket" ]; then
  echo "6) Bucket (todas las versiones)"
  "${PYTHON:-python}" -c "import boto3; b=boto3.resource('s3').Bucket('$BUCKET'); b.object_versions.delete(); b.delete()"   # PYTHON = interprete con boto3
fi
echo "DW eliminado"
