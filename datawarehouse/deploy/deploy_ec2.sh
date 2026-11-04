#!/bin/bash
# Lanza la EC2 con Airflow + dbt del DW (ejecutar desde datawarehouse/ despues de create_infra.sh).
set -euo pipefail
export MSYS_NO_PATHCONV=1
R=us-east-2; P=de-gr02-user-4-dw
source .build/dw.env
BUCKET=$DW_BUCKET

# Endpoints de los servicios administrados -> variables del contenedor
MYSQL_HOST=$(aws rds describe-db-instances --region $R --db-instance-identifier $P-mysql --query 'DBInstances[0].Endpoint.Address' --output text)
MONGO_HOST=$(aws docdb describe-db-clusters --region $R --db-cluster-identifier $P-docdb --query 'DBClusters[0].Endpoint' --output text)
REDSHIFT_HOST=$(aws redshift-serverless get-workgroup --region $R --workgroup-name $P-wg --query 'workgroup.endpoint.address' --output text)
grep -q '^MYSQL_HOST=' .build/dw.env || cat >> .build/dw.env <<EOF
MYSQL_HOST=$MYSQL_HOST
MONGO_HOST=$MONGO_HOST
REDSHIFT_HOST=$REDSHIFT_HOST
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -hex 12)
AIRFLOW_SECRET_KEY=$(openssl rand -hex 24)
EOF
aws s3 cp .build/dw.env s3://$BUCKET/deploy/dw.env --region $R >/dev/null

# Codigo + plugin de Compose (la EC2 no alcanza github.com)
tar czf .build/dw-app.tgz dags src deploy/Dockerfile deploy/docker-compose.yml
aws s3 cp .build/dw-app.tgz s3://$BUCKET/deploy/dw-app.tgz --region $R >/dev/null
aws s3 cp s3://de-gr02-user-4-datalake-802655955683/deploy/docker-compose s3://$BUCKET/deploy/docker-compose --region $R >/dev/null 2>&1 \
  || aws s3 cp ../datalake/.build/docker-compose s3://$BUCKET/deploy/docker-compose --region $R >/dev/null

# Rol de la EC2: solo el bucket del DW (las credenciales de BD van en .env generado)
if ! aws iam get-role --role-name $P-ec2-role >/dev/null 2>&1; then
  aws iam create-role --role-name $P-ec2-role --assume-role-policy-document file://../datalake/deploy/ec2-trust.json >/dev/null
  cat > .build/ec2-policy.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:ListBucket","s3:GetBucketLocation"],
 "Resource":["arn:aws:s3:::$BUCKET","arn:aws:s3:::$BUCKET/*"]}]}
EOF
  aws iam put-role-policy --role-name $P-ec2-role --policy-name dw-s3 --policy-document file://.build/ec2-policy.json
  aws iam create-instance-profile --instance-profile-name $P-ec2-profile >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name $P-ec2-profile --role-name $P-ec2-role
  sleep 12
fi

AMI=$(aws ec2 describe-images --owners amazon --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
      --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text --region $R)
sed "s/__BUCKET__/$BUCKET/" deploy/userdata.sh > .build/userdata.sh
aws ec2 run-instances --region $R --image-id "$AMI" --instance-type t3.medium \
  --iam-instance-profile Name=$P-ec2-profile --security-group-ids "$SG_EC2" \
  --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3,DeleteOnTermination=true}' \
  --user-data file://.build/userdata.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=de-gr02-user-4-dw-airflow},{Key=owner,Value=de-gr02-user-4}]' \
  --query 'Instances[0].InstanceId' --output text
