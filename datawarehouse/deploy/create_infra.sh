#!/bin/bash
# Infraestructura del Data Warehouse en AWS (tamaños mínimos). Ejecutar desde datawarehouse/ con AWS CLI configurado.
# Uso: bash deploy/create_infra.sh <MI_IP_PUBLICA>
# Genera contraseñas aleatorias en .build/dw.env (fuera del repo) y las sube a S3 privado para la EC2.
set -euo pipefail
export MSYS_NO_PATHCONV=1
R=us-east-2; P=de-gr02-user-4-dw
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=$P-$ACCOUNT
VPC=$(aws ec2 describe-vpcs --region $R --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNETS=$(aws ec2 describe-subnets --region $R --filters Name=vpc-id,Values=$VPC --query 'Subnets[].SubnetId' --output text)
MYIP=$1
mkdir -p .build

# 1) Bucket privado (raw/) cifrado y versionado
aws s3api create-bucket --bucket $BUCKET --region $R --create-bucket-configuration LocationConstraint=$R >/dev/null
aws s3api put-public-access-block --bucket $BUCKET --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket $BUCKET --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-versioning --bucket $BUCKET --versioning-configuration Status=Enabled

# 2) Security groups: EC2 (8080 solo mi IP) y bases de datos (solo desde la EC2)
SG_EC2=$(aws ec2 create-security-group --region $R --group-name $P-ec2-sg --description "DW Airflow EC2" --vpc-id $VPC --query GroupId --output text)
aws ec2 authorize-security-group-ingress --region $R --group-id $SG_EC2 --protocol tcp --port 8080 --cidr $MYIP/32 >/dev/null
SG_DB=$(aws ec2 create-security-group --region $R --group-name $P-db-sg --description "DW fuentes y Redshift" --vpc-id $VPC --query GroupId --output text)
for port in 3306 27017 5439; do
  aws ec2 authorize-security-group-ingress --region $R --group-id $SG_DB --protocol tcp --port $port --source-group $SG_EC2 >/dev/null
done

# 3) Contraseñas generadas (nunca versionadas)
MYSQL_PW=$(openssl rand -hex 14); DOCDB_PW=$(openssl rand -hex 14); RS_PW="Rs$(openssl rand -hex 12)A1"
cat > .build/dw.env <<EOF
DW_BUCKET=$BUCKET
AWS_REGION=$R
SG_EC2=$SG_EC2
SG_DB=$SG_DB
MYSQL_USER=etl_admin
MYSQL_PASSWORD=$MYSQL_PW
MYSQL_DATABASE=ecommerce
MONGO_USER=docdbadmin
MONGO_PASSWORD=$DOCDB_PW
MONGO_DATABASE=ecommerce
REDSHIFT_USER=rsadmin
REDSHIFT_PASSWORD=$RS_PW
REDSHIFT_DATABASE=dev
EOF

# 4) Amazon RDS for MySQL (db.t4g.micro, 20 GB, sin backups, privada)
aws rds create-db-instance --region $R --db-instance-identifier $P-mysql --engine mysql --engine-version 8.4.11 \
  --db-instance-class db.t4g.micro --allocated-storage 20 --storage-type gp3 --master-username etl_admin \
  --master-user-password "$MYSQL_PW" --db-name ecommerce --vpc-security-group-ids $SG_DB --no-publicly-accessible \
  --backup-retention-period 0 --no-multi-az --no-deletion-protection --tags Key=owner,Value=de-gr02-user-4 >/dev/null

# 5) Amazon DocumentDB 5.0 (1 instancia db.t4g.medium, TLS activo)
aws docdb create-db-subnet-group --region $R --db-subnet-group-name $P-docdb-subnets \
  --db-subnet-group-description "DW docdb" --subnet-ids $SUBNETS >/dev/null
aws docdb create-db-cluster --region $R --db-cluster-identifier $P-docdb --engine docdb --engine-version 5.0.0 \
  --master-username docdbadmin --master-user-password "$DOCDB_PW" --db-subnet-group-name $P-docdb-subnets \
  --vpc-security-group-ids $SG_DB --backup-retention-period 1 --no-deletion-protection --tags Key=owner,Value=de-gr02-user-4 >/dev/null
aws docdb create-db-instance --region $R --db-instance-identifier $P-docdb-1 --db-instance-class db.t4g.medium \
  --engine docdb --db-cluster-identifier $P-docdb >/dev/null

# 6) Rol IAM para que Redshift lea S3 (COPY)
cat > .build/rs-trust.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["redshift.amazonaws.com","redshift-serverless.amazonaws.com"]},"Action":"sts:AssumeRole"}]}
EOF
cat > .build/rs-s3.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],
 "Resource":["arn:aws:s3:::$BUCKET","arn:aws:s3:::$BUCKET/*"]}]}
EOF
RS_ROLE=$(aws iam create-role --role-name $P-redshift-s3-role --assume-role-policy-document file://.build/rs-trust.json --query Role.Arn --output text)
aws iam put-role-policy --role-name $P-redshift-s3-role --policy-name s3-read-raw --policy-document file://.build/rs-s3.json
echo "REDSHIFT_IAM_ROLE=$RS_ROLE" >> .build/dw.env
sleep 10  # propagacion IAM

# 7) Redshift Serverless: 4 RPU base, privado, limite de uso diario para acotar costos
aws redshift-serverless create-namespace --region $R --namespace-name $P-ns --admin-username rsadmin \
  --admin-user-password "$RS_PW" --db-name dev --iam-roles "$RS_ROLE" --default-iam-role-arn "$RS_ROLE" >/dev/null
aws redshift-serverless create-workgroup --region $R --workgroup-name $P-wg --namespace-name $P-ns --base-capacity 4 \
  --subnet-ids $SUBNETS --security-group-ids $SG_DB --no-publicly-accessible >/dev/null

aws s3 cp .build/dw.env s3://$BUCKET/deploy/dw.env --region $R >/dev/null
echo "OK: bucket=$BUCKET sg_ec2=$SG_EC2 sg_db=$SG_DB role=$RS_ROLE"
