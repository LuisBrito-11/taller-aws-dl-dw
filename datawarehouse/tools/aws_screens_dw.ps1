# Capturas de consola AWS del DW (ejecutar desde datawarehouse/). Reutiliza el perfil de Chrome del Data Lake.
$B="de-gr02-user-4-dw-802655955683"; $S3="https://us-east-2.console.aws.amazon.com/s3/buckets/$B`?region=us-east-2&bucketType=general&tab=objects"
$C="https://us-east-2.console.aws.amazon.com"
$pages = @(
 "dw01_s3_raw=$S3&prefix=raw/",
 "dw02_s3_raw_mysql_orders=$S3&prefix=raw/mysql/orders/",
 "dw03_s3_raw_lote=$S3&prefix=raw/mongodb/reviews/batch_id%3Dbatch_001_initial/",
 "dw04_s3_manifiestos=$S3&prefix=raw/_manifests/",
 "dw05_rds_mysql=$C/rds/home?region=us-east-2#database:id=de-gr02-user-4-dw-mysql;is-cluster=false",
 "dw06_documentdb=$C/docdb/home?region=us-east-2#cluster-details/de-gr02-user-4-dw-docdb",
 "dw07_redshift_workgroup=$C/redshiftv2/home?region=us-east-2#serverless-workgroup?workgroup=de-gr02-user-4-dw-wg",
 "dw08_redshift_namespace=$C/redshiftv2/home?region=us-east-2#serverless-namespace?namespace=de-gr02-user-4-dw-ns",
 "dw09_iam_rol_redshift_s3=$C/iam/home#/roles/details/de-gr02-user-4-dw-redshift-s3-role?section=permissions",
 "dw10_ec2_airflow=$C/ec2/home?region=us-east-2#Instances:tag:Name=de-gr02-user-4-dw-airflow;instanceState=running"
)
& "$env:TEMP\dlvenv\Scripts\python.exe" -u ..\datalake\tools\aws_console_screens.py docs\screenshots @pages
