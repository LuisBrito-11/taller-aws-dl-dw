# Escenarios de calidad, incrementalidad, SCD2 y recuperacion del DW (ejecutar desde datawarehouse/).
param([string]$AirflowIp)
$ErrorActionPreference = "Continue"; $env:PYTHONIOENCODING = "utf-8"
$env:AIRFLOW_URL = "http://${AirflowIp}:8080"
$env:AIRFLOW_PASSWORD = ((Get-Content .build\dw.env | Select-String '^AIRFLOW_ADMIN_PASSWORD=').ToString().Split('=')[1])
$py = "$env:TEMP\dlvenv\Scripts\python.exe"
$counts = "select 'raw.mysql_order_items' t, count(*) n from `"raw`".mysql_order_items union all select 'fct_sales', count(*) from marts.fct_sales union all select 'fct_reviews', count(*) from marts.fct_reviews union all select 'fct_navigation_events', count(*) from marts.fct_navigation_events union all select 'dim_customer', count(*) from marts.dim_customer union all select 'dim_product', count(*) from marts.dim_product union all select 'sum_subtotal_fct_sales', cast(sum(subtotal) as bigint) from marts.fct_sales order by 1"
function T($dag, $run, $conf) { & $py tools\dw_ops.py trigger $dag $run $conf 2>&1 | Select -Last 25 }
function Q($name, $sql) { & $py tools\dw_ops.py sql $name $sql 2>&1 }

"=== 3. Repeticion de lote exitoso"
Q "s3_antes" $counts
T dw_pipeline dw_03_repeticion_lote_exitoso '{\"batch_id\":\"batch_001_initial\"}'
Q "s3_despues" $counts

"=== 4. Nuevo lote incremental (lote diario 1 en las fuentes)"
T dw_sources_admin src_02_append_batch_1 '{\"action\":\"append_batch\",\"batch_number\":1}'
T dw_pipeline dw_04_lote_incremental '{\"batch_id\":\"batch_002_incr\",\"mode\":\"incremental\"}'
Q "s4_despues" $counts
Q "s4_manifiesto_watermark" "select batch_id, status, left(detail, 300) from ops.batch_log where batch_id = 'batch_002_incr' and status in ('extracted','succeeded') order by updated_at"

"=== 5. Cambio en atributo historizado (SCD2)"
T dw_sources_admin src_03_simulate_changes '{\"action\":\"simulate_changes\"}'
T dw_pipeline dw_05_cambio_scd2 '{\"batch_id\":\"batch_003_scd2\",\"mode\":\"incremental\"}'
Q "s5_dim_customer_1" "select customer_sk, customer_id, city, department, valid_from, valid_to, is_current from marts.dim_customer where customer_id = 1 order by valid_from"
Q "s5_dim_product_1" "select product_sk, product_id, category, valid_from, valid_to, is_current from marts.dim_product where product_id = 1 order by valid_from"
Q "s5_despues" $counts

"=== 6. Fallo controlado y reejecucion (lote diario 2)"
T dw_sources_admin src_04_append_batch_2 '{\"action\":\"append_batch\",\"batch_number\":2}'
T dw_pipeline dw_06a_fallo_controlado '{\"batch_id\":\"batch_004_fail\",\"mode\":\"incremental\",\"fail_at\":\"load_redshift\",\"fail_attempts\":5}'
Q "s6_tras_fallo" $counts
T dw_pipeline dw_06b_reejecucion_tras_fallo '{\"batch_id\":\"batch_004_fail\",\"mode\":\"incremental\"}'
Q "s6_tras_reejecucion" $counts
T dw_pipeline dw_06c_fallo_transitorio_retry '{\"batch_id\":\"batch_004_fail\",\"fail_at\":\"dbt_run_marts\",\"fail_attempts\":1}'
Q "s6_tras_retry" $counts
