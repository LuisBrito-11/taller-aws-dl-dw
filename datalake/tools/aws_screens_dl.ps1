# Capturas de consola AWS del Data Lake (ejecutar desde datalake/). Requiere sesion iniciada en la ventana que se abre.
$B="de-gr02-user-4-datalake-802655955683"; $S3="https://us-east-2.console.aws.amazon.com/s3/buckets/$B`?region=us-east-2&bucketType=general&tab=objects"
$G="https://us-east-2.console.aws.amazon.com/glue/home?region=us-east-2#/v2/data-catalog"
$D="de_gr02_user_4_news"
$pages = @(
 "dl01_s3_bucket_raiz=$S3",
 "dl02_s3_bronze_lote=$S3&prefix=bronze/source%3Deltiempo_colombia/ingestion_date%3D2026-11-03/run_id%3Dinitial_load_01/",
 "dl03_s3_silver_news=$S3&prefix=silver/news/",
 "dl04_s3_silver_state=$S3&prefix=silver/_state/",
 "dl05_s3_quarantine=$S3&prefix=silver/quarantine/",
 "dl06_s3_gold=$S3&prefix=gold/",
 "dl07_glue_tablas=$G/tables?database=$D",
 "dl08_glue_silver_news=$G/tables/view/silver_news?database=$D&catalogId=802655955683&versionId=latest",
 "dl10_athena_q1_noticias_dia=athena:SELECT pub_date, medium, section, news_count FROM $D.gold_news_daily ORDER BY pub_date DESC, section",
 "dl11_athena_q2_top_orgs=athena:SELECT entity_text AS organizacion, sum(news_count) AS noticias FROM $D.gold_entity_daily WHERE entity_type = 'ORGANIZATION' GROUP BY entity_key, entity_text ORDER BY noticias DESC, organizacion LIMIT 10",
 "dl12_athena_q3_frases_seccion=athena:SELECT section, key_phrase, noticias FROM (SELECT section, min(key_phrase) AS key_phrase, sum(news_count) AS noticias, row_number() OVER (PARTITION BY section ORDER BY sum(news_count) DESC, key_phrase_norm) AS rn FROM $D.gold_key_phrase_section_daily GROUP BY section, key_phrase_norm) WHERE rn <= 5 ORDER BY section, noticias DESC",
 "dl13_athena_q4_entidad_fechas=athena:SELECT pub_date, entity_type, entity_text, news_count FROM $D.gold_entity_daily WHERE entity_key IN ('bogotá','colombia') ORDER BY entity_key, pub_date",
 "dl14_athena_q5_conciliacion=athena:SELECT s.pub_date, s.n_silver, g.n_gold, s.n_silver - g.n_gold AS diferencia FROM (SELECT pub_date, count(*) AS n_silver FROM $D.silver_news GROUP BY pub_date) s FULL OUTER JOIN (SELECT pub_date, sum(news_count) AS n_gold FROM $D.gold_news_daily GROUP BY pub_date) g ON s.pub_date = g.pub_date ORDER BY 1",
 "dl15_athena_q6_cuarentena=athena:SELECT source_id, reject_reason, bronze_run_id, bronze_key FROM $D.silver_quarantine ORDER BY ingestion_date, source_id",
 "dl16_athena_q7_versiones=athena:SELECT news_id, version, first_seen_run_id, last_run_id, title FROM $D.silver_news WHERE version > 1",
 "dl17_athena_comprehend_entidades=athena:SELECT n.section, n.title, e.entity_type, e.entity_text, e.confidence_score FROM $D.silver_news_entities e JOIN $D.silver_news n ON e.news_id = n.news_id ORDER BY n.title, e.entity_type LIMIT 25",
 "dl18_athena_q8_con_particion=athena:SELECT count(*) AS noticias, count(DISTINCT news_id) AS distintas FROM $D.silver_news WHERE pub_date = '2026-11-03'",
 "dl19_athena_q8_sin_particion=athena:SELECT count(*) AS noticias, count(DISTINCT news_id) AS distintas FROM $D.silver_news WHERE date_format(pub_date_utc, '%Y-%m-%d') = '2026-11-03'",
 "dl20_athena_validacion_unicidad=athena:SELECT count(*) AS filas, count(DISTINCT news_id) AS ids_distintos, count(*) - count(DISTINCT news_id) AS duplicados FROM $D.silver_news",
 "dl21_athena_particiones=athena:SHOW PARTITIONS $D.silver_news"
)
& "$env:TEMP\dlvenv\Scripts\python.exe" -u tools\aws_console_screens.py docs\screenshots @pages
