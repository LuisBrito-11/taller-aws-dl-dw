-- Consultas Athena entregables (base: de_gr02_user_4_news, workgroup: de-gr02-user-4-wg)

-- Q1. Noticias por fecha, medio y seccion (Gold)
SELECT pub_date, medium, section, news_count
FROM gold_news_daily
ORDER BY pub_date DESC, section;

-- Q2a. Top 10 organizaciones que aparecen en mas noticias
SELECT entity_text AS organizacion, sum(news_count) AS noticias
FROM gold_entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity_key, entity_text
ORDER BY noticias DESC, organizacion
LIMIT 10;

-- Q2b. Top 10 personas
SELECT entity_text AS persona, sum(news_count) AS noticias
FROM gold_entity_daily
WHERE entity_type = 'PERSON'
GROUP BY entity_key, entity_text
ORDER BY noticias DESC, persona
LIMIT 10;

-- Q2c. Top 10 lugares
SELECT entity_text AS lugar, sum(news_count) AS noticias
FROM gold_entity_daily
WHERE entity_type = 'LOCATION'
GROUP BY entity_key, entity_text
ORDER BY noticias DESC, lugar
LIMIT 10;

-- Q3. Frases clave mas frecuentes por seccion (top 5 por seccion)
SELECT section, key_phrase, noticias FROM (
  SELECT section, min(key_phrase) AS key_phrase, sum(news_count) AS noticias,
         row_number() OVER (PARTITION BY section ORDER BY sum(news_count) DESC, key_phrase_norm) AS rn
  FROM gold_key_phrase_section_daily
  GROUP BY section, key_phrase_norm
) WHERE rn <= 5
ORDER BY section, noticias DESC;

-- Q4. Presencia de una entidad entre fechas (cambiar el valor de entity_key)
SELECT pub_date, entity_type, entity_text, news_count
FROM gold_entity_daily
WHERE entity_key = 'bogotá'
ORDER BY pub_date;

-- Q5a. Conciliacion Gold vs Silver: noticias por dia
SELECT s.pub_date, s.n_silver, g.n_gold, s.n_silver - g.n_gold AS diferencia
FROM (SELECT pub_date, count(*) AS n_silver FROM silver_news GROUP BY pub_date) s
FULL OUTER JOIN (SELECT pub_date, sum(news_count) AS n_gold FROM gold_news_daily GROUP BY pub_date) g
  ON s.pub_date = g.pub_date
ORDER BY 1;

-- Q5b. Conciliacion de entidades: distintas por noticia (Silver) vs suma Gold
SELECT (SELECT count(*) FROM (SELECT DISTINCT news_id, entity_type, entity_key FROM silver_news_entities)) AS silver,
       (SELECT sum(news_count) FROM gold_entity_daily) AS gold;

-- Q6. Cuarentena con motivo y traza a Bronze
SELECT source_id, reject_reason, bronze_run_id, bronze_key
FROM silver_quarantine
ORDER BY ingestion_date, source_id;

-- Q7. Noticias con mas de una version (modificadas)
SELECT news_id, version, last_run_id, title FROM silver_news WHERE version > 1;

-- Q8. Poda de particiones: comparar bytes escaneados (ver data_scanned_bytes en Athena / scripts/athena_scan_compare.py)
-- Con filtro de particion:
SELECT count(*), count(DISTINCT news_id) FROM silver_news WHERE pub_date = '2026-11-03';
-- Sin filtro de particion (obliga a leer todo; el filtro sobre pub_date_utc no poda particiones):
SELECT count(*), count(DISTINCT news_id) FROM silver_news WHERE date_format(pub_date_utc, '%Y-%m-%d') = '2026-11-03';
