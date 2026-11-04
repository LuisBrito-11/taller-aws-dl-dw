"""Glue Data Catalog (tablas explicitas via Athena DDL) y ejecucion de consultas Athena."""
from __future__ import annotations

import time

from . import common as c

TABLES = {
    # nombre: (prefijo S3, columnas, columna de particion)
    "silver_news": ("silver/news/", [
        ("news_id", "string"), ("source_id", "string"), ("medium", "string"), ("section", "string"),
        ("guid", "string"), ("link", "string"), ("title", "string"), ("description", "string"),
        ("pub_date_raw", "string"), ("pub_date_utc", "timestamp"), ("text_hash", "string"),
        ("content_hash", "string"), ("version", "bigint"), ("first_seen_run_id", "string"),
        ("last_run_id", "string"), ("bronze_run_id", "string"), ("bronze_key", "string"),
        ("ingested_at_utc", "string")], "pub_date"),
    "silver_news_entities": ("silver/news_entities/", [
        ("news_id", "string"), ("language", "string"), ("text_hash", "string"), ("analyzed_at_utc", "string"),
        ("entity_type", "string"), ("entity_text", "string"), ("entity_key", "string"),
        ("confidence_score", "double"), ("mentions", "bigint")], "pub_date"),
    "silver_news_key_phrases": ("silver/news_key_phrases/", [
        ("news_id", "string"), ("language", "string"), ("text_hash", "string"), ("analyzed_at_utc", "string"),
        ("key_phrase", "string"), ("key_phrase_norm", "string"), ("confidence_score", "double"),
        ("mentions", "bigint")], "pub_date"),
    "gold_news_daily": ("gold/news_daily/", [
        ("medium", "string"), ("section", "string"), ("news_count", "bigint")], "pub_date"),
    "gold_entity_daily": ("gold/entity_daily/", [
        ("entity_type", "string"), ("entity_key", "string"), ("entity_text", "string"),
        ("news_count", "bigint")], "pub_date"),
    "gold_key_phrase_section_daily": ("gold/key_phrase_section_daily/", [
        ("section", "string"), ("key_phrase_norm", "string"), ("key_phrase", "string"),
        ("news_count", "bigint")], "pub_date"),
}


def run_query(sql: str, wait: bool = True, timeout: int = 180) -> dict:
    ath = c.client("athena")
    qid = ath.start_query_execution(QueryString=sql, WorkGroup=c.ATHENA_WG,
                                    QueryExecutionContext={"Database": c.GLUE_DB})["QueryExecutionId"]
    if not wait:
        return {"query_id": qid}
    t0 = time.time()
    while True:
        q = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - t0 > timeout:
            raise TimeoutError(f"Athena query {qid} timeout")
        time.sleep(1.5)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena {state}: {q['Status'].get('StateChangeReason')} :: {sql[:200]}")
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[d.get("VarCharValue") for d in r["Data"]] for r in page["ResultSet"]["Rows"]]
    header, data = (rows[0], rows[1:]) if rows else ([], [])
    return {"query_id": qid, "columns": header, "rows": data,
            "data_scanned_bytes": q["Statistics"].get("DataScannedInBytes", 0),
            "engine_ms": q["Statistics"].get("EngineExecutionTimeInMillis")}


def ddl(name: str) -> str:
    prefix, cols, part = TABLES[name]
    col_sql = ",\n  ".join(f"`{n}` {t}" for n, t in cols)
    return (f"CREATE EXTERNAL TABLE IF NOT EXISTS {c.GLUE_DB}.{name} (\n  {col_sql}\n)\n"
            f"PARTITIONED BY (`{part}` string)\nSTORED AS PARQUET\n"
            f"LOCATION 's3://{c.BUCKET}/{prefix}'\nTBLPROPERTIES ('classification'='parquet', "
            f"'parquet.compression'='SNAPPY')")


QUARANTINE_DDL = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {c.GLUE_DB}.silver_quarantine (
  `source_id` string, `medium` string, `section` string, `bronze_run_id` string, `bronze_key` string,
  `ingested_at_utc` string, `reject_reason` string, `raw_item` string, `quarantined_run_id` string
)
PARTITIONED BY (`ingestion_date` string, `run_id` string)
STORED AS PARQUET
LOCATION 's3://{c.BUCKET}/silver/quarantine/'"""


def ensure_database() -> None:
    run_query(f"CREATE DATABASE IF NOT EXISTS {c.GLUE_DB}")


def update_catalog() -> dict:
    """Crea tablas si faltan y registra particiones nuevas (MSCK REPAIR TABLE)."""
    log = c.get_logger("-", "-", "update_catalog")
    ensure_database()
    done = {}
    for name in TABLES:
        run_query(ddl(name))
        run_query(f"MSCK REPAIR TABLE {name}")
        done[name] = "ok"
    run_query(QUARANTINE_DDL)
    run_query("MSCK REPAIR TABLE silver_quarantine")
    done["silver_quarantine"] = "ok"
    log.info("catalogo actualizado %s", done)
    return done
