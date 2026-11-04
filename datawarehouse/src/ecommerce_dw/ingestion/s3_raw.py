"""Extraccion por lote desde RDS MySQL y DocumentDB hacia S3 (raw/) y carga a Redshift con COPY.

Ruta de cada archivo:  s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/part-000.csv.gz
Manifiesto del lote:   s3://<bucket>/raw/_manifests/batch_id=<id>.json   (se escribe AL FINAL: marca el lote como extraido)

Reglas:
* Un lote con manifiesto NUNCA se vuelve a extraer: se reutilizan sus archivos (reproceso sin consultar fuentes).
* Extraccion inicial = copia completa. Incremental = dimensiones completas (necesarias para SCD2) y
  hechos con llave > marca de agua (maximo extraido en lotes anteriores).
* La carga a Redshift borra las filas del mismo _batch_id antes del COPY, en una transaccion: repetir no duplica.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from pymongo import MongoClient

from .raw_sources import connect_mysql, flatten_mongo

LOG = logging.getLogger("ecommerce_dw.ingestion")
BUCKET = os.environ.get("DW_BUCKET", "")
REGION = os.environ.get("AWS_REGION", "us-east-2")
MANIFEST_PREFIX = "raw/_manifests/"

# entidad -> (fuente, llave, modo incremental, columnas con tipo Redshift)
ENTITIES = {
    "customers": ("mysql", "customer_id", "full", [
        ("customer_id", "BIGINT"), ("name", "VARCHAR(256)"), ("email", "VARCHAR(256)"), ("phone", "VARCHAR(64)"),
        ("city", "VARCHAR(128)"), ("department", "VARCHAR(128)"), ("registration_date", "DATE"),
        ("updated_at", "TIMESTAMP")]),
    "products": ("mysql", "product_id", "full", [
        ("product_id", "BIGINT"), ("name", "VARCHAR(256)"), ("category", "VARCHAR(128)"), ("price", "DECIMAL(12,2)"),
        ("description", "VARCHAR(1024)"), ("is_active", "BOOLEAN"), ("updated_at", "TIMESTAMP")]),
    "orders": ("mysql", "order_id", "watermark", [
        ("order_id", "BIGINT"), ("customer_id", "BIGINT"), ("order_date", "TIMESTAMP"), ("status", "VARCHAR(32)"),
        ("sales_channel", "VARCHAR(64)"), ("payment_method", "VARCHAR(64)"), ("total", "DECIMAL(14,2)")]),
    "order_items": ("mysql", "order_item_id", "watermark", [
        ("order_item_id", "BIGINT"), ("order_id", "BIGINT"), ("product_id", "BIGINT"), ("quantity", "INTEGER"),
        ("unit_price", "DECIMAL(12,2)"), ("subtotal", "DECIMAL(14,2)")]),
    "reviews": ("mongodb", "review_id", "watermark", [
        ("review_id", "BIGINT"), ("customer_id", "BIGINT"), ("product_id", "BIGINT"), ("rating", "INTEGER"),
        ("reviewed_at", "TIMESTAMPTZ"), ("document_json", "VARCHAR(65535)")]),
    "navigation_events": ("mongodb", "event_id", "watermark", [
        ("event_id", "BIGINT"), ("event_type", "VARCHAR(64)"), ("occurred_at", "TIMESTAMPTZ"), ("customer_id", "BIGINT"),
        ("product_id", "BIGINT"), ("sales_channel", "VARCHAR(64)"), ("document_json", "VARCHAR(65535)")]),
}
RAW_TABLE = {e: f"\"raw\".{'mysql' if src == 'mysql' else 'mongo'}_{e}" for e, (src, *_rest) in ENTITIES.items()}
AUDIT_COLS = [("_batch_id", "VARCHAR(64)"), ("_extracted_at", "TIMESTAMPTZ"), ("_loaded_at", "TIMESTAMPTZ"),
              ("_source", "VARCHAR(64)")]


def s3():
    return boto3.client("s3", region_name=REGION)


def _exists(key: str) -> bool:
    try:
        s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except s3().exceptions.ClientError:
        return False


def manifest_key(batch_id: str) -> str:
    return f"{MANIFEST_PREFIX}batch_id={batch_id}.json"


def list_manifests() -> list[dict]:
    out = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=MANIFEST_PREFIX):
        for o in page.get("Contents", []):
            out.append(json.loads(s3().get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read()))
    return sorted(out, key=lambda m: m["extracted_at"])


def _fmt(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:  # UTC sin offset: Redshift lo interpreta como UTC en TIMESTAMPTZ
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _csv_gz(columns: list[str], rows) -> tuple[bytes, int]:
    text = io.StringIO()
    w = csv.writer(text, lineterminator="\n")
    w.writerow(columns)
    n = 0
    for row in rows:
        w.writerow([_fmt(v) for v in row])
        n += 1
    return gzip.compress(text.getvalue().encode("utf-8")), n


def _rows_mysql(entity: str, key: str, cols: list[str], watermark):
    conn = connect_mysql()
    try:
        cur = conn.cursor()
        where = f" WHERE {key} > %s" if watermark is not None else ""
        cur.execute(f"SELECT {', '.join(cols)} FROM {entity}{where} ORDER BY {key}",
                    (watermark,) if watermark is not None else None)
        while batch := cur.fetchmany(1000):
            yield from batch
    finally:
        conn.close()


def mongo_client() -> MongoClient:
    return MongoClient(os.environ["MONGODB_URI"], tz_aware=True)


def _rows_mongo(entity: str, key: str, watermark):
    client = mongo_client()
    try:
        db = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        query = {key: {"$gt": watermark}} if watermark is not None else {}
        for doc in db[entity].find(query).sort(key, 1).batch_size(1000):
            _, values = flatten_mongo(entity, doc, None)
            yield values[:-2]
    finally:
        client.close()


def extract_batch(batch_id: str, mode: str = "auto") -> dict:
    """Extrae todas las entidades a S3. Si el lote ya tiene manifiesto, lo reutiliza (no re-consulta fuentes)."""
    mkey = manifest_key(batch_id)
    if _exists(mkey):
        m = json.loads(s3().get_object(Bucket=BUCKET, Key=mkey)["Body"].read())
        LOG.info("batch_id=%s ya extraido (%s); se reutilizan sus archivos sin consultar las fuentes", batch_id, mkey)
        return m | {"reused": True}

    previous = [m for m in list_manifests() if m["batch_id"] != batch_id]
    if mode == "auto":
        mode = "incremental" if previous else "initial"
    extracted_at = datetime.now(timezone.utc)
    entities = {}
    for entity, (source, key, inc, coltypes) in ENTITIES.items():
        cols = [c for c, _ in coltypes]
        watermark = None
        if mode == "incremental" and inc == "watermark":
            marks = [p["entities"][entity]["max_key"] for p in previous if p["entities"][entity]["max_key"] is not None]
            watermark = max(marks) if marks else None
        rows = list(_rows_mysql(entity, key, cols, watermark) if source == "mysql" else _rows_mongo(entity, key, watermark))
        body, n = _csv_gz(cols, rows)
        s3key = f"raw/{source}/{entity}/batch_id={batch_id}/part-000.csv.gz"
        s3().put_object(Bucket=BUCKET, Key=s3key, Body=body, ContentType="application/gzip")
        keys = [r[cols.index(key)] for r in rows if r[cols.index(key)] is not None]
        entities[entity] = {"source": source, "s3_key": s3key, "rows": n, "extract_mode": "full" if watermark is None else "watermark",
                            "watermark_from": watermark, "max_key": max(keys) if keys else watermark,
                            "sha256": hashlib.sha256(body).hexdigest()}
        LOG.info("batch_id=%s source=%s entity=%s rows=%s watermark=%s -> s3://%s/%s",
                 batch_id, source, entity, n, watermark, BUCKET, s3key)
    manifest = {"batch_id": batch_id, "mode": mode, "extracted_at": extracted_at.isoformat(),
                "bucket": BUCKET, "entities": entities}
    s3().put_object(Bucket=BUCKET, Key=mkey, Body=json.dumps(manifest, indent=2, default=str).encode(),
                    ContentType="application/json")  # el manifiesto se escribe al final = commit del lote
    return manifest | {"reused": False}


# ------------------------------------------------------------------ Redshift
def redshift():
    return psycopg2.connect(host=os.environ["REDSHIFT_HOST"], port=int(os.getenv("REDSHIFT_PORT", "5439")),
                            dbname=os.getenv("REDSHIFT_DATABASE", "dev"), user=os.environ["REDSHIFT_USER"],
                            password=os.environ["REDSHIFT_PASSWORD"], sslmode="require", connect_timeout=30)


OPS_DDL = [
    "CREATE SCHEMA IF NOT EXISTS \"raw\"", "CREATE SCHEMA IF NOT EXISTS ops",
    """CREATE TABLE IF NOT EXISTS ops.batch_log (
        batch_id VARCHAR(64), status VARCHAR(32), mode VARCHAR(16), airflow_run_id VARCHAR(256),
        detail VARCHAR(65535), updated_at TIMESTAMPTZ)""",
    """CREATE TABLE IF NOT EXISTS ops.load_audit (
        batch_id VARCHAR(64), table_name VARCHAR(128), rows_in_file BIGINT, rows_loaded BIGINT, loaded_at TIMESTAMPTZ)""",
]


def ensure_schemas(cur) -> None:
    for sql in OPS_DDL:
        cur.execute(sql)
    for entity, (_, _, _, coltypes) in ENTITIES.items():
        cols = ", ".join(f"{c} {t}" for c, t in coltypes + AUDIT_COLS)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {RAW_TABLE[entity]} ({cols})")


def log_batch(batch_id: str, status: str, mode: str = "", run_id: str = "", detail: dict | None = None) -> None:
    """ops.batch_log es append-only: el estado vigente del lote es la fila mas reciente."""
    with redshift() as conn, conn.cursor() as cur:
        ensure_schemas(cur)
        cur.execute("INSERT INTO ops.batch_log VALUES (%s,%s,%s,%s,%s,getdate())",
                    (batch_id, status, mode, run_id, json.dumps(detail or {}, default=str)[:65000]))


def load_batch(batch_id: str) -> dict:
    """COPY de los archivos del lote a raw.* (transaccion: delete del mismo lote + insert desde staging)."""
    manifest = json.loads(s3().get_object(Bucket=BUCKET, Key=manifest_key(batch_id))["Body"].read())
    role = os.environ["REDSHIFT_IAM_ROLE"]
    result = {}
    conn = redshift()
    try:
        cur = conn.cursor()
        ensure_schemas(cur)
        conn.commit()
        for entity, info in manifest["entities"].items():
            source, _, _, coltypes = ENTITIES[entity]
            cols = [c for c, _ in coltypes]
            tmp = f"stage_{entity}"
            cur.execute(f"CREATE TEMP TABLE {tmp} ({', '.join(f'{c} {t}' for c, t in coltypes)})")
            cur.execute(f"""COPY {tmp} ({', '.join(cols)}) FROM 's3://{manifest['bucket']}/{info['s3_key']}'
                IAM_ROLE '{role}' FORMAT AS CSV GZIP IGNOREHEADER 1 EMPTYASNULL TIMEFORMAT 'auto'
                DATEFORMAT 'auto' REGION '{REGION}'""")
            cur.execute(f"DELETE FROM {RAW_TABLE[entity]} WHERE _batch_id = %s", (batch_id,))
            cur.execute(f"""INSERT INTO {RAW_TABLE[entity]} SELECT {', '.join(cols)}, %s, %s::timestamptz, getdate(), %s
                FROM {tmp}""", (batch_id, manifest["extracted_at"][:26], f"{source}.{entity}"))
            cur.execute(f"SELECT count(*) FROM {RAW_TABLE[entity]} WHERE _batch_id = %s", (batch_id,))
            loaded = cur.fetchone()[0]
            cur.execute("INSERT INTO ops.load_audit VALUES (%s,%s,%s,%s,getdate())",
                        (batch_id, RAW_TABLE[entity], info["rows"], loaded))
            cur.execute(f"DROP TABLE {tmp}")
            conn.commit()  # una transaccion por entidad: borrar+insertar del lote es atomico
            if loaded != info["rows"]:
                raise ValueError(f"{entity}: archivo={info['rows']} cargadas={loaded}")
            result[RAW_TABLE[entity]] = loaded
            LOG.info("batch_id=%s COPY %s filas=%s", batch_id, RAW_TABLE[entity], loaded)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return result


def grant_reader() -> None:
    """Permite SELECT al usuario IAM de consulta (solo lectura) en las capas del warehouse."""
    reader = os.getenv("REDSHIFT_READER_USER", "IAM:de-gr02-user-4")
    with redshift() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_user WHERE usename = %s", (reader,))
        if not cur.fetchone():
            LOG.warning("usuario lector %s aun no existe en Redshift; se omite GRANT", reader)
            return
        for schema in ('"raw"', "intermediate", "quarantine", "marts", "ops"):
            cur.execute(f'GRANT USAGE ON SCHEMA {schema} TO "{reader}"')
            cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{reader}"')


def query(sql: str, params=None) -> list[tuple]:
    with redshift() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []
