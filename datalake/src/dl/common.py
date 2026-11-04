"""Utilidades compartidas: configuracion, S3, hashing, logging con contexto."""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import yaml
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
BUCKET = os.environ.get("DL_BUCKET", "de-gr02-user-4-datalake-802655955683")
GLUE_DB = os.environ.get("DL_GLUE_DB", "de_gr02_user_4_news")
ATHENA_WG = os.environ.get("DL_ATHENA_WORKGROUP", "de-gr02-user-4-wg")
CONFIG_PATH = Path(os.environ.get("DL_CONFIG", Path(__file__).resolve().parents[2] / "config" / "sources.yaml"))

_BOTO_CFG = Config(region_name=REGION, retries={"max_attempts": 8, "mode": "adaptive"})


def s3():
    return boto3.client("s3", config=_BOTO_CFG)


def client(name: str):
    return boto3.client(name, config=_BOTO_CFG)


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_run_id(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_=-]", "-", raw)


def new_run_id() -> str:
    return utcnow().strftime("manual_%Y%m%dT%H%M%SZ")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def get_logger(run_id: str = "-", source: str = "-", task: str = "-") -> logging.LoggerAdapter:
    log = logging.getLogger("dl")
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s run_id=%(run_id)s source=%(source)s task=%(task)s %(message)s"))
        log.addHandler(h)
        log.setLevel(logging.INFO)
        log.propagate = False
    return logging.LoggerAdapter(log, {"run_id": run_id, "source": source, "task": task})


# ---------------------------------------------------------------- S3 helpers
def key_exists(key: str) -> bool:
    try:
        s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    s3().put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type)


def get_bytes(key: str) -> bytes:
    return s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()


def put_json(key: str, obj) -> None:
    put_bytes(key, json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8"), "application/json")


def get_json(key: str):
    return json.loads(get_bytes(key).decode("utf-8"))


def put_parquet(key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy", coerce_timestamps="us",
                  allow_truncated_timestamps=True)
    put_bytes(key, buf.getvalue(), "application/octet-stream")


def get_parquet(key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(get_bytes(key)), engine="pyarrow")


def list_keys(prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return keys


def delete_keys(keys: list[str]) -> None:
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        s3().delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in chunk]})


# ---------------------------------------------------------------- key layout
def bronze_prefix(source_id: str, ingestion_date: str, run_id: str) -> str:
    return f"bronze/source={source_id}/ingestion_date={ingestion_date}/run_id={run_id}/"


STATE_NEWS = "silver/_state/news_current.parquet"
STATE_ENRICH = "silver/_state/enrichment_cache.parquet"


def batch_state_key(run_id: str, source_id: str) -> str:
    return f"silver/_state/batches/run_id={run_id}/source={source_id}.json"


def run_status_key(run_id: str) -> str:
    return f"ops/run_status/run_id={run_id}.json"
