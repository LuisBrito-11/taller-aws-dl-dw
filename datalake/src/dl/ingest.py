"""Ingesta RSS -> capa Bronze (respuesta original + manifiesto por lote/fuente)."""
from __future__ import annotations

import xml.etree.ElementTree as ET

import requests

from . import common as c

# Estados de ingesta por fuente. Solo "ok" cuenta como ejecucion completa de esa fuente.
OK, HTTP_ERROR, NETWORK_ERROR, EMPTY, MALFORMED = "ok", "http_error", "network_error", "empty", "malformed_xml"


def classify(status_code: int | None, body: bytes, err: str | None) -> tuple[str, str | None]:
    if err:
        return NETWORK_ERROR, err
    if status_code != 200:
        return HTTP_ERROR, f"HTTP {status_code}"
    if not body or not body.strip():
        return EMPTY, "respuesta vacia"
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        return MALFORMED, f"XML malformado: {e}"
    if not root.findall(".//item"):
        return EMPTY, "feed sin <item>"
    return OK, None


def ingest_source(source: dict, run_id: str, http_cfg: dict, log=None) -> dict:
    """Descarga un feed y lo guarda en Bronze. Idempotente: si el lote ya existe y fue ok, no lo reescribe."""
    log = log or c.get_logger(run_id, source["source_id"], "ingest")
    now = c.utcnow()
    ingestion_date = now.strftime("%Y-%m-%d")

    # Si este run_id/fuente ya fue ingestado en una fecha, reutilizarlo (reintentos no crean copias logicas).
    existing = [k for k in c.list_keys(f"bronze/source={source['source_id']}/")
                if k.endswith(f"/run_id={run_id}/manifest.json")]
    if existing:
        manifest = c.get_json(existing[0])
        if manifest["status"] == OK:
            log.info("lote ya ingestado (status=ok), no se reescribe: %s", existing[0])
            return manifest
        ingestion_date = manifest["ingestion_date"]
        log.warning("lote previo con status=%s; se reintenta la ingesta", manifest["status"])

    status_code, body, err = None, b"", None
    try:
        r = requests.get(source["url"], timeout=http_cfg["timeout_seconds"],
                         headers={"User-Agent": http_cfg["user_agent"]})
        status_code, body = r.status_code, r.content
    except requests.RequestException as e:  # noqa: PERF203
        err = f"{type(e).__name__}: {e}"

    status, detail = classify(status_code, body, err)
    prefix = c.bronze_prefix(source["source_id"], ingestion_date, run_id)
    feed_key = prefix + "feed.xml" if body else None
    if feed_key:
        c.put_bytes(feed_key, body, "application/xml")
    manifest = {
        "run_id": run_id,
        "source_id": source["source_id"],
        "medium": source["medium"],
        "section": source["section"],
        "url": source["url"],
        "ingested_at_utc": c.iso(now),
        "ingestion_date": ingestion_date,
        "http_status": status_code,
        "content_sha256": c.sha256_bytes(body) if body else None,
        "content_bytes": len(body),
        "s3_key": feed_key,
        "status": status,
        "detail": detail,
        "controlled_copy": False,
    }
    c.put_json(prefix + "manifest.json", manifest)
    (log.info if status == OK else log.error)("ingesta status=%s http=%s bytes=%s key=%s %s",
                                              status, status_code, len(body), feed_key, detail or "")
    return manifest


def list_manifests(run_id: str) -> list[dict]:
    keys = [k for k in c.list_keys("bronze/") if k.endswith(f"/run_id={run_id}/manifest.json")]
    return [c.get_json(k) | {"manifest_key": k} for k in keys]


def summarize_run(run_id: str, expected_sources: list[str]) -> dict:
    """completa / parcial / fallida: una fuente ausente o no-ok NUNCA se interpreta como 'sin noticias'."""
    manifests = {m["source_id"]: m for m in list_manifests(run_id)}
    per_source = {sid: (manifests[sid]["status"] if sid in manifests else "not_ingested") for sid in expected_sources}
    n_ok = sum(1 for s in per_source.values() if s == OK)
    status = "complete" if n_ok == len(per_source) else ("partial" if n_ok else "failed")
    return {"run_id": run_id, "ingest_status": status, "sources": per_source}
