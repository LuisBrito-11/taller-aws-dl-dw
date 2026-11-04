"""Orquestacion logica reutilizable (CLI y Airflow): validacion de config, estado de ejecucion."""
from __future__ import annotations

from . import common as c
from .ingest import summarize_run


def validate_config() -> dict:
    """Comprueba configuracion y acceso a S3, Comprehend, Glue y Athena antes de empezar."""
    log = c.get_logger("-", "-", "validate_config")
    cfg = c.load_config()
    assert cfg["sources"], "sin fuentes"
    for s in cfg["sources"]:
        assert all(s.get(k) for k in ("source_id", "medium", "section", "url")), f"fuente incompleta {s}"
        assert s["url"].endswith(".xml"), f"la URL debe ser un feed XML, no una pagina: {s['url']}"
    c.s3().head_bucket(Bucket=c.BUCKET)
    c.client("comprehend").detect_dominant_language(Text="Prueba de acceso a Comprehend")
    if not _db_exists():
        log.warning("base Glue aun no existe (se crea en update_catalog)")
    c.client("athena").get_work_group(WorkGroup=c.ATHENA_WG)
    log.info("configuracion y accesos OK: bucket=%s db=%s workgroup=%s", c.BUCKET, c.GLUE_DB, c.ATHENA_WG)
    return {"sources": [s["source_id"] for s in cfg["sources"]], "bucket": c.BUCKET}


def _db_exists() -> bool:
    try:
        c.client("glue").get_database(Name=c.GLUE_DB)
        return True
    except c.client("glue").exceptions.EntityNotFoundException:
        return False


def register_status(run_id: str, mode: str, transform_summary: dict | None = None, extra: dict | None = None) -> dict:
    cfg = c.load_config()
    expected = [s["source_id"] for s in cfg["sources"]]
    rec = summarize_run(run_id, expected) if mode == "ingest" else {"run_id": run_id, "ingest_status": "reprocess_from_bronze"}
    rec |= {"mode": mode, "finished_at_utc": c.iso(c.utcnow()), "transform": transform_summary, **(extra or {})}
    c.put_json(c.run_status_key(run_id), rec)
    c.get_logger(run_id, "-", "register_status").info("estado registrado %s", rec["ingest_status"])
    return rec
