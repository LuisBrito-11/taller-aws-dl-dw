"""Escenarios de calidad / incrementalidad / recuperacion con copias CONTROLADAS de RSS en Bronze.
Uso: python -m dl.scenarios <escenario>   (ver SCENARIOS)"""
from __future__ import annotations

import copy
import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import timedelta
from email.utils import format_datetime
from pathlib import Path

from . import catalog, cli, common as c, gold, ingest, silver
from .ingest import classify

EVIDENCE_DIR = Path(__file__).resolve().parents[2] / "docs" / "evidence"


# ---------------------------------------------------------------- utilidades
def snapshot() -> dict:
    q = catalog.run_query
    one = lambda sql: int(float(q(sql)["rows"][0][0]))  # noqa: E731
    return {
        "silver_news": one("SELECT count(*) FROM silver_news"),
        "silver_entities": one("SELECT count(*) FROM silver_news_entities"),
        "silver_key_phrases": one("SELECT count(*) FROM silver_news_key_phrases"),
        "gold_news_total": one("SELECT coalesce(sum(news_count),0) FROM gold_news_daily"),
        "quarantine_rows": one("SELECT count(*) FROM silver_quarantine"),
    }


def save_evidence(name: str, payload: dict) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / f"{name}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    c.put_json(f"ops/evidence/{name}.json", payload)


def find_bronze(run_id: str, source_id: str) -> dict:
    ms = [m for m in ingest.list_manifests(run_id) if m["source_id"] == source_id]
    assert ms, f"no hay Bronze para {run_id}/{source_id}"
    return ms[0]


def make_variant(base_run: str, source_id: str, new_run: str, mutate=None, raw_bytes: bytes | None = None) -> dict:
    """Copia controlada de un feed Bronze con modificaciones -> nuevo lote Bronze (controlled_copy=true)."""
    base = find_bronze(base_run, source_id)
    body = raw_bytes if raw_bytes is not None else c.get_bytes(base["s3_key"])
    if mutate and raw_bytes is None:
        root = ET.fromstring(body)
        mutate(root)
        body = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    now = c.utcnow()
    d = now.strftime("%Y-%m-%d")
    prefix = c.bronze_prefix(source_id, d, new_run)
    status, detail = classify(200, body, None)
    c.put_bytes(prefix + "feed.xml", body, "application/xml")
    m = {**{k: v for k, v in base.items() if k != "manifest_key"}, "run_id": new_run, "ingested_at_utc": c.iso(now),
         "ingestion_date": d, "s3_key": prefix + "feed.xml", "content_sha256": c.sha256_bytes(body),
         "content_bytes": len(body), "status": status, "detail": detail, "controlled_copy": True,
         "parent_run_id": base_run}
    c.put_json(prefix + "manifest.json", m)
    return m


def items(root):
    return list(root.iter("item"))


def set_text(item, tag, value):
    el = item.find(tag)
    if el is None:
        el = ET.SubElement(item, tag)
    el.text = value


def new_item(root, idx: int, title: str, pub, guid_prefix="demo-guid"):
    ch = root.find("channel")
    it = ET.SubElement(ch, "item")
    set_text(it, "title", title)
    set_text(it, "link", f"https://www.eltiempo.com/demo/{guid_prefix}-{idx}")
    set_text(it, "guid", f"{guid_prefix}-{idx}")
    set_text(it, "description", f"<p>{title}. Cobertura de prueba sobre el Banco de la Republica y Bogota.</p>")
    set_text(it, "pubDate", format_datetime(pub))
    return it


def run_pipeline(run_id: str, mode: str = "reprocess", catalog_update: bool = True) -> dict:
    return cli._downstream([run_id], run_id, mode, False, update_catalog=catalog_update)


# ---------------------------------------------------------------- escenarios
def sc_repeat() -> dict:
    before = snapshot()
    r = run_pipeline("initial_load_01")
    after = snapshot()
    return {"scenario": "repeticion_lote_exitoso", "before": before, "after": after, "identical": before == after,
            "batch_counts": r["transform"]["batches"], "comprehend_texts_analyzed": r["enrichment"]["comprehend_calls_texts"]}


def sc_defective() -> dict:
    before = snapshot()

    def bad(root):
        it = items(root)
        set_text(it[0], "title", "")                       # titulo faltante
        set_text(it[1], "link", "esto-no-es-un-enlace")    # enlace invalido
        set_text(it[2], "pubDate", "fecha-basura")         # fecha invalida
        it[3].remove(it[3].find("pubDate"))                # fecha faltante
        root.find("channel").append(copy.deepcopy(it[4]))  # duplicado dentro del lote
        new_item(root, 1, "Gobierno anuncia nuevo plan para Medellin", c.utcnow() - timedelta(hours=1), "defect")

    make_variant("initial_load_01", "eltiempo_colombia", "defect_01", bad)
    m = find_bronze("defect_01", "eltiempo_colombia")
    # fuente adicional con XML malformado (truncado)
    base = c.get_bytes(find_bronze("initial_load_01", "eltiempo_tecnosfera")["s3_key"])
    make_variant("initial_load_01", "eltiempo_tecnosfera", "defect_01", raw_bytes=base[: len(base) // 2])
    r = run_pipeline("defect_01")
    after = snapshot()
    q = catalog.run_query("SELECT source_id, reject_reason, bronze_key FROM silver_quarantine WHERE quarantined_run_id='defect_01' ORDER BY 1,2")
    return {"scenario": "registros_defectuosos", "before": before, "after": after, "quarantine": q["rows"],
            "batch_counts": r["transform"]["batches"], "controlled_manifest": m}


def sc_incremental() -> dict:
    before = snapshot()

    def add(root):
        for i in range(1, 4):
            new_item(root, i, f"Economia: dolar y Banco de la Republica cierran jornada {i}", c.utcnow() - timedelta(minutes=10 * i), "incr")
    make_variant("initial_load_01", "eltiempo_economia", "incr_01", add)
    r = run_pipeline("incr_01")
    return {"scenario": "nuevo_lote_incremental", "before": before, "after": snapshot(),
            "batch_counts": r["transform"]["batches"], "comprehend_texts_analyzed": r["enrichment"]["comprehend_calls_texts"]}


def sc_modified() -> dict:
    before = snapshot()

    def mod(root):
        it = items(root)
        set_text(it[5], "title", "TITULO MODIFICADO: Petro y el Congreso discuten reforma en Bogota")  # cambia texto
        dt = silver.parse_pub_date(it[6].find("pubDate").text) + timedelta(hours=1)
        set_text(it[6], "pubDate", format_datetime(dt))  # cambia solo la fecha -> sin re-enriquecer
    make_variant("initial_load_01", "eltiempo_colombia", "modified_01", mod)
    r = run_pipeline("modified_01")
    q = catalog.run_query("SELECT title, version, last_run_id FROM silver_news WHERE version > 1 ORDER BY title")
    return {"scenario": "noticia_modificada", "before": before, "after": snapshot(), "versions_gt_1": q["rows"],
            "batch_counts": r["transform"]["batches"],
            "comprehend_texts_analyzed": r["enrichment"]["comprehend_calls_texts"]}


def sc_failure() -> dict:
    def add(root):
        for i in range(1, 3):
            new_item(root, i, f"Tecnologia: Microsoft y Google lanzan herramienta {i}", c.utcnow() - timedelta(minutes=5 * i), "fail")
    make_variant("initial_load_01", "eltiempo_tecnosfera", "fail_01", add)
    before = snapshot()
    os.environ["DL_FAIL_AT"] = "publish_silver"
    err = None
    try:
        run_pipeline("fail_01")
    except RuntimeError as e:
        err = str(e)
    finally:
        os.environ.pop("DL_FAIL_AT", None)
    mid = snapshot()
    status_exists = c.key_exists(c.run_status_key("fail_01"))
    r = run_pipeline("fail_01")  # reejecucion del MISMO lote
    after = snapshot()
    return {"scenario": "fallo_controlado_y_reejecucion", "error": err, "before": before, "after_failure": mid,
            "run_status_written_on_failure": status_exists, "after_rerun": after,
            "rerun_batch_counts": r["transform"]["batches"]}


def sc_partial_source() -> dict:
    cfg = c.load_config()
    rid = "partial_01"
    for s in cfg["sources"]:
        src = dict(s)
        if s["source_id"] == "eltiempo_economia":
            src["url"] = "https://www.eltiempo.com/rss/seccion-que-no-existe-xyz.xml"
        ingest.ingest_source(src, rid, cfg["http"])
    first = ingest.summarize_run(rid, [s["source_id"] for s in cfg["sources"]])
    for s in cfg["sources"]:  # reejecucion: solo reintenta la que fallo
        ingest.ingest_source(s, rid, cfg["http"])
    second = ingest.summarize_run(rid, [s["source_id"] for s in cfg["sources"]])
    return {"scenario": "ejecucion_parcial_y_recuperacion", "first_attempt": first, "after_retry": second}


def sc_rebuild() -> dict:
    before = snapshot()
    ref = silver.load_state()[["news_id", "content_hash", "version"]].sort_values("news_id").reset_index(drop=True)
    r = cli._downstream([], "rebuild_check", "rebuild", True)
    after = snapshot()
    new = silver.load_state()[["news_id", "content_hash", "version"]].sort_values("news_id").reset_index(drop=True)
    return {"scenario": "reprocesamiento_desde_bronze", "before": before, "after": after,
            "state_identical": bool(ref.equals(new)), "rss_requests_made": 0,
            "comprehend_texts_analyzed": r["enrichment"]["comprehend_calls_texts"]}


def sc_new_partition() -> dict:
    def add(root):
        new_item(root, 1, "Colombia: noticia de una fecha nueva sobre Cali", c.utcnow() - timedelta(days=14), "part")
    make_variant("initial_load_01", "eltiempo_colombia", "part_02", add)
    run_pipeline("part_02", catalog_update=False)
    part = c.utcnow() - timedelta(days=14)
    d = part.strftime("%Y-%m-%d")
    sql = f"SELECT count(*) FROM silver_news WHERE pub_date='{d}'"
    before = int(float(catalog.run_query(sql)["rows"][0][0]))
    files = [k for k in c.list_keys("silver/news/") if f"pub_date={d}" in k]
    catalog.update_catalog()
    after = int(float(catalog.run_query(sql)["rows"][0][0]))
    return {"scenario": "nueva_particion", "partition": d, "s3_files": files, "athena_rows_before_msck": before,
            "athena_rows_after_msck": after}


SCENARIOS = {"repeat": sc_repeat, "defective": sc_defective, "incremental": sc_incremental, "modified": sc_modified,
             "failure": sc_failure, "partial": sc_partial_source, "rebuild": sc_rebuild, "partition": sc_new_partition}

if __name__ == "__main__":
    name = sys.argv[1]
    out = SCENARIOS[name]()
    save_evidence(f"scenario_{name}", out)
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
