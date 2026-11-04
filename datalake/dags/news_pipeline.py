"""DAG del Data Lake de noticias (El Tiempo): Bronze -> Silver (+Comprehend) -> Gold -> Glue/Athena.

Parametros (Trigger DAG w/ config):
  reprocess_run_id : si no es vacio, reprocesa ese lote DESDE BRONZE sin consultar los RSS.
  rebuild_all      : reconstruye Silver/Gold desde TODO Bronze.
  fail_at/fail_attempts : fallo controlado para demostrar reintentos y recuperacion.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from dl import catalog, common as c, gold, ingest, pipeline, silver, validate

DEFAULT_ARGS = {
    "owner": "de-gr02-user-4",
    "retries": 2,                              # reintentos acotados para fallos transitorios
    "retry_delay": timedelta(seconds=45),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


def _ctx():
    ctx = get_current_context()
    p = ctx["params"]
    run_id = c.sanitize_run_id(ctx["run_id"])
    mode = "rebuild" if p["rebuild_all"] else ("reprocess" if p["reprocess_run_id"] else "ingest")
    target = p["reprocess_run_id"] or run_id
    return ctx, p, run_id, mode, target


def _maybe_fail(step: str) -> None:
    ctx, p, *_ = _ctx()
    if p["fail_at"] == step and ctx["ti"].try_number <= int(p["fail_attempts"]):
        raise RuntimeError(f"FALLO CONTROLADO en '{step}' (intento {ctx['ti'].try_number})")


@dag(
    dag_id="news_pipeline",
    description="Pipeline medallon de noticias RSS: Bronze/Silver/Gold + Comprehend + Glue + Athena",
    schedule="*/30 * * * *",                    # programado durante el laboratorio
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,                          # control de concurrencia: nunca dos escrituras a la vez
    default_args=DEFAULT_ARGS,
    params={
        "reprocess_run_id": Param("", type="string", description="run_id de Bronze a reprocesar (vacio = ingesta nueva)"),
        "rebuild_all": Param(False, type="boolean", description="Reconstruir desde todo Bronze"),
        "fail_at": Param("", type="string", description="Paso donde inyectar fallo (enrich|publish_silver|build_gold|update_catalog)"),
        "fail_attempts": Param(0, type="integer", description="Intentos que fallan (1 = se recupera por retry)"),
    },
    tags=["datalake", "eltiempo"],
)
def news_pipeline():
    @task
    def validate_config() -> list[dict]:
        ctx, p, run_id, mode, _ = _ctx()
        c.get_logger(run_id, "-", "validate_config").info("modo=%s params=%s", mode, dict(p))
        pipeline.validate_config()
        return c.load_config()["sources"]

    @task
    def ingest_source(source: dict) -> dict:
        ctx, p, run_id, mode, _ = _ctx()
        if mode != "ingest":
            raise AirflowSkipException(f"modo {mode}: no se consultan los RSS")
        m = ingest.ingest_source(source, run_id, c.load_config()["http"], c.get_logger(run_id, source["source_id"], "ingest"))
        if m["status"] != ingest.OK:   # falla visible: nunca se interpreta como 'sin noticias'
            raise RuntimeError(f"{source['source_id']}: {m['status']} {m['detail']}")
        return {k: m[k] for k in ("source_id", "status", "http_status", "s3_key", "content_sha256")}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def collect_ingest(sources: list[dict]) -> dict:
        ctx, p, run_id, mode, target = _ctx()
        log = c.get_logger(run_id, "-", "collect_ingest")
        if mode != "ingest":
            return {"mode": mode, "run_ids": [target] if mode == "reprocess" else [], "ingest_status": mode}
        s = ingest.summarize_run(run_id, [x["source_id"] for x in sources])
        if s["ingest_status"] == "failed":
            raise AirflowFailException(f"ejecucion FALLIDA: ninguna fuente ok {s['sources']}")
        (log.warning if s["ingest_status"] == "partial" else log.info)("ejecucion %s %s", s["ingest_status"], s["sources"])
        return {"mode": mode, "run_ids": [run_id], **s}

    @task
    def silver_transform(info: dict) -> dict:
        ctx, p, run_id, mode, _ = _ctx()
        return silver.transform(info["run_ids"], rebuild=(mode == "rebuild"))

    @task
    def enrich(_: dict) -> dict:
        _maybe_fail("enrich")
        return silver.enrich()

    @task
    def publish_silver(_: dict) -> dict:
        _maybe_fail("publish_silver")
        return silver.publish_silver()

    @task
    def build_gold(_: dict) -> dict:
        _maybe_fail("build_gold")
        return gold.build_gold()

    @task
    def update_catalog(_: dict) -> dict:
        _maybe_fail("update_catalog")
        return catalog.update_catalog()

    @task
    def athena_validate(_: dict) -> dict:
        ctx, p, run_id, mode, target = _ctx()
        return validate.validate(run_id)

    @task
    def register_status(results: dict, transform: dict, info: dict) -> dict:
        ctx, p, run_id, mode, target = _ctx()
        return pipeline.register_status(run_id, mode, transform,
                                        {"validation": results, "airflow_run_id": ctx["run_id"],
                                         "reprocessed_bronze_run_id": target if mode == "reprocess" else None})

    srcs = validate_config()
    ing = ingest_source.expand(source=srcs)
    info = collect_ingest(srcs)
    ing >> info
    t = silver_transform(info)
    e = enrich(t)
    s = publish_silver(e)
    g = build_gold(s)
    cat = update_catalog(g)
    v = athena_validate(cat)
    register_status(v, t, info)


news_pipeline()
