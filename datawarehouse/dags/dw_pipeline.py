"""Pipeline del Data Warehouse en AWS: RDS MySQL + DocumentDB -> S3 raw/ -> Redshift (COPY) -> dbt -> pruebas.

Parametros (Trigger DAG w/ config):
  batch_id      : id del lote. Vacio = se genera desde la fecha logica (b<AAAAMMDDTHHMMSS>).
                  Si el lote ya existe en S3 se reutiliza SIN consultar las fuentes (reproceso / recuperacion).
  mode          : auto | initial | incremental
  fail_at / fail_attempts : fallo controlado para demostrar reintentos y recuperacion.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from ecommerce_dw.ingestion import s3_raw
from ecommerce_dw.transformations.dbt_runner import run_dbt

LOG = logging.getLogger("dw_pipeline")


def _batch_id() -> str:
    ctx = get_current_context()
    return ctx["params"]["batch_id"] or "b" + ctx["logical_date"].strftime("%Y%m%dT%H%M%S")


def _maybe_fail(step: str) -> None:
    ctx = get_current_context()
    p = ctx["params"]
    if p["fail_at"] == step and ctx["ti"].try_number <= int(p["fail_attempts"]):
        raise RuntimeError(f"FALLO CONTROLADO en '{step}' (intento {ctx['ti'].try_number})")


def _on_failure(context) -> None:
    """Cualquier tarea fallida (sin mas reintentos) marca el lote como failed en ops.batch_log."""
    try:
        bid = context["params"]["batch_id"] or "b" + context["logical_date"].strftime("%Y%m%dT%H%M%S")
        s3_raw.log_batch(bid, "failed", "", context["run_id"],
                         {"task": context["task_instance"].task_id, "error": str(context.get("exception"))[:500]})
    except Exception as e:  # noqa: BLE001
        LOG.error("no se pudo registrar el fallo: %s", e)


DEFAULT_ARGS = {"owner": "de-gr02-user-4", "retries": 2, "retry_delay": timedelta(seconds=30),
                "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(minutes=30), "on_failure_callback": _on_failure}


@dag(
    dag_id="dw_pipeline",
    description="Fuentes AWS -> S3 raw -> Redshift COPY -> dbt (intermediate, quarantine, snapshots, marts) -> tests",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "batch_id": Param("", type="string", description="Lote a procesar (vacio = nuevo lote)"),
        "mode": Param("auto", enum=["auto", "initial", "incremental"]),
        "fail_at": Param("", type="string", description="load_redshift | dbt_run_marts | dbt_test ..."),
        "fail_attempts": Param(0, type="integer"),
    },
    tags=["datawarehouse", "redshift", "dbt"],
)
def dw_pipeline():
    @task
    def validate_connections() -> dict:
        """1. Valida parametros y acceso a RDS, DocumentDB, S3 y Redshift antes de empezar."""
        bid = _batch_id()
        from ecommerce_dw.ingestion.raw_sources import connect_mysql
        conn = connect_mysql()
        conn.cursor().execute("SELECT 1")
        conn.close()
        c = s3_raw.mongo_client()
        c.admin.command("ping")
        c.close()
        s3_raw.s3().head_bucket(Bucket=s3_raw.BUCKET)
        s3_raw.query("SELECT 1")
        s3_raw.log_batch(bid, "started", get_current_context()["params"]["mode"], get_current_context()["run_id"])
        LOG.info("batch_id=%s conexiones OK (RDS, DocumentDB, S3, Redshift)", bid)
        return {"batch_id": bid}

    @task
    def extract_sources(info: dict) -> dict:
        """2. Extrae las fuentes (o reutiliza el lote si ya existe en S3)."""
        m = s3_raw.extract_batch(info["batch_id"], get_current_context()["params"]["mode"])
        s3_raw.log_batch(info["batch_id"], "extracted", m["mode"], get_current_context()["run_id"],
                         {e: v["rows"] for e, v in m["entities"].items()} | {"reused": m["reused"]})
        return {"batch_id": info["batch_id"], "mode": m["mode"], "reused": m["reused"],
                "rows": {e: v["rows"] for e, v in m["entities"].items()}}

    @task
    def verify_s3_files(info: dict) -> dict:
        """3. Verifica que cada archivo del lote existe en S3 y coincide con el manifiesto (hash)."""
        import hashlib
        m = json.loads(s3_raw.s3().get_object(Bucket=s3_raw.BUCKET, Key=s3_raw.manifest_key(info["batch_id"]))["Body"].read())
        for entity, e in m["entities"].items():
            body = s3_raw.s3().get_object(Bucket=s3_raw.BUCKET, Key=e["s3_key"])["Body"].read()
            assert hashlib.sha256(body).hexdigest() == e["sha256"], f"hash distinto en {e['s3_key']}"
            LOG.info("batch_id=%s s3://%s/%s rows=%s OK", info["batch_id"], s3_raw.BUCKET, e["s3_key"], e["rows"])
        return info

    @task
    def load_redshift(info: dict) -> dict:
        """4. COPY de S3 a Redshift raw (idempotente por _batch_id)."""
        _maybe_fail("load_redshift")
        loaded = s3_raw.load_batch(info["batch_id"])
        s3_raw.log_batch(info["batch_id"], "loaded", info["mode"], get_current_context()["run_id"], loaded)
        return info | {"loaded": loaded}

    def dbt_task(task_id: str, command: str, selector: str | None = None):
        @task(task_id=task_id)
        def _run(info: dict) -> dict:
            _maybe_fail(task_id)
            LOG.info("batch_id=%s %s", info["batch_id"], task_id)
            run_dbt(command, selector)
            return info
        return _run

    @task
    def reconcile(info: dict) -> dict:
        """6b. Concilia un indicador agregado: lineas y total vendido en RDS vs Redshift (marts + cuarentena)."""
        from ecommerce_dw.ingestion.raw_sources import connect_mysql
        conn = connect_mysql()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(subtotal),0) FROM order_items")
        src_lines, src_total = cur.fetchone()
        conn.close()
        (fct_lines, fct_total), = s3_raw.query("SELECT count(*), coalesce(sum(subtotal),0) FROM marts.fct_sales")
        (q_lines, q_total), = s3_raw.query("SELECT count(*), coalesce(sum(subtotal),0) FROM quarantine.quarantine_mysql__order_items")
        res = {"source_lines": int(src_lines), "fct_lines": int(fct_lines), "quarantine_lines": int(q_lines),
               "source_total": float(src_total), "fct_plus_quarantine_total": float(fct_total) + float(q_total)}
        LOG.info("batch_id=%s conciliacion %s", info["batch_id"], res)
        if res["source_lines"] != res["fct_lines"] + res["quarantine_lines"] or \
                abs(res["source_total"] - res["fct_plus_quarantine_total"]) > 0.01:
            raise ValueError(f"Conciliacion fallida: {res}")
        return info | {"reconciliation": res}

    @task
    def register_result(info: dict) -> dict:
        """7. Registra el resultado: el lote queda 'succeeded' solo si todo lo anterior paso."""
        counts = s3_raw.query("""select 'fct_sales', count(*) from marts.fct_sales union all
            select 'fct_reviews', count(*) from marts.fct_reviews union all
            select 'fct_navigation_events', count(*) from marts.fct_navigation_events union all
            select 'dim_customer', count(*) from marts.dim_customer union all
            select 'dim_product', count(*) from marts.dim_product""")
        detail = {"marts": {k: int(v) for k, v in counts}, "reconciliation": info.get("reconciliation"),
                  "loaded": info.get("loaded"), "reused_s3_batch": info.get("reused")}
        s3_raw.grant_reader()  # lectura para el usuario IAM que consulta evidencias via Redshift Data API
        s3_raw.log_batch(info["batch_id"], "succeeded", info["mode"], get_current_context()["run_id"], detail)
        LOG.info("batch_id=%s SUCCEEDED %s", info["batch_id"], detail)
        return detail

    v = validate_connections()
    e = extract_sources(v)
    f = verify_s3_files(e)
    ld = load_redshift(f)
    deps = dbt_task("dbt_deps", "deps")(ld)
    seed = dbt_task("dbt_seed", "seed")(deps)
    inter = dbt_task("dbt_run_intermediate", "run", "path:models/intermediate")(seed)
    quar = dbt_task("dbt_run_quarantine", "run", "path:models/quarantine")(inter)
    snap = dbt_task("dbt_snapshot", "snapshot")(quar)
    marts = dbt_task("dbt_run_marts", "run", "path:models/marts")(snap)
    tests = dbt_task("dbt_test", "test")(marts)
    register_result(reconcile(tests))


dw_pipeline()
