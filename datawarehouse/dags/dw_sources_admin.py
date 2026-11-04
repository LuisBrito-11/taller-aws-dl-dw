"""Operaciones sobre las fuentes AWS (RDS MySQL y DocumentDB), ejecutadas DENTRO de la VPC desde la EC2.

action:
  init             : crea esquema MySQL/colecciones y genera los datos sinteticos (seed 42, con defectos)
  append_batch     : agrega el lote diario N (pedidos MySQL + resenas y eventos DocumentDB)
  simulate_changes : cambia ciudad de un cliente y categoria de un producto (demostracion SCD2)
  counts           : solo reporta conteos de las fuentes
"""
from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

LOG = logging.getLogger("dw_sources_admin")


def _run(module: str, *args: str) -> str:
    cmd = [sys.executable, "-m", f"ecommerce_dw.sources.{module}", *args]
    LOG.info("ejecutando %s", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True)
    for line in (p.stdout + p.stderr).splitlines():
        LOG.info(line)
    if p.returncode:
        raise RuntimeError(f"{module} fallo ({p.returncode})")
    return p.stdout


@dag(dag_id="dw_sources_admin", schedule=None, start_date=datetime(2026, 11, 1), catchup=False, max_active_runs=1,
     params={"action": Param("counts", enum=["init", "append_batch", "simulate_changes", "counts"]),
             "batch_number": Param(1, type="integer", minimum=1, maximum=365),
             "city": Param("Medellín", type="string"), "department": Param("Antioquia", type="string"),
             "category": Param("Hogar", type="string"), "changed_at": Param("2025-07-01", type="string")},
     tags=["datawarehouse", "fuentes"])
def dw_sources_admin():
    @task
    def operate() -> dict:
        p = get_current_context()["params"]
        if p["action"] == "init":
            _run("generate_data")
        elif p["action"] == "append_batch":
            _run("append_batch", "--batch-id", str(p["batch_number"]))
        elif p["action"] == "simulate_changes":
            _run("simulate_changes", "--city", p["city"], "--department", p["department"],
                 "--category", p["category"], "--changed-at", p["changed_at"])
        return {"action": p["action"]}

    @task
    def source_counts(_: dict) -> dict:
        from ecommerce_dw.ingestion.raw_sources import connect_mysql
        from ecommerce_dw.ingestion.s3_raw import mongo_client
        conn = connect_mysql()
        cur = conn.cursor()
        out = {}
        for t in ("customers", "products", "orders", "order_items"):
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            out[f"mysql.{t}"] = cur.fetchone()[0]
        cur.execute("SELECT customer_id, city, department, updated_at FROM customers WHERE customer_id = 1")
        out["customer_1"] = [str(x) for x in cur.fetchone()]
        cur.execute("SELECT product_id, category, updated_at FROM products WHERE product_id = 1")
        out["product_1"] = [str(x) for x in cur.fetchone()]
        conn.close()
        c = mongo_client()
        db = c["ecommerce"]
        for coll in ("reviews", "navigation_events"):
            out[f"docdb.{coll}"] = db[coll].count_documents({})
        out["docdb.server_version"] = c.server_info().get("version")
        c.close()
        LOG.info("conteos fuentes %s", out)
        return out

    source_counts(operate())


dw_sources_admin()
