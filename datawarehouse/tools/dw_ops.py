"""Operacion y evidencias del DW desde el equipo local.
  python tools/dw_ops.py trigger <dag_id> <run_id> '<conf json>'   -> dispara, espera y guarda estados de tareas
  python tools/dw_ops.py sql <nombre> "<SQL>"                       -> consulta Redshift (Data API) y guarda resultado
Requiere AIRFLOW_URL y AIRFLOW_PASSWORD en el entorno (no se guardan en archivos versionados)."""
import json
import os
import sys
import time
from pathlib import Path

import boto3
import requests

EVID = Path(__file__).resolve().parents[1] / "docs" / "evidence"
EVID.mkdir(parents=True, exist_ok=True)
API = os.environ.get("AIRFLOW_URL", "").rstrip("/") + "/api/v1"
AUTH = ("admin", os.environ.get("AIRFLOW_PASSWORD", ""))
WG = os.environ.get("RS_WORKGROUP", "de-gr02-user-4-dw-wg")


def trigger(dag: str, run_id: str, conf: dict, timeout: int = 3600) -> dict:
    r = requests.post(f"{API}/dags/{dag}/dagRuns", auth=AUTH, json={"dag_run_id": run_id, "conf": conf})
    if r.status_code not in (200, 409):
        raise SystemExit(f"trigger fallo {r.status_code} {r.text[:300]}")
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = requests.get(f"{API}/dags/{dag}/dagRuns/{run_id}", auth=AUTH).json()["state"]
        if st in ("success", "failed"):
            break
        time.sleep(10)
    tis = requests.get(f"{API}/dags/{dag}/dagRuns/{run_id}/taskInstances?limit=100", auth=AUTH).json()["task_instances"]
    out = {"dag_id": dag, "run_id": run_id, "conf": conf, "state": st,
           "tasks": {t["task_id"]: {"state": t["state"], "try_number": t["try_number"]} for t in tis}}
    (EVID / f"airflow_{run_id}.json").write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    failed = [k for k, v in out["tasks"].items() if v["state"] not in ("success", "skipped")]
    print(f"{dag}/{run_id}: {st}  tareas no exitosas: {failed}")
    for k in failed:  # log del ultimo intento de cada tarea fallida
        n = out["tasks"][k]["try_number"]
        lg = requests.get(f"{API}/dags/{dag}/dagRuns/{run_id}/taskInstances/{k}/logs/{n}?full_content=true",
                          auth=AUTH, headers={"Accept": "text/plain"}).text
        print(f"----- {k} (intento {n}) -----\n" + "\n".join(lg.splitlines()[-40:]))
    return out


def sql(name: str, query: str) -> dict:
    rd = boto3.client("redshift-data", region_name="us-east-2")
    qid = rd.execute_statement(WorkgroupName=WG, Database="dev", Sql=query)["Id"]
    while True:
        d = rd.describe_statement(Id=qid)
        if d["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(1)
    if d["Status"] != "FINISHED":
        raise SystemExit(f"{name}: {d['Status']} {d.get('Error')}")
    rows, cols = [], []
    if d.get("HasResultSet"):
        res = rd.get_statement_result(Id=qid)
        cols = [c["name"] for c in res["ColumnMetadata"]]
        rows = [[None if (not v or v.get("isNull")) else list(v.values())[0] for v in r] for r in res["Records"]]
    out = {"name": name, "sql": query, "columns": cols, "rows": rows}
    (EVID / f"redshift_{name}.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(name, cols)
    for r in rows[:40]:
        print("  ", r)
    return out


if __name__ == "__main__":
    if sys.argv[1] == "trigger":
        trigger(sys.argv[2], sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 else {})
    elif sys.argv[1] == "sql":
        sql(sys.argv[2], sys.argv[3])
