"""Actualiza dags/ y src/ desde s3://<bucket>/deploy/dw-app.tgz (despliegue de cambios sin SSH ni SSM)."""
from __future__ import annotations

import io
import os
import tarfile
from datetime import datetime

import boto3
from airflow.decorators import dag, task


@dag(dag_id="dw_sync_code", schedule=None, start_date=datetime(2026, 11, 1), catchup=False, tags=["datawarehouse", "ops"])
def dw_sync_code():
    @task
    def sync() -> list[str]:
        body = boto3.client("s3").get_object(Bucket=os.environ["DW_BUCKET"], Key="deploy/dw-app.tgz")["Body"].read()
        written = []
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
            for m in tar.getmembers():
                if not m.isfile() or not (m.name.startswith("dags/") or m.name.startswith("src/")):
                    continue
                dest = os.path.join("/opt/airflow", m.name)
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as fh:
                    fh.write(tar.extractfile(m).read())
                written.append(m.name)
        return written

    sync()


dw_sync_code()
