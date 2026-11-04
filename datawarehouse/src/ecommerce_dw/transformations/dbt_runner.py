"""Ejecuta dbt (adaptador Redshift) desde Airflow. dbt corre en la EC2; el SQL se ejecuta en Redshift."""

import logging
import os
import subprocess
from pathlib import Path

DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt"))
LOGGER = logging.getLogger(__name__)


def run_dbt(command, selector=None, extra=None):
    """Ejecuta un comando dbt; un codigo de salida distinto de 0 hace fallar la tarea de Airflow."""
    arguments = [DBT_EXECUTABLE, command, "--project-dir", str(DBT_PROJECT), "--profiles-dir", str(DBT_PROJECT)]
    if selector:
        arguments += ["--select", selector]
    arguments += list(extra or [])
    LOGGER.info("dbt: %s", " ".join(arguments))
    proc = subprocess.run(arguments, capture_output=True, text=True,
                          env={**os.environ, "DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"})
    for line in proc.stdout.splitlines():
        LOGGER.info(line)
    if proc.returncode != 0:
        LOGGER.error(proc.stderr[-4000:])
        raise RuntimeError(f"dbt {command} fallo con codigo {proc.returncode}")
    return proc.stdout[-2000:]
