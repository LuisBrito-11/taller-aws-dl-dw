"""CLI: python -m dl.cli <comando>. Util para ejecutar y probar fuera de Airflow."""
from __future__ import annotations

import argparse
import json
import os

from . import catalog, common as c, gold, ingest, pipeline, silver, validate


def main() -> None:
    p = argparse.ArgumentParser(prog="dl")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("run", help="pipeline completo (ingesta + todo)")
    s.add_argument("--run-id", default=None)
    s = sub.add_parser("reprocess", help="reprocesa un lote desde Bronze (sin consultar RSS)")
    s.add_argument("--run-id", required=True, help="run_id de Bronze a reprocesar")
    s = sub.add_parser("rebuild", help="reconstruye Silver/Gold desde TODO Bronze")
    sub.add_parser("catalog")
    sub.add_parser("validate")
    a = p.parse_args()

    if a.cmd == "run":
        rid = c.sanitize_run_id(a.run_id) if a.run_id else c.new_run_id()
        pipeline.validate_config()
        cfg = c.load_config()
        for src in cfg["sources"]:
            ingest.ingest_source(src, rid, cfg["http"])
        _downstream([rid], rid, "ingest", False)
    elif a.cmd == "reprocess":
        pipeline.validate_config()
        _downstream([a.run_id], a.run_id, "reprocess", False)
    elif a.cmd == "rebuild":
        _downstream([], "rebuild_" + c.new_run_id(), "rebuild", True)
    elif a.cmd == "catalog":
        print(json.dumps(catalog.update_catalog(), indent=2))
    elif a.cmd == "validate":
        print(json.dumps(validate.validate(), indent=2))


def _fail_at(step: str) -> None:
    """Inyeccion de fallo controlado para demostrar recuperacion (DL_FAIL_AT=<paso>)."""
    if os.environ.get("DL_FAIL_AT") == step:
        raise RuntimeError(f"FALLO CONTROLADO inyectado antes de '{step}'")


def _downstream(run_ids: list[str], rid: str, mode: str, rebuild: bool, update_catalog: bool = True) -> dict:
    t = silver.transform(run_ids, rebuild=rebuild)
    _fail_at("enrich")
    e = silver.enrich()
    _fail_at("publish_silver")
    sv = silver.publish_silver()
    _fail_at("build_gold")
    g = gold.build_gold()
    if not update_catalog:
        return {"transform": t, "enrichment": e}
    _fail_at("update_catalog")
    catalog.update_catalog()
    v = validate.validate(rid)
    pipeline.register_status(rid, mode, t, {"enrichment": e, "silver": sv, "gold": g, "validation": v})
    return {"transform": t, "enrichment": e, "silver": sv, "gold": g}


if __name__ == "__main__":
    main()
