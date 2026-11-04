"""Ejecuta sql/athena_queries.sql en Athena y guarda resultados + bytes escaneados como evidencia."""
from __future__ import annotations

import json
import re
from pathlib import Path

from . import catalog
from .scenarios import save_evidence

SQL_FILE = Path(__file__).resolve().parents[2] / "sql" / "athena_queries.sql"


def split_queries(text: str) -> list[tuple[str, str]]:
    out, label = [], "q"
    buf: list[str] = []
    for line in text.splitlines():
        m = re.match(r"--\s*(Q\d+\w*)\.", line)
        if m:
            label = m.group(1)
        if line.strip().startswith("--") or not line.strip():
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            out.append((label if not any(o[0] == label for o in out) else label + "_b", "\n".join(buf).rstrip(";")))
            buf = []
    return out


def main() -> None:
    results = {}
    for label, sql in split_queries(SQL_FILE.read_text(encoding="utf-8")):
        r = catalog.run_query(sql)
        results[label] = {"sql": sql, "columns": r["columns"], "rows": r["rows"][:30],
                          "data_scanned_bytes": r["data_scanned_bytes"], "engine_ms": r["engine_ms"], "query_id": r["query_id"]}
        print(label, len(r["rows"]), "filas", r["data_scanned_bytes"], "bytes")
    save_evidence("athena_queries", results)
    q8 = [v for k, v in results.items() if k.startswith("Q8")]
    if len(q8) >= 2:
        print(json.dumps({"con_filtro_particion_bytes": q8[0]["data_scanned_bytes"],
                          "sin_filtro_particion_bytes": q8[1]["data_scanned_bytes"]}))


if __name__ == "__main__":
    main()
