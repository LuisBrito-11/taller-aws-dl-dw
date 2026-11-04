"""Validaciones finales con Athena (calidad Silver y conciliacion Silver <-> Gold)."""
from __future__ import annotations

from . import common as c
from .catalog import run_query

# nombre -> (SQL que devuelve un entero, condicion esperada: 0 o igualdad)
CHECKS = {
    "silver_news_id_unico": "SELECT count(*) - count(DISTINCT news_id) FROM silver_news",
    "silver_campos_obligatorios": ("SELECT count(*) FROM silver_news WHERE news_id IS NULL OR title IS NULL OR trim(title)='' "
                                   "OR link IS NULL OR pub_date_utc IS NULL OR pub_date_raw IS NULL"),
    "silver_enlaces_validos": "SELECT count(*) FROM silver_news WHERE NOT regexp_like(link, '^https?://[^ ]+$')",
    "silver_fechas_validas": ("SELECT count(*) FROM silver_news WHERE pub_date_utc < TIMESTAMP '2000-01-01 00:00:00' "
                              "OR pub_date_utc > current_timestamp + INTERVAL '1' DAY "
                              "OR pub_date <> date_format(pub_date_utc, '%Y-%m-%d')"),
    "entidades_sin_noticia": ("SELECT count(*) FROM silver_news_entities e LEFT JOIN silver_news n "
                              "ON e.news_id = n.news_id WHERE n.news_id IS NULL"),
    "frases_sin_noticia": ("SELECT count(*) FROM silver_news_key_phrases k LEFT JOIN silver_news n "
                           "ON k.news_id = n.news_id WHERE n.news_id IS NULL"),
    "entidades_duplicadas": ("SELECT count(*) FROM (SELECT news_id, entity_type, entity_key, count(*) c "
                             "FROM silver_news_entities GROUP BY 1,2,3 HAVING count(*) > 1)"),
    "frases_duplicadas": ("SELECT count(*) FROM (SELECT news_id, key_phrase_norm, count(*) c "
                          "FROM silver_news_key_phrases GROUP BY 1,2 HAVING count(*) > 1)"),
}

# conciliaciones: (SQL silver, SQL gold) deben dar el mismo valor
RECONCILE = {
    "noticias_silver_vs_gold": ("SELECT count(*) FROM silver_news", "SELECT coalesce(sum(news_count),0) FROM gold_news_daily"),
    "entidades_silver_vs_gold": (
        "SELECT count(*) FROM (SELECT DISTINCT news_id, entity_type, entity_key FROM silver_news_entities)",
        "SELECT coalesce(sum(news_count),0) FROM gold_entity_daily"),
    "frases_silver_vs_gold": (
        "SELECT count(*) FROM (SELECT DISTINCT k.news_id, k.key_phrase_norm FROM silver_news_key_phrases k "
        "JOIN silver_news n ON k.news_id = n.news_id)",
        "SELECT coalesce(sum(news_count),0) FROM gold_key_phrase_section_daily"),
}


def _scalar(sql: str) -> int:
    return int(float(run_query(sql)["rows"][0][0]))


def validate(run_id: str = "-") -> dict:
    log = c.get_logger(run_id, "-", "athena_validate")
    results, failures = {}, []
    for name, sql in CHECKS.items():
        v = _scalar(sql)
        results[name] = {"violations": v, "ok": v == 0}
        if v:
            failures.append(name)
    for name, (s_sql, g_sql) in RECONCILE.items():
        s, g = _scalar(s_sql), _scalar(g_sql)
        results[name] = {"silver": s, "gold": g, "ok": s == g}
        if s != g:
            failures.append(name)
    for name, r in results.items():
        (log.info if r["ok"] else log.error)("%s -> %s", name, r)
    if failures:
        raise AssertionError(f"Validaciones fallidas: {failures}")
    return results
