"""Capa Gold: agregados Parquet particionados por pub_date, derivados SOLO de Silver vigente."""
from __future__ import annotations

import pandas as pd

from . import common as c
from .silver import NEWS_COLS, build_children, load_state, _load_cache, publish_partitioned


def build_gold() -> dict:
    log = c.get_logger("-", "-", "build_gold")
    news = load_state()
    ents, kps = build_children(news, _load_cache())

    news_daily = (news.groupby(["pub_date", "medium", "section"], as_index=False)
                  .agg(news_count=("news_id", "nunique")))

    # news_id -> section para atribuir frases clave a la seccion
    sec = news[["news_id", "section"]]
    ent_daily = (ents.groupby(["pub_date", "entity_type", "entity_key"], as_index=False)
                 .agg(entity_text=("entity_text", "first"), news_count=("news_id", "nunique")))
    ent_daily = ent_daily[["pub_date", "entity_type", "entity_key", "entity_text", "news_count"]]

    kp_sec = kps.merge(sec, on="news_id", how="inner")
    kp_daily = (kp_sec.groupby(["pub_date", "section", "key_phrase_norm"], as_index=False)
                .agg(key_phrase=("key_phrase", "first"), news_count=("news_id", "nunique")))
    kp_daily = kp_daily[["pub_date", "section", "key_phrase_norm", "key_phrase", "news_count"]]

    out = {
        "gold_news_daily": publish_partitioned("gold/news_daily/", news_daily),
        "gold_entity_daily": publish_partitioned("gold/entity_daily/", ent_daily),
        "gold_key_phrase_section_daily": publish_partitioned("gold/key_phrase_section_daily/", kp_daily),
    }
    log.info("Gold publicado %s", out)
    return out
