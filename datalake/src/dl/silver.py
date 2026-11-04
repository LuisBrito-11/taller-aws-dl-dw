"""Capa Silver: parseo, limpieza, validacion, deduplicacion, cuarentena, enriquecimiento y publicacion."""
from __future__ import annotations

import html
import json
import re
import xml.etree.ElementTree as ET
from datetime import timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import pandas as pd

from . import common as c
from .ingest import MALFORMED, OK, list_manifests

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

NEWS_COLS = ["news_id", "source_id", "medium", "section", "guid", "link", "title", "description",
             "pub_date_raw", "pub_date_utc", "pub_date", "text_hash", "content_hash", "version",
             "first_seen_run_id", "last_run_id", "bronze_run_id", "bronze_key", "ingested_at_utc"]


# ------------------------------------------------------------------ limpieza
def clean_html(text: str | None) -> str:
    if not text:
        return ""
    return WS_RE.sub(" ", html.unescape(TAG_RE.sub(" ", html.unescape(text)))).strip()


def parse_pub_date(raw: str | None):
    """Normaliza a UTC; devuelve None si no es parseable. El valor original se conserva aparte."""
    if not raw or not raw.strip():
        return None
    try:
        dt = parsedate_to_datetime(raw.strip())
    except (TypeError, ValueError):
        try:
            dt = pd.Timestamp(raw.strip()).to_pydatetime()
        except (ValueError, TypeError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def valid_link(link: str | None) -> bool:
    if not link:
        return False
    u = urlparse(link.strip())
    return u.scheme in ("http", "https") and bool(u.netloc)


def make_news_id(guid: str | None, link: str | None) -> str | None:
    """ID estable: GUID si existe; si no, enlace sin query/fragmento. Limitacion: si el medio cambia
    el GUID/enlace de una misma noticia se vera como una noticia nueva."""
    if guid and guid.strip():
        return c.sha256_text("guid:" + guid.strip())[:32]
    if valid_link(link):
        u = urlparse(link.strip())
        return c.sha256_text("link:" + f"{u.netloc.lower()}{u.path}".rstrip("/"))[:32]
    return None


def text_for_analysis(title: str, description: str) -> str:
    return f"{title}. {description}".strip(". ").strip() if description else title


# ------------------------------------------------------------------ parseo
def parse_feed(body: bytes, manifest: dict) -> tuple[list[dict], list[dict]]:
    """Devuelve (validos, rechazados). Cada rechazo lleva motivo y trazabilidad a Bronze."""
    base = {"source_id": manifest["source_id"], "medium": manifest["medium"], "section": manifest["section"],
            "bronze_run_id": manifest["run_id"], "bronze_key": manifest["s3_key"],
            "ingested_at_utc": manifest["ingested_at_utc"], "ingestion_date": manifest["ingestion_date"]}
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        return [], [base | {"reject_reason": f"malformed_xml: {e}", "raw_item": None}]

    valid, rejected = [], []
    for idx, item in enumerate(root.iter("item")):
        def g(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else None
        raw = {"guid": g("guid"), "link": g("link"), "title": g("title"), "description": g("description"),
               "pubDate": g("pubDate"), "item_index": idx}
        title, desc = clean_html(raw["title"]), clean_html(raw["description"])
        dt = parse_pub_date(raw["pubDate"])
        reasons = []
        if not title:
            reasons.append("missing_title")
        if not raw["link"]:
            reasons.append("missing_link")
        elif not valid_link(raw["link"]):
            reasons.append("invalid_link")
        if not raw["pubDate"]:
            reasons.append("missing_pub_date")
        elif dt is None:
            reasons.append("invalid_pub_date")
        elif dt > c.utcnow() + timedelta(days=1):
            reasons.append("future_pub_date")
        news_id = make_news_id(raw["guid"], raw["link"])
        if news_id is None:
            reasons.append("no_identifier")
        if reasons:
            rejected.append(base | {"reject_reason": ";".join(reasons), "raw_item": json.dumps(raw, ensure_ascii=False)})
            continue
        link = raw["link"].strip()
        text_hash = c.sha256_text(text_for_analysis(title, desc))
        content_hash = c.sha256_text("|".join([title, desc, link, dt.isoformat()]))
        valid.append(base | {
            "news_id": news_id, "guid": raw["guid"], "link": link, "title": title, "description": desc,
            "pub_date_raw": raw["pubDate"], "pub_date_utc": dt, "pub_date": dt.strftime("%Y-%m-%d"),
            "text_hash": text_hash, "content_hash": content_hash})
    return valid, rejected


# ------------------------------------------------------------------ estado Silver (vigente)
def load_state() -> pd.DataFrame:
    if c.key_exists(c.STATE_NEWS):
        return c.get_parquet(c.STATE_NEWS)
    return pd.DataFrame(columns=NEWS_COLS)


def save_state(df: pd.DataFrame) -> None:
    c.put_parquet(c.STATE_NEWS, df[NEWS_COLS].sort_values("news_id").reset_index(drop=True))


def process_manifest(manifest: dict, state: dict[str, dict], run_id: str, log) -> dict:
    """Aplica un lote Bronze/fuente al estado vigente (dict news_id -> fila). Devuelve conteos."""
    counts = {"new": 0, "unchanged": 0, "updated": 0, "rejected": 0, "duplicate_in_batch": 0, "stale_ignored": 0}
    body = c.get_bytes(manifest["s3_key"])
    valid, rejected = parse_feed(body, manifest)

    seen: dict[str, dict] = {}
    for row in valid:  # duplicados dentro del lote: se conserva la ultima aparicion
        if row["news_id"] in seen:
            counts["duplicate_in_batch"] += 1
        seen[row["news_id"]] = row

    for nid, row in seen.items():
        cur = state.get(nid)
        if cur is None:
            state[nid] = row | {"version": 1, "first_seen_run_id": run_id, "last_run_id": run_id}
            counts["new"] += 1
        elif cur["content_hash"] == row["content_hash"]:
            counts["unchanged"] += 1
        elif str(row["ingested_at_utc"]) < str(cur["ingested_at_utc"]):
            # reprocesar un lote VIEJO no debe revertir una version mas reciente ya vigente
            counts["stale_ignored"] += 1
        else:
            state[nid] = row | {"version": int(cur["version"]) + 1,
                                "first_seen_run_id": cur["first_seen_run_id"], "last_run_id": run_id}
            counts["updated"] += 1

    if rejected:
        counts["rejected"] = len(rejected)
        q = pd.DataFrame(rejected)
        q["quarantined_run_id"] = run_id
        key = (f"silver/quarantine/ingestion_date={manifest['ingestion_date']}/"
               f"run_id={manifest['run_id']}/{manifest['source_id']}.parquet")
        c.put_parquet(key, q.drop(columns=["ingestion_date"]))  # clave deterministica: reproceso la sobrescribe
    log.info("lote procesado %s", counts)
    return counts


def transform(run_ids: list[str], rebuild: bool = False) -> dict:
    """Bronze -> estado Silver vigente. Idempotente: reprocesar un lote ya aplicado deja todo 'unchanged'."""
    log = c.get_logger(",".join(run_ids) if len(run_ids) < 4 else f"{len(run_ids)}-runs", "-", "silver_transform")
    if rebuild:
        log.warning("REBUILD: se descarta el estado vigente y se reconstruye desde todos los lotes Bronze")
        run_ids = sorted({k.split("/run_id=")[1].split("/")[0] for k in c.list_keys("bronze/")
                          if k.endswith("manifest.json")})
        c.delete_keys(c.list_keys("silver/quarantine/"))
        state: dict[str, dict] = {}
    else:
        cur = load_state()
        state = {r["news_id"]: r for r in cur.to_dict("records")}

    # orden cronologico para que 'vigente' sea la version mas reciente
    plan = []
    for rid in run_ids:
        for m in list_manifests(rid):
            # malformed_xml tambien se procesa: sus filas van a cuarentena con motivo y traza a Bronze
            if m["status"] in (OK, MALFORMED) and m.get("s3_key"):
                plan.append(m)
    plan.sort(key=lambda m: (m["ingested_at_utc"], m["source_id"]))

    summary = {}
    for m in plan:
        mlog = c.get_logger(m["run_id"], m["source_id"], "silver_transform")
        counts = process_manifest(m, state, m["run_id"], mlog)
        bkey = c.batch_state_key(m["run_id"], m["source_id"])
        prev = c.get_json(bkey) if c.key_exists(bkey) else {"history": []}
        prev["history"].append({"processed_at_utc": c.iso(c.utcnow()), "counts": counts})
        prev.update({"run_id": m["run_id"], "source_id": m["source_id"], "bronze_key": m["s3_key"],
                     "last_counts": counts, "status": "processed"})
        c.put_json(bkey, prev)
        summary[f"{m['run_id']}/{m['source_id']}"] = counts

    df = pd.DataFrame(list(state.values()), columns=NEWS_COLS) if state else pd.DataFrame(columns=NEWS_COLS)
    if len(df):
        df["pub_date_utc"] = pd.to_datetime(df["pub_date_utc"], utc=True)
        df["version"] = df["version"].astype("int64")
    save_state(df)
    log.info("estado vigente: %d noticias", len(df))
    return {"current_news": len(df), "batches": summary}


# ------------------------------------------------------------------ enriquecimiento (Comprehend)
def _load_cache() -> pd.DataFrame:
    cols = ["text_hash", "text_analyzed", "language", "language_score", "status", "truncated",
            "entities_json", "key_phrases_json", "analyzed_at_utc"]
    if c.key_exists(c.STATE_ENRICH):
        return c.get_parquet(c.STATE_ENRICH)
    return pd.DataFrame(columns=cols)


def _truncate_utf8(text: str, max_bytes: int) -> tuple[str, bool]:
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text, False
    return raw[:max_bytes].decode("utf-8", errors="ignore"), True


def analyze_text(text: str, cfg: dict, comprehend) -> dict:
    """Llama a Comprehend para UN texto. Manejo explicito de vacio, extenso e idioma no soportado."""
    if not text or not text.strip():
        return {"status": "skipped_empty_text", "language": None, "language_score": None, "truncated": False,
                "entities": [], "key_phrases": [], "text_analyzed": ""}
    text, truncated = _truncate_utf8(text, cfg["max_text_bytes"])
    langs = comprehend.detect_dominant_language(Text=text)["Languages"]
    lang = max(langs, key=lambda x: x["Score"]) if langs else None
    if not lang or lang["LanguageCode"] not in cfg["supported_languages"]:
        return {"status": "skipped_unsupported_language", "language": lang["LanguageCode"] if lang else None,
                "language_score": lang["Score"] if lang else None, "truncated": truncated,
                "entities": [], "key_phrases": [], "text_analyzed": text}
    code = lang["LanguageCode"]
    ents = comprehend.detect_entities(Text=text, LanguageCode=code)["Entities"]
    phrases = comprehend.detect_key_phrases(Text=text, LanguageCode=code)["KeyPhrases"]
    entities = [{"text": e["Text"], "type": e["Type"], "score": round(e["Score"], 4)}
                for e in ents if e["Type"] in cfg["entity_types"] and e["Score"] >= cfg["min_score"]]
    key_phrases = [{"text": k["Text"], "score": round(k["Score"], 4)} for k in phrases if k["Score"] >= cfg["min_score"]]
    return {"status": "analyzed", "language": code, "language_score": lang["Score"], "truncated": truncated,
            "entities": entities, "key_phrases": key_phrases, "text_analyzed": text}


def enrich() -> dict:
    """Enriquece solo los text_hash vigentes que NO estan en la cache (evita re-llamadas a Comprehend)."""
    log = c.get_logger("-", "-", "enrich")
    cfg = c.load_config()["comprehend"]
    news = load_state()
    cache = _load_cache()
    cached = set(cache["text_hash"])
    pending = news.drop_duplicates("text_hash")
    pending = pending[~pending["text_hash"].isin(cached)]
    log.info("textos vigentes=%d, en cache=%d, a analizar=%d", news["text_hash"].nunique(), len(cached), len(pending))

    comprehend = c.client("comprehend")
    rows = []
    for r in pending.itertuples():
        res = analyze_text(text_for_analysis(r.title, r.description), cfg, comprehend)
        rows.append({"text_hash": r.text_hash, "text_analyzed": res["text_analyzed"], "language": res["language"],
                     "language_score": res["language_score"], "status": res["status"], "truncated": res["truncated"],
                     "entities_json": json.dumps(res["entities"], ensure_ascii=False),
                     "key_phrases_json": json.dumps(res["key_phrases"], ensure_ascii=False),
                     "analyzed_at_utc": c.iso(c.utcnow())})
    if rows:
        cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
        c.put_parquet(c.STATE_ENRICH, cache)
    stats = pd.Series([r["status"] for r in rows]).value_counts().to_dict() if rows else {}
    log.info("Comprehend: %d textos nuevos analizados %s (cache total=%d)", len(rows), stats, len(cache))
    return {"comprehend_calls_texts": len(rows), "by_status": stats, "cache_size": len(cache)}


# ------------------------------------------------------------------ publicacion Silver (Parquet particionado)
def build_children(news: pd.DataFrame, cache: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Una fila por entidad (o frase) DISTINTA por noticia vigente."""
    by_hash = cache.set_index("text_hash")
    ent_rows, kp_rows = [], []
    for r in news.itertuples():
        if r.text_hash not in by_hash.index:
            continue
        e = by_hash.loc[r.text_hash]
        meta = {"news_id": r.news_id, "pub_date": r.pub_date, "language": e["language"],
                "text_hash": r.text_hash, "analyzed_at_utc": e["analyzed_at_utc"]}
        ents: dict[tuple, dict] = {}
        for x in json.loads(e["entities_json"]):
            k = (x["type"], x["text"].strip().lower())
            d = ents.setdefault(k, {"entity_type": x["type"], "entity_text": x["text"].strip(),
                                    "entity_key": k[1], "confidence_score": x["score"], "mentions": 0})
            d["confidence_score"] = max(d["confidence_score"], x["score"])
            d["mentions"] += 1
        ent_rows += [meta | d for d in ents.values()]
        phr: dict[str, dict] = {}
        for x in json.loads(e["key_phrases_json"]):
            k = x["text"].strip().lower()
            d = phr.setdefault(k, {"key_phrase": x["text"].strip(), "key_phrase_norm": k,
                                   "confidence_score": x["score"], "mentions": 0})
            d["confidence_score"] = max(d["confidence_score"], x["score"])
            d["mentions"] += 1
        kp_rows += [meta | d for d in phr.values()]
    ecols = ["news_id", "pub_date", "language", "text_hash", "analyzed_at_utc", "entity_type", "entity_text",
             "entity_key", "confidence_score", "mentions"]
    kcols = ["news_id", "pub_date", "language", "text_hash", "analyzed_at_utc", "key_phrase", "key_phrase_norm",
             "confidence_score", "mentions"]
    return pd.DataFrame(ent_rows, columns=ecols), pd.DataFrame(kp_rows, columns=kcols)


def publish_partitioned(dataset_prefix: str, df: pd.DataFrame, part_col: str = "pub_date") -> dict:
    """Reescribe el dataset completo con UN objeto determinista por particion (PUT atomico) y luego
    elimina particiones obsoletas. Reejecutar produce exactamente el mismo conjunto de objetos:
    nunca hay archivos duplicados ni parciales visibles para Athena."""
    written = set()
    for part, grp in df.groupby(part_col):
        key = f"{dataset_prefix}{part_col}={part}/data.parquet"
        c.put_parquet(key, grp.drop(columns=[part_col]).reset_index(drop=True))
        written.add(key)
    stale = [k for k in c.list_keys(dataset_prefix) if k not in written]
    if stale:
        c.delete_keys(stale)
    return {"partitions": len(written), "rows": int(len(df)), "stale_deleted": len(stale)}


def publish_silver() -> dict:
    log = c.get_logger("-", "-", "publish_silver")
    news = load_state()
    cache = _load_cache()
    ents, kps = build_children(news, cache)
    pub = news.copy()
    pub["pub_date_utc"] = pd.to_datetime(pub["pub_date_utc"], utc=True)
    out = {
        "news": publish_partitioned("silver/news/", pub[[x for x in NEWS_COLS]]),
        "news_entities": publish_partitioned("silver/news_entities/", ents),
        "news_key_phrases": publish_partitioned("silver/news_key_phrases/", kps),
    }
    log.info("Silver publicado %s", out)
    return out
