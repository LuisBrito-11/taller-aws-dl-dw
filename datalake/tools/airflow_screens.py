"""Capturas reales de la UI de Airflow con Chrome automatizado (Playwright).
Uso: AIRFLOW_URL=http://<ip>:8080 AIRFLOW_PASSWORD=... python tools/airflow_screens.py <salida_dir> [pagina=ruta ...]"""
import os
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = os.environ["AIRFLOW_URL"].rstrip("/")
OUT = Path(sys.argv[1])
PAGES = dict(a.split("=", 1) for a in sys.argv[2:])

OUT.mkdir(parents=True, exist_ok=True)
with sync_playwright() as p:
    b = p.chromium.launch(channel="chrome", headless=True)
    pg = b.new_page(viewport={"width": 1600, "height": 1250}, device_scale_factor=1)
    pg.goto(f"{BASE}/login/")
    pg.fill("#username", "admin")
    pg.fill("#password", os.environ["AIRFLOW_PASSWORD"])
    pg.click("input[type=submit], button[type=submit]")
    pg.wait_for_load_state("networkidle")
    for name, path in PAGES.items():
        pg.goto(f"{BASE}{path}")
        pg.wait_for_load_state("networkidle")
        pg.wait_for_timeout(3500)
        for grp in pg.get_by_text("Post task execution logs").all():  # expandir el traceback/errores
            try:
                grp.click()
            except Exception:
                pass
        pg.wait_for_timeout(800)
        pg.screenshot(path=str(OUT / f"{name}.png"), full_page=False)
        print("ok", name)
    b.close()

