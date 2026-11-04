"""Capturas reales de la consola AWS. Abre una ventana de Chrome con perfil propio (persistente en .build/),
el usuario inicia sesion manualmente (el script NUNCA escribe credenciales) y luego navega y captura.
Uso: python tools/aws_console_screens.py <salida_dir> nombre=URL ... nombre=athena:<SQL> ...
Las paginas 'athena:' escriben la consulta en el editor de Athena, la ejecutan y capturan el resultado."""
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(sys.argv[1])
PAGES = dict(a.split("=", 1) for a in sys.argv[2:])
PROFILE = Path(__file__).resolve().parents[1] / ".build" / "chrome-aws-profile"
ATHENA = "https://us-east-2.console.aws.amazon.com/athena/home?region=us-east-2#/query-editor"
OUT.mkdir(parents=True, exist_ok=True)


def click_if(pg, selectors, timeout=1500):
    for s in selectors:
        try:
            loc = pg.locator(s).first
            if loc.is_visible(timeout=timeout):
                loc.click()
                pg.wait_for_timeout(600)
                return True
        except Exception:
            pass
    return False


def dismiss(pg):
    # banner de cookies (solo cookies no esenciales del perfil de automatizacion) y modal del workgroup de Athena
    click_if(pg, ["button[data-id='awsccc-cb-btn-decline']:visible", "button:visible:has-text('Rechazar')",
                  "button:visible:has-text('Decline')"])
    for _ in range(2):
        click_if(pg, ["[role=dialog]:visible button:visible:has-text('Aceptar')",
                      "[role=dialog]:visible button:visible:has-text('Acknowledge')",
                      "[role=dialog]:visible :text-is('Aceptar')"])


def goto(pg, url):
    try:
        pg.goto(url)
        pg.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass
    pg.wait_for_timeout(4000)
    dismiss(pg)


def athena(pg, sql):
    goto(pg, ATHENA)
    ed = pg.locator(".ace_text-input, .monaco-editor textarea").first
    pg.locator(".ace_content, .monaco-editor").first.click()
    ed.press("Control+a")
    ed.press("Delete")
    pg.keyboard.insert_text(sql)
    pg.wait_for_timeout(500)
    run = pg.get_by_role("button", name=re.compile(r"^(Ejecutar|Run)")).first
    if run.is_visible():
        run.click()
    else:
        ed.press("Control+Enter")
    pg.wait_for_timeout(4000)
    for _ in range(60):  # esperar a que termine
        txt = pg.locator("body").inner_text()
        busy = any(k in txt for k in ("En ejecución", "En cola", "Running", "Queued"))
        if not busy and any(k in txt for k in ("Completado", "Completed", "Con error", "Failed")):
            break
        pg.wait_for_timeout(1500)
    pg.wait_for_timeout(2000)
    dismiss(pg)


with sync_playwright() as p:
    ctx = p.chromium.launch_persistent_context(str(PROFILE), channel="chrome", headless=False,
                                               viewport={"width": 1600, "height": 1250})
    pg = ctx.pages[0] if ctx.pages else ctx.new_page()
    pg.goto("https://us-east-2.console.aws.amazon.com/console/home?region=us-east-2")
    print("ESPERANDO_LOGIN: inicia sesion en la ventana de Chrome que se abrio", flush=True)
    t0 = time.time()
    nav = "#awsc-nav-header, #awsc-navigation-container, [data-testid='awsc-nav-account-menu-button']"
    while time.time() - t0 < 900:  # hasta 15 min para iniciar sesion
        found = None
        for cand in ctx.pages:  # el usuario puede haber iniciado sesion en otra pestana de esta ventana
            try:
                if "console.aws.amazon.com" in cand.url and cand.locator(nav).count() > 0:
                    found = cand
                    break
            except Exception:
                pass
        if found:
            pg = found
            break
        print("url actual:", [c.url[:90] for c in ctx.pages], flush=True)
        time.sleep(4)
    else:
        sys.exit("no se detecto inicio de sesion")
    print("LOGIN_OK", flush=True)
    pg.wait_for_timeout(3000)
    dismiss(pg)
    for name, target in PAGES.items():
        try:
            if target.startswith("athena:"):
                athena(pg, target[len("athena:"):])
            else:
                goto(pg, target)
            pg.screenshot(path=str(OUT / f"{name}.png"))
            print("ok", name, flush=True)
        except Exception as e:
            print("ERROR", name, type(e).__name__, str(e)[:150], flush=True)
    ctx.close()
