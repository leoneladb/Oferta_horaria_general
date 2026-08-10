"""
Proxy/scraper para la Oferta Académica de UNAJ, reescrito en Python con FastAPI.
Equivalente funcional del server.js original (Express).

Endpoints:
  GET  /api/materias/{carrer_id}
  POST /api/horarios
  POST /api/comments/check   (rate-limit por IP para comentarios anónimos)
  GET  /api/test
"""

import asyncio
import hashlib
import hmac
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="UNAJ Oferta Académica Proxy")

# ---------------- CORS (equivalente a app.use(cors())) ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}

# ==================== RESPALDO EN UPSTASH REDIS (REST API) ====================
# Idea: cada vez que conseguimos datos reales (materias u horarios) desde
# la página oficial, los guardamos en Upstash Redis. Si en un pedido futuro
# la página oficial falla (timeout, caída, cambio de formato, etc.), en vez
# de devolver un array vacío usamos lo último que tengamos guardado para
# esa misma consulta, avisando al frontend con "fromCache": true.
#
# Por qué Redis y no un archivo local: en Vercel las funciones corren en un
# filesystem de solo lectura (salvo /tmp, que es efímero y se borra en cada
# cold start / redeploy). Upstash Redis es un almacenamiento persistente al
# que se accede por HTTP, así que sobrevive a reinicios, redeploys y a que
# haya varias instancias de la función corriendo en paralelo.
#
# Guardamos una key de Redis por entrada (no todo en un JSON gigante), con
# este esquema de nombres:
#   materias:<carrerId>   -> {"data": [...], "updatedAt": "..."}
#   horarios:<hash>       -> {"data": [...], "updatedAt": "..."}
#   period:current        -> {"value": 11,   "updatedAt": "..."}
UPSTASH_REDIS_REST_URL = os.environ.get(
    "UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

_UPSTASH_HEADERS = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}

# ==================== PRESUPUESTO DE TIEMPO PARA INTENTOS EN VIVO ====================
# Problema que resuelve esto: cuando la UNAJ está caída pero "se cuelga"
# (no rechaza la conexión al toque, sino que tarda en responder o no
# responde nunca), la cadena completa de intentos (detectar período +
# paginación + siusync + 5 variantes RSC/HTML) puede tardar mucho más que
# el límite de ejecución de una función serverless en Vercel (10s en el
# plan Hobby). Si Vercel mata la función antes de que lleguemos a leer el
# respaldo de Upstash, el usuario recibe un error o un array vacío en vez
# del respaldo, aunque el respaldo esté perfecto. De ahí el síntoma de "a
# veces carga el respaldo y a veces no": depende de si la UNAJ falla
# rápido (llegamos al respaldo a tiempo) o se cuelga (Vercel corta antes).
#
# La solución es ponerle un techo total a TODOS los intentos en vivo
# combinados con asyncio.wait_for. Si se agota, cancelamos lo que sea que
# esté colgado y pasamos directo al respaldo, garantizando que siempre
# llegamos a esa parte a tiempo.
LIVE_FETCH_BUDGET_SECONDS = float(
    os.environ.get("LIVE_FETCH_BUDGET_SECONDS", "6"))

# ==================== TECHO TOTAL POR REQUEST (fix del "a veces sí, a veces no") ====================
# Problema real encontrado: LIVE_FETCH_BUDGET_SECONDS sólo acotaba el intento
# en vivo, pero backup_get()/backup_set() tenían SU PROPIO timeout fijo de 8s
# aparte, sin descontar lo ya gastado. En el peor caso (intento en vivo que
# tarda los 8s completos + Redis que tarda otros tantos) la función podía
# necesitar hasta 16s, muy por encima del límite de 10s de Vercel (plan
# Hobby sin Fluid Compute). Vercel mataba la función a mitad de camino,
# justo antes de devolver el respaldo, aunque el respaldo estuviera bien
# guardado. De ahí la intermitencia: dependía de si la suma total alcanzaba
# a entrar en los 10s o no.
#
# La solución: UN SOLO deadline total por request (TOTAL_REQUEST_BUDGET),
# del que se va descontando el tiempo ya usado antes de llamar al respaldo.
# Así el presupuesto para Redis se achica automáticamente si el intento en
# vivo ya consumió tiempo, y la suma nunca puede superar el techo total.
#
# Si tu función en Vercel tiene un maxDuration distinto a 10s (por ejemplo
# 60s con Fluid Compute o plan Pro), subí este valor por variable de entorno.
TOTAL_REQUEST_BUDGET_SECONDS = float(
    os.environ.get("TOTAL_REQUEST_BUDGET_SECONDS", "9"))


def _remaining_budget(deadline: float, minimum: float = 0.5, maximum: float = 3.0) -> float:
    """Tiempo que le queda al request antes del deadline total, acotado
    entre `minimum` y `maximum` para no pedirle a Redis un timeout de 0s
    (fallaría siempre) ni uno absurdamente largo."""
    return max(minimum, min(maximum, deadline - time.monotonic()))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Zona horaria usada para decidir "cambió el día". OJO: si esto se
# calculara en UTC, el backup completo se dispararía a las 21hs de
# Argentina (cuando en UTC ya es el día siguiente) en vez de a la
# medianoche real, así que usamos siempre la hora de Buenos Aires.
_ARG_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _today_str() -> str:
    """Fecha de hoy (YYYY-MM-DD) en horario de Argentina."""
    return datetime.now(_ARG_TZ).strftime("%Y-%m-%d")


def _backup_configured() -> bool:
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        print(
            "⚠ UPSTASH_REDIS_REST_URL/TOKEN no configuradas: el respaldo está desactivado.")
        return False
    return True


async def backup_set(section: str, key: str, data: Any, timeout: float = 3.0) -> None:
    """Guarda datos frescos en Redis (sección 'materias' u 'horarios')."""
    if not _backup_configured():
        return
    redis_key = f"{section}:{key}"
    value = json.dumps(
        {"data": data, "updatedAt": _now_iso()}, ensure_ascii=False)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{UPSTASH_REDIS_REST_URL}/set/{redis_key}",
                headers=_UPSTASH_HEADERS,
                content=value.encode("utf-8"),
            )
            resp.raise_for_status()
    except Exception as err:
        # Si Redis falla al GUARDAR no queremos romper la respuesta al
        # usuario: sólo lo logueamos y seguimos.
        print(f"⚠ No se pudo guardar respaldo en Redis ({redis_key}): {err}")


async def backup_get(section: str, key: str, timeout: float = 3.0) -> Optional[dict[str, Any]]:
    """Lee una entrada puntual del respaldo en Redis, o None si no existe/falla."""
    if not _backup_configured():
        return None
    redis_key = f"{section}:{key}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(
                f"{UPSTASH_REDIS_REST_URL}/get/{redis_key}",
                headers=_UPSTASH_HEADERS,
            )
            resp.raise_for_status()
            body = resp.json()
    except Exception as err:
        print(f"⚠ No se pudo leer respaldo en Redis ({redis_key}): {err}")
        return None

    raw_value = body.get("result")
    if raw_value is None:
        return None
    try:
        return json.loads(raw_value)
    except Exception as err:
        print(f"⚠ Respaldo en Redis con formato inválido ({redis_key}): {err}")
        return None


async def backup_set_period(period: int) -> None:
    await backup_set("period", "current", period)


async def backup_get_period() -> Optional[int]:
    entry = await backup_get("period", "current")
    if isinstance(entry, dict):
        return entry.get("data")
    return None


def _build_horarios_cache_key(payload: Any) -> str:
    """
    Genera una clave estable para un pedido de horarios, a partir de
    instituteId/subjectId/careerId (ignora academicPeriodId a propósito,
    así el respaldo sigue siendo válido aunque cambie el período vigente).
    """
    items = payload if isinstance(payload, list) else [payload]
    normalized = sorted(
        (
            item.get("instituteId"),
            item.get("subjectId"),
            item.get("careerId"),
        )
        for item in items
        if isinstance(item, dict)
    )
    raw = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ---------------- DETECCIÓN AUTOMÁTICA DEL PERÍODO ACADÉMICO ----------------
# En vez de hardcodear academicPeriodId (5, 6, 11...) y tener que ir
# cambiándolo a mano cada vez que la UNAJ pasa de período, lo detectamos
# pegándole a la propia página oficial (sin fijar período) y buscando qué
# academicPeriodId aparece en su respuesta. Se cachea un rato para no
# pegarle a la página oficial en cada request de un usuario.
PERIOD_CACHE_TTL_SECONDS = 60 * 60  # 1 hora
_period_cache: dict[str, Any] = {"value": None, "ts": 0.0}


async def _detect_current_period() -> Optional[int]:
    """
    Pega contra la página oficial sin fijar academicPeriodId y junta todas
    las apariciones de "academicPeriodId": N en la respuesta. Asumimos que
    el período vigente es el más alto (los IDs son incrementales), ya que
    la propia página arma sus componentes/props con el período activo.
    """
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
        try:
            resp = await client.get(
                "https://oferta-academica.espacios.unaj.edu.ar/",
                headers={"Accept": "text/x-component",
                         "RSC": "1", **DEFAULT_HEADERS},
            )
            raw = resp.text or ""
        except Exception as err:
            print(f"⚠ Error detectando período académico: {err}")
            return None

    matches = re.findall(r'"?academicPeriodId"?\s*:\s*(\d+)', raw)
    if not matches:
        print("⚠ No se encontró academicPeriodId en la página oficial.")
        return None

    detected = max(int(m) for m in matches)
    print(f"✓ Período académico detectado: {detected}")
    return detected


async def get_current_period(force_refresh: bool = False) -> int:
    """
    Devuelve el academicPeriodId vigente, usando cache en memoria.
    Si la detección falla, reusa el último valor cacheado (aunque esté
    vencido) antes de caer a un valor fijo de emergencia.
    """
    now = time.time()
    cached = _period_cache["value"]
    fresh = cached is not None and (
        now - _period_cache["ts"]) < PERIOD_CACHE_TTL_SECONDS

    if fresh and not force_refresh:
        return cached

    detected = await _detect_current_period()
    if detected is not None:
        _period_cache["value"] = detected
        _period_cache["ts"] = now
        await backup_set_period(detected)
        return detected

    if cached is not None:
        print(f"  → usando valor cacheado en memoria: {cached}")
        return cached

    # Sin detección y sin cache en memoria (p. ej. el server se acaba de
    # reiniciar): probamos con el último valor que quedó guardado en el
    # JSON de respaldo antes de recurrir al valor fijo de emergencia.
    backed_up = await backup_get_period()
    if backed_up is not None:
        print(
            f"  → sin detección ni cache en memoria, usando respaldo JSON: {backed_up}")
        _period_cache["value"] = backed_up
        _period_cache["ts"] = now
        return backed_up

    print("  → sin detección, sin cache ni respaldo, usando valor de emergencia: 11")
    return 11

# ---------------- RATE LIMIT DE COMENTARIOS (en memoria) ----------------
# ADVERTENCIA: esto vive en memoria del proceso. Si corrés varias instancias
# (varios workers, o en un entorno serverless con múltiples réplicas),
# cada una tiene su propio registro y el límite no queda 100% garantizado.
# Para producción real conviene mover esto a Redis o una tabla en la DB.
COMMENT_COOLDOWN_SECONDS = 10
_last_comment_by_ip: dict[str, float] = {}

# ADVERTENCIA DE SEGURIDAD: /api/backup/ensure-daily es pública (la llama
# el frontend sin login) y dispara TODO el proceso pesado de backup
# (scraping en cascada de N carreras + sus horarios). Sin límite, cualquiera
# que conozca la URL puede spamearla y agotar la cuota gratuita de Vercel
# (Function Invocations / Fluid Active CPU) o de Upstash en minutos. Este
# cooldown por IP es una mitigación básica: no reemplaza tener CRON_SECRET
# configurado en /api/backup/cron-trigger, que sigue siendo la vía "oficial".
ENSURE_DAILY_COOLDOWN_SECONDS = float(
    os.environ.get("ENSURE_DAILY_COOLDOWN_SECONDS", "30"))
_last_ensure_daily_by_ip: dict[str, float] = {}


def _get_client_ip(request: Request) -> str:
    # VULNERABILIDAD ORIGINAL: se tomaba el PRIMER valor de X-Forwarded-For
    # (`forwarded.split(",")[0]`). Ese primer valor lo pone el cliente, no
    # el proxy: cualquiera puede mandar "X-Forwarded-For: 1.2.3.4" (o un
    # valor random distinto en cada pedido) y aparentar ser una IP nueva
    # en cada request, esquivando por completo el rate limit por IP de
    # /api/comments/check y /api/backup/ensure-daily.
    #
    # Fix: en Vercel, el proxy/edge agrega la IP real de la conexión al
    # FINAL de la cadena X-Forwarded-For (los valores previos, si los hay,
    # son los que mandó el cliente) y además expone esa misma IP real en
    # X-Real-IP, que el cliente no puede pisar. Preferimos X-Real-IP y,
    # si no está, usamos el ÚLTIMO tramo de X-Forwarded-For en vez del
    # primero.
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        parts = [p.strip() for p in forwarded.split(",") if p.strip()]
        if parts:
            return parts[-1]
    return request.client.host if request.client else "unknown"


def _prune_stale_ip_entries(store: dict[str, float], max_age_seconds: float) -> None:
    """
    VULNERABILIDAD ORIGINAL: `_last_comment_by_ip` y `_last_ensure_daily_by_ip`
    son diccionarios en memoria que nunca se limpian. Como la clave es la
    IP (que además, antes del fix de _get_client_ip, ni siquiera hacía
    falta spoofear con cuidado), cualquiera puede mandar muchísimos
    pedidos con IPs/valores distintos y hacer crecer el diccionario sin
    límite hasta agotar la memoria del proceso (DoS). Se poda antes de
    insertar una entrada nueva, sacando lo que ya venció hace rato.
    """
    if len(store) < 1000:
        return
    now = time.time()
    stale = [ip for ip, ts in store.items() if (now - ts) > max_age_seconds]
    for ip in stale:
        store.pop(ip, None)


# ==================== HELPERS ====================

def extract_balanced_array(text: str, start_index: int) -> Optional[str]:
    """
    Extrae un array balanceado [...] a partir de start_index,
    contando profundidad de corchetes (igual que la versión JS).
    """
    i = start_index
    n = len(text)
    while i < n and text[i] != "[":
        i += 1
    if i >= n:
        return None

    depth = 0
    start = i
    while i < n:
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
        i += 1
    return None


def parse_html_table(raw_html: str) -> list[dict]:
    """
    Parser 'manual' de tabla HTML (fallback), equivalente a parseHtmlTable en JS.
    No usa un parser de DOM real, sino regex, igual que el original.
    """
    if not raw_html or not isinstance(raw_html, str):
        return []

    table_match = re.search(
        r'<table[^>]*class="[^"]*MuiTable-root[^"]*"[^>]*>[\s\S]*?</table>',
        raw_html,
        re.IGNORECASE,
    ) or re.search(r"<table[\s\S]*?</table>", raw_html, re.IGNORECASE)

    if not table_match:
        return []

    table_html = table_match.group(0)
    tr_matches = re.findall(r"<tr[\s\S]*?</tr>", table_html, re.IGNORECASE)

    rows_text = []
    for tr in tr_matches:
        cell_matches = re.findall(
            r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr, re.IGNORECASE)
        cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]*>", "", c)).strip()
                 for c in cell_matches]
        rows_text.append(cells)

    parsed: list[dict] = []
    current_name: Optional[str] = None

    for r in rows_text:
        if not r:
            continue

        first = r[0] if len(r) > 0 else ""

        if first:
            current_name = first
            if len(r) >= 6:
                parsed.append({
                    "name": current_name,
                    "dayTime": r[1] if len(r) > 1 else "",
                    "hours": r[2] if len(r) > 2 else "",
                    "modality": r[3] if len(r) > 3 else "",
                    "periodicity": r[4] if len(r) > 4 else "",
                    "teacher": r[5] if len(r) > 5 else "",
                    "classroom": r[6] if len(r) > 6 else "",
                    "building": r[7] if len(r) > 7 else "",
                    "headquarter": r[8] if len(r) > 8 else "",
                    "observations": r[9] if len(r) > 9 else "",
                })
            continue
        else:
            parsed.append({
                "name": current_name or "-",
                "dayTime": r[0] if len(r) > 0 else "",
                "hours": r[1] if len(r) > 1 else "",
                "modality": r[2] if len(r) > 2 else "",
                "periodicity": r[3] if len(r) > 3 else "",
                "teacher": r[4] if len(r) > 4 else "",
                "classroom": r[5] if len(r) > 5 else "",
                "building": r[6] if len(r) > 6 else "",
                "headquarter": r[7] if len(r) > 7 else "",
                "observations": r[8] if len(r) > 8 else "",
            })

    return parsed


# ==================== RUTA: MATERIAS ====================
def _dedupe_key(item: dict) -> str:
    sid = item.get("subjectId")
    if sid is not None:
        return f"id:{sid}"
    code = (item.get("code") or "").strip().lower()
    name = (item.get("name") or "").strip().lower()
    return f"nc:{code}|{name}"


async def _fetch_materias_live(carrer_id: str) -> Optional[list]:
    """
    Intenta conseguir las materias EN VIVO desde la UNAJ, probando en orden:
    paginación por offset -> siusync directo -> variantes RSC/HTML.
    Devuelve la lista si consiguió algo, o None si ninguna vía funcionó.

    Esta función corre bajo un timeout global (asyncio.wait_for con
    LIVE_FETCH_BUDGET_SECONDS) desde get_materias, así que los timeouts
    de cada request individual son cortos a propósito: no necesitan cubrir
    todo el presupuesto ellos solos, solo evitar que UN request cuelgue
    más de la cuenta dentro del presupuesto total.
    """
    # 1) Intento principal: endpoint real de la página oficial, paginando
    #    con "offset" de a 10 en 10 (confirmado viendo el Network tab:
    #    https://oferta-academica.espacios.unaj.edu.ar/?academicPeriodId=5&
    #    limit=10&sortField=name&sortDirection=asc&carrerId={id}&offset=N).
    #    Antes se pedía una sola vez a la API siusync con Limit=200,
    #    asumiendo que traía todo de un saque, pero siusync ignora ese Limit
    #    y siempre devuelve como máximo 10 materias por página, así que nos
    #    quedábamos cortos (p. ej. faltaba "Física II", "Autómatas y
    #    Lenguajes", etc.). Acá paginamos de verdad hasta agotar resultados.
    all_items: list[dict] = []
    seen_keys: set[str] = set()
    PAGE_SIZE = 10
    MAX_PAGES = 60  # tope de seguridad
    MAX_PAGE_RETRIES = 2  # reintentos por página antes de dar la paginación por rota
    offset = 0
    period = await get_current_period()
    pagination_complete = False  # True solo si llegamos al final de forma "limpia"

    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
        for _ in range(MAX_PAGES):
            items_page = None
            last_err = None

            # Reintentamos la MISMA página unas pocas veces antes de rendirnos:
            # un timeout o 5xx puntual de la UNAJ a mitad de la paginación no
            # debería hacernos cortar con un resultado parcial (eso es lo que
            # causaba el bug de "a veces 500 materias, a veces 100": una
            # página que fallaba a mitad de camino se guardaba como si fuera
            # el listado completo, pisando el respaldo bueno).
            for attempt in range(MAX_PAGE_RETRIES + 1):
                try:
                    resp = await client.get(
                        "https://oferta-academica.espacios.unaj.edu.ar/",
                        params={
                            "academicPeriodId": period,
                            "limit": PAGE_SIZE,
                            "sortField": "name",
                            "sortDirection": "asc",
                            "carrerId": carrer_id,
                            "offset": offset,
                        },
                        headers={
                            "Accept": "text/x-component",
                            "RSC": "1",
                            "User-Agent": "Mozilla/5.0",
                        },
                    )
                    resp.raise_for_status()
                    raw = resp.text or ""

                    m = re.search(r'"items"\s*:\s*(\[[\s\S]*?\])\s*[,}]', raw)
                    if not m:
                        print(
                            f"  → offset={offset}: no se encontró \"items\" en la respuesta.")
                        items_page = []
                        break

                    items_page = json.loads(m.group(1))
                    last_err = None
                    break
                except Exception as err:
                    last_err = err
                    print(f"  ⚠ Error pidiendo offset={offset} "
                          f"(intento {attempt + 1}/{MAX_PAGE_RETRIES + 1}): {err}")

            if last_err is not None:
                # Se agotaron los reintentos para esta página: cortamos, pero
                # marcamos que fue por ERROR, no porque se acabaron los datos.
                # pagination_complete queda en False.
                break

            if not isinstance(items_page, list) or len(items_page) == 0:
                print(
                    f"  → offset={offset}: página vacía, fin de la paginación.")
                pagination_complete = True
                break

            new_count = 0
            for it in items_page:
                key = _dedupe_key(it)
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_items.append(it)
                    new_count += 1

            print(f"  → offset={offset}: {len(items_page)} items recibidos, "
                  f"{new_count} nuevos, total acumulado {len(all_items)}")

            if len(items_page) < PAGE_SIZE or new_count == 0:
                pagination_complete = True
                break

            offset += PAGE_SIZE
        else:
            # Se agotó MAX_PAGES sin llegar a una página corta/vacía: no
            # sabemos con certeza que sea el final real, así que no lo
            # tratamos como completo.
            pagination_complete = False

    if pagination_complete and len(all_items) > 0:
        print(
            f"  ✓✓ TOTAL FINAL: {len(all_items)} materias para carrera {carrer_id}")
        return all_items

    if not pagination_complete and len(all_items) > 0:
        print(f"  ⚠ Paginación cortada por error en offset={offset} con "
              f"{len(all_items)} materias parciales acumuladas: se descartan "
              f"para no pisar un respaldo mejor con datos incompletos.")

    print("  ⚠ Paginación por offset no devolvió nada usable. Probando siusync directo...")

    # 2) FALLBACK: API oficial siusync (comportamiento original, por si acaso)
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(
                "https://siusync.espacios.unaj.edu.ar/api/v1/Subject",
                params={
                    "carrerId": carrer_id,
                    "Limit": 200,
                    "AcademicPeriodId": period,
                    "sortField": "name",
                    "sortDirection": "asc",
                },
                headers=DEFAULT_HEADERS,
            )
            data = resp.json()
            if isinstance(data, dict) and isinstance(data.get("items"), list) and data["items"]:
                return data["items"]
        except Exception as err:
            print(f"  ⚠ Error siusync: {err}")

    # 3) FALLBACK: variantes RSC / HTML sobre la página Next.js
    variants = [
        {"name": "rsc_accept", "headers": {"Accept": "text/x-component",
                                           **DEFAULT_HEADERS}, "qs": f"?carrerId={carrer_id}"},
        {"name": "rsc_accept_rsc1", "headers": {"Accept": "text/x-component",
                                                "RSC": "1", **DEFAULT_HEADERS}, "qs": f"?carrerId={carrer_id}"},
        {"name": "rsc_params", "headers": {"Accept": "text/x-component",
                                           **DEFAULT_HEADERS}, "qs": f"?carrerId={carrer_id}&_rsc=1"},
        {"name": "rsc_full", "headers": {"Accept": "text/x-component", "RSC": "1",
                                         "Next-Router-State-Tree": "[]", **DEFAULT_HEADERS}, "qs": f"?carrerId={carrer_id}&_rsc=1"},
        {"name": "plain_html", "headers": {"Accept": "text/html",
                                           **DEFAULT_HEADERS}, "qs": f"?carrerId={carrer_id}"},
    ]

    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
        for variant in variants:
            try:
                url = f"https://oferta-academica.espacios.unaj.edu.ar/{variant['qs']}"
                resp = await client.get(url, headers=variant["headers"])
                raw = resp.text or ""

                found = None
                m = re.search(
                    r'"subjects"\s*:\s*\{\s*"items"\s*:\s*(\[[\s\S]*?\])', raw)
                if m:
                    found = m.group(1)

                if not found:
                    m = re.search(r'"items"\s*:\s*(\[[\s\S]*?\])', raw)
                    if m:
                        found = m.group(1)

                if not found:
                    arrays = re.findall(r"\[[\s\S]{200,}\]", raw)
                    for a in arrays:
                        try:
                            parsed = json.loads(a)
                            if isinstance(parsed, list) and parsed and (
                                parsed[0].get("subjectId") or parsed[0].get(
                                    "name") or parsed[0].get("code")
                            ):
                                return parsed
                        except Exception:
                            pass

                if found:
                    try:
                        materias = json.loads(found)
                        if isinstance(materias, list) and materias:
                            return materias
                    except Exception:
                        pass

                parsed_from_html = parse_html_table(raw)
                if parsed_from_html:
                    mapped = [
                        {
                            "subjectId": None,
                            "code": None,
                            "name": p.get("name") or p.get("dayTime") or f"Materia {idx + 1}",
                            "institute": {"id": None, "name": None},
                            "carrer": {"id": carrer_id, "name": ""},
                            "instituteId": None,
                            "raw": p,
                        }
                        for idx, p in enumerate(parsed_from_html)
                    ]
                    return mapped
            except Exception:
                pass

    # Ninguna vía en vivo consiguió nada usable.
    return None


@app.get("/api/materias/{carrer_id}")
async def get_materias(carrer_id: str):
    print(f"→ /api/materias/{carrer_id} (inicio)")

    # Deadline total del request: TODO lo que sigue (intento en vivo +
    # lectura de respaldo) tiene que entrar acá, para no depender de que
    # cada paso individual "adivine" bien su propio timeout.
    deadline = time.monotonic() + TOTAL_REQUEST_BUDGET_SECONDS

    try:
        result = await asyncio.wait_for(
            _fetch_materias_live(carrer_id),
            timeout=min(LIVE_FETCH_BUDGET_SECONDS, _remaining_budget(
                deadline, minimum=0.5, maximum=LIVE_FETCH_BUDGET_SECONDS)),
        )
    except Exception as err:
        kind = "timeout" if isinstance(
            err, asyncio.TimeoutError) else "error"
        print(f"  ⚠ {kind} trayendo materias en vivo ({err}), "
              f"se pasa directo al respaldo.")
        result = None

    if result:
        print(
            f"  ✓✓ TOTAL FINAL: {len(result)} materias para carrera {carrer_id}")
        # OJO: en Vercel (serverless) el proceso puede congelarse apenas
        # se devuelve la respuesta HTTP, así que un guardado "en segundo
        # plano" (asyncio.create_task sin esperar) corre el riesgo real de
        # quedar a mitad de hacer y nunca completarse. Por eso se espera
        # acá, pero con un timeout acotado por lo que quede del deadline
        # total, para no arriesgar el límite de tiempo de la función.
        await backup_set("materias", carrer_id, result, timeout=_remaining_budget(deadline))
        return result

    # ÚLTIMA OPCIÓN: si la página oficial no devolvió nada usable por
    # ninguna vía (o se agotó el presupuesto de tiempo), usamos el último
    # resultado bueno que hayamos guardado en el respaldo de Upstash.
    # El timeout de esta lectura se achica solo según cuánto del deadline
    # total ya se consumió arriba: así la suma nunca puede superar
    # TOTAL_REQUEST_BUDGET_SECONDS, sin importar qué tan lento haya sido
    # el intento en vivo.
    backed_up = await backup_get("materias", carrer_id, timeout=_remaining_budget(deadline))
    if backed_up:
        print(f"  → usando respaldo (Upstash) para carrera {carrer_id} "
              f"(guardado el {backed_up['updatedAt']})")
        return {
            "items": backed_up["data"],
            "fromCache": True,
            "cachedAt": backed_up["updatedAt"],
        }

    print("  ❌ Ninguna variante extrajo materias y no hay respaldo. Enviando [] como fallback.")
    return []

# ==================== RUTA: HORARIOS ====================


async def _fetch_horarios_live(payload: Any) -> list:
    """
    Intenta conseguir los horarios/comisiones EN VIVO desde la UNAJ.
    Devuelve la lista (puede ser [] si la materia legítimamente no tiene
    comisiones). Deja que cualquier excepción (de red, parseo, etc.) se
    propague — el llamador decide qué hacer con el respaldo.

    Corre bajo un timeout global (asyncio.wait_for con
    LIVE_FETCH_BUDGET_SECONDS) desde get_horarios.
    """
    # Pisamos el academicPeriodId que venga del frontend con el detectado
    # automáticamente, así el HTML nunca necesita saber cuál es el período
    # vigente ni hace falta tocarlo a mano.
    period = await get_current_period()
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                item["academicPeriodId"] = period
    elif isinstance(payload, dict):
        payload["academicPeriodId"] = period

    # IMPORTANTE: mantené este hash actualizado si la web original lo cambia
    NEXT_ACTION = "4089e22bca8943bcf018b9b5d8177263d5f601e6dd"

    async with httpx.AsyncClient(timeout=6.0) as client:
        resp = await client.post(
            "https://oferta-academica.espacios.unaj.edu.ar/",
            headers={
                "Accept": "text/x-component",
                "Content-Type": "text/plain;charset=UTF-8",
                "Next-Action": NEXT_ACTION,
                "User-Agent": "Mozilla/5.0",
                "Origin": "https://oferta-academica.espacios.unaj.edu.ar",
                "Referer": "https://oferta-academica.espacios.unaj.edu.ar/",
            },
            content=json.dumps(payload),
        )

    raw = resp.text or ""
    commissions_json = None

    items_index = raw.find('"items"')
    if items_index != -1:
        start = raw.find("[", items_index)
        if start != -1:
            extracted = extract_balanced_array(raw, start)
            if extracted:
                try:
                    parsed = json.loads(extracted)
                    if isinstance(parsed, list):
                        commissions_json = parsed
                except Exception:
                    pass

    if commissions_json is None:
        m = re.search(r'"commissions"\s*:\s*(\[[\s\S]*?\])', raw)
        if m:
            try:
                commissions_json = json.loads(m.group(1))
            except Exception:
                pass

    if commissions_json is None:
        parsed_from_html = parse_html_table(raw)
        if parsed_from_html:
            commissions_json = [
                {
                    "name": item.get("name"),
                    "day": item.get("dayTime"),
                    "time": item.get("hours"),
                    "teacherName": item.get("teacher"),
                    "classroomName": item.get("classroom"),
                    "buildingName": item.get("building"),
                    "headquarterName": item.get("headquarter"),
                    "observations": item.get("observations"),
                    # Este fallback (parseo manual de tabla HTML) no
                    # tiene forma de saber si el aula está confirmada o
                    # no. En vez de dejar el campo ausente (lo que el
                    # frontend interpreta como "sí está asignada" y
                    # muestra el aula), declaramos explícitamente que
                    # no sabemos, para que se muestre "Aula sin
                    # asignar" en lugar de arriesgar un dato incorrecto.
                    "assignament": {"id": None, "description": "No asignada"},
                    "raw": item,
                }
                for item in parsed_from_html
            ]

    if not isinstance(commissions_json, list):
        commissions_json = []

    return commissions_json


@app.post("/api/horarios")
async def get_horarios(request: Request):
    payload = await request.json()

    # Clave estable para este pedido (institute+subject+career), usada
    # tanto para guardar como para buscar en el respaldo de Upstash.
    cache_key = _build_horarios_cache_key(payload)
    deadline = time.monotonic() + TOTAL_REQUEST_BUDGET_SECONDS

    try:
        commissions_json = await asyncio.wait_for(
            _fetch_horarios_live(payload),
            timeout=min(LIVE_FETCH_BUDGET_SECONDS, _remaining_budget(
                deadline, minimum=0.5, maximum=LIVE_FETCH_BUDGET_SECONDS)),
        )
    except Exception as error:
        kind = "timeout" if isinstance(
            error, asyncio.TimeoutError) else "error"
        print(
            f"❌ {kind} en /api/horarios ({error}), se intentará usar respaldo.")
        backed_up = await backup_get("horarios", cache_key, timeout=_remaining_budget(deadline))
        if backed_up:
            print(
                f"  → usando respaldo (Upstash) (guardado el {backed_up['updatedAt']})")
            return {
                "items": backed_up["data"],
                "fromCache": True,
                "cachedAt": backed_up["updatedAt"],
            }
        return JSONResponse(status_code=500, content={"error": "Error interno extrayendo horarios y no hay respaldo disponible"})

    if commissions_json:
        # Sólo pisamos el respaldo cuando conseguimos datos reales;
        # un resultado vacío legítimo (materia sin comisiones) no
        # borra lo último bueno que teníamos guardado.
        await backup_set("horarios", cache_key, commissions_json, timeout=_remaining_budget(deadline))
        # Devolvemos el JSON original completo (sin aplanar)
        return commissions_json

    # commissions_json vino vacío SIN que _fetch_horarios_live tirara
    # excepción/timeout (p. ej. la UNAJ respondió pero cambió el formato,
    # el Next-Action quedó desactualizado, o hubo un glitch de parseo).
    # Antes esto se devolvía tal cual como [] y el frontend mostraba
    # "Sin comisiones disponibles" aunque hubiera respaldo bueno guardado.
    # Consultamos el respaldo también en este caso, no sólo cuando hay
    # excepción, y sólo lo usamos si tiene datos (si el respaldo también
    # está vacío o no existe, ahí sí es un [] legítimo).
    backed_up = await backup_get("horarios", cache_key, timeout=_remaining_budget(deadline))
    if backed_up and backed_up.get("data"):
        print(
            f"  → fetch en vivo devolvió vacío, usando respaldo (Upstash) "
            f"(guardado el {backed_up['updatedAt']})")
        return {
            "items": backed_up["data"],
            "fromCache": True,
            "cachedAt": backed_up["updatedAt"],
        }

    return commissions_json


# ==================== BACKUP COMPLETO POR CAMBIO DE FECHA ====================
# Objetivo: no depender de que cada usuario haya entrado justo a la
# carrera/materia puntual que se quiere respaldar (backup "parcial" que
# ya existe en get_materias/get_horarios). En cambio: la PRIMERA vez que
# alguien entra a la página en un día distinto al último backup completo,
# se vuelve a pedir en vivo TODO: las materias de cada carrera Y los
# horarios de cada una de esas materias (los horarios cambian de un día
# para el otro — aula, comisión, docente — así que no alcanza con
# respaldar sólo la lista de materias).
#
# La lista de carreras no está hardcodeada acá: la manda el frontend en
# el body (carrerIds), porque el frontend ya la tiene (es la misma lista
# que usa para armar el selector de carreras) y así nunca se desincroniza
# si agregás/sacás una carrera de un solo lado. Los horarios a respaldar
# SÍ se calculan acá: salen de las materias que se acaban de traer para
# cada carrera (cada materia trae su subjectId/instituteId, con eso se
# arma el mismo payload que usa /api/horarios).
#
# LÍMITE DURO DEL PLAN HOBBY DE VERCEL: 10 segundos por invocación, sin
# excepción (a menos que actives Fluid Compute). Como ahora el trabajo
# total es "carreras × materias por carrera", en un backend con muchas
# materias esto NO entra en un solo pedido ni loco. Por eso todo corre en
# TANDAS con dos colas persistidas en Redis:
#   1) pendingCarreras  -> carreras a las que hay que pedirles su lista
#                          de materias (y de ahí sacar qué horarios pedir).
#   2) pendingHorarios  -> horarios puntuales (por materia) a pedir.
# Cada visita procesa lo que entra en FULL_BACKUP_TOTAL_BUDGET_SECONDS y
# corta ahí, guardando qué quedó pendiente. La PRÓXIMA visita del mismo
# día retoma exactamente donde quedó (primero termina las carreras que
# faltan, después sigue con los horarios), y además se auto-dispara sola
# para la siguiente tanda (ver _trigger_next_backup_link más abajo), así
# NO depende de que alguien vuelva a entrar a la página para terminar la
# ronda completa del día — es el precio de que NUNCA se pase de 10s pase
# lo que pase, en vez de arriesgarse a que Vercel mate la función a
# mitad de camino sin ninguna garantía de qué alcanzó a guardarse.
FULL_BACKUP_CONCURRENCY = int(os.environ.get("FULL_BACKUP_CONCURRENCY", "3"))
FULL_BACKUP_PER_ITEM_TIMEOUT = float(
    os.environ.get("FULL_BACKUP_PER_ITEM_TIMEOUT", "5"))


def _extract_horario_payload(item: Any, carrer_id: str) -> Optional[dict]:
    """
    A partir de una materia (tal como la devuelve _fetch_materias_live),
    arma el payload {instituteId, subjectId, careerId} que necesita
    /api/horarios para pedir los horarios de ESA materia puntual.

    Devuelve None si a la materia le falta el subjectId o el
    instituteId (por ejemplo, las que vinieron del parseo manual de
    tabla HTML como último recurso, que no tienen esos IDs) — esas no se
    pueden respaldar porque no hay forma de pedir sus horarios sin ellos.
    El careerId NO se saca del item: usamos directamente el carrer_id que
    ya sabemos que estamos recorriendo, para no depender de con qué
    nombre de campo venga (si es que viene) dentro del item.
    """
    if not isinstance(item, dict):
        return None
    subject_id = item.get("subjectId")
    institute_id = item.get("instituteId")
    if institute_id is None and isinstance(item.get("institute"), dict):
        institute_id = item["institute"].get("id")
    if subject_id is None or institute_id is None:
        return None
    return {"instituteId": institute_id, "subjectId": subject_id, "careerId": carrer_id}


async def _backup_one_carrera(cid: str) -> tuple[str, list[dict]]:
    """Pide en vivo las materias de UNA carrera, las guarda, y devuelve
    también la lista de payloads de horarios que hay que respaldar a
    partir de esas materias. Nunca tira excepción hacia afuera."""
    try:
        items = await asyncio.wait_for(
            _fetch_materias_live(cid), timeout=FULL_BACKUP_PER_ITEM_TIMEOUT
        )
    except Exception as err:
        return f"error: {err}", []
    if not items:
        # No pisamos el respaldo anterior si esta vez no conseguimos
        # nada (backup_set ya protege esto solo).
        return "vacío (se conserva respaldo anterior)", []
    await backup_set("materias", cid, items, timeout=3.0)
    horario_payloads = []
    for it in items:
        payload = _extract_horario_payload(it, cid)
        if payload:
            horario_payloads.append(payload)
    return f"ok ({len(items)} materias)", horario_payloads


async def _backup_one_horario(payload: dict) -> str:
    """Pide en vivo los horarios/comisiones de UNA materia puntual y los
    guarda en Redis, en la MISMA key que ya usa /api/horarios (mismo
    _build_horarios_cache_key), así ambos caminos comparten el respaldo."""
    try:
        commissions = await asyncio.wait_for(
            _fetch_horarios_live(dict(payload)),
            timeout=FULL_BACKUP_PER_ITEM_TIMEOUT,
        )
    except Exception as err:
        return f"error: {err}"
    if isinstance(commissions, list) and commissions:
        cache_key = _build_horarios_cache_key(payload)
        await backup_set("horarios", cache_key, commissions, timeout=3.0)
        return f"ok ({len(commissions)} comisiones)"
    # Vacío legítimo (materia sin comisiones) o glitch puntual: no
    # pisamos un respaldo anterior bueno con nada.
    return "vacío (se conserva respaldo anterior)"


async def _run_full_backup_batch(
    pending_carreras: list[str],
    pending_horarios: list[dict],
    deadline: float,
) -> tuple[dict[str, str], list[str], list[dict], dict[str, str]]:
    """
    Fase 1: drena `pending_carreras` (materias por carrera) hasta
    agotarla o quedarse sin tiempo. Cada carrera que termina bien agrega
    sus materias como nuevos pendientes de horarios (deduplicados por
    clave de cache).
    Fase 2: con el tiempo que quede después de la fase 1, drena
    `pending_horarios`.
    Nunca cruza `deadline` (tiempo absoluto de time.monotonic()).
    Devuelve (resultados de materias, carreras que quedaron pendientes,
    horarios que quedaron pendientes, resultados de horarios).
    """
    materias_results: dict[str, str] = {}
    horarios_results: dict[str, str] = {}

    remaining_carreras = list(pending_carreras)
    all_horarios = list(pending_horarios)
    seen_horario_keys = {_build_horarios_cache_key(p) for p in all_horarios}

    while remaining_carreras and time.monotonic() < deadline:
        chunk = remaining_carreras[:FULL_BACKUP_CONCURRENCY]
        chunk_results = await asyncio.gather(
            *(_backup_one_carrera(cid) for cid in chunk)
        )
        for cid, (res, new_payloads) in zip(chunk, chunk_results):
            materias_results[cid] = res
            for p in new_payloads:
                key = _build_horarios_cache_key(p)
                if key not in seen_horario_keys:
                    seen_horario_keys.add(key)
                    all_horarios.append(p)
        remaining_carreras = remaining_carreras[len(chunk):]

    remaining_horarios = list(all_horarios)
    while remaining_horarios and time.monotonic() < deadline:
        chunk = remaining_horarios[:FULL_BACKUP_CONCURRENCY]
        chunk_results = await asyncio.gather(
            *(_backup_one_horario(p) for p in chunk)
        )
        for p, res in zip(chunk, chunk_results):
            horarios_results[_build_horarios_cache_key(p)] = res
        remaining_horarios = remaining_horarios[len(chunk):]

    return materias_results, remaining_carreras, remaining_horarios, horarios_results


FULL_BACKUP_TOTAL_BUDGET_SECONDS = float(
    os.environ.get("FULL_BACKUP_TOTAL_BUDGET_SECONDS", "6.5"))
FULL_BACKUP_CHAIN_LINK_TIMEOUT_SECONDS = float(
    os.environ.get("FULL_BACKUP_CHAIN_LINK_TIMEOUT_SECONDS", "1.5"))
FULL_BACKUP_MAX_CHAIN_LINKS_PER_DAY = int(
    os.environ.get("FULL_BACKUP_MAX_CHAIN_LINKS_PER_DAY", "80"))
# Lista de carreras para cuando dispara el CRON (no hay "frontend" que
# mande el body ahí). Se configura una vez como variable de entorno en
# Vercel, separada por comas: BACKUP_CARRER_IDS=1,2,3,4,5
BACKUP_CARRER_IDS_ENV = os.environ.get("BACKUP_CARRER_IDS", "")
CRON_SECRET = os.environ.get("CRON_SECRET", "")


async def _trigger_next_backup_link(base_url: str, carrer_ids: list[str]) -> None:
    """
    Dispara el SIGUIENTE eslabón de la cadena de backup completo: un
    pedido a esta misma ruta, para que arranque la próxima tanda de
    trabajo. A propósito NO esperamos a que esa próxima tanda termine
    (usamos un timeout corto y lo ignoramos si salta): en Vercel cada
    pedido HTTP corre como una invocación de función aparte, así que en
    cuanto el pedido sale, la próxima tanda ya está corriendo sola e
    independiente, sin importar si nosotros seguimos esperando su
    respuesta o no. Si esperáramos la respuesta completa, terminaríamos
    encadenando todo dentro de una sola invocación y volveríamos a
    chocar con el límite de 10s de Vercel.
    """
    url = base_url.rstrip("/") + "/api/backup/ensure-daily"
    try:
        async with httpx.AsyncClient(timeout=FULL_BACKUP_CHAIN_LINK_TIMEOUT_SECONDS) as client:
            await client.post(url, json={"carrerIds": carrer_ids})
        print("  → siguiente eslabón de la cadena confirmado (respondió antes del timeout corto).")
    except Exception:
        # Lo esperado la mayoría de las veces: nos quedamos sin tiempo
        # de espera ANTES de que el próximo eslabón termine de
        # responder. Eso es justo el comportamiento buscado: el pedido
        # ya salió y esa próxima invocación sigue corriendo sola.
        print("  → siguiente eslabón de la cadena disparado (no se esperó su respuesta completa, a propósito).")


async def _ensure_daily_backup_core(carrer_ids: list[str], base_url: str) -> dict:
    """
    Lógica compartida entre el endpoint que llama el frontend (con
    carrerIds en el body) y el endpoint que dispara el cron de Vercel
    (sin body, con carrerIds fijos por variable de entorno). Hace UNA
    tanda de trabajo (~FULL_BACKUP_TOTAL_BUDGET_SECONDS) y, si queda
    algo pendiente, se auto-dispara para la siguiente tanda antes de
    devolver la respuesta — así la ronda del día se termina sola, sin
    depender de que alguien vuelva a entrar a la página ni de que el
    cron vuelva a disparar (el cron en Hobby sólo corre una vez al día,
    pero esta cadena sigue sola las veces que haga falta ese mismo día).
    """
    today = _today_str()
    deadline = time.monotonic() + FULL_BACKUP_TOTAL_BUDGET_SECONDS

    progress_entry = await backup_get("backup", "fullBackupProgress")
    progress = progress_entry.get("data") if isinstance(
        progress_entry, dict) else None

    if isinstance(progress, dict) and progress.get("date") == today:
        pending_carreras = list(progress.get("pendingCarreras") or [])
        done_carreras = list(progress.get("doneCarreras") or [])
        pending_horarios = list(progress.get("pendingHorarios") or [])
        done_horarios_count = int(progress.get("doneHorariosCount") or 0)
        chain_links_today = int(progress.get("chainLinksToday") or 0)
        if not pending_carreras and not pending_horarios:
            return {"ranFullBackup": False, "reason": "ya se completó hoy", "date": today}
    else:
        # Fecha distinta a la última vez (o nunca se guardó nada):
        # arrancamos de cero con TODAS las carreras recibidas.
        pending_carreras = list(carrer_ids)
        done_carreras = []
        pending_horarios = []
        done_horarios_count = 0
        chain_links_today = 0

    print(
        f"→ Backup completo ({today}), eslabón #{chain_links_today + 1}: "
        f"{len(pending_carreras)} carreras y {len(pending_horarios)} horarios pendientes."
    )
    materias_results, remaining_carreras, remaining_horarios, horarios_results = (
        await _run_full_backup_batch(pending_carreras, pending_horarios, deadline)
    )

    processed_carreras = [
        c for c in pending_carreras if c not in remaining_carreras]
    done_carreras = done_carreras + processed_carreras
    done_horarios_count += len(horarios_results)
    chain_links_today += 1
    finished = not remaining_carreras and not remaining_horarios

    await backup_set(
        "backup",
        "fullBackupProgress",
        {
            "date": today,
            "pendingCarreras": remaining_carreras,
            "doneCarreras": done_carreras,
            "pendingHorarios": remaining_horarios,
            "doneHorariosCount": done_horarios_count,
            "chainLinksToday": chain_links_today,
        },
        timeout=3.0,
    )

    print(
        f"  → eslabón #{chain_links_today}: {len(processed_carreras)} carreras y "
        f"{len(horarios_results)} horarios procesados; quedan "
        f"{len(remaining_carreras)} carreras y {len(remaining_horarios)} horarios "
        f"({'COMPLETO' if finished else 'sigue encadenando solo'})."
    )

    if not finished:
        if chain_links_today < FULL_BACKUP_MAX_CHAIN_LINKS_PER_DAY:
            await _trigger_next_backup_link(base_url, carrer_ids)
        else:
            # Freno de seguridad: si esto se disparó, algo no anda bien
            # (por ejemplo la UNAJ caída de forma sostenida), no una
            # cantidad normal de materias. Mejor cortar la cadena acá y
            # que retome la próxima visita real o el cron de mañana, que
            # seguir encadenando sin límite.
            print(
                f"  ⚠ Se alcanzó el máximo de eslabones encadenados hoy "
                f"({FULL_BACKUP_MAX_CHAIN_LINKS_PER_DAY}); freno de seguridad activado. "
                f"Queda pendiente para la próxima visita real o el próximo cron."
            )

    return {
        "ranFullBackup": True,
        "date": today,
        "completedToday": finished,
        "chainLinksToday": chain_links_today,
        "processedThisCall": {
            "carreras": len(processed_carreras),
            "horarios": len(horarios_results),
        },
        "totalDone": {
            "carreras": len(done_carreras),
            "horarios": done_horarios_count,
        },
        "totalPending": {
            "carreras": len(remaining_carreras),
            "horarios": len(remaining_horarios),
        },
        "results": {
            "materias": materias_results,
            "horarios": horarios_results,
        },
    }


@app.post("/api/backup/ensure-daily")
async def ensure_daily_backup(request: Request):
    """
    Pensada para que el FRONTEND la llame una sola vez cuando alguien
    entra a la página (por ejemplo en el layout raíz, sin bloquear el
    render). Body esperado (JSON): {"carrerIds": ["1", "2", "3", ...]}
    (la lista completa de carreras que ya tiene el propio frontend).

    Ya NO hace falta que alguien se quede en la página ni que vuelva a
    entrar para que termine: si después de esta tanda queda trabajo
    pendiente, esta misma ruta se vuelve a disparar sola (ver
    _ensure_daily_backup_core) hasta terminar TODA la ronda del día,
    encadenando tandas de forma automática.
    """
    # ---- Mitigación de abuso: rate limit por IP ----
    # Ver advertencia junto a ENSURE_DAILY_COOLDOWN_SECONDS más arriba.
    ip = _get_client_ip(request)
    now = time.time()
    last = _last_ensure_daily_by_ip.get(ip)
    if last is not None and (now - last) < ENSURE_DAILY_COOLDOWN_SECONDS:
        remaining = round(ENSURE_DAILY_COOLDOWN_SECONDS - (now - last), 1)
        return JSONResponse(
            status_code=429,
            content={"error": "Demasiados pedidos, esperá un momento.",
                     "waitSeconds": remaining},
        )
    _prune_stale_ip_entries(
        _last_ensure_daily_by_ip, ENSURE_DAILY_COOLDOWN_SECONDS * 10)
    _last_ensure_daily_by_ip[ip] = now

    body = await request.json()
    carrer_ids = [str(c)
                  for c in (body.get("carrerIds") or []) if c is not None]

    if not carrer_ids:
        return JSONResponse(
            status_code=400,
            content={
                "error": "Falta carrerIds (lista de IDs de carrera) en el body."},
        )

    # Tope duro de cantidad: nunca deberías tener más carreras que las que
    # ya conocés de antemano (BACKUP_CARRER_IDS). Si alguien manda una
    # lista inventada gigante, la cortamos acá en vez de dejar que dispare
    # cientos de pedidos en cascada a la UNAJ.
    MAX_CARRER_IDS_PER_CALL = 50
    if len(carrer_ids) > MAX_CARRER_IDS_PER_CALL:
        carrer_ids = carrer_ids[:MAX_CARRER_IDS_PER_CALL]

    result = await _ensure_daily_backup_core(carrer_ids, str(request.base_url))
    return result


@app.get("/api/backup/cron-trigger")
async def cron_trigger_backup(request: Request):
    """
    Ruta pensada para un Vercel Cron Job (ver vercel.json), NO para el
    frontend. El cron manda un GET simple sin body, así que acá la
    lista de carreras sale de la variable de entorno BACKUP_CARRER_IDS
    (separada por comas) en vez de venir en el pedido. Con esto, el
    backup completo arranca SOLO todos los días a la hora que
    configures en el cron, sin depender de que entre ningún visitante
    real — y una vez que arranca, se auto-encadena igual que si lo
    hubiera disparado el frontend, hasta terminar la ronda completa.

    Seguridad: si configuraste la variable de entorno CRON_SECRET en
    Vercel, esta ruta exige que el pedido traiga
    "Authorization: Bearer <CRON_SECRET>" (Vercel se lo manda solo a
    sus propios cron jobs). Si no configuraste CRON_SECRET, la ruta
    igual funciona, pero cualquiera que conozca la URL podría
    dispararla a mano — se recomienda configurarlo.
    """
    if CRON_SECRET:
        auth_header = request.headers.get("authorization", "")
        expected = f"Bearer {CRON_SECRET}"
        # Comparación en tiempo constante: usar "!=" filtra por timing
        # cuántos caracteres iniciales coinciden, lo que en teoría permite
        # adivinar el secreto carácter a carácter.
        if not hmac.compare_digest(auth_header, expected):
            return JSONResponse(status_code=401, content={"error": "No autorizado."})

    carrer_ids = [c.strip()
                  for c in BACKUP_CARRER_IDS_ENV.split(",") if c.strip()]
    if not carrer_ids:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Falta configurar la variable de entorno BACKUP_CARRER_IDS "
                         "(lista de IDs de carrera separada por comas) en Vercel."
            },
        )

    print(
        f"→ Cron diario disparó el backup completo ({len(carrer_ids)} carreras configuradas).")
    result = await _ensure_daily_backup_core(carrer_ids, str(request.base_url))
    return result


# ==================== RUTA: RATE LIMIT DE COMENTARIOS ====================

@app.post("/api/comments/check")
async def check_comment_rate_limit(request: Request):
    """
    El frontend llama a esto ANTES de escribir un comentario en Firebase.
    Si la misma IP ya comentó hace menos de COMMENT_COOLDOWN_SECONDS,
    devuelve 429 con los segundos restantes. Si está permitido, registra
    el timestamp y devuelve 200.
    """
    ip = _get_client_ip(request)
    now = time.time()

    last = _last_comment_by_ip.get(ip)
    if last is not None:
        elapsed = now - last
        if elapsed < COMMENT_COOLDOWN_SECONDS:
            remaining = round(COMMENT_COOLDOWN_SECONDS - elapsed, 1)
            return JSONResponse(
                status_code=429,
                content={"allowed": False, "waitSeconds": remaining},
            )

    _prune_stale_ip_entries(_last_comment_by_ip, COMMENT_COOLDOWN_SECONDS * 10)
    _last_comment_by_ip[ip] = now
    return {"allowed": True}


# ==================== RUTA: PERÍODO ACADÉMICO ====================

@app.get("/api/period")
async def get_period(refresh: bool = False):
    """
    Devuelve el academicPeriodId que el backend está usando actualmente.
    Pasar ?refresh=true fuerza volver a detectarlo contra la página oficial
    (ignorando el cache) — útil para chequear a mano si cambió.
    """
    period = await get_current_period(force_refresh=refresh)
    return {"academicPeriodId": period, "cachedAt": _period_cache["ts"]}


# ==================== RUTA: TEST ====================

@app.get("/api/test")
async def test():
    return "SERVIDOR FUNCIONANDO (FastAPI)"


# ==================== RUTA: ESTADO DEL RESPALDO ====================

@app.get("/api/backup-status")
async def backup_status():
    """
    Info de diagnóstico: qué hay guardado en el respaldo de Upstash Redis
    (carreras con materias guardadas, cantidad de combos de horarios
    guardados, y período académico respaldado). No expone los datos
    completos, sólo un resumen, para no volver la respuesta gigante.
    """
    if not _backup_configured():
        return {
            "configured": False,
            "detail": "Faltan las variables de entorno UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN.",
        }

    async def _keys(pattern: str) -> list[str]:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    f"{UPSTASH_REDIS_REST_URL}/keys/{pattern}",
                    headers=_UPSTASH_HEADERS,
                )
                resp.raise_for_status()
                return resp.json().get("result", []) or []
        except Exception as err:
            print(f"⚠ Error listando keys de Redis ({pattern}): {err}")
            return []

    materias_keys = await _keys("materias:*")
    horarios_keys = await _keys("horarios:*")

    materias_resumen = {}
    for k in materias_keys:
        carrer_id = k.split(":", 1)[1] if ":" in k else k
        entry = await backup_get("materias", carrer_id)
        if entry:
            materias_resumen[carrer_id] = {
                "cantidad": len(entry.get("data", []) or []),
                "updatedAt": entry.get("updatedAt"),
            }

    period_entry = await backup_get("period", "current")
    full_backup_progress_entry = await backup_get("backup", "fullBackupProgress")
    full_backup_progress = (
        full_backup_progress_entry.get("data")
        if isinstance(full_backup_progress_entry, dict)
        else None
    )

    return {
        "configured": True,
        "materias": materias_resumen,
        "horariosGuardados": len(horarios_keys),
        "period": period_entry,
        "fullBackupProgress": full_backup_progress,
    }
