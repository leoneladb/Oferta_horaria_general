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
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

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

# ==================== RESPALDO EN UN ÚNICO JSON LOCAL ====================
# Idea: cada vez que conseguimos datos reales (materias u horarios) desde
# la página oficial, los guardamos acá. Si en un pedido futuro la página
# oficial falla (timeout, caída, cambio de formato, etc.), en vez de
# devolver un array vacío usamos lo último que tengamos guardado para esa
# misma consulta, avisando al frontend con "fromCache": true.
#
# Todo vive en UN solo archivo JSON (BACKUP_FILE), con esta forma:
# {
#   "materias": { "<carrerId>": {"data": [...], "updatedAt": "..."} },
#   "horarios": { "<hash>":     {"data": [...], "updatedAt": "..."} },
#   "period":   { "value": 11, "updatedAt": "..." }
# }
BACKUP_FILE = os.environ.get(
    "BACKUP_FILE",
    os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "backup_data.json"),
)

# Lock para que dos requests concurrentes no pisen el archivo al mismo tiempo.
_backup_lock = asyncio.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_backup_file() -> dict[str, Any]:
    """Lectura sincrónica y tolerante a fallos del JSON de respaldo."""
    if not os.path.exists(BACKUP_FILE):
        return {"materias": {}, "horarios": {}, "period": None}
    try:
        with open(BACKUP_FILE, "r", encoding="utf-8") as f:
            content = json.load(f)
            content.setdefault("materias", {})
            content.setdefault("horarios", {})
            content.setdefault("period", None)
            return content
    except Exception as err:
        print(f"⚠ No se pudo leer el respaldo local ({BACKUP_FILE}): {err}")
        return {"materias": {}, "horarios": {}, "period": None}


def _write_backup_file(content: dict[str, Any]) -> None:
    """Escritura atómica (archivo temporal + rename) para no corromper el JSON."""
    try:
        tmp_path = f"{BACKUP_FILE}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, BACKUP_FILE)
    except Exception as err:
        print(
            f"⚠ No se pudo escribir el respaldo local ({BACKUP_FILE}): {err}")


async def backup_set(section: str, key: str, data: Any) -> None:
    """Guarda datos frescos en el respaldo local (sección 'materias' u 'horarios')."""
    async with _backup_lock:
        content = _read_backup_file()
        content.setdefault(section, {})
        content[section][key] = {"data": data, "updatedAt": _now_iso()}
        _write_backup_file(content)


async def backup_get(section: str, key: str) -> Optional[dict[str, Any]]:
    """Lee una entrada puntual del respaldo local, o None si no existe."""
    async with _backup_lock:
        content = _read_backup_file()
        return content.get(section, {}).get(key)


async def backup_set_period(period: int) -> None:
    async with _backup_lock:
        content = _read_backup_file()
        content["period"] = {"value": period, "updatedAt": _now_iso()}
        _write_backup_file(content)


async def backup_get_period() -> Optional[int]:
    async with _backup_lock:
        content = _read_backup_file()
        period_entry = content.get("period")
        if isinstance(period_entry, dict):
            return period_entry.get("value")
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
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
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


def _get_client_ip(request: Request) -> str:
    # Si el server corre detrás de un proxy/CDN (ej. Vercel), la IP real
    # suele venir en X-Forwarded-For. Si no está, usamos la IP de conexión directa.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


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


@app.get("/api/materias/{carrer_id}")
async def get_materias(carrer_id: str):
    print(f"→ /api/materias/{carrer_id} (inicio)")

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
    offset = 0
    period = await get_current_period()

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for _ in range(MAX_PAGES):
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
            except Exception as err:
                print(f"  ⚠ Error pidiendo offset={offset}: {err}")
                break

            m = re.search(r'"items"\s*:\s*(\[[\s\S]*?\])\s*[,}]', raw)
            if not m:
                print(
                    f"  → offset={offset}: no se encontró \"items\" en la respuesta.")
                break

            try:
                items_page = json.loads(m.group(1))
            except Exception as e:
                print(f"  ⚠ No se pudo parsear items en offset={offset}: {e}")
                break

            if not isinstance(items_page, list) or len(items_page) == 0:
                print(
                    f"  → offset={offset}: página vacía, fin de la paginación.")
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
                break

            offset += PAGE_SIZE

    if len(all_items) > 0:
        print(
            f"  ✓✓ TOTAL FINAL: {len(all_items)} materias para carrera {carrer_id}")
        await backup_set("materias", carrer_id, all_items)
        return all_items

    print("  ⚠ Paginación por offset no devolvió nada. Probando siusync directo...")

    # 2) FALLBACK: API oficial siusync (comportamiento original, por si acaso)
    async with httpx.AsyncClient(timeout=12.0) as client:
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
                await backup_set("materias", carrer_id, data["items"])
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

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
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
                                await backup_set("materias", carrer_id, parsed)
                                return parsed
                        except Exception:
                            pass

                if found:
                    try:
                        materias = json.loads(found)
                        if isinstance(materias, list) and materias:
                            await backup_set("materias", carrer_id, materias)
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
                    await backup_set("materias", carrer_id, mapped)
                    return mapped
            except Exception:
                pass

    # 4) ÚLTIMA OPCIÓN: si la página oficial no devolvió nada usable por
    #    ninguna vía, usamos el último resultado bueno que hayamos guardado
    #    en el JSON de respaldo local para esta carrera.
    backed_up = await backup_get("materias", carrer_id)
    if backed_up:
        print(f"  → usando respaldo JSON local para carrera {carrer_id} "
              f"(guardado el {backed_up['updatedAt']})")
        return {
            "items": backed_up["data"],
            "fromCache": True,
            "cachedAt": backed_up["updatedAt"],
        }

    print("  ❌ Ninguna variante extrajo materias y no hay respaldo. Enviando [] como fallback.")
    return []

# ==================== RUTA: HORARIOS ====================


@app.post("/api/horarios")
async def get_horarios(request: Request):
    payload = await request.json()

    # Clave estable para este pedido (institute+subject+career), usada
    # tanto para guardar como para buscar en el respaldo JSON local.
    cache_key = _build_horarios_cache_key(payload)

    try:
        # Pisamos el academicPeriodId que venga del frontend con el
        # detectado automáticamente, así el HTML nunca necesita saber
        # cuál es el período vigente ni hace falta tocarlo a mano.
        period = await get_current_period()
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    item["academicPeriodId"] = period
        elif isinstance(payload, dict):
            payload["academicPeriodId"] = period

        # IMPORTANTE: mantené este hash actualizado si la web original lo cambia
        NEXT_ACTION = "4089e22bca8943bcf018b9b5d8177263d5f601e6dd"

        async with httpx.AsyncClient(timeout=20.0) as client:
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
                        "raw": item,
                    }
                    for item in parsed_from_html
                ]

        if not isinstance(commissions_json, list):
            commissions_json = []

        if commissions_json:
            # Sólo pisamos el respaldo cuando conseguimos datos reales;
            # un resultado vacío legítimo (materia sin comisiones) no
            # borra lo último bueno que teníamos guardado.
            await backup_set("horarios", cache_key, commissions_json)

        # Devolvemos el JSON original completo (sin aplanar)
        return commissions_json

    except Exception as error:
        print(f"❌ Error en /api/horarios, se intentará usar respaldo: {error}")
        backed_up = await backup_get("horarios", cache_key)
        if backed_up:
            print(
                f"  → usando respaldo JSON local (guardado el {backed_up['updatedAt']})")
            return {
                "items": backed_up["data"],
                "fromCache": True,
                "cachedAt": backed_up["updatedAt"],
            }
        return JSONResponse(status_code=500, content={"error": "Error interno extrayendo horarios y no hay respaldo disponible"})


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
    Info de diagnóstico: qué hay guardado en el JSON de respaldo local
    (carreras con materias guardadas, cantidad de combos de horarios
    guardados, y período académico respaldado). No expone los datos
    completos, sólo un resumen, para no volver la respuesta gigante.
    """
    async with _backup_lock:
        content = _read_backup_file()

    materias = content.get("materias", {})
    horarios = content.get("horarios", {})
    period = content.get("period")

    return {
        "backupFile": BACKUP_FILE,
        "materias": {
            carrer_id: {
                "cantidad": len(entry.get("data", [])),
                "updatedAt": entry.get("updatedAt"),
            }
            for carrer_id, entry in materias.items()
        },
        "horariosGuardados": len(horarios),
        "period": period,
    }
