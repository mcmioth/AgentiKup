"""
FastAPI backend per la dashboard OpenCUP.
Serve API REST + file statici del frontend.
Autenticazione via endpoint AgenTik.
"""

import csv
import io
import json
import os
import time
from typing import Optional

import httpx
from fastapi import FastAPI, Query, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from .queries import Database

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")

# --- Configurazione Auth ---
SESSION_SECRET = os.environ.get("SESSION_SECRET", "dev-secret-change-in-production")
AGENTIK_AUTH_URL = os.environ.get("AGENTIK_AUTH_URL", "https://localhost/AgenTik/api/verify-auth.php")
AGENTIK_API_KEY = os.environ.get("AGENTIK_API_KEY", "CHANGE_ME_IN_PRODUCTION")
SESSION_MAX_AGE = 8 * 3600  # 8 ore
VERIFY_SSL = os.environ.get("VERIFY_SSL", "0") == "1"  # False in locale, True in produzione
SESSION_COOKIE = "agentikup_session"

serializer = URLSafeTimedSerializer(SESSION_SECRET)

app = FastAPI(title="OpenCUP Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()


@app.on_event("shutdown")
def shutdown():
    db.close()


# --- Auth helpers ---

PUBLIC_PATHS = {"/api/auth/login", "/login.html", "/favicon.ico"}

def get_session_user(request: Request) -> Optional[dict]:
    """Legge e verifica il cookie di sessione. Ritorna i dati utente o None."""
    cookie = request.cookies.get(SESSION_COOKIE)
    if not cookie:
        return None
    try:
        data = serializer.loads(cookie, max_age=SESSION_MAX_AGE)
        return data
    except (BadSignature, SignatureExpired):
        return None


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Middleware di autenticazione: protegge tutte le rotte tranne quelle pubbliche."""
    path = request.url.path

    # Rotte pubbliche
    if path in PUBLIC_PATHS or path.startswith("/static/"):
        return await call_next(request)

    user = get_session_user(request)

    # API: ritorna 401 JSON
    if path.startswith("/api/") and not user:
        return JSONResponse({"error": "Non autenticato"}, status_code=401)

    # Pagine: redirect a login
    if not user and path in ("/", "/index.html"):
        return RedirectResponse("/login.html", status_code=302)

    # Salva utente nel request state
    request.state.user = user
    return await call_next(request)


# --- Auth Endpoints ---

@app.post("/api/auth/login")
async def login(email: str = Form(...), password: str = Form(...)):
    """Verifica credenziali tramite API AgenTik e crea sessione."""
    try:
        async with httpx.AsyncClient(timeout=10.0, verify=VERIFY_SSL, follow_redirects=True) as client:
            resp = await client.post(
                AGENTIK_AUTH_URL,
                json={"email": email, "password": password},
                headers={"X-API-Key": AGENTIK_API_KEY},
            )
    except httpx.RequestError:
        return JSONResponse(
            {"ok": False, "error": "Servizio di autenticazione non raggiungibile"},
            status_code=502,
        )

    try:
        body = resp.json()
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "Risposta non valida dal servizio di autenticazione"},
            status_code=502,
        )

    if not body.get("ok"):
        error_msg = body.get("error", "Credenziali non valide")
        return JSONResponse({"ok": False, "error": error_msg}, status_code=resp.status_code)

    # Crea sessione
    user_data = body["user"]
    session_value = serializer.dumps(user_data)

    response = JSONResponse({"ok": True, "user": user_data})
    response.set_cookie(
        SESSION_COOKIE,
        session_value,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=os.environ.get("HTTPS", "") == "1",
    )
    return response


@app.post("/api/auth/logout")
async def logout():
    """Invalida sessione cancellando il cookie."""
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE)
    return response


@app.get("/api/auth/me")
async def get_me(request: Request):
    """Ritorna dati utente corrente dalla sessione."""
    user = get_session_user(request)
    if not user:
        return JSONResponse({"error": "Non autenticato"}, status_code=401)
    return {"ok": True, "user": user}


# --- API Endpoints ---

@app.get("/api/stats")
def get_stats():
    """Statistiche pre-aggregate per la dashboard."""
    return db.get_stats()


@app.get("/api/filters/options")
def get_filter_options():
    """Valori distinti per i filtri dropdown."""
    return db.get_filter_options()


def _parse_filters(request: Request) -> dict:
    """Estrae i filtri dai query params."""
    filters = {}
    for key, val in request.query_params.items():
        if key in ("q", "limit", "offset", "sort", "order"):
            continue
        if val:
            if "," in val:
                filters[key] = val.split(",")
            else:
                filters[key] = val
    return filters


@app.get("/api/projects")
def search_projects(
    request: Request,
    q: str = "",
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    sort: Optional[str] = None,
    order: str = "ASC",
):
    """
    Lista progetti paginata con filtri.

    Query params:
    - q: testo di ricerca
    - limit/offset: paginazione
    - sort/order: ordinamento
    - qualsiasi colonna filtro = valore (es. STATO_PROGETTO=ATTIVO)
    """
    filters = _parse_filters(request)
    rows, total = db.search_projects(
        q=q, filters=filters, sort_col=sort, sort_dir=order,
        limit=limit, offset=offset,
    )
    return {
        "data": rows,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@app.get("/api/projects/{cup}/cig")
def get_cig_for_project(cup: str):
    """CIG associati a un CUP."""
    cigs = db.get_cigs_for_cup(cup)
    return {"data": cigs, "total": len(cigs)}


@app.get("/api/projects/{cup}/aggiudicatari")
def get_aggiudicatari_for_project(cup: str):
    """Aggiudicatari di tutti i CIG associati a un CUP."""
    results = db.get_aggiudicatari_for_cup(cup)
    return {"data": results, "total": len(results)}


@app.get("/api/projects/{cup}")
def get_project(cup: str):
    """Dettaglio completo di un progetto per CUP."""
    results = db.get_project_detail(cup)
    if not results:
        return {"error": "Progetto non trovato"}
    return {"data": results}


@app.get("/api/cig/filters/options")
def get_cig_filter_options():
    """Valori distinti per i filtri CIG."""
    return db.get_cig_filter_options()


@app.get("/api/cig/search")
def search_cigs(
    request: Request,
    q: str = "",
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    sort: Optional[str] = None,
    order: str = "ASC",
):
    """Lista CIG paginata con filtri."""
    filters = _parse_filters(request)
    rows, total = db.search_cigs(
        q=q, filters=filters, sort_col=sort, sort_dir=order,
        limit=limit, offset=offset,
    )
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/api/cig/export")
def export_cig_csv(
    request: Request,
    q: str = "",
):
    """Export CSV dei CIG filtrati (max 100k righe)."""
    filters = _parse_filters(request)
    columns, rows = db.export_cigs(q=q, filters=filters)

    def generate():
        output = io.StringIO()
        writer = csv.writer(output, delimiter=";")
        writer.writerow(columns)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in rows:
            writer.writerow(row)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=cig_export.csv"
        },
    )


@app.get("/api/cig/{cig}/aggiudicatari")
def get_cig_aggiudicatari(cig: str):
    """Aggiudicatari associati a un CIG."""
    results = db.get_aggiudicatari_for_cig(cig)
    return {"data": results, "total": len(results)}


@app.get("/api/cig/{cig}")
def get_cig_detail(cig: str):
    """Dettaglio completo di un CIG."""
    results = db.get_cig_detail(cig)
    if not results:
        return {"error": "CIG non trovato"}
    return {"data": results}


@app.get("/api/aggregations/{field}")
def get_aggregation(
    request: Request,
    field: str,
    q: str = "",
):
    """Aggregazione dinamica per un campo specifico."""
    filters = _parse_filters(request)
    return db.get_aggregation(field, filters=filters, q=q)


@app.get("/api/export")
def export_csv(
    request: Request,
    q: str = "",
):
    """Export CSV dei risultati filtrati (max 100k righe)."""
    filters = _parse_filters(request)
    columns, rows = db.export_query(q=q, filters=filters)

    def generate():
        output = io.StringIO()
        writer = csv.writer(output, delimiter=";")
        writer.writerow(columns)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in rows:
            writer.writerow(row)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=opencup_export.csv"
        },
    )


# --- Frontend static files ---

@app.get("/login.html")
def serve_login():
    return FileResponse(os.path.join(FRONTEND_DIR, "login.html"))


@app.get("/")
def serve_index(request: Request):
    # Il middleware gestisce il redirect se non autenticato
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
