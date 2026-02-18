"""
FastAPI backend per la dashboard OpenCUP.
Serve API REST + file statici del frontend.
"""

import csv
import io
import os
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from .queries import Database

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")

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

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
