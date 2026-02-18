# CS AgentiKup - Dashboard OpenCUP

**Repo**: https://github.com/mcmioth/AgentiKup

Dashboard web per esplorare i dati OpenCUP (Comitato per la programmazione economica) con ~11.5M di progetti e relativi CIG (Codici Identificativi Gara).

## Stack tecnologico

- **Backend**: Python 3.13 + FastAPI + DuckDB (query dirette su Parquet)
- **Frontend**: HTML/CSS/JS vanilla + AG Grid Community 32.3.3
- **Font**: Plus Jakarta Sans (Google Fonts)
- **Dati**: file Parquet compressi ZSTD in `data/`

## Struttura progetto

```
backend/
  main.py          # FastAPI app, API REST, serve frontend statico
  queries.py       # Classe Database con tutte le query DuckDB
frontend/
  index.html       # Layout: header, sidebar filtri, griglia AG Grid, modale dettaglio
  app.js           # Logica frontend: tab switching, filtri, paginazione, modali
  style.css        # Design system con CSS variables (colore primario: #ef9135)
scripts/
  convert_to_parquet.py  # Converte CSV OpenCUP + Localizzazione + Soggetti + CIG in Parquet
data/
  progetti.parquet       # ~11.5M righe, join Progetti+Localizzazione+Soggetti
  cig.parquet            # CIG con dettagli gara (mappatura CIG-CUP + dettagli da zip JSON)
  stats.json             # Statistiche pre-aggregate
```

## Avvio

```bash
bash run.sh
# oppure:
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Prerequisiti: `pip install -r requirements.txt` (fastapi, uvicorn, duckdb, pyarrow)

Se i file Parquet non esistono, eseguire prima: `python scripts/convert_to_parquet.py`

## API endpoints

| Endpoint | Descrizione |
|---|---|
| `GET /api/stats` | Statistiche pre-aggregate (da stats.json) |
| `GET /api/filters/options` | Valori distinti per dropdown filtri progetti |
| `GET /api/projects` | Ricerca progetti paginata (q, filtri, sort, limit/offset) |
| `GET /api/projects/{cup}` | Dettaglio completo progetto |
| `GET /api/projects/{cup}/cig` | CIG associati a un CUP |
| `GET /api/export` | Export CSV progetti filtrati (max 100k) |
| `GET /api/cig/filters/options` | Valori distinti per dropdown filtri CIG |
| `GET /api/cig/search` | Ricerca CIG paginata |
| `GET /api/cig/{cig}` | Dettaglio completo CIG |
| `GET /api/cig/export` | Export CSV CIG filtrati (max 100k) |
| `GET /api/aggregations/{field}` | Aggregazione dinamica per campo |

## Funzionalita frontend

- **Due tab**: Progetti e CIG, con sidebar filtri dedicata per ciascuno
- **Filtri progetti**: CUP, CIG, ha CIG, categoria/sottocategoria soggetto, localizzazione (area geo/regione/provincia/comune), stato, anno, settore, natura, area, categoria/sottosettore/tipologia intervento, strumento programmazione, tipologia CUP, range costo
- **Filtri CIG**: codice CIG/CUP, stato, esito, settore, tipo scelta contraente, anno pubblicazione, provincia, sezione regionale, modalita realizzazione, strumento svolgimento, solo PNRR, solo con dettaglio, range importo
- **Filtri di default** (tab Progetti): Categoria Soggetto = "UNIVERSITA' ED ALTRI ENTI DI ISTRUZIONE", Sottocategoria = "ISTITUTI PUBBLICI DI ISTRUZIONE SCOLASTICA"
- **SearchableSelect**: componente custom per filtri con molti valori (autocomplete con ricerca)
- **Modale dettaglio**: click su riga apre dettaglio completo + tabella CIG associati (per progetti)
- **Paginazione server-side**: 50 risultati per pagina
- **Ordinamento server-side**: click header colonna AG Grid
- **Export CSV**: esporta risultati filtrati (max 100k righe, separatore `;`)
- **Responsive**: sidebar collassabile su mobile

## Convenzioni codice

- Backend: Python, docstring in italiano, query parametrizzate con `?` placeholder
- Frontend: JS vanilla (no framework), naming `camelCase`, costanti `UPPER_CASE`
- CSS: design system con variabili CSS in `:root`, BEM-like class naming
- I filtri usano query params REST (ogni colonna = parametro)
- Colonne mostrate in tabella definite in `DEFAULT_COLUMNS` / `CIG_DEFAULT_COLUMNS`
- Colonne filtrabili definite in `FILTER_COLUMNS` / `CIG_FILTER_COLUMNS`

## Dati sorgente

I file sorgente vanno posizionati nella root del progetto per la conversione:

- `OpenCup_Progetti0.csv` ... `OpenCup_Progetti6.csv` (~7 file, vari GB)
- `OpenCup_Localizzazione.csv` (area geo, regione, provincia, comune per CUP)
- `OpenCup_Soggetti.csv` (categoria/sottocategoria soggetto per PIVA)
- `OpenCup_Fonti_Copertura.csv` (non ancora usato)
- `cup_json/cup_json.json` (mappatura CIG-CUP)
- `cup_json/cig_json_*.zip` (dettagli gara mensili, ~72 file zip)

Tutti i file sorgente sono in `.gitignore` (*.csv, data/).

## Aggiornamento dati

1. Posizionare i CSV nella root del progetto e la cartella `cup_json/` con i JSON/zip
2. Eseguire `python scripts/convert_to_parquet.py`
3. Verificare che `data/progetti.parquet`, `data/cig.parquet` e `data/stats.json` siano stati generati
4. (Opzionale) Eliminare i CSV sorgente per risparmiare spazio

## Note tecniche

- DuckDB legge i Parquet direttamente senza caricarli in memoria (memory_limit 4GB)
- Il frontend serve come file statici da FastAPI (`/static/` -> `frontend/`)
- La conversione CSV->Parquet fa JOIN con Localizzazione e Soggetti e dedup per CUP/PIVA
- I CIG vengono deduplicati per codice CIG tenendo il record piu recente
- Le colonne numeriche (costo, finanziamento) sono stringhe nel Parquet, cast con `TRY_CAST` nelle query
