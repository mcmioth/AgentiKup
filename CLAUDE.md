# CS AgentiKup - Dashboard OpenCUP

**Repo**: https://github.com/mcmioth/AgentiKup
**Produzione**: http://72.62.93.58 (Hostinger VPS KVM 2, Ubuntu 24.04)

Dashboard web per esplorare i dati OpenCUP (Comitato per la programmazione economica) con ~11.5M di progetti e relativi CIG (Codici Identificativi Gara).

## Stack tecnologico

- **Backend**: Python 3.13 + FastAPI + DuckDB (query dirette su Parquet)
- **Frontend**: HTML/CSS/JS vanilla + AG Grid Community 32.3.3
- **Font**: Plus Jakarta Sans (Google Fonts)
- **Dati**: file Parquet compressi ZSTD in `data/`
- **Auth**: sessione cookie firmata (itsdangerous), credenziali verificate via API AgenTik (PHP)
- **Dipendenze extra**: httpx, itsdangerous, python-multipart

## Struttura progetto

```
backend/
  main.py          # FastAPI app, API REST, auth middleware, serve frontend statico
  queries.py       # Classe Database con tutte le query DuckDB
frontend/
  index.html       # Layout: header con logo e greeting, sidebar filtri, griglia AG Grid, modale dettaglio
  login.html       # Pagina di login stile AgenTik (logo grande, card con bordo arancione, footer CampuStore)
  app.js           # Logica frontend: tab switching, filtri, paginazione, modali, logout
  style.css        # Design system con CSS variables (colore primario: #ef9135)
  agentikup-logo.svg    # Logo AgentiKup (header e login)
  logo-campustore.svg   # Logo CampuStore (footer login)
  favicon.ico/svg/png   # Favicon set generato dal logo (ico 16/32/48, svg, png 16/32/96, apple-touch-icon 180, PWA 192/512)
scripts/
  convert_to_parquet.py  # Converte CSV OpenCUP + Localizzazione + Soggetti + CIG in Parquet
data/
  progetti.parquet       # ~11.5M righe, join Progetti+Localizzazione+Soggetti
  cig.parquet            # CIG con dettagli gara + aggiudicazioni (mappatura CIG-CUP + dettagli + aggiudicazioni da zip JSON)
  aggiudicatari.parquet  # Aggiudicatari per CIG (1:N, denominazione, CF, ruolo, tipo soggetto)
  stats.json             # Statistiche pre-aggregate
```

## Avvio

```bash
bash run.sh
# oppure:
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Prerequisiti: `pip install -r requirements.txt` (fastapi, uvicorn, duckdb, pyarrow, httpx, itsdangerous, python-multipart)

Se i file Parquet non esistono, eseguire prima: `python scripts/convert_to_parquet.py`

## API endpoints

| Endpoint | Descrizione |
|---|---|
| `POST /api/auth/login` | Login (email+password → verifica via AgenTik API → cookie sessione) |
| `POST /api/auth/logout` | Logout (cancella cookie sessione) |
| `GET /api/auth/me` | Dati utente corrente dalla sessione |
| `GET /api/stats` | Statistiche pre-aggregate (da stats.json) |
| `GET /api/filters/options` | Valori distinti per dropdown filtri progetti |
| `GET /api/projects` | Ricerca progetti paginata (q, filtri, sort, limit/offset) |
| `GET /api/projects/{cup}` | Dettaglio completo progetto |
| `GET /api/projects/{cup}/cig` | CIG associati a un CUP |
| `GET /api/export` | Export CSV progetti filtrati (max 100k) |
| `GET /api/cig/filters/options` | Valori distinti per dropdown filtri CIG |
| `GET /api/cig/search` | Ricerca CIG paginata |
| `GET /api/cig/{cig}` | Dettaglio completo CIG |
| `GET /api/cig/{cig}/aggiudicatari` | Aggiudicatari associati a un CIG |
| `GET /api/cig/export` | Export CSV CIG filtrati (max 100k) |
| `GET /api/aggregations/{field}` | Aggregazione dinamica per campo |

## Funzionalita frontend

- **Due tab**: Progetti e CIG, con sidebar filtri dedicata per ciascuno
- **Filtri progetti**: CUP, CIG, ha CIG, categoria/sottocategoria soggetto, localizzazione (area geo/regione/provincia/comune), stato, anno, settore, natura, area, categoria/sottosettore/tipologia intervento, strumento programmazione, tipologia CUP, range costo
- **Filtri CIG**: codice CIG/CUP, stato, esito, settore, tipo scelta contraente, anno pubblicazione, provincia, sezione regionale, modalita realizzazione, strumento svolgimento, criterio aggiudicazione, prestazioni comprese, solo PNRR, subappalto, solo con dettaglio, range importo
- **Filtri di default** (tab Progetti): Categoria Soggetto = "UNIVERSITA' ED ALTRI ENTI DI ISTRUZIONE", Sottocategoria = "ISTITUTI PUBBLICI DI ISTRUZIONE SCOLASTICA"
- **SearchableSelect**: componente custom per filtri con molti valori (autocomplete con ricerca)
- **Modale dettaglio**: click su riga apre dettaglio completo + tabella CIG associati (per progetti) + dati aggiudicazione e tabella aggiudicatari (per CIG)
- **Paginazione server-side**: 50 risultati per pagina
- **Ordinamento server-side**: click header colonna AG Grid
- **Export CSV**: esporta risultati filtrati (max 100k righe, separatore `;`)
- **Responsive**: sidebar collassabile su mobile
- **Header**: logo AgentiKup, tab CUP/CIG, greeting "Ciao, Nome" + pulsante "Esci"
- **Login**: pagina dedicata `/login.html` stile AgenTik (logo grande, card con bordo arancione 4px, uppercase labels, shadow arancione su focus, footer CampuStore con copyright)
- **Favicon**: set completo generato dal logo SVG (ico, svg, png multi-size, apple-touch-icon, icone PWA)
- **Nessuna barra di ricerca generica**: si usano esclusivamente i filtri nella sidebar sinistra

## Convenzioni codice

- Backend: Python, docstring in italiano, query parametrizzate con `?` placeholder
- Frontend: JS vanilla (no framework), naming `camelCase`, costanti `UPPER_CASE`
- CSS: design system con variabili CSS in `:root`, BEM-like class naming
- I filtri usano query params REST (ogni colonna = parametro)
- Colonne mostrate in tabella definite in `DEFAULT_COLUMNS` / `CIG_DEFAULT_COLUMNS`
- Colonne filtrabili definite in `FILTER_COLUMNS` / `CIG_FILTER_COLUMNS`
- Ordine colonne CUP: CUP, Anno, Sogg. Titolare, Descrizione, Stato, Costo, ...
- Ordine colonne CIG: CIG, CUP, Anno, Amm. Appaltante, Oggetto Gara, Importo, ...

## Dati sorgente

I file sorgente vanno posizionati nella root del progetto per la conversione:

- `OpenCup_Progetti0.csv` ... `OpenCup_Progetti6.csv` (~7 file, vari GB)
- `OpenCup_Localizzazione.csv` (area geo, regione, provincia, comune per CUP)
- `OpenCup_Soggetti.csv` (categoria/sottocategoria soggetto per PIVA)
- `OpenCup_Fonti_Copertura.csv` (non ancora usato)
- `cup_json/cup_json.json` (mappatura CIG-CUP)
- `cup_json/cig_json_*.zip` (dettagli gara mensili, ~72 file zip)
- `cup_json/*-aggiudicazioni_json.zip` (esiti aggiudicazione per CIG, mensili)
- `cup_json/*-aggiudicatari_json.zip` (aggiudicatari per CIG, mensili)

Tutti i file sorgente sono in `.gitignore` (*.csv, data/).

## Aggiornamento dati

1. Posizionare i CSV nella root del progetto e la cartella `cup_json/` con i JSON/zip (inclusi aggiudicazioni e aggiudicatari)
2. Eseguire `python scripts/convert_to_parquet.py`
3. Verificare che `data/progetti.parquet`, `data/cig.parquet`, `data/aggiudicatari.parquet` e `data/stats.json` siano stati generati
4. (Opzionale) Eliminare i CSV sorgente per risparmiare spazio

## Deploy (Hostinger VPS)

- **Server**: Hostinger KVM 2 (8 GB RAM, 2 vCPU, 100 GB NVMe)
- **IP**: 72.62.93.58
- **OS**: Ubuntu 24.04 LTS
- **Accesso SSH**: `ssh root@72.62.93.58`
- **Autenticazione**: sessione cookie firmata via API AgenTik (nginx Basic Auth rimosso)
- **App path**: `/opt/AgentiKup`
- **Venv**: `/opt/AgentiKup/venv`
- **Servizio**: `systemctl {start|stop|restart|status} agentikup`
- **Nginx config**: `/etc/nginx/sites-available/agentikup`
- **Logs**: `journalctl -u agentikup -f`

### Aggiornamento codice sul server

```bash
ssh root@72.62.93.58
cd /opt/AgentiKup && git pull && systemctl restart agentikup
```

### Aggiornamento dati sul server

```bash
scp data/progetti.parquet root@72.62.93.58:/opt/AgentiKup/data/
scp data/cig.parquet root@72.62.93.58:/opt/AgentiKup/data/
scp data/aggiudicatari.parquet root@72.62.93.58:/opt/AgentiKup/data/
scp data/stats.json root@72.62.93.58:/opt/AgentiKup/data/
ssh root@72.62.93.58 "systemctl restart agentikup"
```

## Note tecniche

- DuckDB legge i Parquet direttamente senza caricarli in memoria (memory_limit 4GB)
- Il frontend serve come file statici da FastAPI (`/static/` -> `frontend/`), con route dedicate per `/favicon.ico` e `/login.html`
- La conversione CSV->Parquet fa JOIN con Localizzazione e Soggetti e dedup per CUP/PIVA
- I CIG vengono deduplicati per codice CIG tenendo il record piu recente
- Le colonne numeriche (costo, finanziamento) sono stringhe nel Parquet, cast con `TRY_CAST` nelle query

## Autenticazione

Integrazione con **AgenTik** (`https://campusagentik.com/`, PHP/MySQL su Hostinger shared hosting).

### Flusso

1. Utente apre `http://72.62.93.58` → middleware redirige a `/login.html`
2. Submit form → `POST /api/auth/login` → backend chiama `POST https://campusagentik.com/api/verify-auth.php` con email, password e header `X-API-Key`
3. AgenTik verifica credenziali (bcrypt), controlla `attivo=1` e `mostra_kup=1`
4. Se ok → AgentiKup crea cookie firmato (`agentikup_session`, HttpOnly, SameSite=Lax, 8h TTL)
5. Tutte le API protette dal middleware; 401 → redirect a login

### Variabili ambiente (systemd)

| Variabile | Descrizione |
|---|---|
| `SESSION_SECRET` | Segreto per firma cookie (itsdangerous) |
| `AGENTIK_AUTH_URL` | URL endpoint verifica credenziali (`https://campusagentik.com/api/verify-auth.php`) |
| `AGENTIK_API_KEY` | API key condivisa con AgenTik (header `X-API-Key`) |
| `VERIFY_SSL` | `1` in produzione, `0` in locale (self-signed cert Laragon) |
| `HTTPS` | `1` per attivare flag `Secure` sul cookie (non impostato, VPS senza HTTPS) |

### Abilitazione utenti

Dal pannello admin AgenTik (`/admin/utenti.php`): toggle colonna **KUP** per abilitare/disabilitare l'accesso per utente. Colonna DB: `utenti.mostra_kup` (TINYINT, default 0).

### File AgenTik coinvolti

- `api/verify-auth.php` — endpoint API protetto da API key, rate limiting 10/min per IP
- `admin/utenti.php` — toggle `mostra_kup` nel pannello admin
- `index.php` — tab AgentiKup con link al VPS (visibile solo se `mostra_kup=1`)
- `sql/006_mostra_kup.sql` — migration per aggiungere colonna

### Deploy Hostinger (AgenTik)

- **Hosting**: Hostinger shared hosting
- **SSH**: `ssh -p 65002 u198590004@82.25.102.248`
- **Document root**: `~/domains/campusagentik.com/public_html/`
- **API key**: configurata in `.htaccess` (`SetEnv AGENTIKUP_API_KEY ...`)
- **Upload file**: `scp -P 65002 <file> u198590004@82.25.102.248:domains/campusagentik.com/public_html/<path>`
