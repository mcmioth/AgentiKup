"""
Converte i 7 file CSV OpenCUP + Localizzazione in un unico file Parquet.
Genera anche file JSON pre-aggregati per la dashboard.

Uso: python scripts/convert_to_parquet.py
"""

import duckdb
import json
import os
import time
import zipfile
import tempfile
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = BASE_DIR
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET_FILE = os.path.join(DATA_DIR, "progetti.parquet")
STATS_FILE = os.path.join(DATA_DIR, "stats.json")
LOC_CSV = os.path.join(CSV_DIR, "OpenCup_Localizzazione.csv").replace(os.sep, "/")
SOGG_CSV = os.path.join(CSV_DIR, "OpenCup_Soggetti.csv").replace(os.sep, "/")
CIG_CUP_JSON = os.path.join(BASE_DIR, "..", "cup_json", "cup_json.json").replace(os.sep, "/")
CIG_DETAIL_DIR = os.path.join(BASE_DIR, "..", "cup_json").replace(os.sep, "/")
CIG_PARQUET = os.path.join(DATA_DIR, "cig.parquet")

os.makedirs(DATA_DIR, exist_ok=True)


def get_csv_files():
    files = sorted([
        os.path.join(CSV_DIR, f)
        for f in os.listdir(CSV_DIR)
        if f.startswith("OpenCup_Progetti") and f.endswith(".csv")
    ])
    print(f"Trovati {len(files)} file CSV Progetti:")
    for f in files:
        size_gb = os.path.getsize(f) / (1024**3)
        print(f"  {os.path.basename(f)}: {size_gb:.2f} GB")
    return files


def convert_csv_to_parquet(con, csv_files):
    print("\n--- Conversione CSV -> Parquet (con Localizzazione) ---")
    start = time.time()

    csv_list = ", ".join(f"'{f.replace(os.sep, '/')}'" for f in csv_files)
    pq_path = PARQUET_FILE.replace(os.sep, "/")

    # Deduplica localizzazioni: prendi la prima per CUP
    print("  Preparazione localizzazioni (dedup per CUP)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE loc AS
        SELECT DISTINCT ON (CUP)
            CUP as LOC_CUP,
            AREA_GEOGRAFICA,
            REGIONE,
            SIGLA_PROVINCIA,
            PROVINCIA,
            COMUNE
        FROM read_csv(
            '{LOC_CSV}',
            delim = ';', header = true, quote = '"',
            all_varchar = true, ignore_errors = true
        )
    """)
    loc_count = con.execute("SELECT COUNT(*) FROM loc").fetchone()[0]
    print(f"  Localizzazioni uniche: {loc_count:,}")

    # Deduplica soggetti: prendi il primo per PIVA
    print("  Preparazione soggetti (dedup per PIVA)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sogg AS
        SELECT DISTINCT ON (PIVA_CODFISCALE_SOG_TITOLARE)
            PIVA_CODFISCALE_SOG_TITOLARE as SOGG_PIVA,
            CATEGORIA_SOGGETTO,
            SOTTOCATEGORIA_SOGGETTO
        FROM read_csv(
            '{SOGG_CSV}',
            delim = ';', header = true, quote = '"',
            all_varchar = true, ignore_errors = true
        )
    """)
    sogg_count = con.execute("SELECT COUNT(*) FROM sogg").fetchone()[0]
    print(f"  Soggetti unici: {sogg_count:,}")

    # JOIN Progetti + Localizzazione + Soggetti
    print("  Join e scrittura Parquet...")
    con.execute(f"""
        COPY (
            SELECT p.*, l.AREA_GEOGRAFICA, l.REGIONE,
                   l.SIGLA_PROVINCIA, l.PROVINCIA, l.COMUNE,
                   s.CATEGORIA_SOGGETTO, s.SOTTOCATEGORIA_SOGGETTO
            FROM read_csv(
                [{csv_list}],
                delim = ';', header = true, quote = '"',
                all_varchar = true, ignore_errors = true,
                filename = false
            ) p
            LEFT JOIN loc l ON p.CUP = l.LOC_CUP
            LEFT JOIN sogg s ON p.PIVA_CODFISCALE_SOG_TITOLARE = s.SOGG_PIVA
        ) TO '{pq_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)

    elapsed = time.time() - start
    size_gb = os.path.getsize(PARQUET_FILE) / (1024**3)
    print(f"  Parquet creato: {PARQUET_FILE}")
    print(f"  Dimensione: {size_gb:.2f} GB")
    print(f"  Tempo: {elapsed:.1f}s")


def generate_stats(con):
    print("\n--- Generazione statistiche pre-aggregate ---")
    start = time.time()

    pq = PARQUET_FILE.replace(os.sep, '/')
    stats = {}

    row = con.execute(f"""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT CUP) as unique_cups,
            SUM(TRY_CAST(COSTO_PROGETTO AS BIGINT)) as total_costo,
            SUM(TRY_CAST(FINANZIAMENTO_PROGETTO AS BIGINT)) as total_finanziamento
        FROM '{pq}'
    """).fetchone()
    stats["totals"] = {
        "progetti": row[0],
        "cup_unici": row[1],
        "costo_totale": row[2],
        "finanziamento_totale": row[3],
    }

    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, default=str)

    elapsed = time.time() - start
    print(f"  Statistiche salvate in: {STATS_FILE}")
    print(f"  Tempo: {elapsed:.1f}s")


def convert_cig_to_parquet(con):
    """Converte la mappatura CIG-CUP + dettagli CIG (da 72 zip) in un Parquet."""
    print("\n--- Conversione CIG -> Parquet ---")
    start = time.time()

    cig_cup = CIG_CUP_JSON.replace(os.sep, "/")
    cig_pq = CIG_PARQUET.replace(os.sep, "/")
    detail_dir = CIG_DETAIL_DIR.replace(os.sep, "/")

    # Carica mappatura CIG-CUP
    print("  Caricamento mappatura CIG-CUP...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cig_cup AS
        SELECT CIG, CUP
        FROM read_json('{cig_cup}',
            format = 'newline_delimited',
            columns = {{'CIG': 'VARCHAR', 'CUP': 'VARCHAR'}}
        )
    """)
    mapping_count = con.execute("SELECT COUNT(*) FROM cig_cup").fetchone()[0]
    print(f"  Mappature CIG-CUP: {mapping_count:,}")

    # Trova tutti i file zip CIG
    zip_files = sorted([
        os.path.join(detail_dir, f)
        for f in os.listdir(detail_dir.replace("/", os.sep))
        if f.startswith("cig_json_") and f.endswith(".zip")
    ])
    print(f"  Trovati {len(zip_files)} file zip CIG dettaglio")

    if zip_files:
        # Estrai tutti i JSON in una cartella temporanea
        tmp_dir = tempfile.mkdtemp(prefix="cig_extract_")
        tmp_dir_fwd = tmp_dir.replace(os.sep, "/")
        print(f"  Estrazione zip in cartella temporanea...")

        json_paths = []
        for zpath in zip_files:
            zf = zipfile.ZipFile(zpath.replace("/", os.sep))
            inner = zf.namelist()[0]
            zf.extract(inner, tmp_dir)
            json_paths.append(f"{tmp_dir_fwd}/{inner}")
            zf.close()

        print(f"  Estratti {len(json_paths)} file JSON")

        # Carica tutti i dettagli con UNION ALL
        json_list = ", ".join(f"'{p}'" for p in json_paths)
        print("  Caricamento dettagli CIG (tutti i mesi)...")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE cig_det_raw AS
            SELECT *
            FROM read_json([{json_list}],
                format = 'newline_delimited',
                union_by_name = true,
                ignore_errors = true
            )
        """)
        raw_count = con.execute("SELECT COUNT(*) FROM cig_det_raw").fetchone()[0]
        print(f"  Record CIG totali (con duplicati): {raw_count:,}")

        # Dedup per CIG: tieni il record piu recente (anno_pubblicazione DESC, data_pubblicazione DESC)
        print("  Deduplicazione per CIG...")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE cig_det AS
            SELECT DISTINCT ON (cig) *
            FROM cig_det_raw
            ORDER BY cig, anno_pubblicazione DESC NULLS LAST, data_pubblicazione DESC NULLS LAST
        """)
        det_count = con.execute("SELECT COUNT(*) FROM cig_det").fetchone()[0]
        print(f"  CIG unici con dettaglio: {det_count:,}")

        # Pulizia temp
        con.execute("DROP TABLE cig_det_raw")

        # Join mappatura + dettagli arricchiti
        print("  Join e scrittura Parquet CIG...")
        con.execute(f"""
            COPY (
                SELECT
                    m.CIG, m.CUP,
                    d.oggetto_gara,
                    d.importo_complessivo_gara,
                    d.importo_lotto,
                    d.oggetto_lotto,
                    d.stato as stato_cig,
                    d.settore as settore_cig,
                    d.tipo_scelta_contraente,
                    d.denominazione_amministrazione_appaltante as amm_appaltante,
                    d.data_pubblicazione,
                    d.data_scadenza_offerta,
                    d.descrizione_cpv,
                    d.ESITO as esito_cig,
                    d.provincia as provincia_cig,
                    d.anno_pubblicazione,
                    d.modalita_realizzazione,
                    d.sezione_regionale,
                    d.STRUMENTO_SVOLGIMENTO as strumento_svolgimento,
                    d.DURATA_PREVISTA as durata_prevista,
                    d.numero_gara,
                    d.cf_amministrazione_appaltante as cf_amm_appaltante,
                    d.FLAG_PNRR_PNC as flag_pnrr_pnc,
                    d.oggetto_principale_contratto,
                    d.DATA_ULTIMO_PERFEZIONAMENTO as data_ultimo_perfezionamento,
                    d.DATA_COMUNICAZIONE_ESITO as data_comunicazione_esito
                FROM cig_cup m
                LEFT JOIN cig_det d ON m.CIG = d.cig
            ) TO '{cig_pq}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)

        # Pulizia cartella temporanea
        shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        print("  Nessun file zip CIG trovato, solo mappatura...")
        con.execute(f"""
            COPY (SELECT * FROM cig_cup) TO '{cig_pq}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)

    elapsed = time.time() - start
    size_mb = os.path.getsize(CIG_PARQUET) / (1024**2)
    print(f"  CIG Parquet creato: {CIG_PARQUET}")
    print(f"  Dimensione: {size_mb:.1f} MB")
    print(f"  Tempo: {elapsed:.1f}s")


def main():
    csv_files = get_csv_files()
    if not csv_files:
        print("Nessun file CSV trovato!")
        return

    has_loc = os.path.exists(LOC_CSV.replace("/", os.sep))
    print(f"File Localizzazione: {'trovato' if has_loc else 'NON trovato'}")
    if not has_loc:
        print("ATTENZIONE: senza Localizzazione non ci saranno dati geografici")

    con = duckdb.connect()
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET threads TO 4")

    convert_csv_to_parquet(con, csv_files)
    convert_cig_to_parquet(con)
    generate_stats(con)

    # Verifica
    pq = PARQUET_FILE.replace(os.sep, '/')
    count = con.execute(f"SELECT COUNT(*) FROM '{pq}'").fetchone()[0]
    print(f"\nVerifica: {count:,} righe nel file Parquet")

    r = con.execute(f"""
        SELECT COUNT(*) as con_geo,
               COUNT(DISTINCT REGIONE) as regioni
        FROM '{pq}'
        WHERE REGIONE IS NOT NULL AND REGIONE != ''
    """).fetchone()
    print(f"Righe con localizzazione: {r[0]:,} | Regioni distinte: {r[1]}")

    cig_pq = CIG_PARQUET.replace(os.sep, '/')
    r = con.execute(f"""
        SELECT COUNT(*) as tot, COUNT(DISTINCT CIG) as cig_unici, COUNT(DISTINCT CUP) as cup_unici
        FROM '{cig_pq}'
    """).fetchone()
    print(f"CIG totali: {r[0]:,} | CIG unici: {r[1]:,} | CUP collegati: {r[2]:,}")

    con.close()
    print("\nConversione completata!")


if __name__ == "__main__":
    main()
