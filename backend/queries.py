"""
Modulo query DuckDB per l'API OpenCUP.
Gestisce connessione, query filtrate, aggregazioni e cache.
"""

import duckdb
import json
import os
from functools import lru_cache

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET_FILE = os.path.join(DATA_DIR, "progetti.parquet").replace(os.sep, "/")
CIG_PARQUET = os.path.join(DATA_DIR, "cig.parquet").replace(os.sep, "/")
STATS_FILE = os.path.join(DATA_DIR, "stats.json")

# Colonne CIG mostrate nella tabella
CIG_DEFAULT_COLUMNS = [
    "CIG", "CUP", "oggetto_gara", "importo_complessivo_gara",
    "stato_cig", "esito_cig", "tipo_scelta_contraente",
    "amm_appaltante", "data_pubblicazione", "provincia_cig",
    "anno_pubblicazione", "flag_pnrr_pnc",
]

# Colonne CIG filtrabili
CIG_FILTER_COLUMNS = [
    "stato_cig", "esito_cig", "settore_cig", "tipo_scelta_contraente",
    "anno_pubblicazione", "provincia_cig", "sezione_regionale",
    "modalita_realizzazione", "strumento_svolgimento",
]

# Colonne CIG ricercabili
CIG_SEARCH_COLUMNS = ["CIG", "CUP", "oggetto_gara", "amm_appaltante"]

# Colonne CIG numeriche per ordinamento
CIG_NUMERIC_COLUMNS = {
    "importo_complessivo_gara", "importo_lotto",
    "anno_pubblicazione", "durata_prevista", "flag_pnrr_pnc",
}

# Tutte le colonne CIG
CIG_ALL_COLUMNS = [
    "CIG", "CUP", "oggetto_gara", "importo_complessivo_gara",
    "importo_lotto", "oggetto_lotto", "stato_cig", "settore_cig",
    "tipo_scelta_contraente", "amm_appaltante", "data_pubblicazione",
    "data_scadenza_offerta", "descrizione_cpv", "esito_cig",
    "provincia_cig", "anno_pubblicazione", "modalita_realizzazione",
    "sezione_regionale", "strumento_svolgimento", "durata_prevista",
    "numero_gara", "cf_amm_appaltante", "flag_pnrr_pnc",
    "oggetto_principale_contratto", "data_ultimo_perfezionamento",
    "data_comunicazione_esito",
]

# Colonne mostrate di default nella tabella
DEFAULT_COLUMNS = [
    "CUP", "DESCRIZIONE_SINTETICA_CUP", "ANNO_DECISIONE", "STATO_PROGETTO",
    "COSTO_PROGETTO", "FINANZIAMENTO_PROGETTO", "SOGGETTO_TITOLARE",
    "NATURA_INTERVENTO", "SETTORE_INTERVENTO", "AREA_INTERVENTO",
    "REGIONE", "PROVINCIA", "COMUNE",
]

# Colonne filtrabili (con dropdown)
FILTER_COLUMNS = [
    "STATO_PROGETTO", "ANNO_DECISIONE", "SETTORE_INTERVENTO",
    "NATURA_INTERVENTO", "AREA_INTERVENTO", "CATEGORIA_INTERVENTO",
    "SOTTOSETTORE_INTERVENTO", "TIPOLOGIA_INTERVENTO",
    "STRUMENTO_PROGRAMMAZIONE", "TIPOLOGIA_CUP", "NATURA_DIPE",
    "AREA_GEOGRAFICA", "REGIONE", "PROVINCIA", "COMUNE",
    "CATEGORIA_SOGGETTO", "SOTTOCATEGORIA_SOGGETTO",
]

# Colonne ricercabili (full-text)
SEARCH_COLUMNS = [
    "CUP", "DESCRIZIONE_SINTETICA_CUP", "SOGGETTO_TITOLARE",
    "DENOMINAZIONE_BENEFICIARIO",
]

# Tutte le colonne del dataset
ALL_COLUMNS = [
    "CUP", "DESCRIZIONE_SINTETICA_CUP", "ANNO_DECISIONE", "STATO_PROGETTO",
    "COSTO_PROGETTO", "FINANZIAMENTO_PROGETTO", "SOGGETTO_TITOLARE",
    "PIVA_CODFISCALE_SOG_TITOLARE", "CODICE_NATURA_INTERVENTO",
    "NATURA_INTERVENTO", "COD_NATURA_DIPE", "NATURA_DIPE",
    "CODICE_TIPO_INTERVENTO", "TIPOLOGIA_INTERVENTO",
    "CODICE_AREA_INTERVENTO", "AREA_INTERVENTO",
    "CODICE_SETTORE_INTERVENTO", "SETTORE_INTERVENTO",
    "CODICE_SOTTOSETTORE_INTERVENTO", "SOTTOSETTORE_INTERVENTO",
    "CODICE_CATEGORIA_INTERVENTO", "CATEGORIA_INTERVENTO",
    "TIPOLOGIA_CUP", "DESCRIZIONE_INTERVENTO",
    "DENO_IMPRESA_STABILIMENTO", "PIVA_CF_BENEFICIARIO",
    "DENO_IMPRESA_STABILIMENTO_PREC", "DENOMINAZIONE_BENEFICIARIO",
    "STRUTTURA_INFRASTRUTTURA", "INDIRIZZO_INTERVENTO",
    "NUMERO_DELIBERA_CIPE", "ANNO_DELIBERA",
    "FLAG_LEGGE_OBIETTIVO", "FLAG_TIPO_GENERICO",
    "CUP_IN_RELAZIONE", "RUOLO_IN_RELAZIONE", "DESC_TIPO_RELAZIONE",
    "DATA_ULTIMA_MODIFICA_SSC", "DATA_ULTIMA_MODIFICA_UTENTE",
    "DATA_CHIUSURA_REVOCA", "CODICE_LOCALE_PROGETTO",
    "CODICE_STRUMENTO_PROGRAM", "STRUMENTO_PROGRAMMAZIONE",
    "FINANZA_PROGETTO", "SPONSORIZZAZIONI", "ALTRE_INFORMAZIONI",
    "DATA_GENERAZIONE_CUP", "CONTROLLO_QUALITA",
    "CUP_MASTER", "RAGIONI_COLLEGAMENTO",
    "COD_SEZIONE_ATECO", "SEZIONE_ATECO",
    "COD_DIVISIONE_ATECO", "DIVISIONE_ATECO",
    "COD_GRUPPO_ATECO", "GRUPPO_ATECO",
    "COD_CLASSE_ATECO", "CLASSE_ATECO",
    "COD_CATEGORIA_ATECO", "CATEGORIA_ATECO",
    "COD_SOTTOCATEG_ATECO", "SOTTOCATEGORIA_ATECO",
    "AREA_GEOGRAFICA", "REGIONE", "SIGLA_PROVINCIA", "PROVINCIA", "COMUNE",
    "CATEGORIA_SOGGETTO", "SOTTOCATEGORIA_SOGGETTO",
]


class Database:
    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("SET memory_limit = '4GB'")
        self._stats_cache = None
        self._filter_options_cache = None

    def close(self):
        self.con.close()

    def get_stats(self):
        """Ritorna le statistiche pre-calcolate dal file JSON."""
        if self._stats_cache is None:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                self._stats_cache = json.load(f)
        return self._stats_cache

    def get_filter_options(self):
        """Ritorna i valori distinti per ogni colonna filtro."""
        if self._filter_options_cache is not None:
            return self._filter_options_cache

        options = {}
        for col in FILTER_COLUMNS:
            rows = self.con.execute(f"""
                SELECT DISTINCT "{col}"
                FROM '{PARQUET_FILE}'
                WHERE "{col}" IS NOT NULL AND "{col}" != ''
                ORDER BY "{col}"
            """).fetchall()
            options[col] = [r[0] for r in rows]

        self._filter_options_cache = options
        return options

    def search_projects(self, q="", filters=None, sort_col=None,
                        sort_dir="ASC", limit=50, offset=0):
        """
        Ricerca progetti con filtri, ordinamento e paginazione.
        Ritorna (rows, total_count).
        """
        where_clauses = []
        params = []

        # Ricerca testuale
        if q:
            search_parts = []
            for col in SEARCH_COLUMNS:
                search_parts.append(f'LOWER("{col}") LIKE ?')
                params.append(f"%{q.lower()}%")
            # Cerca anche per codice CIG
            search_parts.append(
                f"CUP IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}' WHERE LOWER(CIG) LIKE ?)"
            )
            params.append(f"%{q.lower()}%")
            where_clauses.append(f"({' OR '.join(search_parts)})")

        # Filtri
        if filters:
            for col, val in filters.items():
                if col in FILTER_COLUMNS and val:
                    if isinstance(val, list):
                        placeholders = ", ".join(["?"] * len(val))
                        where_clauses.append(f'"{col}" IN ({placeholders})')
                        params.extend(val)
                    else:
                        where_clauses.append(f'"{col}" = ?')
                        params.append(val)

            # Filtro Ha CIG
            if filters.get("HAS_CIG") == "SI":
                where_clauses.append(
                    f"CUP IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}')"
                )
            elif filters.get("HAS_CIG") == "NO":
                where_clauses.append(
                    f"CUP NOT IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}')"
                )

            # Ricerca CUP dedicata (match prefisso)
            if filters.get("SEARCH_CUP"):
                where_clauses.append("CUP LIKE ?")
                params.append(f"{filters['SEARCH_CUP'].upper()}%")

            # Ricerca CIG dedicata (lookup diretto)
            if filters.get("SEARCH_CIG"):
                where_clauses.append(
                    f"CUP IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}' WHERE CIG LIKE ?)"
                )
                params.append(f"{filters['SEARCH_CIG'].upper()}%")

            # Range costo
            if "costo_min" in filters and filters["costo_min"]:
                where_clauses.append(
                    "TRY_CAST(COSTO_PROGETTO AS BIGINT) >= ?"
                )
                params.append(int(filters["costo_min"]))
            if "costo_max" in filters and filters["costo_max"]:
                where_clauses.append(
                    "TRY_CAST(COSTO_PROGETTO AS BIGINT) <= ?"
                )
                params.append(int(filters["costo_max"]))

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # Ordinamento (cast numerico per colonne numeriche)
        NUMERIC_COLUMNS = {
            "ANNO_DECISIONE", "COSTO_PROGETTO", "FINANZIAMENTO_PROGETTO",
            "ANNO_DELIBERA",
        }
        order_sql = ""
        if sort_col and sort_col in ALL_COLUMNS:
            direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"
            if sort_col in NUMERIC_COLUMNS:
                order_sql = f'ORDER BY TRY_CAST("{sort_col}" AS BIGINT) {direction} NULLS LAST'
            else:
                order_sql = f'ORDER BY "{sort_col}" {direction} NULLS LAST'
        else:
            order_sql = "ORDER BY CUP"

        cols = ", ".join(f'"{c}"' for c in DEFAULT_COLUMNS)

        # Count totale
        count_query = f"""
            SELECT COUNT(*) FROM '{PARQUET_FILE}' {where_sql}
        """
        total = self.con.execute(count_query, params).fetchone()[0]

        # Dati paginati
        data_query = f"""
            SELECT {cols}
            FROM '{PARQUET_FILE}'
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
        """
        rows = self.con.execute(
            data_query, params + [limit, offset]
        ).fetchall()

        results = []
        for row in rows:
            results.append(dict(zip(DEFAULT_COLUMNS, row)))

        return results, total

    def get_project_detail(self, cup):
        """Ritorna tutti i dettagli di un singolo progetto per CUP."""
        cols = ", ".join(f'"{c}"' for c in ALL_COLUMNS)
        rows = self.con.execute(f"""
            SELECT {cols}
            FROM '{PARQUET_FILE}'
            WHERE CUP = ?
            LIMIT 10
        """, [cup]).fetchall()

        results = []
        for row in rows:
            results.append(dict(zip(ALL_COLUMNS, row)))
        return results

    def get_aggregation(self, field, filters=None, q=""):
        """Aggregazione dinamica per un campo specifico."""
        if field not in ALL_COLUMNS:
            return []

        where_clauses = [f'"{field}" IS NOT NULL', f'"{field}" != \'\'']
        params = []

        if q:
            search_parts = []
            for col in SEARCH_COLUMNS:
                search_parts.append(f'LOWER("{col}") LIKE ?')
                params.append(f"%{q.lower()}%")
            where_clauses.append(f"({' OR '.join(search_parts)})")

        if filters:
            for col, val in filters.items():
                if col in FILTER_COLUMNS and val:
                    if isinstance(val, list):
                        placeholders = ", ".join(["?"] * len(val))
                        where_clauses.append(f'"{col}" IN ({placeholders})')
                        params.extend(val)
                    else:
                        where_clauses.append(f'"{col}" = ?')
                        params.append(val)

            if filters.get("HAS_CIG") == "SI":
                where_clauses.append(
                    f"CUP IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}')"
                )
            elif filters.get("HAS_CIG") == "NO":
                where_clauses.append(
                    f"CUP NOT IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}')"
                )

        where_sql = "WHERE " + " AND ".join(where_clauses)

        rows = self.con.execute(f"""
            SELECT "{field}", COUNT(*) as n,
                   SUM(TRY_CAST(COSTO_PROGETTO AS BIGINT)) as costo
            FROM '{PARQUET_FILE}'
            {where_sql}
            GROUP BY "{field}"
            ORDER BY n DESC
            LIMIT 30
        """, params).fetchall()

        return [
            {"value": r[0], "count": r[1], "costo": r[2]}
            for r in rows
        ]

    def export_query(self, q="", filters=None, limit=100000):
        """Ritorna dati per export CSV (max 100k righe)."""
        where_clauses = []
        params = []

        if q:
            search_parts = []
            for col in SEARCH_COLUMNS:
                search_parts.append(f'LOWER("{col}") LIKE ?')
                params.append(f"%{q.lower()}%")
            where_clauses.append(f"({' OR '.join(search_parts)})")

        if filters:
            for col, val in filters.items():
                if col in FILTER_COLUMNS and val:
                    if isinstance(val, list):
                        placeholders = ", ".join(["?"] * len(val))
                        where_clauses.append(f'"{col}" IN ({placeholders})')
                        params.extend(val)
                    else:
                        where_clauses.append(f'"{col}" = ?')
                        params.append(val)

            if filters.get("HAS_CIG") == "SI":
                where_clauses.append(
                    f"CUP IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}')"
                )
            elif filters.get("HAS_CIG") == "NO":
                where_clauses.append(
                    f"CUP NOT IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}')"
                )

            if filters.get("SEARCH_CUP"):
                where_clauses.append("CUP LIKE ?")
                params.append(f"{filters['SEARCH_CUP'].upper()}%")

            if filters.get("SEARCH_CIG"):
                where_clauses.append(
                    f"CUP IN (SELECT DISTINCT CUP FROM '{CIG_PARQUET}' WHERE CIG LIKE ?)"
                )
                params.append(f"{filters['SEARCH_CIG'].upper()}%")

            if "costo_min" in filters and filters["costo_min"]:
                where_clauses.append(
                    "TRY_CAST(COSTO_PROGETTO AS BIGINT) >= ?"
                )
                params.append(int(filters["costo_min"]))
            if "costo_max" in filters and filters["costo_max"]:
                where_clauses.append(
                    "TRY_CAST(COSTO_PROGETTO AS BIGINT) <= ?"
                )
                params.append(int(filters["costo_max"]))

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        cols = ", ".join(f'"{c}"' for c in DEFAULT_COLUMNS)
        rows = self.con.execute(f"""
            SELECT {cols}
            FROM '{PARQUET_FILE}'
            {where_sql}
            ORDER BY CUP
            LIMIT ?
        """, params + [limit]).fetchall()

        return DEFAULT_COLUMNS, rows

    def export_cigs(self, q="", filters=None, limit=100000):
        """Ritorna dati CIG per export CSV (max 100k righe)."""
        where_clauses = []
        params = []

        if q:
            search_parts = []
            for col in CIG_SEARCH_COLUMNS:
                search_parts.append(f'LOWER(CAST("{col}" AS VARCHAR)) LIKE ?')
                params.append(f"%{q.lower()}%")
            where_clauses.append(f"({' OR '.join(search_parts)})")

        if filters:
            for col, val in filters.items():
                if col in CIG_FILTER_COLUMNS and val:
                    if isinstance(val, list):
                        placeholders = ", ".join(["?"] * len(val))
                        where_clauses.append(f'CAST("{col}" AS VARCHAR) IN ({placeholders})')
                        params.extend([str(v) for v in val])
                    else:
                        where_clauses.append(f'CAST("{col}" AS VARCHAR) = ?')
                        params.append(str(val))

            if filters.get("ONLY_PNRR") == "SI":
                where_clauses.append("flag_pnrr_pnc = 1")

            if filters.get("HAS_DETAIL") == "SI":
                where_clauses.append("oggetto_gara IS NOT NULL")

            if filters.get("importo_min"):
                where_clauses.append("importo_complessivo_gara >= ?")
                params.append(float(filters["importo_min"]))
            if filters.get("importo_max"):
                where_clauses.append("importo_complessivo_gara <= ?")
                params.append(float(filters["importo_max"]))

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        cols = ", ".join(f'"{c}"' for c in CIG_DEFAULT_COLUMNS)
        rows = self.con.execute(f"""
            SELECT {cols}
            FROM '{CIG_PARQUET}'
            {where_sql}
            ORDER BY CIG
            LIMIT ?
        """, params + [limit]).fetchall()

        return CIG_DEFAULT_COLUMNS, rows

    def get_cig_filter_options(self):
        """Ritorna i valori distinti per ogni filtro CIG."""
        options = {}
        for col in CIG_FILTER_COLUMNS:
            rows = self.con.execute(f"""
                SELECT DISTINCT "{col}"
                FROM '{CIG_PARQUET}'
                WHERE "{col}" IS NOT NULL AND CAST("{col}" AS VARCHAR) != ''
                ORDER BY "{col}"
            """).fetchall()
            options[col] = [r[0] for r in rows]
        return options

    def search_cigs(self, q="", filters=None, sort_col=None,
                    sort_dir="ASC", limit=50, offset=0):
        """Ricerca CIG con filtri, ordinamento e paginazione."""
        where_clauses = []
        params = []

        # Ricerca testuale
        if q:
            search_parts = []
            for col in CIG_SEARCH_COLUMNS:
                search_parts.append(f'LOWER(CAST("{col}" AS VARCHAR)) LIKE ?')
                params.append(f"%{q.lower()}%")
            where_clauses.append(f"({' OR '.join(search_parts)})")

        # Filtri
        if filters:
            for col, val in filters.items():
                if col in CIG_FILTER_COLUMNS and val:
                    if isinstance(val, list):
                        placeholders = ", ".join(["?"] * len(val))
                        where_clauses.append(f'CAST("{col}" AS VARCHAR) IN ({placeholders})')
                        params.extend([str(v) for v in val])
                    else:
                        where_clauses.append(f'CAST("{col}" AS VARCHAR) = ?')
                        params.append(str(val))

            # Flag PNRR
            if filters.get("ONLY_PNRR") == "SI":
                where_clauses.append("flag_pnrr_pnc = 1")

            # Solo con dettaglio
            if filters.get("HAS_DETAIL") == "SI":
                where_clauses.append("oggetto_gara IS NOT NULL")

            # Range importo
            if filters.get("importo_min"):
                where_clauses.append("importo_complessivo_gara >= ?")
                params.append(float(filters["importo_min"]))
            if filters.get("importo_max"):
                where_clauses.append("importo_complessivo_gara <= ?")
                params.append(float(filters["importo_max"]))

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # Ordinamento
        order_sql = ""
        if sort_col and sort_col in CIG_ALL_COLUMNS:
            direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"
            if sort_col in CIG_NUMERIC_COLUMNS:
                order_sql = f'ORDER BY "{sort_col}" {direction} NULLS LAST'
            else:
                order_sql = f'ORDER BY "{sort_col}" {direction} NULLS LAST'
        else:
            order_sql = "ORDER BY CIG"

        cols = ", ".join(f'"{c}"' for c in CIG_DEFAULT_COLUMNS)

        # Count
        count_query = f"SELECT COUNT(*) FROM '{CIG_PARQUET}' {where_sql}"
        total = self.con.execute(count_query, params).fetchone()[0]

        # Dati paginati
        data_query = f"""
            SELECT {cols}
            FROM '{CIG_PARQUET}'
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
        """
        rows = self.con.execute(data_query, params + [limit, offset]).fetchall()

        results = [dict(zip(CIG_DEFAULT_COLUMNS, row)) for row in rows]
        return results, total

    def get_cig_detail(self, cig):
        """Ritorna tutti i dettagli di un singolo CIG."""
        cols = ", ".join(f'"{c}"' for c in CIG_ALL_COLUMNS)
        rows = self.con.execute(f"""
            SELECT {cols}
            FROM '{CIG_PARQUET}'
            WHERE CIG = ?
            LIMIT 10
        """, [cig]).fetchall()
        return [dict(zip(CIG_ALL_COLUMNS, row)) for row in rows]

    def get_cigs_for_cup(self, cup):
        """Ritorna i CIG associati a un CUP."""
        try:
            rows = self.con.execute(f"""
                SELECT *
                FROM '{CIG_PARQUET}'
                WHERE CUP = ?
                ORDER BY CIG
            """, [cup]).fetchall()
        except Exception:
            return []

        if not rows:
            return []

        # Get column names from the query
        cols = [desc[0] for desc in self.con.description]
        return [dict(zip(cols, row)) for row in rows]

    def search_by_cig(self, cig):
        """Cerca un CIG e ritorna i CUP associati."""
        try:
            rows = self.con.execute(f"""
                SELECT DISTINCT CUP
                FROM '{CIG_PARQUET}'
                WHERE CIG = ?
            """, [cig]).fetchall()
        except Exception:
            return []

        return [r[0] for r in rows]
