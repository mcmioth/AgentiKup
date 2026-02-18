/**
 * OpenCUP Dashboard - Frontend Application
 */

const API = "";
const PAGE_SIZE = 50;

let currentTab = "progetti";
let currentPage = 0;
let totalResults = 0;
let gridApi = null;
let currentSort = { col: null, dir: "ASC" };

// CIG tab state
let cigPage = 0;
let cigTotal = 0;
let cigGridApi = null;
let cigSort = { col: null, dir: "ASC" };
let cigFiltersLoaded = false;

// Registry of SearchableSelect instances, keyed by element id
const ssInstances = {};

// ============================================
// SEARCHABLE SELECT COMPONENT
// ============================================

class SearchableSelect {
    constructor(container) {
        this.container = container;
        this.id = container.id;
        this.placeholder = container.dataset.placeholder || "Tutti";
        this.options = [];
        this.selectedValue = "";
        this.highlightIdx = -1;
        this.build();
    }

    build() {
        this.container.classList.add("ss-wrap");
        this.container.innerHTML = `
            <input class="ss-input" type="text" placeholder="${this.placeholder}" autocomplete="off">
            <button class="ss-clear" type="button">&times;</button>
            <div class="ss-dropdown"></div>
        `;
        this.input = this.container.querySelector(".ss-input");
        this.clearBtn = this.container.querySelector(".ss-clear");
        this.dropdown = this.container.querySelector(".ss-dropdown");

        this.input.addEventListener("focus", () => this.open());
        this.input.addEventListener("input", () => this.filter());
        this.input.addEventListener("keydown", (e) => this.onKey(e));
        this.clearBtn.addEventListener("click", () => this.clear());

        document.addEventListener("click", (e) => {
            if (!this.container.contains(e.target)) this.close();
        });
    }

    setOptions(values) {
        this.options = values.map(v => String(v));
    }

    get value() { return this.selectedValue; }

    set value(v) {
        this.selectedValue = v;
        if (v) {
            this.input.value = v;
            this.input.classList.add("has-value");
            this.container.classList.add("has-value");
        } else {
            this.input.value = "";
            this.input.classList.remove("has-value");
            this.container.classList.remove("has-value");
            this.input.placeholder = this.placeholder;
        }
    }

    open() {
        if (this.selectedValue) this.input.select();
        this.filter();
        this.dropdown.classList.add("open");
    }

    close() {
        this.dropdown.classList.remove("open");
        this.highlightIdx = -1;
        if (this.selectedValue) {
            this.input.value = this.selectedValue;
        } else {
            this.input.value = "";
        }
    }

    filter() {
        const q = this.input.value.toLowerCase().trim();
        const filtered = q
            ? this.options.filter(o => o.toLowerCase().includes(q))
            : this.options;

        let html = `<div class="ss-count">${filtered.length} di ${this.options.length}</div>`;
        html += `<div class="ss-option all-option" data-val="">${this.placeholder} (tutti)</div>`;

        const show = filtered.slice(0, 200);
        for (const val of show) {
            html += `<div class="ss-option" data-val="${escapeAttr(val)}">${highlight(val, q)}</div>`;
        }
        if (filtered.length > 200) {
            html += `<div class="ss-count">...e altri ${filtered.length - 200}. Digita per filtrare.</div>`;
        }

        this.dropdown.innerHTML = html;
        this.highlightIdx = -1;

        this.dropdown.querySelectorAll(".ss-option").forEach(el => {
            el.addEventListener("mousedown", (e) => {
                e.preventDefault();
                this.select(el.dataset.val);
            });
        });
    }

    select(val) { this.value = val; this.close(); }
    clear() { this.value = ""; this.input.focus(); }

    onKey(e) {
        const items = this.dropdown.querySelectorAll(".ss-option");
        if (e.key === "ArrowDown") {
            e.preventDefault();
            this.highlightIdx = Math.min(this.highlightIdx + 1, items.length - 1);
            this.updateHighlight(items);
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            this.highlightIdx = Math.max(this.highlightIdx - 1, 0);
            this.updateHighlight(items);
        } else if (e.key === "Enter") {
            e.preventDefault();
            if (this.highlightIdx >= 0 && items[this.highlightIdx]) {
                this.select(items[this.highlightIdx].dataset.val);
            }
        } else if (e.key === "Escape") {
            this.close();
            this.input.blur();
        }
    }

    updateHighlight(items) {
        items.forEach((el, i) => {
            el.classList.toggle("highlighted", i === this.highlightIdx);
            if (i === this.highlightIdx) el.scrollIntoView({ block: "nearest" });
        });
    }
}

function escapeAttr(s) {
    return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

function highlight(text, q) {
    if (!q) return escapeHtml(text);
    const idx = text.toLowerCase().indexOf(q);
    if (idx < 0) return escapeHtml(text);
    const before = escapeHtml(text.substring(0, idx));
    const match = escapeHtml(text.substring(idx, idx + q.length));
    const after = escapeHtml(text.substring(idx + q.length));
    return `${before}<b>${match}</b>${after}`;
}

// ============================================
// INIT
// ============================================

document.addEventListener("DOMContentLoaded", async () => {
    initSearchableSelects();
    initGrid();
    bindEvents();
    showLoading(true);
    try {
        await loadFilterOptions();
        setFilterValue("f-catsogg", "UNIVERSITA' ED ALTRI ENTI DI ISTRUZIONE");
        setFilterValue("f-sottocatsogg", "ISTITUTI PUBBLICI DI ISTRUZIONE SCOLASTICA");
        await loadProjects();
    } finally {
        showLoading(false);
    }
});

function initSearchableSelects() {
    document.querySelectorAll("[data-ss]").forEach(el => {
        ssInstances[el.id] = new SearchableSelect(el);
    });
}

// ============================================
// TAB SWITCHING
// ============================================

function switchTab(tab, skipLoad) {
    if (tab === currentTab) return;

    // Auto-reset filters of the tab we're leaving
    if (currentTab === "progetti") {
        clearProgettiFields();
    } else {
        clearCigFields();
    }
    document.getElementById("search-input").value = "";

    // Restore default filters for the tab we're entering
    if (tab === "progetti") {
        setFilterValue("f-catsogg", "UNIVERSITA' ED ALTRI ENTI DI ISTRUZIONE");
        setFilterValue("f-sottocatsogg", "ISTITUTI PUBBLICI DI ISTRUZIONE SCOLASTICA");
    }

    currentTab = tab;

    // Update tab buttons
    document.querySelectorAll(".tab-btn").forEach(btn => {
        btn.classList.toggle("active", btn.dataset.tab === tab);
    });

    // Toggle sidebars
    document.getElementById("sidebar-progetti").style.display = tab === "progetti" ? "" : "none";
    document.getElementById("sidebar-cig").style.display = tab === "cig" ? "" : "none";

    // Destroy whichever grid is currently active
    const gridDiv = document.getElementById("data-grid");
    if (cigGridApi) { cigGridApi.destroy(); cigGridApi = null; }
    if (gridApi) { gridApi.destroy(); gridApi = null; }
    gridDiv.innerHTML = "";

    if (tab === "progetti") {
        document.getElementById("search-input").placeholder = "Cerca per CUP, descrizione, soggetto titolare...";
        currentSort = { col: null, dir: "ASC" };
        currentPage = 0;
        initGrid();
        if (!skipLoad) loadProjects();
    } else {
        document.getElementById("search-input").placeholder = "Cerca per CIG, CUP, oggetto gara, amm. appaltante...";
        cigSort = { col: null, dir: "ASC" };
        cigPage = 0;
        initCigGrid();
        if (!skipLoad) {
            if (!cigFiltersLoaded) {
                loadCigFilterOptions().then(() => loadCigs());
            } else {
                loadCigs();
            }
        }
    }
}

// ============================================
// AG GRID - PROGETTI
// ============================================

function initGrid() {
    const columnDefs = [
        { field: "CUP", width: 170, pinned: "left", cellClass: "cup-link" },
        { field: "SOGGETTO_TITOLARE", headerName: "Sogg. Titolare", flex: 1, minWidth: 180 },
        { field: "DESCRIZIONE_SINTETICA_CUP", headerName: "Descrizione", flex: 2, minWidth: 250 },
        { field: "ANNO_DECISIONE", headerName: "Anno", width: 90, comparator: numComp },
        { field: "STATO_PROGETTO", headerName: "Stato", width: 110 },
        {
            field: "COSTO_PROGETTO", headerName: "Costo",
            width: 140, valueFormatter: p => formatCurrency(p.value), comparator: numComp,
        },
        {
            field: "FINANZIAMENTO_PROGETTO", headerName: "Finanziamento",
            width: 140, valueFormatter: p => formatCurrency(p.value), comparator: numComp,
        },
        { field: "NATURA_INTERVENTO", headerName: "Natura", width: 180 },
        { field: "SETTORE_INTERVENTO", headerName: "Settore", width: 180 },
        { field: "AREA_INTERVENTO", headerName: "Area", width: 160 },
        { field: "REGIONE", headerName: "Regione", width: 140 },
        { field: "PROVINCIA", headerName: "Provincia", width: 130 },
        { field: "COMUNE", headerName: "Comune", width: 140 },
    ];

    const gridOptions = {
        columnDefs,
        defaultColDef: { sortable: true, resizable: true, filter: false },
        rowModelType: "clientSide",
        domLayout: "normal",
        rowHeight: 36, headerHeight: 38,
        animateRows: false, suppressCellFocus: true,
        onRowClicked: (e) => openDetail(e.data.CUP),
        onSortChanged: (e) => {
            const sortModel = e.api.getColumnState().filter(c => c.sort).map(c => ({ colId: c.colId, sort: c.sort }));
            if (sortModel.length > 0) {
                currentSort.col = sortModel[0].colId;
                currentSort.dir = sortModel[0].sort.toUpperCase();
            } else {
                currentSort.col = null;
                currentSort.dir = "ASC";
            }
            currentPage = 0;
            loadProjects();
        },
        overlayNoRowsTemplate: '<span style="padding:20px;">Nessun risultato trovato</span>',
    };

    gridApi = agGrid.createGrid(document.getElementById("data-grid"), gridOptions);
}

// ============================================
// AG GRID - CIG
// ============================================

function initCigGrid() {
    const columnDefs = [
        { field: "CIG", width: 130, pinned: "left", cellClass: "cup-link" },
        { field: "CUP", width: 170 },
        { field: "amm_appaltante", headerName: "Amm. Appaltante", flex: 1, minWidth: 200 },
        { field: "oggetto_gara", headerName: "Oggetto Gara", flex: 2, minWidth: 250 },
        {
            field: "importo_complessivo_gara", headerName: "Importo Gara",
            width: 150, valueFormatter: p => formatCurrency(p.value), comparator: numComp,
        },
        { field: "stato_cig", headerName: "Stato", width: 100 },
        { field: "esito_cig", headerName: "Esito", width: 140 },
        { field: "tipo_scelta_contraente", headerName: "Tipo Contraente", width: 200 },
        { field: "data_pubblicazione", headerName: "Data Pubbl.", width: 120 },
        { field: "provincia_cig", headerName: "Provincia", width: 120 },
        { field: "anno_pubblicazione", headerName: "Anno", width: 80, comparator: numComp },
        {
            field: "flag_pnrr_pnc", headerName: "PNRR", width: 80,
            cellRenderer: p => p.value === 1 ? '<span class="pnrr-badge">PNRR</span>' : "",
        },
    ];

    const gridOptions = {
        columnDefs,
        defaultColDef: { sortable: true, resizable: true, filter: false },
        rowModelType: "clientSide",
        domLayout: "normal",
        rowHeight: 36, headerHeight: 38,
        animateRows: false, suppressCellFocus: true,
        onRowClicked: (e) => openCigDetail(e.data.CIG),
        onSortChanged: (e) => {
            const sortModel = e.api.getColumnState().filter(c => c.sort).map(c => ({ colId: c.colId, sort: c.sort }));
            if (sortModel.length > 0) {
                cigSort.col = sortModel[0].colId;
                cigSort.dir = sortModel[0].sort.toUpperCase();
            } else {
                cigSort.col = null;
                cigSort.dir = "ASC";
            }
            cigPage = 0;
            loadCigs();
        },
        overlayNoRowsTemplate: '<span style="padding:20px;">Nessun risultato trovato</span>',
    };

    cigGridApi = agGrid.createGrid(document.getElementById("data-grid"), gridOptions);
}

// ============================================
// DATA LOADING - PROGETTI
// ============================================

const FILTER_MAPPING = {
    "STATO_PROGETTO": "f-stato",
    "ANNO_DECISIONE": "f-anno",
    "SETTORE_INTERVENTO": "f-settore",
    "NATURA_INTERVENTO": "f-natura",
    "AREA_INTERVENTO": "f-area",
    "CATEGORIA_INTERVENTO": "f-categoria",
    "SOTTOSETTORE_INTERVENTO": "f-sottosettore",
    "TIPOLOGIA_INTERVENTO": "f-tipointervento",
    "STRUMENTO_PROGRAMMAZIONE": "f-strumento",
    "TIPOLOGIA_CUP": "f-tipologia",
    "CATEGORIA_SOGGETTO": "f-catsogg",
    "SOTTOCATEGORIA_SOGGETTO": "f-sottocatsogg",
    "AREA_GEOGRAFICA": "f-areageo",
    "REGIONE": "f-regione",
    "PROVINCIA": "f-provincia",
    "COMUNE": "f-comune",
};

async function loadFilterOptions() {
    const options = await fetchApi("/api/filters/options");
    if (!options) return;

    for (const [col, elId] of Object.entries(FILTER_MAPPING)) {
        if (!options[col]) continue;
        if (ssInstances[elId]) {
            ssInstances[elId].setOptions(options[col]);
            continue;
        }
        const select = document.getElementById(elId);
        if (!select) continue;
        for (const val of options[col]) {
            const opt = document.createElement("option");
            opt.value = val;
            opt.textContent = val;
            select.appendChild(opt);
        }
    }
}

async function loadProjects() {
    showLoading(true);
    const params = buildQueryParams();
    params.set("limit", PAGE_SIZE);
    params.set("offset", currentPage * PAGE_SIZE);

    try {
        const result = await fetchApi(`/api/projects?${params}`);
        if (!result || !gridApi) return;

        totalResults = result.total;
        gridApi.setGridOption("rowData", result.data);

        document.getElementById("results-info").textContent = `${formatNumber(totalResults)} risultati`;
        document.getElementById("header-info").textContent = `${formatNumber(totalResults)} progetti trovati`;
        renderPagination(currentPage, totalResults, (p) => { currentPage = p; loadProjects(); });
    } finally {
        showLoading(false);
    }
}

// ============================================
// DATA LOADING - CIG
// ============================================

const CIG_FILTER_MAPPING = {
    "stato_cig": "fc-stato",
    "esito_cig": "fc-esito",
    "settore_cig": "fc-settore",
    "tipo_scelta_contraente": "fc-tipo",
    "anno_pubblicazione": "fc-anno",
    "provincia_cig": "fc-provincia",
    "sezione_regionale": "fc-sezione",
    "modalita_realizzazione": "fc-modalita",
    "strumento_svolgimento": "fc-strumento",
};

async function loadCigFilterOptions() {
    const options = await fetchApi("/api/cig/filters/options");
    if (!options) return;

    for (const [col, elId] of Object.entries(CIG_FILTER_MAPPING)) {
        if (!options[col]) continue;
        if (ssInstances[elId]) {
            ssInstances[elId].setOptions(options[col]);
            continue;
        }
        const select = document.getElementById(elId);
        if (!select) continue;
        for (const val of options[col]) {
            const opt = document.createElement("option");
            opt.value = val;
            opt.textContent = val;
            select.appendChild(opt);
        }
    }
    cigFiltersLoaded = true;
}

async function loadCigs() {
    showLoading(true);
    const params = buildCigQueryParams();
    params.set("limit", PAGE_SIZE);
    params.set("offset", cigPage * PAGE_SIZE);

    try {
        const result = await fetchApi(`/api/cig/search?${params}`);
        if (!result || !cigGridApi) return;

        cigTotal = result.total;
        cigGridApi.setGridOption("rowData", result.data);

        document.getElementById("results-info").textContent = `${formatNumber(cigTotal)} risultati`;
        document.getElementById("header-info").textContent = `${formatNumber(cigTotal)} CIG trovati`;
        renderPagination(cigPage, cigTotal, (p) => { cigPage = p; loadCigs(); });
    } finally {
        showLoading(false);
    }
}

// ============================================
// FILTERS - PROGETTI
// ============================================

function getFilterValue(elId) {
    if (ssInstances[elId]) return ssInstances[elId].value;
    const el = document.getElementById(elId);
    return el ? el.value : "";
}

function setFilterValue(elId, val) {
    if (ssInstances[elId]) {
        ssInstances[elId].value = val;
    } else {
        const el = document.getElementById(elId);
        if (el) el.value = val;
    }
}

function buildQueryParams() {
    const params = new URLSearchParams();
    const q = document.getElementById("search-input").value.trim();
    if (q) params.set("q", q);

    for (const [param, elId] of Object.entries(FILTER_MAPPING)) {
        const val = getFilterValue(elId);
        if (val) params.set(param, val);
    }

    const searchCup = document.getElementById("f-cup").value.trim();
    if (searchCup) params.set("SEARCH_CUP", searchCup);
    const searchCig = document.getElementById("f-cig").value.trim();
    if (searchCig) params.set("SEARCH_CIG", searchCig);
    const hasCig = document.getElementById("f-hascig").value;
    if (hasCig) params.set("HAS_CIG", hasCig);
    const costoMin = document.getElementById("f-costo-min").value;
    const costoMax = document.getElementById("f-costo-max").value;
    if (costoMin) params.set("costo_min", costoMin);
    if (costoMax) params.set("costo_max", costoMax);

    if (currentSort.col) {
        params.set("sort", currentSort.col);
        params.set("order", currentSort.dir);
    }
    return params;
}

function applyFilters() {
    currentPage = 0;
    loadProjects();
}

function clearProgettiFields() {
    for (const elId of Object.values(FILTER_MAPPING)) setFilterValue(elId, "");
    document.getElementById("f-cup").value = "";
    document.getElementById("f-cig").value = "";
    document.getElementById("f-hascig").value = "";
    document.getElementById("f-costo-min").value = "";
    document.getElementById("f-costo-max").value = "";
}

function resetFilters() {
    document.getElementById("search-input").value = "";
    clearProgettiFields();
    setFilterValue("f-catsogg", "UNIVERSITA' ED ALTRI ENTI DI ISTRUZIONE");
    setFilterValue("f-sottocatsogg", "ISTITUTI PUBBLICI DI ISTRUZIONE SCOLASTICA");
    currentSort = { col: null, dir: "ASC" };
    currentPage = 0;
    if (gridApi) gridApi.applyColumnState({ defaultState: { sort: null } });
    loadProjects();
}

// ============================================
// FILTERS - CIG
// ============================================

function buildCigQueryParams() {
    const params = new URLSearchParams();
    const q = document.getElementById("search-input").value.trim();
    if (q) params.set("q", q);

    for (const [param, elId] of Object.entries(CIG_FILTER_MAPPING)) {
        const val = getFilterValue(elId);
        if (val) params.set(param, val);
    }

    const cigCode = document.getElementById("fc-cig").value.trim();
    if (cigCode) params.set("q", cigCode);  // override search with CIG code
    const cupCode = document.getElementById("fc-cup").value.trim();
    if (cupCode) params.set("q", cupCode);  // override search with CUP code

    const pnrr = document.getElementById("fc-pnrr").value;
    if (pnrr) params.set("ONLY_PNRR", pnrr);
    const detail = document.getElementById("fc-detail").value;
    if (detail) params.set("HAS_DETAIL", detail);
    const importoMin = document.getElementById("fc-importo-min").value;
    const importoMax = document.getElementById("fc-importo-max").value;
    if (importoMin) params.set("importo_min", importoMin);
    if (importoMax) params.set("importo_max", importoMax);

    if (cigSort.col) {
        params.set("sort", cigSort.col);
        params.set("order", cigSort.dir);
    }
    return params;
}

function applyCigFilters() {
    cigPage = 0;
    loadCigs();
}

function clearCigFields() {
    for (const elId of Object.values(CIG_FILTER_MAPPING)) setFilterValue(elId, "");
    document.getElementById("fc-cig").value = "";
    document.getElementById("fc-cup").value = "";
    document.getElementById("fc-pnrr").value = "";
    document.getElementById("fc-detail").value = "";
    document.getElementById("fc-importo-min").value = "";
    document.getElementById("fc-importo-max").value = "";
}

function resetCigFilters() {
    document.getElementById("search-input").value = "";
    clearCigFields();
    cigSort = { col: null, dir: "ASC" };
    cigPage = 0;
    if (cigGridApi) cigGridApi.applyColumnState({ defaultState: { sort: null } });
    loadCigs();
}

// ============================================
// PAGINATION (shared)
// ============================================

function renderPagination(page, total, onPageChange) {
    const totalPages = Math.ceil(total / PAGE_SIZE);
    const pageInfo = document.getElementById("page-info");
    const pageButtons = document.getElementById("page-buttons");

    const start = page * PAGE_SIZE + 1;
    const end = Math.min((page + 1) * PAGE_SIZE, total);
    pageInfo.textContent = total > 0
        ? `${formatNumber(start)}-${formatNumber(end)} di ${formatNumber(total)}`
        : "Nessun risultato";

    pageButtons.innerHTML = "";

    const addBtn = (label, p, disabled = false, active = false) => {
        const btn = document.createElement("button");
        btn.textContent = label;
        btn.disabled = disabled;
        if (active) btn.classList.add("active");
        if (!disabled) btn.addEventListener("click", () => onPageChange(p));
        pageButtons.appendChild(btn);
    };

    addBtn("\u00AB", 0, page === 0);
    addBtn("\u2039", page - 1, page === 0);

    const range = 2;
    let startPage = Math.max(0, page - range);
    let endPage = Math.min(totalPages - 1, page + range);

    if (startPage > 0) {
        addBtn("1", 0);
        if (startPage > 1) {
            const dots = document.createElement("span");
            dots.textContent = "..."; dots.style.padding = "0 4px";
            pageButtons.appendChild(dots);
        }
    }
    for (let i = startPage; i <= endPage; i++) {
        addBtn(String(i + 1), i, false, i === page);
    }
    if (endPage < totalPages - 1) {
        if (endPage < totalPages - 2) {
            const dots = document.createElement("span");
            dots.textContent = "..."; dots.style.padding = "0 4px";
            pageButtons.appendChild(dots);
        }
        addBtn(String(totalPages), totalPages - 1);
    }

    addBtn("\u203A", page + 1, page >= totalPages - 1);
    addBtn("\u00BB", totalPages - 1, page >= totalPages - 1);
}

// ============================================
// DETAIL MODAL - PROGETTO
// ============================================

async function openDetail(cup) {
    if (!cup) return;
    showLoading(true);
    try {
        const result = await fetchApi(`/api/projects/${encodeURIComponent(cup)}`);
        if (!result || !result.data || result.data.length === 0) return;

        const project = result.data[0];
        document.getElementById("modal-title").textContent = `CUP: ${cup}`;

        const body = document.getElementById("modal-body");
        body.innerHTML = "";

        const grid = document.createElement("div");
        grid.className = "detail-grid";

        const LABELS = {
            CUP: "CUP", DESCRIZIONE_SINTETICA_CUP: "Descrizione Sintetica",
            ANNO_DECISIONE: "Anno Decisione", STATO_PROGETTO: "Stato",
            COSTO_PROGETTO: "Costo Progetto", FINANZIAMENTO_PROGETTO: "Finanziamento",
            SOGGETTO_TITOLARE: "Soggetto Titolare",
            CATEGORIA_SOGGETTO: "Categoria Soggetto", SOTTOCATEGORIA_SOGGETTO: "Sotto Categoria Soggetto",
            NATURA_INTERVENTO: "Natura Intervento", TIPOLOGIA_INTERVENTO: "Tipologia Intervento",
            AREA_INTERVENTO: "Area Intervento", SETTORE_INTERVENTO: "Settore Intervento",
            SOTTOSETTORE_INTERVENTO: "Sottosettore", CATEGORIA_INTERVENTO: "Categoria Intervento",
            STRUMENTO_PROGRAMMAZIONE: "Strumento Programmazione",
            AREA_GEOGRAFICA: "Area Geografica", REGIONE: "Regione", PROVINCIA: "Provincia", COMUNE: "Comune",
            DENOMINAZIONE_BENEFICIARIO: "Beneficiario",
            STRUTTURA_INFRASTRUTTURA: "Struttura/Infrastruttura", INDIRIZZO_INTERVENTO: "Indirizzo",
            DESCRIZIONE_INTERVENTO: "Descrizione Intervento",
            DATA_GENERAZIONE_CUP: "Data Generazione CUP", DATA_ULTIMA_MODIFICA_UTENTE: "Ultima Modifica",
            SEZIONE_ATECO: "Sezione ATECO", DIVISIONE_ATECO: "Divisione ATECO",
        };

        for (const [key, label] of Object.entries(LABELS)) {
            const val = project[key];
            if (!val || val === "DATO NON PRESENTE") continue;
            const item = document.createElement("div");
            item.className = "detail-item";
            const formatted = (key === "COSTO_PROGETTO" || key === "FINANZIAMENTO_PROGETTO") ? formatCurrency(val) : val;
            item.innerHTML = `<div class="dl">${label}</div><div class="dv">${escapeHtml(String(formatted))}</div>`;
            grid.appendChild(item);
        }
        body.appendChild(grid);

        // Load CIG data
        const cigResult = await fetchApi(`/api/projects/${encodeURIComponent(cup)}/cig`);
        if (cigResult && cigResult.data && cigResult.data.length > 0) {
            body.appendChild(buildCigTable(cigResult.data, cigResult.total));
        }

        document.getElementById("modal-overlay").classList.add("active");
    } finally {
        showLoading(false);
    }
}

// ============================================
// DETAIL MODAL - CIG
// ============================================

async function openCigDetail(cigCode) {
    if (!cigCode) return;
    showLoading(true);
    try {
        const result = await fetchApi(`/api/cig/${encodeURIComponent(cigCode)}`);
        if (!result || !result.data || result.data.length === 0) return;

        const cig = result.data[0];
        document.getElementById("modal-title").textContent = `CIG: ${cigCode}`;

        const body = document.getElementById("modal-body");
        body.innerHTML = "";

        const grid = document.createElement("div");
        grid.className = "detail-grid";

        const LABELS = {
            CIG: "CIG", CUP: "CUP",
            oggetto_gara: "Oggetto Gara", oggetto_lotto: "Oggetto Lotto",
            importo_complessivo_gara: "Importo Gara", importo_lotto: "Importo Lotto",
            stato_cig: "Stato", esito_cig: "Esito",
            settore_cig: "Settore", tipo_scelta_contraente: "Tipo Scelta Contraente",
            amm_appaltante: "Amm. Appaltante", cf_amm_appaltante: "CF Amm. Appaltante",
            descrizione_cpv: "CPV", oggetto_principale_contratto: "Oggetto Contratto",
            provincia_cig: "Provincia", sezione_regionale: "Sezione Regionale",
            data_pubblicazione: "Data Pubblicazione", data_scadenza_offerta: "Data Scadenza Offerta",
            data_comunicazione_esito: "Data Esito", data_ultimo_perfezionamento: "Data Perfezionamento",
            modalita_realizzazione: "Modalita Realizzazione",
            strumento_svolgimento: "Strumento Svolgimento",
            durata_prevista: "Durata Prevista (gg)", numero_gara: "N. Gara",
            anno_pubblicazione: "Anno Pubblicazione",
            flag_pnrr_pnc: "PNRR/PNC",
        };

        for (const [key, label] of Object.entries(LABELS)) {
            let val = cig[key];
            if (val == null || val === "" || val === "null") continue;
            const item = document.createElement("div");
            item.className = "detail-item";
            if (key.startsWith("importo")) val = formatCurrency(val);
            if (key === "flag_pnrr_pnc") val = val === 1 ? "SI" : "NO";
            item.innerHTML = `<div class="dl">${label}</div><div class="dv">${escapeHtml(String(val))}</div>`;
            grid.appendChild(item);
        }

        body.appendChild(grid);

        // Link to project if CUP exists
        if (cig.CUP && cig.CUP !== "ND" && cig.CUP !== "000000000000000") {
            const link = document.createElement("div");
            link.style.cssText = "margin-top:16px;";
            const cupVal = cig.CUP;
            const btn = document.createElement("button");
            btn.className = "btn btn-primary";
            btn.textContent = `Vai al Progetto ${cupVal}`;
            btn.addEventListener("click", () => {
                closeDetail();
                switchTab("progetti", true);
                document.getElementById("f-cup").value = cupVal;
                applyFilters();
            });
            link.appendChild(btn);
            body.appendChild(link);
        }

        document.getElementById("modal-overlay").classList.add("active");
    } finally {
        showLoading(false);
    }
}

// ============================================
// CIG TABLE (in project detail modal)
// ============================================

function buildCigTable(cigs, total) {
    const cigSection = document.createElement("div");
    cigSection.style.marginTop = "20px";

    const withDetail = cigs.filter(c => c.oggetto_gara);
    const headerText = `CIG Associati (${total}` +
        (withDetail.length < total ? `, ${withDetail.length} con dettaglio` : "") + ")";
    cigSection.innerHTML = `<h4 style="font-size:0.95rem;margin-bottom:10px;color:var(--primary);">${headerText}</h4>`;

    const table = document.createElement("table");
    table.style.cssText = "width:100%;border-collapse:collapse;font-size:0.82rem;";
    table.innerHTML = `<thead><tr style="background:var(--bg);text-align:left;">
        <th style="padding:6px 8px;border-bottom:2px solid var(--border);">CIG</th>
        <th style="padding:6px 8px;border-bottom:2px solid var(--border);">Oggetto Gara</th>
        <th style="padding:6px 8px;border-bottom:2px solid var(--border);">Importo</th>
        <th style="padding:6px 8px;border-bottom:2px solid var(--border);">Stato</th>
        <th style="padding:6px 8px;border-bottom:2px solid var(--border);">Esito</th>
        <th style="padding:6px 8px;border-bottom:2px solid var(--border);"></th>
    </tr></thead>`;

    const tbody = document.createElement("tbody");
    for (const cig of cigs) {
        const hasDet = !!cig.oggetto_gara;
        const pnrrBadge = cig.flag_pnrr_pnc === 1 ? ' <span class="pnrr-badge">PNRR</span>' : "";

        const tr = document.createElement("tr");
        tr.style.cssText = "border-bottom:1px solid var(--border);cursor:" + (hasDet ? "pointer" : "default");
        tr.innerHTML = `
            <td style="padding:5px 8px;font-family:monospace;">${escapeHtml(cig.CIG || "")}${pnrrBadge}</td>
            <td style="padding:5px 8px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escapeAttr(cig.oggetto_gara || "")}">${escapeHtml(cig.oggetto_gara || "-")}</td>
            <td style="padding:5px 8px;white-space:nowrap;">${cig.importo_complessivo_gara ? formatCurrency(cig.importo_complessivo_gara) : "-"}</td>
            <td style="padding:5px 8px;">${escapeHtml(cig.stato_cig || "-")}</td>
            <td style="padding:5px 8px;">${escapeHtml(cig.esito_cig || "-")}</td>
            <td style="padding:5px 8px;color:var(--primary);">${hasDet ? "&#9660;" : ""}</td>
        `;

        if (hasDet) {
            const detailRow = document.createElement("tr");
            detailRow.style.display = "none";
            detailRow.innerHTML = `<td colspan="6" style="padding:8px 16px;background:var(--bg);font-size:0.8rem;">
                ${cigFieldsHtml(cig)}
            </td>`;
            tr.addEventListener("click", () => {
                const open = detailRow.style.display !== "none";
                detailRow.style.display = open ? "none" : "table-row";
                tr.querySelector("td:last-child").innerHTML = open ? "&#9660;" : "&#9650;";
            });
            tbody.appendChild(tr);
            tbody.appendChild(detailRow);
        } else {
            tbody.appendChild(tr);
        }
    }
    table.appendChild(tbody);
    cigSection.appendChild(table);
    return cigSection;
}

function cigFieldsHtml(cig) {
    const fields = [
        ["Tipo Scelta Contraente", cig.tipo_scelta_contraente],
        ["Amm. Appaltante", cig.amm_appaltante],
        ["CF Amm. Appaltante", cig.cf_amm_appaltante],
        ["Oggetto Lotto", cig.oggetto_lotto],
        ["Importo Lotto", cig.importo_lotto ? formatCurrency(cig.importo_lotto) : null],
        ["Settore", cig.settore_cig],
        ["CPV", cig.descrizione_cpv],
        ["Oggetto Contratto", cig.oggetto_principale_contratto],
        ["Provincia", cig.provincia_cig],
        ["Sezione Regionale", cig.sezione_regionale],
        ["Data Pubblicazione", cig.data_pubblicazione],
        ["Data Scadenza Offerta", cig.data_scadenza_offerta],
        ["Data Esito", cig.data_comunicazione_esito],
        ["Data Perfezionamento", cig.data_ultimo_perfezionamento],
        ["Modalita Realizzazione", cig.modalita_realizzazione],
        ["Strumento Svolgimento", cig.strumento_svolgimento],
        ["Durata Prevista (gg)", cig.durata_prevista],
        ["N. Gara", cig.numero_gara],
        ["Anno Pubblicazione", cig.anno_pubblicazione],
    ];
    return fields
        .filter(([, v]) => v != null && v !== "" && v !== "null")
        .map(([label, val]) =>
            `<div style="display:inline-block;width:48%;padding:3px 0;vertical-align:top;">
                <span style="color:var(--text-secondary);font-size:0.72rem;text-transform:uppercase;">${label}</span><br>
                <span>${escapeHtml(String(val))}</span>
            </div>`
        ).join("");
}

function closeDetail() {
    document.getElementById("modal-overlay").classList.remove("active");
}

// ============================================
// EXPORT
// ============================================

function exportCsv() {
    if (currentTab === "cig") {
        const params = buildCigQueryParams();
        window.open(`${API}/api/cig/export?${params}`, "_blank");
    } else {
        const params = buildQueryParams();
        window.open(`${API}/api/export?${params}`, "_blank");
    }
}

// ============================================
// EVENTS
// ============================================

function bindEvents() {
    // Tab buttons
    document.querySelectorAll(".tab-btn").forEach(btn => {
        btn.addEventListener("click", () => switchTab(btn.dataset.tab));
    });

    // Progetti filters
    document.getElementById("btn-apply").addEventListener("click", applyFilters);
    document.getElementById("btn-reset").addEventListener("click", resetFilters);

    // CIG filters
    document.getElementById("btn-cig-apply").addEventListener("click", applyCigFilters);
    document.getElementById("btn-cig-reset").addEventListener("click", resetCigFilters);

    document.getElementById("btn-export").addEventListener("click", exportCsv);
    document.getElementById("modal-close").addEventListener("click", closeDetail);
    document.getElementById("modal-overlay").addEventListener("click", (e) => {
        if (e.target === e.currentTarget) closeDetail();
    });

    // Search with debounce
    let searchTimeout;
    document.getElementById("search-input").addEventListener("input", () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            if (currentTab === "progetti") {
                currentPage = 0;
                loadProjects();
            } else {
                cigPage = 0;
                loadCigs();
            }
        }, 400);
    });

    // Mobile menu
    document.getElementById("btn-menu").addEventListener("click", () => {
        const activeSidebar = currentTab === "progetti" ? "sidebar-progetti" : "sidebar-cig";
        document.getElementById(activeSidebar).classList.toggle("open");
    });

    // Keyboard
    document.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeDetail();
    });

    // Show mobile menu button on small screens
    const mq = window.matchMedia("(max-width: 768px)");
    const updateMenu = (e) => {
        document.getElementById("btn-menu").style.display = e.matches ? "block" : "none";
    };
    mq.addEventListener("change", updateMenu);
    updateMenu(mq);
}

// ============================================
// UTILS
// ============================================

async function fetchApi(url) {
    try {
        const res = await fetch(`${API}${url}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json();
    } catch (err) {
        console.error("API Error:", err);
        return null;
    }
}

function showLoading(show) {
    document.getElementById("loading").classList.toggle("active", show);
}

function formatNumber(n) {
    if (n == null) return "-";
    return Number(n).toLocaleString("it-IT");
}

function formatCurrency(n) {
    if (n == null || n === "" || n === "DATO NON PRESENTE") return "-";
    const num = Number(n);
    if (isNaN(num)) return n;
    return num.toLocaleString("it-IT") + " \u20AC";
}

function numComp(a, b) {
    return (Number(a) || 0) - (Number(b) || 0);
}

function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
}
