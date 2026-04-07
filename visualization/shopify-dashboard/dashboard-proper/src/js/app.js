import { state, UI_ELEMENTS } from "./config.js";
import { setStatus } from "./utils/formatters.js";
import { 
  initDuckDB, 
  loadDefaultData, 
  fetchRowsFromView, 
  loadUploadedFile 
} from "./services/duckdb-service.js";
import { applyKpis } from "./components/kpi-manager.js";
import { updateCharts } from "./components/chart-manager.js";
import { renderTable } from "./components/table-manager.js";
import { buildFilteredRows, refreshFilterDomains } from "./components/filter-manager.js";

function updateDashboard() {
  const filteredRows = buildFilteredRows();
  applyKpis(filteredRows);
  updateCharts(filteredRows);
  renderTable(filteredRows);
}

function bindEvents() {
  UI_ELEMENTS.weekSelect.addEventListener("change", () => {
    state.filters.week = UI_ELEMENTS.weekSelect.value;
    state.table.page = 1;
    updateDashboard();
  });

  UI_ELEMENTS.dateStart.addEventListener("change", () => {
    state.filters.dateStart = UI_ELEMENTS.dateStart.value;
    state.table.page = 1;
    updateDashboard();
  });

  UI_ELEMENTS.dateEnd.addEventListener("change", () => {
    state.filters.dateEnd = UI_ELEMENTS.dateEnd.value;
    state.table.page = 1;
    updateDashboard();
  });

  UI_ELEMENTS.productSelect.addEventListener("change", () => {
    const selected = Array.from(UI_ELEMENTS.productSelect.selectedOptions).map((o) => o.value);
    state.filters.products = new Set(selected);
    state.table.page = 1;
    updateDashboard();
  });

  UI_ELEMENTS.fileInput.addEventListener("change", async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    try {
      setStatus(UI_ELEMENTS, `Loading ${file.name}...`);
      await loadUploadedFile(file);
      await fetchRowsFromView();
      await refreshFilterDomains();
      updateDashboard();
      setStatus(UI_ELEMENTS, `Loaded ${file.name} successfully.`);
    } catch (error) {
      console.error(error);
      setStatus(UI_ELEMENTS, `Failed to load file: ${error.message}`, true);
    }
  });

  UI_ELEMENTS.resetFiltersBtn.addEventListener("click", () => {
    UI_ELEMENTS.weekSelect.value = "ALL";
    state.filters.week = "ALL";

    for (const option of UI_ELEMENTS.productSelect.options) {
      option.selected = false;
    }
    state.filters.products = new Set();

    UI_ELEMENTS.dateStart.value = state.bounds.minDate;
    UI_ELEMENTS.dateEnd.value = state.bounds.maxDate;
    state.filters.dateStart = state.bounds.minDate;
    state.filters.dateEnd = state.bounds.maxDate;
    state.table.page = 1;
    updateDashboard();
  });

  document.querySelectorAll(".sort-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.getAttribute("data-sort");
      if (state.table.sortBy === key) {
        state.table.sortDir = state.table.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.table.sortBy = key;
        state.table.sortDir = "asc";
      }
      renderTable(buildFilteredRows());
    });
  });

  UI_ELEMENTS.prevPageBtn.addEventListener("click", () => {
    if (state.table.page > 1) {
      state.table.page -= 1;
      renderTable(buildFilteredRows());
    }
  });

  UI_ELEMENTS.nextPageBtn.addEventListener("click", () => {
    const totalRows = buildFilteredRows().length;
    const totalPages = Math.max(1, Math.ceil(totalRows / state.table.pageSize));
    if (state.table.page < totalPages) {
      state.table.page += 1;
      renderTable(buildFilteredRows());
    }
  });
}

async function boot() {
  try {
    setStatus(UI_ELEMENTS, "Initializing DuckDB WASM engine...");
    await initDuckDB();

    setStatus(UI_ELEMENTS, "Loading default data...");
    await loadDefaultData();

    await fetchRowsFromView();
    await refreshFilterDomains();
    
    bindEvents();
    updateDashboard();

    setStatus(UI_ELEMENTS, "Ready. Filters and charts are live.");
  } catch (error) {
    console.error(error);
    setStatus(UI_ELEMENTS, `Initialization failed: ${error.message}`, true);
  }
}

// Initial Bootstrap
boot();
