const DATA_PATHS = {
  metrics: "../data/bol_conversion_metrics.csv",
  daily: "../data/bol_conversion_daily.csv",
};

const state = {
  mode: "WEEK",
  periodLabel: "",
  search: "",
  countryView: "TOTAL",
  sortKey: "conversion_rate",
  sortDir: "desc",
  metricsRows: [],
  dailyRows: [],
};

const el = {
  modeWeekly: document.getElementById("modeWeekly"),
  modeMonthly: document.getElementById("modeMonthly"),
  periodSelect: document.getElementById("periodSelect"),
  productSearch: document.getElementById("productSearch"),
  countryView: document.getElementById("countryView"),
  updatedAt: document.getElementById("updatedAt"),
  tableBody: document.getElementById("tableBody"),
  tableMeta: document.getElementById("tableMeta"),
  kpiOrders: document.getElementById("kpiOrders"),
  kpiVisits: document.getElementById("kpiVisits"),
  kpiConv: document.getElementById("kpiConv"),
  kpiTop: document.getElementById("kpiTop"),
};

const charts = {
  visits: null,
  orders: null,
  trend: null,
};

let renderQueued = false;

function csvToRows(csvText) {
  const lines = csvText.trim().split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = values[i] ?? "";
    });
    return row;
  });
}

function parseCsvLine(line) {
  const out = [];
  let value = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        value += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(value);
      value = "";
      continue;
    }

    value += ch;
  }

  out.push(value);
  return out;
}

async function loadData() {
  const [metricsRes, dailyRes] = await Promise.all([
    fetch(DATA_PATHS.metrics),
    fetch(DATA_PATHS.daily),
  ]);

  if (!metricsRes.ok) {
    throw new Error(`Failed to load ${DATA_PATHS.metrics}`);
  }
  if (!dailyRes.ok) {
    throw new Error(`Failed to load ${DATA_PATHS.daily}`);
  }

  const metricsRows = dedupeMetricsRows(csvToRows(await metricsRes.text()).map(normalizeRow));
  const dailyRows = csvToRows(await dailyRes.text()).map(normalizeRow);

  state.metricsRows = metricsRows;
  state.dailyRows = dailyRows;
}

function dedupeMetricsRows(rows) {
  const byKey = new Map();

  rows.forEach((row) => {
    const key = `${row.offer_id}::${row.period_type}::${row.period_label}`;
    const existing = byKey.get(key);

    if (!existing) {
      byKey.set(key, row);
      return;
    }

    // Keep the newest record if duplicates are present in source exports.
    if (String(row.date) >= String(existing.date)) {
      byKey.set(key, row);
    }
  });

  return [...byKey.values()];
}

function normalizeRow(row) {
  const visitsNl = Number(row.visits_nl || 0);
  const visitsBe = Number(row.visits_be || 0);
  const visitsTotal = Number(row.visits_total || 0);
  const orders = Number(row.orders || 0);
  const conversionRate = Number(row.conversion_rate || 0);

  return {
    offer_id: row.offer_id,
    product_name: row.product_name || "Unknown",
    ean: row.ean || "",
    date: row.date,
    period_type: row.period_type,
    period_label: row.period_label,
    visits_nl: visitsNl,
    visits_be: visitsBe,
    visits_total: visitsTotal,
    orders,
    conversion_rate: conversionRate,
  };
}

function getPeriodsByMode() {
  const labels = new Set(
    state.metricsRows
      .filter((r) => r.period_type === state.mode)
      .map((r) => r.period_label)
  );

  return [...labels].sort((a, b) => (a < b ? 1 : -1));
}

function activeVisitValue(row) {
  if (state.countryView === "NL") return row.visits_nl;
  if (state.countryView === "BE") return row.visits_be;
  return row.visits_total;
}

function filteredMetrics() {
  const search = state.search.trim().toLowerCase();

  return state.metricsRows
    .filter((r) => r.period_type === state.mode)
    .filter((r) => r.period_label === state.periodLabel)
    .filter((r) => {
      if (!search) return true;
      return (
        r.product_name.toLowerCase().includes(search) ||
        r.offer_id.toLowerCase().includes(search)
      );
    })
    .map((r) => ({
      ...r,
      visits_active: activeVisitValue(r),
      conversion_rate: activeVisitValue(r) > 0 ? (r.orders / activeVisitValue(r)) * 100 : 0,
    }));
}

function filteredDaily() {
  const byOffer = new Set(filteredMetrics().map((r) => r.offer_id));
  return state.dailyRows
    .filter((r) => r.period_type === state.mode)
    .filter((r) => r.period_label === state.periodLabel)
    .filter((r) => byOffer.has(r.offer_id));
}

function sortRows(rows) {
  const factor = state.sortDir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const av = a[state.sortKey];
    const bv = b[state.sortKey];
    if (typeof av === "number" && typeof bv === "number") {
      return (av - bv) * factor;
    }
    return String(av).localeCompare(String(bv)) * factor;
  });
}

function renderPeriodOptions() {
  const periods = getPeriodsByMode();
  el.periodSelect.innerHTML = periods.map((p) => `<option value="${p}">${p}</option>`).join("");

  if (!periods.length) {
    state.periodLabel = "";
    return;
  }

  if (!periods.includes(state.periodLabel)) {
    state.periodLabel = periods[0];
  }
  el.periodSelect.value = state.periodLabel;
}

function renderKpis(rows) {
  const totalOrders = rows.reduce((sum, r) => sum + r.orders, 0);
  const totalVisits = rows.reduce((sum, r) => sum + r.visits_active, 0);
  const avgConversion = totalVisits > 0 ? (totalOrders / totalVisits) * 100 : 0;
  const top = [...rows].sort((a, b) => b.conversion_rate - a.conversion_rate)[0];

  el.kpiOrders.textContent = totalOrders.toLocaleString();
  el.kpiVisits.textContent = totalVisits.toLocaleString();
  el.kpiConv.textContent = `${avgConversion.toFixed(2)}%`;
  el.kpiTop.textContent = top ? `${top.product_name} (${top.conversion_rate.toFixed(2)}%)` : "-";
}

function renderTable(rows) {
  const sorted = sortRows(rows);
  el.tableBody.innerHTML = sorted
    .map((r) => {
      return `
        <tr>
          <td>${escapeHtml(r.product_name)}</td>
          <td>${escapeHtml(r.offer_id)}</td>
          <td>${r.orders.toLocaleString()}</td>
          <td>${r.visits_active.toLocaleString()}</td>
          <td>${r.conversion_rate.toFixed(2)}%</td>
        </tr>
      `;
    })
    .join("");

  el.tableMeta.textContent = `${sorted.length} product rows · sorted by ${state.sortKey} (${state.sortDir})`;
}

function renderCharts(metricRows, dailyRows) {
  renderVisitsChart(dailyRows);
  renderOrdersChart(dailyRows);
  renderTrendChart(metricRows);
}

function dailyAxisAndSeries(dailyRows, field) {
  const dayMap = new Map();

  dailyRows.forEach((row) => {
    if (!dayMap.has(row.date)) dayMap.set(row.date, {});
    const product = row.product_name;
    const current = dayMap.get(row.date);
    current[product] = (current[product] || 0) + row[field];
  });

  const labels = [...dayMap.keys()].sort();
  const totals = labels.map((day) => {
    const productMap = dayMap.get(day) || {};
    return Object.values(productMap).reduce((sum, n) => sum + Number(n), 0);
  });

  return { labels, totals };
}

function renderVisitsChart(dailyRows) {
  const mapped = dailyRows.map((r) => ({ ...r, visits_active: activeVisitValue(r) }));
  const { labels, totals } = dailyAxisAndSeries(mapped, "visits_active");

  if (charts.visits) charts.visits.destroy();
  charts.visits = new Chart(document.getElementById("visitsChart"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Visits",
        data: totals,
        borderColor: "#4fb2ff",
        backgroundColor: "rgba(79, 178, 255, 0.2)",
        tension: 0.2,
        fill: true,
      }],
    },
    options: chartOptions("Visits"),
  });
}

function renderOrdersChart(dailyRows) {
  const { labels, totals } = dailyAxisAndSeries(dailyRows, "orders");

  if (charts.orders) charts.orders.destroy();
  charts.orders = new Chart(document.getElementById("ordersChart"), {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Orders",
        data: totals,
        backgroundColor: "rgba(71, 217, 168, 0.7)",
        borderColor: "#47d9a8",
        borderWidth: 1,
      }],
    },
    options: chartOptions("Orders"),
  });
}

function renderTrendChart(metricRows) {
  const weeklyRows = state.metricsRows.filter((r) => r.period_type === "WEEK");
  const labels = [...new Set(weeklyRows.map((r) => r.period_label))].sort();

  const grouped = new Map();
  weeklyRows.forEach((row) => {
    const product = row.product_name;
    if (!grouped.has(product)) grouped.set(product, new Map());

    const visits =
      state.countryView === "NL" ? row.visits_nl :
      state.countryView === "BE" ? row.visits_be : row.visits_total;

    const conversion = visits > 0 ? (row.orders / visits) * 100 : 0;
    grouped.get(product).set(row.period_label, conversion);
  });

  const topProducts = [...grouped.keys()]
    .map((name) => {
      const latest = grouped.get(name).get(labels[labels.length - 1]) || 0;
      return { name, latest };
    })
    .sort((a, b) => b.latest - a.latest)
    .slice(0, 5)
    .map((p) => p.name);

  const palette = ["#ff6a3d", "#47d9a8", "#4fb2ff", "#ffd166", "#c77dff"];
  const datasets = topProducts.map((product, idx) => ({
    label: product,
    data: labels.map((label) => grouped.get(product).get(label) || 0),
    borderColor: palette[idx % palette.length],
    tension: 0.2,
    fill: false,
  }));

  if (charts.trend) charts.trend.destroy();
  charts.trend = new Chart(document.getElementById("trendChart"), {
    type: "line",
    data: { labels, datasets },
    options: chartOptions("Conversion %"),
  });
}

function chartOptions(yTitle) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: "#d4e1f0" } },
    },
    scales: {
      x: { ticks: { color: "#8fa6bf" }, grid: { color: "rgba(143, 166, 191, 0.12)" } },
      y: { ticks: { color: "#8fa6bf" }, grid: { color: "rgba(143, 166, 191, 0.12)" }, title: { display: true, text: yTitle, color: "#8fa6bf" } },
    },
  };
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function bindEvents() {
  el.modeWeekly.addEventListener("click", () => setMode("WEEK"));
  el.modeMonthly.addEventListener("click", () => setMode("MONTH"));

  el.periodSelect.addEventListener("change", () => {
    state.periodLabel = el.periodSelect.value;
    render();
  });

  el.productSearch.addEventListener("input", () => {
    state.search = el.productSearch.value;
    render();
  });

  el.countryView.addEventListener("change", () => {
    state.countryView = el.countryView.value;
    render();
  });

  document.querySelectorAll(".sort-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = btn.dataset.key;
      if (state.sortKey === key) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = key;
        state.sortDir = ["product_name", "offer_id"].includes(key) ? "asc" : "desc";
      }
      render();
    });
  });
}

function setMode(mode) {
  state.mode = mode;
  el.modeWeekly.classList.toggle("active", mode === "WEEK");
  el.modeMonthly.classList.toggle("active", mode === "MONTH");
  renderPeriodOptions();
  render();
}

function render() {
  if (renderQueued) return;
  renderQueued = true;

  requestAnimationFrame(() => {
    renderQueued = false;

  const metricRows = filteredMetrics();
  const dailyRows = filteredDaily();

  renderKpis(metricRows);
  renderTable(metricRows);
  renderCharts(metricRows, dailyRows);
  });
}

async function init() {
  try {
    await loadData();
    const periods = getPeriodsByMode();
    state.periodLabel = periods[0] || "";
    renderPeriodOptions();
    bindEvents();
    render();

    const now = new Date();
    el.updatedAt.textContent = `Updated ${now.toLocaleString()}`;
  } catch (error) {
    console.error(error);
    el.updatedAt.textContent = `Error: ${error.message}`;
  }
}

if (!window.__bolConversionDashboardInit) {
  window.__bolConversionDashboardInit = true;
  init();
}
