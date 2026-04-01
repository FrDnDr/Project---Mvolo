import { state, UI_ELEMENTS } from "../config.js";
import { escapeHtml } from "../utils/formatters.js";

export function buildFilteredRows() {
  const { week, dateStart, dateEnd, products } = state.filters;
  const selected = Array.from(products);

  return state.rows.filter((row) => {
    if (week !== "ALL" && row.week_number !== Number(week)) return false;
    if (dateStart && row.order_date < dateStart) return false;
    if (dateEnd && row.order_date > dateEnd) return false;
    if (selected.length && !products.has(row.product_name)) return false;
    return true;
  });
}

export function refreshFilterDomains() {
  const weekSet = new Set(state.rows.map(r => r.week_number).filter(n => Number.isFinite(n)));
  state.weeks = Array.from(weekSet).sort((a,b) => a - b);

  const productSet = new Set(state.rows.map(r => r.product_name).filter(Boolean));
  state.products = Array.from(productSet).sort();

  const minDate = state.rows.length ? state.rows.reduce((m, r) => r.order_date && r.order_date < m ? r.order_date : m, "9999-12-31") : "";
  const maxDate = state.rows.length ? state.rows.reduce((m, r) => r.order_date && r.order_date > m ? r.order_date : m, "0000-01-01") : "";

  state.bounds.minDate = minDate === "9999-12-31" ? "" : minDate;
  state.bounds.maxDate = maxDate === "0000-01-01" ? "" : maxDate;

  UI_ELEMENTS.weekSelect.innerHTML = `<option value="ALL">All Weeks</option>${state.weeks.map((w) => `<option value="${w}">Week ${w}</option>`).join("")}`;
  UI_ELEMENTS.productSelect.innerHTML = state.products.map((p) => `<option value="${escapeHtml(p)}">${escapeHtml(p)}</option>`).join("");
  UI_ELEMENTS.dateStart.value = state.bounds.minDate;
  UI_ELEMENTS.dateEnd.value = state.bounds.maxDate;
  
  state.filters.dateStart = state.bounds.minDate;
  state.filters.dateEnd = state.bounds.maxDate;
  state.filters.week = "ALL";
  state.filters.products = new Set();
  state.table.page = 1;
}
