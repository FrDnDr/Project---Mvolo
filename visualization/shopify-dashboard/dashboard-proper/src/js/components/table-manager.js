import { state, UI_ELEMENTS } from "../config.js";
import { escapeHtml, intFmt, euro, shortProductName } from "../utils/formatters.js";

function sortRows(rows) {
  const { sortBy, sortDir } = state.table;
  const direction = sortDir === "asc" ? 1 : -1;

  return [...rows].sort((a, b) => {
    const av = a[sortBy];
    const bv = b[sortBy];

    if (typeof av === "number" && typeof bv === "number") return (av - bv) * direction;
    if (typeof av === "boolean" && typeof bv === "boolean") return ((av ? 1 : 0) - (bv ? 1 : 0)) * direction;
    return String(av || "").localeCompare(String(bv || ""), "en", { sensitivity: "base" }) * direction;
  });
}

function updateSortIndicators() {
  document.querySelectorAll("[data-indicator]").forEach((el) => {
    const key = el.getAttribute("data-indicator");
    if (key === state.table.sortBy) {
      el.textContent = state.table.sortDir === "asc" ? "↑" : "↓";
    } else {
      el.textContent = "↕";
    }
  });
}

export function renderTable(rows) {
  const sorted = sortRows(rows);
  const totalRows = sorted.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / state.table.pageSize));
  if (state.table.page > totalPages) state.table.page = totalPages;

  const startIdx = (state.table.page - 1) * state.table.pageSize;
  const paged = sorted.slice(startIdx, startIdx + state.table.pageSize);

  UI_ELEMENTS.tableBody.innerHTML = paged
    .map((r) => `
      <tr>
        <td>${r.order_date || "-"}</td>
        <td title="${escapeHtml(r.product_name)}">${escapeHtml(shortProductName(r.product_name, 22))}</td>
        <td>${euro.format(r.cogs)}</td>
        <td>${intFmt.format(r.units_sold)}</td>
        <td>${euro.format(r.original_price)}</td>
        <td>${euro.format(r.selling_price)}</td>
        <td class="highlight">${euro.format(r.net_selling_price)}</td>
        <td>${r.discount_used_pct.toFixed(1)}%</td>
        <td>${euro.format(r.estimated_ad_cost)}</td>
        <td>${euro.format(r.net_margin_eur)}</td>
        <td class="highlight">${r.net_margin_pct.toFixed(1)}%</td>
        <td>${euro.format(r.revenue)}</td>
        <td>${euro.format(r.revenue_ex_vat)}</td>
      </tr>
    `)
    .join("");

  UI_ELEMENTS.pageInfo.textContent = `Page ${state.table.page} / ${totalPages}`;
  UI_ELEMENTS.tableMeta.textContent = `${intFmt.format(totalRows)} filtered rows`;

  UI_ELEMENTS.prevPageBtn.disabled = state.table.page <= 1;
  UI_ELEMENTS.nextPageBtn.disabled = state.table.page >= totalPages;
  updateSortIndicators();
}
