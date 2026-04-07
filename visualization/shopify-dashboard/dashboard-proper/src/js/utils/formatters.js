export const euro = new Intl.NumberFormat("nl-NL", { 
  style: "currency", 
  currency: "EUR", 
  maximumFractionDigits: 2 
});

export const intFmt = new Intl.NumberFormat("nl-NL", { 
  maximumFractionDigits: 0 
});

export function shortProductName(name, max = 22) {
  if (!name) return "(Unknown)";
  return name.length > max ? `${name.slice(0, max - 1)}…` : name;
}

export function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function toDiscountBool(v) {
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v > 0;
  const t = String(v || "").trim().toLowerCase();
  return ["yes", "y", "true", "1", "discount", "with_discount"].includes(t);
}

export function setStatus(ui, text, isError = false) {
  if (!ui.status) return;
  ui.status.textContent = text;
  ui.status.style.color = isError ? "#ff8d8d" : "#9ca9bf";
}
