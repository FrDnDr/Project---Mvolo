import { intFmt, euro, shortProductName } from "../utils/formatters.js";
import { UI_ELEMENTS } from "../config.js";

export function applyKpis(rows) {
  const totalUnits = rows.reduce((sum, r) => sum + r.units_sold, 0);
  const totalRevenue = rows.reduce((sum, r) => sum + r.revenue, 0);
  const totalRevenueEx = rows.reduce((sum, r) => sum + r.revenue_ex_vat, 0);
  
  const uniqueWeeks = new Set(rows.map(r => r.week_number));
  const weekCount = uniqueWeeks.size || 1;
  const avgWeeklyRevenue = totalRevenue / weekCount;
  const avgWeeklyRevenueEx = totalRevenueEx / weekCount;

  const avgMargin = rows.length ? rows.reduce((sum, r) => sum + r.net_margin_pct, 0) / rows.length : 0;
  const discountRate = rows.length ? (rows.filter((r) => r.discount_applied).length / rows.length) * 100 : 0;

  // ── Aggregate by product ──
  const productMap = new Map();
  for (const r of rows) {
    const name = r.product_name;
    if (!productMap.has(name)) {
      productMap.set(name, { units: 0, profit: 0, revenue: 0 });
    }
    const p = productMap.get(name);
    p.units += r.units_sold;
    p.profit += r.net_margin_eur * r.units_sold;
    p.revenue += r.revenue;
  }

  // Top Seller (most units sold)
  let topSellerName = "-";
  let topSellerUnits = 0;
  let topSellerRevenue = 0;
  for (const [name, data] of productMap) {
    if (data.units > topSellerUnits) {
      topSellerUnits = data.units;
      topSellerName = name;
      topSellerRevenue = data.revenue;
    }
  }

  // Most Profitable (highest total profit)
  let mostProfitableName = "-";
  let mostProfitableProfit = -Infinity;
  let mostProfitableUnits = 0;
  for (const [name, data] of productMap) {
    if (data.profit > mostProfitableProfit) {
      mostProfitableProfit = data.profit;
      mostProfitableName = name;
      mostProfitableUnits = data.units;
    }
  }

  // ── Update UI ──
  UI_ELEMENTS.kpiUnits.textContent = intFmt.format(totalUnits);
  UI_ELEMENTS.kpiRevenue.textContent = euro.format(totalRevenue);
  UI_ELEMENTS.kpiRevenueEx.textContent = euro.format(totalRevenueEx);
  
  // Dynamic Labels
  const tag = (weekCount > 1) ? "Avg Weekly" : "Selected Week";
  
  const weeklyInclLabel = UI_ELEMENTS.kpiRevenueWeekly.previousElementSibling;
  if (weeklyInclLabel) weeklyInclLabel.textContent = `${tag} Rev (Incl)`;
  
  const weeklyExLabel = UI_ELEMENTS.kpiRevenueWeeklyEx.previousElementSibling;
  if (weeklyExLabel) weeklyExLabel.textContent = `${tag} Rev (Ex)`;

  UI_ELEMENTS.kpiRevenueWeekly.textContent = euro.format(avgWeeklyRevenue);
  UI_ELEMENTS.kpiRevenueWeeklyEx.textContent = euro.format(avgWeeklyRevenueEx);
  UI_ELEMENTS.kpiMargin.textContent = `${avgMargin.toFixed(1)}%`;
  UI_ELEMENTS.kpiDiscountRate.textContent = `${discountRate.toFixed(1)}%`;

  // Top Seller
  const scope = (weekCount > 1) ? "Overall" : `Week ${[...uniqueWeeks][0]}`;
  UI_ELEMENTS.kpiTopSellerLabel.textContent = `🏆 Top Seller (${scope})`;
  UI_ELEMENTS.kpiTopSeller.textContent = shortProductName(topSellerName, 28);
  UI_ELEMENTS.kpiTopSeller.title = topSellerName;
  UI_ELEMENTS.kpiTopSellerSub.textContent = `${intFmt.format(topSellerUnits)} units · ${euro.format(topSellerRevenue)}`;

  // Most Profitable
  UI_ELEMENTS.kpiMostProfitableLabel.textContent = `💰 Most Profitable (${scope})`;
  UI_ELEMENTS.kpiMostProfitable.textContent = shortProductName(mostProfitableName, 28);
  UI_ELEMENTS.kpiMostProfitable.title = mostProfitableName;
  UI_ELEMENTS.kpiMostProfitableSub.textContent = `${euro.format(mostProfitableProfit)} profit · ${intFmt.format(mostProfitableUnits)} units`;
}
