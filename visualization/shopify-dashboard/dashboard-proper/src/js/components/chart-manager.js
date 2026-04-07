import { state, UI_ELEMENTS } from "../config.js";
import { shortProductName, euro } from "../utils/formatters.js";

function createCommonOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: "#d7e1f1",
          font: { family: "IBM Plex Mono" },
        },
      },
      tooltip: {
        backgroundColor: "rgba(10, 14, 22, 0.95)",
        borderColor: "rgba(126, 217, 74, 0.35)",
        borderWidth: 1,
        titleColor: "#f0f4fc",
        bodyColor: "#c9d6eb",
        displayColors: true,
        padding: 10,
      },
      datalabels: {
        display: false,
      },
    },
    scales: {
      x: {
        ticks: { color: "#9fb0c9", font: { family: "IBM Plex Mono" } },
        grid: { color: "rgba(148, 171, 204, 0.12)" },
      },
      y: {
        ticks: { color: "#9fb0c9", font: { family: "IBM Plex Mono" } },
        grid: { color: "rgba(148, 171, 204, 0.12)" },
      },
    },
  };
}

function upsertChart(key, config) {
  if (state.charts[key]) {
    state.charts[key].destroy();
  }
  state.charts[key] = new Chart(config.ctx, config.options);
}

function aggregateByProduct(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = row.product_name;
    if (!map.has(key)) {
      map.set(key, { product_name: key, units_sold: 0, revenue: 0, margin_total: 0, margin_count: 0 });
    }
    const acc = map.get(key);
    acc.units_sold += row.units_sold;
    acc.revenue += row.revenue;
    acc.margin_total += row.net_margin_pct;
    acc.margin_count += 1;
  }
  return Array.from(map.values()).map((item) => ({
    product_name: item.product_name,
    units_sold: item.units_sold,
    revenue: item.revenue,
    margin_pct: item.margin_count ? item.margin_total / item.margin_count : 0,
  }));
}

export function updateCharts(filteredRows) {
  const weekLabel = state.filters.week === "ALL" ? "All Weeks" : `Week ${state.filters.week}`;
  UI_ELEMENTS.chart1Title.textContent = `Total Products Sold for ${weekLabel} in Shopify`;
  UI_ELEMENTS.chart2Title.textContent = `Discount Frequency in Shopify on ${weekLabel}`;

  const productAgg = aggregateByProduct(filteredRows).sort((a, b) => b.units_sold - a.units_sold);
  const labels = productAgg.map((r) => r.product_name);
  const shortLabels = labels.map((n) => shortProductName(n));
  const units = productAgg.map((r) => Number(r.units_sold.toFixed(2)));
  const margins = productAgg.map((r) => Number(r.margin_pct.toFixed(1)));
  const revenues = productAgg.map((r) => Number(r.revenue.toFixed(2)));

  const discountYes = filteredRows.filter((r) => r.discount_applied).length;
  const discountNo = Math.max(0, filteredRows.length - discountYes);

  const common = createCommonOptions();

  upsertChart("units", {
    ctx: document.getElementById("chartUnits"),
    options: {
      type: "bar",
      data: {
        labels: shortLabels,
        datasets: [{
          label: "Units Sold",
          data: units,
          backgroundColor: "rgba(126, 217, 74, 0.8)",
          borderColor: "#7ed94a",
          borderWidth: 1,
          borderRadius: 6,
        }],
      },
      options: {
        ...common,
        indexAxis: "y",
        plugins: {
          ...common.plugins,
          tooltip: {
            ...common.plugins.tooltip,
            callbacks: { title: (items) => labels[items[0].dataIndex] || "" },
          },
        },
        scales: {
          x: { ...common.scales.x, title: { display: true, text: "Units", color: "#9fb0c9" } },
          y: { ...common.scales.y, title: { display: true, text: "Products", color: "#9fb0c9" } },
        },
      },
    },
  });

  upsertChart("discount", {
    ctx: document.getElementById("chartDiscount"),
    options: {
      type: "pie",
      data: {
        labels: ["With Discount", "No Discount"],
        datasets: [{
          data: [discountYes, discountNo],
          backgroundColor: ["#7ed94a", "#e84040"],
          borderColor: ["#0d1118", "#0d1118"],
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: common.plugins.legend,
          tooltip: {
            ...common.plugins.tooltip,
            callbacks: {
              label: (ctx) => {
                const total = ctx.dataset.data.reduce((a, b) => a + b, 0) || 1;
                const v = ctx.raw || 0;
                const pct = (v / total) * 100;
                return `${ctx.label}: ${v} (${pct.toFixed(1)}%)`;
              },
            },
          },
          datalabels: {
            color: "#edf3ff",
            font: { family: "IBM Plex Mono", weight: "600" },
            formatter: (value, context) => {
              const total = context.dataset.data.reduce((a, b) => a + b, 0) || 1;
              return `${((value / total) * 100).toFixed(1)}%`;
            },
            display: true,
          },
        },
      },
    },
  });

  upsertChart("margin", {
    ctx: document.getElementById("chartMargin"),
    options: {
      type: "bar",
      data: {
        labels: shortLabels,
        datasets: [{
          label: "Margin %",
          data: margins,
          backgroundColor: "rgba(126, 217, 74, 0.75)",
          borderColor: "#7ed94a",
          borderWidth: 1,
          borderRadius: 6,
        }],
      },
      options: {
        ...common,
        scales: {
          x: { ...common.scales.x, ticks: { ...common.scales.x.ticks, callback: (_v, idx) => shortLabels[idx] } },
          y: { ...common.scales.y, ticks: { ...common.scales.y.ticks, callback: (v) => `${v}%` } },
        },
        plugins: {
          ...common.plugins,
          tooltip: {
            ...common.plugins.tooltip,
            callbacks: {
              title: (items) => labels[items[0].dataIndex] || "",
              label: (ctx) => `Margin: ${ctx.raw}%`,
            },
          },
        },
      },
    },
  });

  upsertChart("revenue", {
    ctx: document.getElementById("chartRevenue"),
    options: {
      type: "line",
      data: {
        labels: shortLabels,
        datasets: [{
          label: "Revenue",
          data: revenues,
          borderColor: "#7ed94a",
          backgroundColor: "rgba(126, 217, 74, 0.28)",
          borderWidth: 2,
          tension: 0.2,
          pointRadius: 4,
          pointHoverRadius: 5,
          pointBackgroundColor: "#a7eb84",
          fill: true,
        }],
      },
      options: {
        ...common,
        plugins: {
          ...common.plugins,
          datalabels: {
            align: "top",
            anchor: "end",
            color: "#cbe3ff",
            font: { family: "IBM Plex Mono", size: 10, weight: "600" },
            formatter: (value) => euro.format(value),
            clip: true,
            display: true,
          },
          tooltip: {
            ...common.plugins.tooltip,
            callbacks: {
              title: (items) => labels[items[0].dataIndex] || "",
              label: (ctx) => `Revenue: ${euro.format(ctx.raw || 0)}`,
            },
          },
        },
        scales: {
          x: { ...common.scales.x, ticks: { ...common.scales.x.ticks, maxRotation: 0, callback: (_v, idx) => shortLabels[idx] } },
          y: { ...common.scales.y, ticks: { ...common.scales.y.ticks, callback: (v) => euro.format(v) } },
        },
      },
    },
  });
}
