import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";
import { setStatus, toDiscountBool } from "../utils/formatters.js";
import { state, UI_ELEMENTS } from "../config.js";

function normalizeToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function quoteIdent(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

function parseProfitabilityFileName(fileName) {
  const match = /^profitability_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})_Week_?(\d{1,2})\.csv$/i.exec(fileName);
  if (!match) return null;
  return {
    fileName,
    start: match[1],
    end: match[2],
    week: Number.parseInt(match[3], 10),
  };
}

async function resolveDefaultSourceUrls() {
  if (state.activeSourceName) {
    return [new URL(state.activeSourceName, window.location.href).toString()];
  }

  const dirUrl = new URL(state.defaultDataDir, window.location.href).toString();

  try {
    const res = await fetch(dirUrl, { cache: "no-store" });
    if (!res.ok) throw new Error(`Could not list ${dirUrl}`);

    const html = await res.text();
    const links = Array.from(
      html.matchAll(/href=["']([^"']+\.csv)["']/gi),
      (m) => decodeURIComponent(m[1].split("?")[0])
    );

    const candidates = links
      .map((href) => href.split("/").pop() || "")
      .map(parseProfitabilityFileName)
      .filter(Boolean);

    if (candidates.length > 0) {
      candidates.sort((a, b) => {
        const startDiff = a.start.localeCompare(b.start);
        if (startDiff !== 0) return startDiff;
        const endDiff = a.end.localeCompare(b.end);
        if (endDiff !== 0) return endDiff;
        return a.week - b.week;
      });
      return candidates.map((c) =>
        new URL(`${state.defaultDataDir}${c.fileName}`, window.location.href).toString()
      );
    }
  } catch (err) {
    console.warn("Could not auto-discover profitability CSV. Falling back.", err);
  }

  return [new URL(state.defaultFallbackCsv, window.location.href).toString()];
}

export async function initDuckDB() {
  try {
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    
    const workerResponse = await fetch(bundle.mainWorker);
    const workerBlob = new Blob([await workerResponse.text()], { type: "text/javascript" });
    const workerUrl = URL.createObjectURL(workerBlob);
    const worker = new Worker(workerUrl);
    
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    const conn = await db.connect();
    state.db = db;
    state.conn = conn;
  } catch (err) {
    setStatus(UI_ELEMENTS, `Initialization failed: ${err.message}`, true);
    throw err;
  }
}

async function describeColumns(relationSql) {
  const rs = await state.conn.query(`DESCRIBE SELECT * FROM ${relationSql};`);
  const columns = new Map();
  for (const row of rs.toArray()) {
    const rawName = row.column_name ?? row.ColumnName ?? row["column_name"];
    if (!rawName) continue;
    const key = normalizeToken(rawName);
    if (!columns.has(key)) {
      columns.set(key, String(rawName));
    }
  }
  return columns;
}

function pickColumnExpr(columns, candidates, sqlType, fallbackSql = null) {
  for (const candidate of candidates) {
    const found = columns.get(normalizeToken(candidate));
    if (found) {
      return `CAST(${quoteIdent(found)} AS ${sqlType})`;
    }
  }
  if (fallbackSql) {
    return `CAST(${fallbackSql} AS ${sqlType})`;
  }
  return `NULL::${sqlType}`;
}

export async function buildNormalizedView(relationSql) {
  const columns = await describeColumns(relationSql);

  const productExpr = pickColumnExpr(columns, ["product_name", "product", "producttitle", "title"], "VARCHAR", pickColumnExpr(columns, ["Product"], "VARCHAR"));
  const unitsExpr = pickColumnExpr(columns, ["units_sold", "unitssold", "quantity", "qty", "units"], "DOUBLE", pickColumnExpr(columns, ["Units_Sold"], "DOUBLE", "0"));
  const revenueExpr = pickColumnExpr(columns, ["revenue", "total_revenue", "sales", "turnover"], "DOUBLE", pickColumnExpr(columns, ["Total_Revenue"], "DOUBLE", "0"));
  const marginExpr = pickColumnExpr(columns, ["net_margin_pct", "margin_pct", "margin_percent", "margin_percentage", "margin"], "DOUBLE", pickColumnExpr(columns, ["Margin_PCT_Avg"], "DOUBLE", "0"));
  const salePriceExpr = pickColumnExpr(columns, ["selling_price", "sale_price", "sell_price_avg", "price"], "DOUBLE", pickColumnExpr(columns, ["Sell_Price_Avg"], "DOUBLE", "0"));
  const originalPriceExpr = pickColumnExpr(columns, ["original_price", "list_price", "price_original"], "DOUBLE", pickColumnExpr(columns, ["Original_Price_Avg"], "DOUBLE", "0"));
  const orderDateExpr = pickColumnExpr(columns, ["order_date", "date", "latest_order", "created_at"], "TIMESTAMP", pickColumnExpr(columns, ["Latest_Order"], "TIMESTAMP", "CURRENT_TIMESTAMP"));
  const weekRawExpr = pickColumnExpr(columns, ["week_number", "week", "week_no", "weeknr"], "VARCHAR", `CAST(EXTRACT(WEEK FROM ${orderDateExpr}) AS VARCHAR)`);
  const weekExpr = `COALESCE(
    TRY_CAST(${weekRawExpr} AS INTEGER),
    TRY_CAST(regexp_extract(CAST(${weekRawExpr} AS VARCHAR), '(\\d+)', 1) AS INTEGER),
    EXTRACT(WEEK FROM ${orderDateExpr})::INTEGER
  )`;
  const discountRawExpr = pickColumnExpr(columns, ["discount_applied", "discount", "discounted"], "VARCHAR", pickColumnExpr(columns, ["Discount_Pct_Avg"], "VARCHAR", "'No'"));
  const eanExpr = pickColumnExpr(columns, ["ean", "sku"], "VARCHAR", "''");
  const cogsExpr = pickColumnExpr(columns, ["cogs", "cogs_avg", "cost"], "DOUBLE", "0");
  const adCostExpr = pickColumnExpr(columns, ["estimated_ad_cost", "ad_cost", "ads", "ad_cost_avg"], "DOUBLE", "0");
  const marginEurExpr = pickColumnExpr(columns, ["net_margin_eur", "margin_eur", "margin_eur_avg", "profit", "total_profit"], "DOUBLE", "0");
  const netSellExpr = pickColumnExpr(columns, ["net_selling_price", "net_sell"], "DOUBLE", `(${salePriceExpr} / 1.21)`);
  const discPctExpr = pickColumnExpr(columns, ["discount_used_pct", "discount_pct", "discount_pct_avg"], "DOUBLE", "0");

  const discountExpr = `CASE
    WHEN lower(trim(CAST(${discountRawExpr} AS VARCHAR))) IN ('yes','y','true','1','with_discount') THEN TRUE
    WHEN TRY_CAST(${discountRawExpr} AS DOUBLE) IS NOT NULL AND TRY_CAST(${discountRawExpr} AS DOUBLE) > 0 THEN TRUE
    ELSE FALSE
  END`;

  const sql = `
    CREATE OR REPLACE TEMP VIEW sales_norm AS
    SELECT
      COALESCE(${productExpr}, '(Unknown)') AS product_name,
      COALESCE(${eanExpr}, '') AS ean,
      COALESCE(${unitsExpr}, 0) AS units_sold,
      COALESCE(${revenueExpr}, 0) AS revenue,
      ROUND(COALESCE(${unitsExpr}, 0) * COALESCE(${netSellExpr}, 0), 2) AS revenue_ex_vat,
      COALESCE(${netSellExpr}, 0) AS net_selling_price,
      COALESCE(${salePriceExpr}, 0) AS selling_price,
      COALESCE(NULLIF(${originalPriceExpr}, 0), ${salePriceExpr}, 0) AS original_price,
      CASE 
        WHEN ${originalPriceExpr} > 0 AND ${salePriceExpr} > 0 
        THEN ROUND((${originalPriceExpr} - ${salePriceExpr}) / ${originalPriceExpr} * 100, 1)
        ELSE COALESCE(${discPctExpr}, 0)
      END AS discount_used_pct,
      COALESCE(${cogsExpr}, 0) AS cogs,
      COALESCE(${adCostExpr}, 0) AS estimated_ad_cost,
      COALESCE(${marginEurExpr}, 0) AS net_margin_eur,
      GREATEST(-100, LEAST(100, COALESCE(${marginExpr}, 0))) AS net_margin_pct,
      CAST(${orderDateExpr} AS DATE) AS order_date,
      COALESCE(${weekExpr}, EXTRACT(WEEK FROM ${orderDateExpr}))::INTEGER AS week_number,
      ${discountExpr} AS discount_applied
    FROM ${relationSql}
  `;

  await state.conn.query(sql);
}

export async function loadDefaultData() {
  try {
    const absUrls = await resolveDefaultSourceUrls();

    const relations = [];
    for (let i = 0; i < absUrls.length; i += 1) {
      const registered = `sales_source_${i}.csv`;
      await state.db.registerFileURL(registered, absUrls[i], duckdb.DuckDBDataProtocol.HTTP, true);
      relations.push(`SELECT * FROM read_csv_auto('${registered}', header=true, SAMPLE_SIZE=-1)`);
    }

    const relationSql = relations.length === 1
      ? `(${relations[0]})`
      : `(${relations.join(" UNION ALL ")})`;

    await buildNormalizedView(relationSql);

    if (absUrls.length === 1) {
      setStatus(UI_ELEMENTS, `Loaded default source: ${absUrls[0].split("/").pop()}`);
    } else {
      setStatus(UI_ELEMENTS, `Loaded ${absUrls.length} weekly source files.`);
    }
  } catch (err) {
    console.warn("Default data loading failed.", err);
    setStatus(UI_ELEMENTS, `Default data not found. Please click "Choose File" to load your data.`, false);
  }
}

export async function loadUploadedFile(file) {
  const fileName = file.name;
  const safeName = `upload_${Date.now()}_${fileName.replace(/[^a-zA-Z0-9._-]/g, "_")}`;
  const bytes = new Uint8Array(await file.arrayBuffer());
  await state.db.registerFileBuffer(safeName, bytes);

  const lower = fileName.toLowerCase();
  if (lower.endsWith(".csv")) {
    await buildNormalizedView(`read_csv_auto('${safeName}', header=true, SAMPLE_SIZE=-1)`);
  } else if (lower.endsWith(".parquet")) {
    await buildNormalizedView(`read_parquet('${safeName}')`);
  } else if (lower.endsWith(".duckdb")) {
    await state.conn.query("DROP VIEW IF EXISTS sales_norm");
    try {
      await state.conn.query("DETACH uploaded_db;");
    } catch (_err) {}
    await state.conn.query(`ATTACH '${safeName}' AS uploaded_db (READ_ONLY);`);
    const tablesRes = await state.conn.query("SHOW TABLES FROM uploaded_db;");
    const tables = tablesRes.toArray().map((r) => r.name ?? r.table_name ?? Object.values(r)[0]).filter(Boolean);
    if (tables.length === 0) throw new Error("No tables found in uploaded DuckDB file.");
    const firstTable = String(tables[0]);
    await buildNormalizedView(`uploaded_db.${quoteIdent(firstTable)}`);
  } else {
    throw new Error("Unsupported format.");
  }
}

export async function fetchRowsFromView() {
  const rs = await state.conn.query(`
    SELECT
      product_name,
      ean,
      CAST(units_sold AS DOUBLE) AS units_sold,
      CAST(revenue AS DOUBLE) AS revenue,
      CAST(revenue_ex_vat AS DOUBLE) AS revenue_ex_vat,
      CAST(net_selling_price AS DOUBLE) AS net_selling_price,
      CAST(selling_price AS DOUBLE) AS selling_price,
      CAST(original_price AS DOUBLE) AS original_price,
      CAST(discount_used_pct AS DOUBLE) AS discount_used_pct,
      CAST(cogs AS DOUBLE) AS cogs,
      CAST(estimated_ad_cost AS DOUBLE) AS estimated_ad_cost,
      CAST(net_margin_eur AS DOUBLE) AS net_margin_eur,
      CAST(net_margin_pct AS DOUBLE) AS net_margin_pct,
      CAST(order_date AS VARCHAR) AS order_date,
      CAST(week_number AS INTEGER) AS week_number,
      discount_applied
    FROM sales_norm
  `);

  state.rows = rs.toArray().map((r) => ({
    product_name: String(r.product_name ?? "(Unknown)"),
    ean: String(r.ean ?? ""),
    units_sold: Number(r.units_sold ?? 0),
    revenue: Number(r.revenue ?? 0),
    revenue_ex_vat: Number(r.revenue_ex_vat ?? 0),
    net_selling_price: Number(r.net_selling_price ?? 0),
    selling_price: Number(r.selling_price ?? 0),
    original_price: Number(r.original_price ?? 0),
    discount_used_pct: Number(r.discount_used_pct ?? 0),
    cogs: Number(r.cogs ?? 0),
    estimated_ad_cost: Number(r.estimated_ad_cost ?? 0),
    net_margin_eur: Number(r.net_margin_eur ?? 0),
    net_margin_pct: Number(r.net_margin_pct ?? 0),
    order_date: r.order_date ? String(r.order_date).slice(0, 10) : "",
    week_number: Number(r.week_number ?? 0),
    discount_applied: toDiscountBool(r.discount_applied),
  }));
}
