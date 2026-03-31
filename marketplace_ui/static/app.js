async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

function badgeClass(status) {
  if (!status) return "warn";
  const value = String(status).toLowerCase();
  if (value.includes("pass")) return "pass";
  if (value.includes("fail")) return "fail";
  return "warn";
}

// ---------------------------------------------------------------------------
// Tab navigation with lazy loading
// ---------------------------------------------------------------------------
const loaded = {};

function initTabs() {
  const tabs = document.querySelectorAll(".tab");
  const panels = document.querySelectorAll(".tab-content");

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      tabs.forEach((t) => t.classList.remove("active"));
      panels.forEach((p) => p.classList.remove("active"));
      tab.classList.add("active");
      const target = tab.dataset.tab;
      document.getElementById(`tab-${target}`).classList.add("active");

      if (target === "analytics" && !loaded.analytics) {
        loaded.analytics = true;
        loadAnalytics();
      }
      if (target === "quality" && !loaded.quality) {
        loaded.quality = true;
        loadQuality();
      }
      if (target === "features" && !loaded.features) {
        loaded.features = true;
        loadFeatures();
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Existing: Health check
// ---------------------------------------------------------------------------
async function loadHealth() {
  const el = document.getElementById("health");
  try {
    const data = await fetchJson("/api/health");
    if (data.status === "ok") {
      el.textContent = `Connected: ${data.snowflake.ACCOUNT_NAME} @ ${data.snowflake.TS}`;
      el.style.color = "#15803d";
    } else {
      el.textContent = `Connection issue: ${data.message}`;
      el.style.color = "#b91c1c";
    }
  } catch (err) {
    el.textContent = `Connection issue: ${err.message}`;
    el.style.color = "#b91c1c";
  }
}

// ---------------------------------------------------------------------------
// Existing: Products
// ---------------------------------------------------------------------------
async function loadProducts() {
  const container = document.getElementById("products");
  container.innerHTML = "Loading...";
  try {
    const products = await fetchJson("/api/products");
    if (!products.length) {
      container.innerHTML = "No products found.";
      return;
    }

    container.innerHTML = products
      .map((p) => `
        <article class="product">
          <span class="badge ${badgeClass(p.SLA_STATUS)}">${p.SLA_STATUS}</span>
          <h3>${p.PRODUCT_NAME}</h3>
          <small>Owner: ${p.OWNER} | Refresh: ${p.REFRESH_FREQUENCY}</small>
          <small>Rows: ${p.CURRENT_ROW_COUNT} | Lag hours/days: ${p.HOURS_SINCE_REFRESH}</small>
          <small>Last refreshed: ${p.LAST_REFRESHED_AT}</small>
          <small>${p.TABLE_COMMENT || "No table comment available."}</small>
        </article>
      `)
      .join("");
  } catch (err) {
    container.innerHTML = `<span style="color:#b91c1c">${err.message}</span>`;
  }
}

// ---------------------------------------------------------------------------
// Existing: Search
// ---------------------------------------------------------------------------
async function runSearch(event) {
  event.preventDefault();
  const q = document.getElementById("query").value.trim();
  const container = document.getElementById("results");
  if (q.length < 2) return;

  container.innerHTML = "Searching...";
  try {
    const results = await fetchJson(`/api/search?q=${encodeURIComponent(q)}`);
    if (!results.length) {
      container.innerHTML = "No semantic metadata matches found.";
      return;
    }

    container.innerHTML = results
      .map((r) => `
        <article class="result">
          <strong>${r.ENTITY_NAME}.${r.COLUMN_NAME}</strong>
          <div>${r.BUSINESS_DEFINITION}</div>
          <small>Example: ${r.EXAMPLE_VALUE || "-"} | Score: ${r.RELEVANCE_SCORE} | Refreshed: ${r._REFRESHED_AT}</small>
        </article>
      `)
      .join("");
  } catch (err) {
    container.innerHTML = `<span style="color:#b91c1c">${err.message}</span>`;
  }
}

// ---------------------------------------------------------------------------
// Existing: Manual gate
// ---------------------------------------------------------------------------
async function markGate(event) {
  event.preventDefault();
  const note = document.getElementById("gateNote").value.trim();
  const out = document.getElementById("gateResponse");
  try {
    const payload = await fetchJson(`/api/manual-gate/marketplace-pass?note=${encodeURIComponent(note)}`, {
      method: "POST",
    });
    out.textContent = JSON.stringify(payload, null, 2);
  } catch (err) {
    out.textContent = err.message;
  }
}

// ---------------------------------------------------------------------------
// Analytics tab
// ---------------------------------------------------------------------------
const COLORS = { pass: "#15803d", fail: "#b91c1c", warn: "#f59e0b", teal: "#0f766e", amber: "#f59e0b", muted: "#6b7280" };

async function loadAnalytics() {
  try {
    const [sla, rows, monthly] = await Promise.all([
      fetchJson("/api/analytics/sla-summary"),
      fetchJson("/api/analytics/row-counts"),
      fetchJson("/api/analytics/monthly-sales"),
    ]);
    renderSlaChart(sla);
    renderRowChart(rows);
    renderMonthlySalesChart(monthly);
  } catch (err) {
    document.getElementById("tab-analytics").innerHTML =
      `<section class="card"><span style="color:#b91c1c">Failed to load analytics: ${err.message}</span></section>`;
  }
}

function renderSlaChart(data) {
  const ctx = document.getElementById("chartSla").getContext("2d");
  const labels = data.map((d) => d.SLA_STATUS || "UNKNOWN");
  const values = data.map((d) => d.CNT);
  const colors = labels.map((l) => {
    const lc = l.toLowerCase();
    if (lc.includes("pass")) return COLORS.pass;
    if (lc.includes("fail")) return COLORS.fail;
    return COLORS.warn;
  });
  new Chart(ctx, {
    type: "doughnut",
    data: { labels, datasets: [{ data: values, backgroundColor: colors, borderWidth: 0 }] },
    options: { responsive: true, plugins: { legend: { position: "bottom" } } },
  });
}

function renderRowChart(data) {
  const ctx = document.getElementById("chartRows").getContext("2d");
  new Chart(ctx, {
    type: "bar",
    data: {
      labels: data.map((d) => d.PRODUCT_NAME),
      datasets: [{ label: "Row Count", data: data.map((d) => d.CURRENT_ROW_COUNT), backgroundColor: COLORS.teal, borderRadius: 6 }],
    },
    options: { indexAxis: "y", responsive: true, plugins: { legend: { display: false } } },
  });
}

function renderMonthlySalesChart(data) {
  const ctx = document.getElementById("chartMonthlySales").getContext("2d");
  new Chart(ctx, {
    type: "line",
    data: {
      labels: data.map((d) => d.YEAR_MONTH),
      datasets: [
        { label: "Revenue", data: data.map((d) => d.TOTAL_REVENUE), borderColor: COLORS.teal, backgroundColor: "rgba(15,118,110,0.1)", fill: true, tension: 0.3, yAxisID: "y" },
        { label: "Orders", data: data.map((d) => d.TOTAL_ORDERS), borderColor: COLORS.amber, backgroundColor: "rgba(245,158,11,0.1)", fill: true, tension: 0.3, yAxisID: "y1" },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { type: "linear", position: "left", title: { display: true, text: "Revenue" } },
        y1: { type: "linear", position: "right", title: { display: true, text: "Orders" }, grid: { drawOnChartArea: false } },
      },
    },
  });
}

// ---------------------------------------------------------------------------
// Quality & Governance tab
// ---------------------------------------------------------------------------
async function loadQuality() {
  try {
    const [gates, dq, rejected, jobs] = await Promise.all([
      fetchJson("/api/quality/release-gates"),
      fetchJson("/api/quality/dq-log"),
      fetchJson("/api/quality/rejected"),
      fetchJson("/api/quality/snowpark-jobs"),
    ]);
    renderGates(gates);
    renderDqLog(dq);
    renderRejected(rejected);
    renderSnowparkJobs(jobs);
  } catch (err) {
    document.getElementById("tab-quality").innerHTML =
      `<section class="card"><span style="color:#b91c1c">Failed to load quality data: ${err.message}</span></section>`;
  }
}

function renderGates(data) {
  const el = document.getElementById("gates");
  if (!data.length) { el.innerHTML = "No release gate results found."; return; }
  el.innerHTML = data.map((g) => `
    <article class="product">
      <span class="badge ${badgeClass(g.STATUS)}">${g.STATUS}</span>
      <h3>${g.GATE_ID}: ${g.GATE_NAME}</h3>
      <small>Actual: ${g.ACTUAL_VALUE} | Expected: ${g.EXPECTED_VALUE}</small>
      <small>${g.DETAILS || ""}</small>
      <small>Evaluated: ${g.EVALUATED_AT}</small>
    </article>
  `).join("");
}

function renderDqLog(data) {
  const el = document.getElementById("dqLog");
  if (!data.length) { el.innerHTML = "No DQ log entries found."; return; }
  el.innerHTML = `<table class="data-table">
    <thead><tr><th>Table</th><th>Dimension</th><th>Description</th><th>Passed</th><th>Failed</th><th>Pass %</th><th>Status</th></tr></thead>
    <tbody>${data.map((r) => `<tr>
      <td>${r.TARGET_TABLE}</td><td>${r.DQ_DIMENSION}</td><td>${r.CHECK_DESCRIPTION || ""}</td>
      <td>${r.RECORDS_PASSED}</td><td>${r.RECORDS_FAILED}</td><td>${r.PASS_RATE_PCT}%</td>
      <td><span class="badge ${badgeClass(r.STATUS)}">${r.STATUS}</span></td>
    </tr>`).join("")}</tbody>
  </table>`;
}

function renderRejected(data) {
  const el = document.getElementById("rejected");
  if (!data.length) { el.innerHTML = "No rejected records found."; return; }
  el.innerHTML = data.map((r) => `
    <article class="product">
      <h3>${r.REJECTION_REASON}</h3>
      <small>${r.REJECTED_ROWS.toLocaleString()} rejected rows</small>
    </article>
  `).join("");
}

function renderSnowparkJobs(data) {
  const el = document.getElementById("snowparkJobs");
  if (!data.length) { el.innerHTML = "No Snowpark job records found."; return; }
  el.innerHTML = `<table class="data-table">
    <thead><tr><th>Job</th><th>Status</th><th>Message</th><th>Run At</th></tr></thead>
    <tbody>${data.map((r) => `<tr>
      <td>${r.JOB_NAME}</td>
      <td><span class="badge ${badgeClass(r.STATUS)}">${r.STATUS}</span></td>
      <td>${r.MESSAGE || ""}</td><td>${r.RUN_AT}</td>
    </tr>`).join("")}</tbody>
  </table>`;
}

// ---------------------------------------------------------------------------
// Feature Store tab
// ---------------------------------------------------------------------------
async function loadFeatures() {
  try {
    const [summary, registry, lineage] = await Promise.all([
      fetchJson("/api/features/summary"),
      fetchJson("/api/features/registry"),
      fetchJson("/api/features/lineage"),
    ]);
    renderFeatureSummary(summary);
    renderFeatureRegistry(registry);
    renderFeatureLineage(lineage);
  } catch (err) {
    document.getElementById("tab-features").innerHTML =
      `<section class="card"><span style="color:#b91c1c">Failed to load feature store: ${err.message}</span></section>`;
  }
}

function renderFeatureSummary(data) {
  const el = document.getElementById("featureSummary");
  el.innerHTML = data.map((m) => `
    <div class="stat-card">
      <span class="stat-value">${m.VALUE}</span>
      <span class="stat-label">${m.METRIC}</span>
    </div>
  `).join("");
}

function renderFeatureRegistry(data) {
  const el = document.getElementById("featureRegistry");
  if (!data.length) { el.innerHTML = "No features found."; return; }
  el.innerHTML = data.map((f) => {
    const flags = [];
    if (f.IS_POINT_IN_TIME) flags.push("PIT");
    if (f.OFFLINE_ENABLED) flags.push("Offline");
    if (f.ONLINE_ENABLED) flags.push("Online");
    return `
      <article class="product feature-card">
        <div class="feature-header">
          <span class="badge entity-badge">${f.ENTITY_TYPE}</span>
          <span class="feature-version">v${f.VERSION}</span>
        </div>
        <h3>${f.FEATURE_NAME}</h3>
        <small>${f.DESCRIPTION || "No description"}</small>
        <small>Type: ${f.DATA_TYPE} | Owner: ${f.OWNER_TEAM}</small>
        <small>Source: ${f.LINEAGE_SOURCE_TABLE || "-"}</small>
        ${f.TAGS ? `<small>Tags: ${f.TAGS}</small>` : ""}
        <div class="feature-flags">${flags.map((fl) => `<span class="flag">${fl}</span>`).join("")}</div>
      </article>
    `;
  }).join("");
}

function renderFeatureLineage(data) {
  const el = document.getElementById("featureLineage");
  if (!data.length) { el.innerHTML = "No lineage data found."; return; }

  // Group by downstream feature
  const groups = {};
  data.forEach((d) => {
    const key = d.DOWNSTREAM_NAME || d.DOWNSTREAM_FEATURE_ID;
    if (!groups[key]) groups[key] = [];
    groups[key].push(d);
  });

  el.innerHTML = Object.entries(groups).map(([downstream, deps]) => `
    <div class="lineage-group">
      <div class="lineage-downstream">${downstream}</div>
      <div class="lineage-deps">
        ${deps.map((d) => `
          <div class="lineage-dep">
            <span class="lineage-arrow">&#8592;</span>
            <span>${d.UPSTREAM_NAME}</span>
            <span class="lineage-type">${d.DEPENDENCY_TYPE}</span>
          </div>
        `).join("")}
      </div>
    </div>
  `).join("");
}

async function searchFeatures(event) {
  event.preventDefault();
  const q = document.getElementById("featureQuery").value.trim();
  if (q.length < 2) return;
  const el = document.getElementById("featureRegistry");
  el.innerHTML = "Searching...";
  try {
    const data = await fetchJson(`/api/features/registry?q=${encodeURIComponent(q)}`);
    renderFeatureRegistry(data);
  } catch (err) {
    el.innerHTML = `<span style="color:#b91c1c">${err.message}</span>`;
  }
}

async function showAllFeatures() {
  const el = document.getElementById("featureRegistry");
  el.innerHTML = "Loading...";
  try {
    const data = await fetchJson("/api/features/registry");
    renderFeatureRegistry(data);
  } catch (err) {
    el.innerHTML = `<span style="color:#b91c1c">${err.message}</span>`;
  }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
window.addEventListener("DOMContentLoaded", () => {
  initTabs();
  loadHealth();
  loadProducts();

  document.getElementById("refreshProducts").addEventListener("click", loadProducts);
  document.getElementById("searchForm").addEventListener("submit", runSearch);
  document.getElementById("gateForm").addEventListener("submit", markGate);
  document.getElementById("refreshGates").addEventListener("click", () => { loadQuality(); });
  document.getElementById("featureSearchForm").addEventListener("submit", searchFeatures);
  document.getElementById("featureShowAll").addEventListener("click", showAllFeatures);
});
