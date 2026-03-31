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

window.addEventListener("DOMContentLoaded", () => {
  loadHealth();
  loadProducts();

  document.getElementById("refreshProducts").addEventListener("click", loadProducts);
  document.getElementById("searchForm").addEventListener("submit", runSearch);
  document.getElementById("gateForm").addEventListener("submit", markGate);
});
