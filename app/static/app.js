const $ = (id) => document.getElementById(id);
const api = (p) => fetch(p).then(r => r.json());

// ---------- Tabs ----------
document.querySelectorAll(".tab").forEach(t => {
  t.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(x => x.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(x => x.classList.remove("active"));
    t.classList.add("active");
    $(t.dataset.tab).classList.add("active");
  });
});

// ---------- Load categories ----------
let CATS = [];
api("/api/categories").then(data => {
  CATS = data;
  const sel = $("n_type");
  data.forEach(c => {
    const o = document.createElement("option");
    o.value = c.type_id; o.textContent = `${c.category_name} (health ${c.health_score})`;
    sel.appendChild(o);
  });
});

// ---------- Search ----------
function renderItem(e, actions = "") {
  const fresh = e.fresh_food ? `<span class="badge fresh">fresh</span>` : "";
  return `<div class="item">
    <div>
      <div><strong>${escapeHtml(e.name)}</strong> ${fresh}</div>
      <div class="meta">${escapeHtml(e.address || "")} ${e.zipcode ? "• " + e.zipcode : ""}</div>
      <div class="meta">tract ${e.tract_id || "—"} • ${e.category_name}</div>
    </div>
    <div>${actions}</div>
  </div>`;
}
function escapeHtml(s) { return (s ?? "").toString().replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c])); }

$("searchBtn").addEventListener("click", async () => {
  const params = new URLSearchParams();
  if ($("zip").value) params.set("zip", $("zip").value.trim());
  if ($("name").value) params.set("name", $("name").value.trim());
  const data = await api("/api/establishments?" + params);
  const sm = data.filter(e => e.health_score >= 5);
  const ff = data.filter(e => e.health_score < 5);
  $("smCount").textContent = sm.length;
  $("ffCount").textContent = ff.length;
  $("smList").innerHTML = sm.map(e => renderItem(e)).join("") || '<div class="item meta">None found</div>';
  $("ffList").innerHTML = ff.map(e => renderItem(e)).join("") || '<div class="item meta">None found</div>';
});

// ---------- Swamp ----------
let swampMode = "county";

// Load county autocomplete
api("/api/counties").then(data => {
  const dl = $("countyList");
  data.forEach(c => {
    const o = document.createElement("option");
    o.value = c;
    dl.appendChild(o);
  });
});

// Mode toggle
document.querySelectorAll(".toggle-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".toggle-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    swampMode = btn.dataset.mode;
    $("countyInputs").style.display = swampMode === "county" ? "" : "none";
    $("zipInputs").style.display    = swampMode === "zip"    ? "" : "none";
    $("swampResult").innerHTML = "";
  });
});

$("swampBtn").addEventListener("click", async () => {
  let url;
  if (swampMode === "county") {
    const v = $("countyQ").value.trim();
    if (!v) { $("swampResult").innerHTML = '<div class="hint">Enter a county name.</div>'; return; }
    url = `/api/swamp-index?county=${encodeURIComponent(v)}`;
  } else {
    const v = $("zipQ").value.trim();
    if (!/^\d{5}$/.test(v)) { $("swampResult").innerHTML = '<div class="hint">Enter a valid 5-digit ZIP code.</div>'; return; }
    url = `/api/swamp-index?zipcode=${encodeURIComponent(v)}`;
  }

  $("swampResult").innerHTML = '<div class="hint">Calculating…</div>';
  const r = await api(url);
  if (r.error) { $("swampResult").innerHTML = `<div class="hint">${r.error}</div>`; return; }

  const income = r.median_income ? "$" + r.median_income.toLocaleString() : "—";
  const tractLabel = r.tract_count === 1 ? "1 census tract" : `${r.tract_count} census tracts`;
  $("swampResult").innerHTML = `
    <h3>${escapeHtml(r.area_name)}</h3>
    <div class="swamp-verdict">${r.interpretation}</div>
    <div class="stats">
      <div class="stat"><div class="n">${r.healthy_count}</div><div class="l">Healthy</div></div>
      <div class="stat"><div class="n">${r.unhealthy_count}</div><div class="l">Unhealthy</div></div>
      <div class="stat"><div class="n">${r.rfei ?? "∞"}</div><div class="l">RFEI</div></div>
      <div class="stat"><div class="n">${r.obesity_rate ? r.obesity_rate.toFixed(1) + "%" : "—"}</div><div class="l">Obesity (${r.obesity_year ?? "—"})</div></div>
    </div>
    <div class="hint">${tractLabel} &nbsp;•&nbsp; Population: ${r.population?.toLocaleString() ?? "—"} &nbsp;•&nbsp; Median Income: ${income}</div>
    <h4>Sample establishments</h4>
    <div class="list">${r.sample.map(s => `<div class="item">
        <div><strong>${escapeHtml(s.name)}</strong> <span class="meta">${s.category_name}</span></div>
        <div class="meta">${s.latitude.toFixed(4)}, ${s.longitude.toFixed(4)}</div>
      </div>`).join("") || '<div class="item meta">No establishments found</div>'}</div>
  `;
});

// ---------- Analytics ----------
$("analyticsBtn").addEventListener("click", async () => {
  const n = $("topN").value || 25;
  const rows = await api("/api/analytics/by-tract?limit=" + n);
  const tbody = document.querySelector("#analyticsTable tbody");
  tbody.innerHTML = rows.map(r => {
    const rfei = r.healthy > 0 ? (r.unhealthy / r.healthy).toFixed(2) : "∞";
    const income = r.median_income ? "$" + r.median_income.toLocaleString() : "—";
    return `<tr>
      <td>${r.tract_id}</td><td>${r.county}</td>
      <td>${r.fast_food}</td><td>${r.convenience}</td>
      <td>${r.supermarket}</td><td>${r.food_bank}</td>
      <td>${r.total}</td><td>${rfei}</td>
      <td>${r.population?.toLocaleString() ?? "—"}</td><td>${income}</td>
    </tr>`;
  }).join("");
});

// ---------- Manage ----------
$("addBtn").addEventListener("click", async () => {
  const body = {
    name: $("n_name").value.trim(),
    latitude: parseFloat($("n_lat").value),
    longitude: parseFloat($("n_lon").value),
    address: $("n_addr").value.trim(),
    zipcode: $("n_zip").value.trim(),
    tract_id: $("n_tract").value.trim() || null,
    type_id: parseInt($("n_type").value, 10),
    fresh_food: $("n_fresh").checked,
  };
  const res = await fetch("/api/establishments", {
    method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body)
  }).then(r => r.json());
  $("addMsg").textContent = res.error ? "Error: " + res.error : `Added store_id ${res.store_id}`;
});

$("m_searchBtn").addEventListener("click", async () => {
  const q = $("m_q").value.trim();
  const params = new URLSearchParams();
  if (/^\d+$/.test(q)) params.set("zip", q); else if (q) params.set("name", q);
  params.set("limit", "50");
  const data = await api("/api/establishments?" + params);
  $("manageList").innerHTML = data.map(e => renderItem(e, `
    <button data-id="${e.store_id}" data-fresh="${e.fresh_food ? 0 : 1}" class="toggle">Toggle fresh</button>
    <button data-id="${e.store_id}" class="del danger">Delete</button>
  `)).join("") || '<div class="item meta">None</div>';

  $("manageList").querySelectorAll(".toggle").forEach(b => b.addEventListener("click", async () => {
    await fetch(`/api/establishments/${b.dataset.id}`, {
      method: "PATCH", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ fresh_food: b.dataset.fresh === "1" })
    });
    $("m_searchBtn").click();
  }));
  $("manageList").querySelectorAll(".del").forEach(b => b.addEventListener("click", async () => {
    if (!confirm("Delete this establishment?")) return;
    await fetch(`/api/establishments/${b.dataset.id}`, { method: "DELETE" });
    $("m_searchBtn").click();
  }));
});
