(() => {
  "use strict";
  const root = document.querySelector("[data-setup-analytics]");
  if (!root) return;
  const form = root.querySelector("[data-analytics-filters]");
  const status = root.querySelector("[data-analytics-status]");
  const kpis = root.querySelector("[data-analytics-kpis]");
  const ledger = root.querySelector("[data-analytics-ledger]");
  const sort = root.querySelector("[data-analytics-sort]");
  const pageLabel = root.querySelector("[data-page-label]");
  const previous = root.querySelector("[data-page-prev]");
  const next = root.querySelector("[data-page-next]");
  let page = 1;
  let lastPayload = null;
  let contractQuantity = 3;
  const contractInput = root.querySelector("[data-analytics-contracts]");
  const contractFeedback = root.querySelector("[data-contracts-feedback]");

  const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[char]));
  const number = (value, digits = 1) => value === null || value === undefined ? "—" : Number(value).toFixed(digits);
  const percent = (value) => value === null || value === undefined ? "—" : `${number(value)}%`;
  const formatStamp = (value) => value ? new Intl.DateTimeFormat("en-US", {month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York"}).format(new Date(value)) : "—";
  const outcomeLabel = (value) => ({target_reached: "Target reached", invalidated: "Invalidated", open: "Awaiting resolution", ambiguous: "Ambiguous", unavailable: "Historical outcome not captured"}[value] || value || "Outcome unavailable");
  const outcomeClass = (value) => `is-${value === "target_reached" ? "target" : value || "unavailable"}`;
  const empty = (message) => `<div class="setupAnalyticsEmpty">${escapeHtml(message)}</div>`;

  function query() {
    const params = new URLSearchParams(new FormData(form));
    [...params.entries()].forEach(([key, value]) => { if (!value) params.delete(key); });
    params.set("sort", sort.value);
    params.set("page", String(page));
    return params;
  }

  function setOptions(name, values, selected) {
    const select = form.elements[name];
    const first = select.options[0].outerHTML;
    select.innerHTML = first + values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value.replaceAll("_", " "))}</option>`).join("");
    select.value = selected || "";
  }

  function renderInsight(insights, coverage) {
    const insight = insights.best_time_bucket;
    const copy = root.querySelector("[data-analytics-insight]");
    copy.innerHTML = insight ? `<span class="setupAnalyticsKicker">Best window in this sample</span><strong>${escapeHtml(insight.label)}</strong><p>${percent(insight.target_reached_rate)} target · ${insight.target_reached_count}/${insight.evaluated_count} evaluated${insight.early_evidence ? " · <em>early evidence</em>" : ""}</p>` : `<span class="setupAnalyticsKicker">Best window</span><strong>Evidence still building</strong><p>Frequency is available; outcomes are incomplete.</p>`;
    const coveragePercent = coverage.outcome_coverage_percent || 0;
    root.querySelector("[data-analytics-gauge]").innerHTML = `<div class="setupAnalyticsGaugeRing" style="--coverage:${coveragePercent * 3.6}deg"><strong>${percent(coverage.outcome_coverage_percent)}</strong></div><span>Outcome coverage<small>${coverage.complete_outcome_count} of ${coverage.complete_outcome_count + coverage.missing_outcome_count} setups evaluated</small></span>`;
  }

  function renderKpis(metrics, coverage) {
    const cards = [
      ["Setups", metrics.total_setups, `${metrics.covered_sessions} sessions`, "frequency"],
      ["Per session", number(metrics.setups_per_session, 2), "average frequency", "frequency"],
      ["Target reached", percent(metrics.target_reached_rate), `${metrics.target_reached_count}/${metrics.terminal_sample_size} evaluated`, "positive"],
      ["Invalidated", percent(metrics.invalidation_rate), `${metrics.invalidated_count}/${metrics.terminal_sample_size} evaluated`, "negative"],
      ["Median favorable / adverse", `${number(metrics.median_mfe.value)} / ${number(metrics.median_mae.value)}`, `SPX pts · n=${Math.min(metrics.median_mfe.sample_size, metrics.median_mae.sample_size)}`, "neutral"],
      ["Legacy", coverage.missing_outcome_count, "frequency only", "legacy"],
    ];
    kpis.innerHTML = cards.map(([label, value, note, tone]) => `<article class="setupAnalyticsKpi is-${tone}"><small>${label}</small><strong>${value}</strong><span>${note}</span></article>`).join("");
  }

  function heatmapMarkup(rows) {
    if (!rows.length) return empty("No setup evidence in this range.");
    const maxOccurrences = Math.max(1, ...rows.map((row) => row.total_occurrences));
    return `<div class="setupAnalyticsProfilePlot">${rows.map((row) => {
      const rate = row.target_reached_rate;
      const tone = rate === null ? "unknown" : rate >= 60 ? "strong" : rate >= 40 ? "mixed" : "weak";
      const height = Math.max(12, row.total_occurrences / maxOccurrences * 100);
      return `<figure class="setupAnalyticsProfileBucket is-${tone}" title="${escapeHtml(row.bucket)} · ${row.total_occurrences} setups · ${row.evaluated_count} completed outcomes"><div class="setupAnalyticsProfileValue"><span>${row.total_occurrences}</span><small>setups</small></div><div class="setupAnalyticsProfileBar"><i style="height:${height}%"></i><b>${rate === null ? "No completed outcomes" : percent(rate)}</b></div><figcaption><span>${escapeHtml(row.bucket)}</span><small>completed ${row.evaluated_count} of ${row.total_occurrences}</small></figcaption></figure>`;
    }).join("")}</div><div class="setupAnalyticsProfileLegend"><span><i class="is-strong"></i>60%+ target</span><span><i class="is-mixed"></i>40–59%</span><span><i class="is-weak"></i>Below 40%</span><span><i class="is-unknown"></i>No captured outcomes</span></div>`;
  }

  function renderHeatmap(rows) {
    const markup = heatmapMarkup(rows);
    root.querySelectorAll('[data-chart^="heatmap-"]').forEach((node) => { node.innerHTML = markup; });
  }

  function anchorFor(assumptions, direction) {
    return assumptions.anchors?.[direction] || {contract_cost: assumptions.contract_cost, absolute_delta: assumptions.absolute_delta, pricing_mode: "fallback_estimate", fallback_reason: "direction_not_available"};
  }

  function validContractQuantity(value) {
    const quantity = Number(value);
    return Number.isInteger(quantity) && quantity >= 1 && quantity <= 1000;
  }

  function profitEstimate(mfe, assumptions, direction, quantity = 3) {
    if (mfe === null || mfe === undefined) return null;
    const anchor = anchorFor(assumptions, direction);
    const displayedMfe = Number(Number(mfe).toFixed(1));
    const dollars = displayedMfe * anchor.absolute_delta * assumptions.multiplier;
    const returnPercent = dollars / anchor.contract_cost * 100;
    return {anchor, displayedMfe, dollars, returnPercent, totalDollars: dollars * quantity, totalCost: anchor.contract_cost * quantity};
  }

  function occurrenceMarkup(row) {
    const events = row.occurrences || [];
    if (!events.length) return "";
    const latest = events[0];
return `<p class="setupAnalyticsOccurrenceLatest">Latest: ${formatStamp(latest.signal_time)} ET · ${escapeHtml(latest.level_label)} ${number(latest.level_value, 2)}</p><details class="setupAnalyticsOccurrences"><summary>What happened · ${events.length < row.total_occurrences ? "latest " : ""}${events.length} ${events.length === 1 ? "setup" : "setups"}</summary>${events.map((event) => `<article><strong>${formatStamp(event.signal_time)} ET · ${escapeHtml(event.pattern_code || "Pattern not recorded")}</strong><p>${escapeHtml(event.direction)} · ${escapeHtml(event.level_label)} at ${number(event.level_value, 2)}</p><p>Entry ${number(event.entry_value, 2)} → Target ${number(event.target_value, 2)} ${escapeHtml(event.target_label || "")}</p><p>${escapeHtml(outcomeLabel(event.outcome_state))}${event.resolution_time ? ` · ${formatStamp(event.resolution_time)} ET` : ""}</p></article>`).join("")}<small>Recorded signal prices. — means not captured. Group statistics are not a single trade.</small></details>`;
  }

  function familyMarkup(rows, assumptions) {
    if (!rows.length) return empty("No setup families in this range.");
    return rows.map((row) => {
      const favorable = row.median_mfe.value === null ? "Not available" : `${number(row.median_mfe.value)} SPX pts · n=${row.median_mfe.sample_size}`;
      const adverse = row.median_mae.value === null ? "Not available" : `${number(row.median_mae.value)} SPX pts · n=${row.median_mae.sample_size}`;
      const estimate = profitEstimate(row.median_mfe.value, assumptions, row.direction, contractQuantity);
      const anchor = estimate?.anchor || anchorFor(assumptions, row.direction);
      const source = anchor.pricing_mode === "tradier_current_quote" ? `Current Tradier NTM reference: ${anchor.contract_label} at $${number(anchor.mid, 2)} mid; SPX ${number(anchor.spot, 2)}; ${number(anchor.spot_distance, 2)} points from spot; $${number(anchor.spread, 2)} spread; |Δ| ${number(anchor.absolute_delta, 2)}; ${anchor.dte} DTE; quoted ${formatStamp(anchor.as_of)}.` : `Fallback reference: $${number(anchor.contract_cost, 0)} premium and |Δ| ${number(anchor.absolute_delta, 2)} because ${String(anchor.fallback_reason || "quote unavailable").replaceAll("_", " ")}.`;
const estimateMarkup = estimate ? `<section class="setupAnalyticsProfitEstimate is-${anchor.pricing_mode === "tradier_current_quote" ? "live" : "fallback"}"><div class="setupAnalyticsProfitHeadline"><span>Estimated trade profit</span><strong>$${estimate.totalDollars.toLocaleString("en-US", {maximumFractionDigits: 0})} <small>${contractQuantity} ${contractQuantity === 1 ? "contract" : "contracts"}</small></strong><b>+${estimate.returnPercent.toFixed(1)}%</b></div><p>Based on the ${number(estimate.displayedMfe)}-point median favorable SPX move.</p><em>${anchor.pricing_mode === "tradier_current_quote" ? "Tradier reference" : "Assumed pricing"} · Illustrative, not realized P&amp;L.</em><details><summary>How this was estimated</summary><div><p>$${number(estimate.dollars, 2)} estimated gain per contract × ${contractQuantity} = $${number(estimate.totalDollars, 2)} total. Reference cost: $${number(anchor.contract_cost, 2)} per contract / $${number(estimate.totalCost, 2)} total.</p><p>${number(estimate.displayedMfe)} SPX points × |Δ| ${number(anchor.absolute_delta, 2)} × ${assumptions.multiplier} = approximately $${estimate.dollars.toFixed(0)}.</p><p>${escapeHtml(source)}</p><p>Uses today’s reference contract, not the historical setup’s option price. IV, theta, gamma path, slippage, and fills are excluded.</p></div></details></section>` : `<section class="setupAnalyticsProfitEstimate is-unavailable"><span>Estimated trade profit</span><strong>Not enough move data</strong><em>No profit estimate was calculated.</em></section>`;
      const rateValue = row.evaluated_count ? percent(row.target_reached_rate) : "—";
      const rateLabel = row.evaluated_count ? "reached target" : "No completed outcomes";
      return `<article class="setupAnalyticsFamilyCard"><header><strong>${escapeHtml(row.family)}</strong><span>${row.total_occurrences} total setups</span></header>${occurrenceMarkup(row)}<div class="setupAnalyticsFamilyResult"><b>${rateValue}</b><span>${rateLabel}</span><small>${row.target_reached_count} of ${row.evaluated_count} completed outcomes · ${row.open_count} awaiting · ${row.unavailable_count} historical unavailable</small></div><dl class="setupAnalyticsFamilyStats"><div><dt>Completed outcomes</dt><dd>${row.evaluated_count} of ${row.total_occurrences} setups</dd></div><div><dt>Median favorable move</dt><dd>${favorable}</dd></div><div><dt>Median adverse move</dt><dd>${adverse}</dd></div></dl>${estimateMarkup}</article>`;
    }).join("");
  }

  function renderFamilyCards(rows, assumptions) {
    root.querySelector('[data-family-cards="overview"]').innerHTML = rows.length ? familyMarkup(rows.slice(0, 4), assumptions) : empty("No setup families in this range.");
    root.querySelector('[data-family-cards="all"]').innerHTML = rows.length ? familyMarkup(rows, assumptions) : empty("No setup families in this range.");
  }

  function barSvg(rows, labelKey, valueKey) {
    if (!rows.length) return empty("No setup evidence in this range.");
    const width = Math.max(680, rows.length * 112), height = 270, top = 28, bottom = 54;
    const max = Math.max(1, ...rows.map((row) => Number(row[valueKey] || 0)));
    const step = (width - 64) / rows.length;
    const grid = [0.25, 0.5, 0.75, 1].map((ratio) => {
      const y = top + (1 - ratio) * (height - top - bottom);
      return `<line x1="42" y1="${y}" x2="${width - 14}" y2="${y}" stroke="#1c3951" stroke-dasharray="3 5"/><text x="34" y="${y + 4}" text-anchor="end" fill="#647c95" font-size="10">${Math.ceil(max * ratio)}</text>`;
    }).join("");
    const bars = rows.map((row, index) => {
      const value = Number(row[valueKey] || 0), h = value / max * (height - top - bottom), barWidth = Math.min(62, step - 20), x = 48 + index * step + (step - barWidth) / 2, y = height - bottom - h;
      const shortLabel = /^\d{4}-\d{2}-\d{2}$/.test(row[labelKey]) ? `${row[labelKey].slice(5, 7)}/${row[labelKey].slice(8, 10)}` : row[labelKey];
      return `<g><title>${escapeHtml(row[labelKey])}: ${value} setups</title><rect x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="7" fill="url(#sessionBar)"/><text x="${x + barWidth/2}" y="${Math.max(18, y - 7)}" text-anchor="middle" fill="#e4f6ef" font-size="12" font-weight="700">${value}</text><text x="${x + barWidth/2}" y="${height - 24}" text-anchor="middle" fill="#91a7be" font-size="11">${escapeHtml(shortLabel)}</text></g>`;
    }).join("");
    return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Setup occurrences by session"><defs><linearGradient id="sessionBar" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#54e8c1"/><stop offset="1" stop-color="#2986a8"/></linearGradient></defs>${grid}<line x1="42" y1="${height-bottom}" x2="${width-14}" y2="${height-bottom}" stroke="#31506d"/>${bars}</svg>`;
  }

  function scatter(rows) {
    if (!rows.length) return empty("MFE and MAE are unavailable for this selection.");
    const width = 560, height = 230, pad = 34;
    const maxX = Math.max(1, ...rows.map((row) => Number(row.mae || 0)));
    const maxY = Math.max(1, ...rows.map((row) => Number(row.mfe || 0)));
    const dots = rows.map((row) => {
      const x = pad + Number(row.mae || 0) / maxX * (width - pad * 2);
      const y = height - pad - Number(row.mfe || 0) / maxY * (height - pad * 2);
      const color = row.outcome === "target_reached" ? "#45d9ad" : row.outcome === "invalidated" ? "#ff607c" : "#f4be59";
      return `<circle cx="${x}" cy="${y}" r="6" fill="${color}" opacity=".85"><title>${escapeHtml(row.family)} · favorable ${number(row.mfe)} SPX pts · adverse ${number(row.mae)} SPX pts</title></circle>`;
    }).join("");
    return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Favorable versus adverse SPX movement"><line x1="${pad}" y1="${height-pad}" x2="${width-pad}" y2="${height-pad}" stroke="#31506d"/><line x1="${pad}" y1="${pad}" x2="${pad}" y2="${height-pad}" stroke="#31506d"/><text x="${width/2}" y="${height-6}" fill="#8298b3" text-anchor="middle" font-size="11">Adverse move · SPX points</text><text x="10" y="${height/2}" fill="#8298b3" text-anchor="middle" font-size="11" transform="rotate(-90 10 ${height/2})">Favorable move · SPX points</text>${dots}</svg>`;
  }

  function renderExcursionLegend(assumptions) {
    const [lowReturn, highReturn] = assumptions.planned_return_range;
    const [lowProfit, highProfit] = assumptions.planned_profit_range;
    const quoted = Object.values(assumptions.anchors || {}).filter((anchor) => anchor.pricing_mode === "tradier_current_quote");
    const sourceCopy = quoted.length ? quoted.map((anchor) => `${anchor.direction}: ${anchor.contract_label}, SPX ${number(anchor.spot, 2)}, ${number(anchor.spot_distance, 2)} pts from spot, mid $${number(anchor.mid, 2)}, spread $${number(anchor.spread, 2)}, |Δ| ${number(anchor.absolute_delta, 2)}, ${anchor.dte} DTE, ${formatStamp(anchor.as_of)}`).join(" · ") : `Tradier anchor unavailable; using $${assumptions.contract_cost} premium and |Δ| ${number(assumptions.absolute_delta, 2)}.`;
    root.querySelector("[data-excursion-legend]").innerHTML = `<div><b>MFE · favorable move</b><span>Farthest SPX moved in the setup's favor after the signal.</span></div><div><b>MAE · adverse move</b><span>Farthest SPX moved against the setup after the signal.</span></div><div class="is-profit"><b>Current premium anchor · projection only</b><span>${escapeHtml(sourceCopy)}</span></div><p>Planning benchmark: ${lowReturn}–${highReturn}% ≈ $${number(lowProfit, 2)}–$${number(highProfit, 2)} per contract using fallback premium. Current Tradier quotes are planning anchors, not historical fills. IV, theta, gamma path, spread, slippage, and fills are excluded.</p>`;
  }

  function renderCharts(payload) {
    renderHeatmap(payload.time_heatmap);
    renderFamilyCards(payload.family_comparison, payload.profit_estimate_assumptions);
    renderExcursionLegend(payload.profit_estimate_assumptions);
    root.querySelector('[data-chart="sessions"]').innerHTML = barSvg(payload.charts.occurrences_by_session, "session_date", "count");
    root.querySelector('[data-chart="excursion"]').innerHTML = scatter(payload.charts.mfe_vs_mae);
  }

  function renderLedger(data) {
    ledger.innerHTML = data.rows.length ? data.rows.map((row) => `<details class="setupAnalyticsEventRow ${row.outcome_state === "unavailable" ? "is-legacy" : ""}"><summary><span class="setupAnalyticsEventTime"><b>${formatStamp(row.signal_time)}</b><small>${escapeHtml(row.pattern_code || "—")}</small></span><span class="setupAnalyticsEventFamily"><b>${escapeHtml(row.family_label || row.family)}</b><small>${escapeHtml(row.direction)} · ${escapeHtml(row.level_label || row.level_key)}</small></span><span class="setupAnalyticsEventGrade"><b>${escapeHtml(row.grade || "—")}</b><small>${row.score}/100</small></span><span class="setupAnalyticsOutcome ${outcomeClass(row.outcome_state)}">${outcomeLabel(row.outcome_state)}</span><span class="setupAnalyticsEventMove"><b>${number(row.mfe)} / ${number(row.mae)}</b><small>MFE / MAE</small></span><i aria-hidden="true">⌄</i></summary><div class="setupAnalyticsEventDetails"><dl><div><dt>Level</dt><dd>${escapeHtml(row.level_label || row.level_key)} ${number(row.level_value, 2)}</dd></div><div><dt>Target</dt><dd>${escapeHtml(row.target_label || row.target_key || "No target")} ${number(row.target_value, 2)}</dd></div><div><dt>Resolved</dt><dd>${formatStamp(row.resolution_time)}</dd></div><div><dt>Progress</dt><dd>${percent(row.target_progress_percent)}</dd></div></dl><details class="setupAnalyticsEvidence"><summary>Frozen evidence</summary><pre>${escapeHtml(JSON.stringify(row.evidence || {}, null, 2))}</pre></details><a class="btn" href="${escapeHtml(row.replay_url)}">Open Replay</a></div></details>`).join("") : empty("No setups in this range.");
    pageLabel.textContent = data.pages ? `Page ${data.page} of ${data.pages}` : "No pages";
    previous.disabled = data.page <= 1;
    next.disabled = !data.pages || data.page >= data.pages;
  }

  function renderCoverage(payload) {
    const coverage = payload.coverage;
    root.querySelector("[data-analytics-coverage]").innerHTML = `<strong>Coverage boundary</strong><span>${coverage.session_count} sessions · ${formatStamp(coverage.earliest_signal_time)} through ${formatStamp(coverage.latest_signal_time)} · evaluated through ${formatStamp(coverage.evaluated_through)}</span><span>${coverage.missing_outcome_count} legacy outcomes · ${coverage.missing_excursion_count} missing excursion samples</span><small>${escapeHtml(payload.interpretation)}</small>`;
  }

  function updateFilterSummary(filters) {
    const active = [filters.family, filters.pattern, filters.direction, filters.outcome].filter(Boolean);
    const presetLabels = {today: "Today", last_3_sessions: "Last 3 sessions", this_week: "This week", last_20_sessions: "Last 20 sessions", all_history: "All history", custom: "Custom"};
    const dateLabel = filters.preset !== "custom" ? (presetLabels[filters.preset] || "Custom")
      : filters.start_date === root.dataset.today && filters.end_date === root.dataset.today
      ? "Today"
      : filters.start_date === filters.end_date
        ? filters.start_date
        : `${filters.start_date} → ${filters.end_date}`;
    const scope = active.length ? active.map((value) => value.replaceAll("_", " ")).join(" · ") : "all setups";
    root.querySelector("[data-filter-summary]").textContent = `${dateLabel} · ${scope}`;
    root.querySelectorAll("[data-preset]").forEach((button) => button.setAttribute("aria-pressed", String(button.dataset.preset === filters.preset)));
  }

  function selectView(name, focus = false) {
    root.querySelectorAll("[data-analytics-tab]").forEach((button) => {
      const selected = button.dataset.analyticsTab === name;
      button.setAttribute("aria-selected", String(selected));
      if (selected && focus) button.focus();
    });
    root.querySelectorAll("[data-analytics-view]").forEach((view) => {
      const selected = view.dataset.analyticsView === name;
      view.hidden = !selected;
      view.classList.toggle("is-active", selected);
    });
  }

  async function load() {
    status.classList.remove("is-error");
    status.textContent = "Loading setup evidence…";
    try {
      const url = new URL(root.dataset.apiUrl, window.location.origin);
      query().forEach((value, key) => url.searchParams.set(key, value));
      const response = await fetch(url, {credentials: "same-origin", cache: "no-store"});
      const body = await response.json();
      if (!response.ok || !body.ok) throw new Error(body.message || "Setup analytics unavailable.");
      const payload = body.payload;
      lastPayload = payload;
      form.elements.start_date.value = payload.filters.start_date;
      form.elements.end_date.value = payload.filters.end_date;
      form.elements.preset.value = payload.filters.preset;
      renderInsight(payload.insights, payload.coverage);
      renderKpis(payload.metrics, payload.coverage);
      renderCharts(payload);
      renderLedger(payload.ledger);
      renderCoverage(payload);
      updateFilterSummary(payload.filters);
      ["family", "pattern", "level", "grade"].forEach((name) => setOptions(name, payload.options[`${name === "family" ? "families" : name === "pattern" ? "patterns" : name === "level" ? "levels" : "grades"}`], payload.filters[name]));
      status.textContent = payload.empty ? "No setups in this range." : `${payload.metrics.total_setups} setups · ${payload.coverage.complete_outcome_count} evaluated · ${payload.coverage.missing_outcome_count} legacy`;
    } catch (error) {
      status.classList.add("is-error");
      status.innerHTML = `${escapeHtml(error.message)} <button class="btn" type="button" data-retry>Retry</button>`;
      root.querySelectorAll(".setupAnalyticsChart,.setupAnalyticsTimeline,.setupAnalyticsFamilyCards").forEach((node) => { node.innerHTML = empty("Analytics unavailable — retry when ready."); });
      status.querySelector("[data-retry]")?.addEventListener("click", load, {once: true});
    }
  }

  contractInput.addEventListener("input", () => {
    const valid = validContractQuantity(contractInput.value);
    contractInput.setAttribute("aria-invalid", String(!valid));
    if (!valid) {
      contractFeedback.textContent = `Enter 1–1,000 whole contracts. Estimates still use ${contractQuantity}.`;
      return;
    }
    contractQuantity = Number(contractInput.value);
    contractFeedback.textContent = `Estimates use ${contractQuantity} ${contractQuantity === 1 ? "contract" : "contracts"}.`;
    if (lastPayload) renderFamilyCards(lastPayload.family_comparison, lastPayload.profit_estimate_assumptions);
  });
  form.addEventListener("submit", (event) => { event.preventDefault(); form.elements.preset.value = "custom"; page = 1; load(); root.querySelector("[data-filter-drawer]").open = false; });
  root.querySelectorAll("[data-preset]").forEach((button) => button.addEventListener("click", () => {
    form.elements.preset.value = button.dataset.preset;
    page = 1;
    load();
  }));
  root.querySelector("[data-analytics-reset]").addEventListener("click", () => {
    form.reset();
    form.elements.start_date.value = root.dataset.today;
    form.elements.end_date.value = root.dataset.today;
    form.elements.preset.value = "today";
    page = 1;
    load();
  });
  sort.addEventListener("change", () => { page = 1; load(); });
  previous.addEventListener("click", () => { if (page > 1) { page -= 1; load(); } });
  next.addEventListener("click", () => { if (lastPayload && page < lastPayload.ledger.pages) { page += 1; load(); } });
  root.querySelectorAll("[data-analytics-tab]").forEach((button) => button.addEventListener("click", () => selectView(button.dataset.analyticsTab)));
  root.querySelectorAll("[data-open-view]").forEach((button) => button.addEventListener("click", () => selectView(button.dataset.openView, true)));
  load();
})();
