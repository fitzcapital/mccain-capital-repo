const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const source = fs.readFileSync(
  path.join(__dirname, "../../static/js/market_pulse_setup_analytics.js"),
  "utf8",
);
const styles = fs.readFileSync(
  path.join(__dirname, "../../static/css/market_pulse_setup_analytics.css"),
  "utf8",
);

test("setup analytics keeps one filter query for every panel", () => {
  assert.match(source, /query\(\)\.forEach/);
  assert.match(source, /renderInsight\(\{best_time_bucket: payload\.leaders\.time\}/);
  assert.match(source, /renderKpis\(payload\.metrics/);
  assert.match(source, /renderCharts\(payload\)/);
  assert.match(source, /renderLedger\(payload\.ledger\)/);
  assert.match(source, /renderCoverage\(payload\)/);
  assert.match(source, /renderLeaders\(payload\.leaders\)/);
});

test("history leaders explain evidence and drill into the existing filters", () => {
  assert.match(source, /Best setup \+ time/);
  assert.match(source, /\$\{leader\.target_reached_count\}\/\$\{leader\.evaluated_count\}/);
  assert.match(source, /Median favorable/);
  assert.match(source, /Median adverse/);
  assert.match(source, /Study these setups/);
  assert.match(source, /data-study-leader/);
  assert.match(source, /form\.elements\.family\.value/);
  assert.match(source, /form\.elements\.start_time\.value/);
  assert.match(source, /form\.elements\.end_time\.value/);
  assert.match(source, /data-clear-leader/);
  assert.match(styles, /setupAnalyticsLeaderGrid\{display:grid;grid-template-columns:repeat\(3,minmax\(0,1fr\)\)/);
  assert.match(styles, /overflow-wrap:anywhere/);
});

test("setup analytics opens on today and keeps historical filters available", () => {
  assert.match(source, /filters\.start_date === root\.dataset\.today/);
  assert.match(source, /form\.elements\.start_date\.value = root\.dataset\.today/);
  assert.match(source, /form\.elements\.end_date\.value = root\.dataset\.today/);
  assert.match(source, /payload\.filters\.start_date/);
  assert.match(source, /last_3_sessions/);
  assert.match(source, /last_20_sessions/);
  assert.match(source, /all_history/);
});

test("setup analytics separates legacy frequency from performance evidence", () => {
  assert.match(source, /Historical outcome not captured/);
  assert.match(source, /Awaiting resolution/);
  assert.match(source, /No completed outcomes/);
  assert.doesNotMatch(source, /No outcomes/);
  assert.match(source, /renderHeatmap/);
  assert.match(source, /renderFamilyCards/);
  assert.match(source, /insight\.evidence\.label/);
  assert.match(source, /duplicate_rows_excluded/);
  assert.match(source, /duplicate .* excluded/);
});

test("setup analytics uses tabbed views and compact event rows", () => {
  assert.match(source, /function selectView/);
  assert.match(source, /aria-selected/);
  assert.match(source, /setupAnalyticsEventRow/);
  assert.match(source, /rows\.slice\(0, 4\)/);
});

test("setup analytics charts preserve complete labels", () => {
  assert.match(source, /setupAnalyticsProfilePlot/);
  assert.match(source, /shortLabel/);
  assert.match(source, /Setup occurrences by session/);
  assert.match(styles, /text-overflow:clip!important/);
  assert.match(styles, /white-space:normal!important/);
  assert.match(styles, /data-analytics-view=timing\]\{grid-template-columns:1fr/);
  assert.match(styles, /data-analytics-view=overview\]\{grid-template-columns:minmax\(0,1fr\)/);
  assert.match(styles, /setupAnalyticsFamilyCards\.is-preview\{grid-template-columns:repeat\(2,minmax\(0,1fr\)\)/);
});

test("timing chart stays compact and handles every returned time bucket", () => {
  assert.match(source, /--bucket-count:\$\{rows\.length\}/);
  assert.match(source, /const result = rate === null \? "—" : percent\(rate\)/);
  assert.match(source, /Hover a bar for its sample details/);
  assert.match(source, /setupAnalyticsChartHelp/);
  assert.doesNotMatch(source, /resolved ·/);
  assert.match(styles, /repeat\(var\(--bucket-count\),minmax\(0,1fr\)\)/);
  assert.match(styles, /setupAnalyticsChartHelp p\{position:absolute/);
});

test("family analysis explains denominators and SPX-point units", () => {
  assert.match(source, /Completed outcomes/);
  assert.match(source, /Median favorable move/);
  assert.match(source, /Median adverse move/);
  assert.match(source, /SPX pts/);
  assert.match(source, /completed outcomes/);
  assert.doesNotMatch(source, /Coverage \$\{percent\(row\.outcome_coverage_percent\)/);
  assert.doesNotMatch(source, /<span>MFE \$\{/);
  assert.doesNotMatch(source, /<span>MAE \$\{/);
});

test("family analysis explains excursion and estimated profit assumptions", () => {
  assert.match(source, /MFE · favorable move/);
  assert.match(source, /MAE · adverse move/);
  assert.match(source, /Estimated trade profit/);
  assert.match(source, /Illustrative, not realized P&amp;L/);
  assert.match(source, /How this was estimated/);
  assert.match(source, /Current Tradier NTM reference/);
  assert.match(source, /points from spot/);
  assert.match(source, /IV, theta, gamma path, spread, slippage, and fills are excluded/);
  assert.match(source, /profitEstimate/);
  assert.doesNotMatch(source, /<details open/);
  assert.match(styles, /setupAnalyticsProfitHeadline/);
  assert.match(styles, /data-analytics-view=families\]\{grid-template-columns:minmax\(0,1fr\)/);
  assert.match(styles, /setupAnalyticsFamilyCards\{grid-template-columns:repeat\(2,minmax\(0,1fr\)\)/);
});

test("setup analytics includes empty, retry, paging, and New York time contracts", () => {
  assert.match(source, /America\/New_York/);
  assert.match(source, /No setups in this range/);
  assert.match(source, /data-retry/);
  assert.match(source, /data-page-prev/);
  assert.match(source, /data-page-next/);
});

test("setup analytics never labels outcomes as profit or win rate", () => {
  assert.doesNotMatch(source, /win rate/i);
  assert.doesNotMatch(source, /realized profit.*:/i);
});

test("trade estimates default to three and scale dollars without scaling return", () => {
  const vm = require("node:vm");
  const helpers = source.slice(source.indexOf("  function anchorFor"), source.indexOf("  function occurrenceMarkup"));
  const context = vm.createContext({});
  vm.runInContext(helpers, context);
  const assumptions = {contract_cost: 750, absolute_delta: 0.4, multiplier: 100};
  const three = context.profitEstimate(9.4, assumptions, "bearish");
  const one = context.profitEstimate(9.4, assumptions, "bearish", 1);
  assert.ok(Math.abs(three.totalDollars - 1128) < 0.001);
  assert.equal(three.totalCost, 2250);
  assert.equal(three.totalDollars, one.totalDollars * 3);
  assert.equal(three.returnPercent, one.returnPercent);
  assert.equal(context.profitEstimate(null, assumptions, "bearish"), null);
  for (const value of ["", "0", "-1", "1.5", "1001", "NaN"]) assert.equal(context.validContractQuantity(value), false);
  for (const value of ["1", "3", "10", "1000"]) assert.equal(context.validContractQuantity(value), true);
  assert.match(source, /let contractQuantity = 3/);
  assert.match(source, /if \(lastPayload\) renderFamilyCards/);
});

test("family occurrences expose recorded price context without inventing missing prices", () => {
  assert.match(source, /What happened/);
  assert.match(source, /event\.entry_value/);
  assert.match(source, /event\.target_value/);
  assert.match(source, /event\.resolution_time/);
  assert.match(source, /means not captured/);
});

test("quantity input rerenders both views and keeps the last valid quantity", () => {
  const vm = require("node:vm");
  const nodes = new Map();
  const root = {
    querySelector(selector) {
      if (!nodes.has(selector)) nodes.set(selector, {
        value: "3", innerHTML: "", textContent: "", listeners: {},
        addEventListener(event, callback) { this.listeners[event] = callback; },
        setAttribute(name, value) { this[name] = value; },
      });
      return nodes.get(selector);
    },
    querySelectorAll() { return []; },
  };
  const row = {
    family: "Call Wall rejection", direction: "bearish", total_occurrences: 1,
    evaluated_count: 1, target_reached_count: 1, target_reached_rate: 100,
    open_count: 0, unavailable_count: 0,
    median_mfe: {value: 9.4, sample_size: 1}, median_mae: {value: 0.8, sample_size: 1},
    occurrences: [{signal_time: "2026-09-02T14:30:00Z", level_label: "Call Wall",
      level_value: 7670, entry_value: 7668, target_value: 7650, direction: "bearish",
      pattern_code: "2-2 REV D", outcome_state: "target_reached"}],
  };
  const payload = {family_comparison: [row], profit_estimate_assumptions: {
    contract_cost: 750, absolute_delta: 0.4, multiplier: 100,
  }};
  // Replace only initial network load; execute actual listeners and rendering unchanged.
  const executable = source.replace(/  load\(\);\n\}\)\(\);\s*$/, "  lastPayload = fixture; renderFamilyCards(fixture.family_comparison, fixture.profit_estimate_assumptions);\n})();");
  vm.runInNewContext(executable, {document: {querySelector: () => root}, fixture: payload});
  const input = root.querySelector("[data-analytics-contracts]");
  const overview = root.querySelector('[data-family-cards="overview"]');
  const all = root.querySelector('[data-family-cards="all"]');
  assert.match(overview.innerHTML, /\$1,128/);
  assert.match(overview.innerHTML, /7670.00/);
  assert.match(overview.innerHTML, /Entry 7668.00 → Target 7650.00/);
  input.value = "5";
  input.listeners.input();
  assert.match(overview.innerHTML, /\$1,880/);
  assert.match(overview.innerHTML, /\+50.1%/);
  assert.equal(overview.innerHTML, all.innerHTML);
  input.value = "";
  input.listeners.input();
  assert.equal(input["aria-invalid"], "true");
  assert.match(overview.innerHTML, /\$1,880/);
});
