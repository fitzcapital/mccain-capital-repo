const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const source = fs.readFileSync(path.join(__dirname, "../../static/js/spx_hero_chart.js"), "utf8");

test("five-minute chart frames the complete 78-bar regular session", () => {
  assert.match(source, /const INTERVAL_VISIBLE_BARS = \{[\s\S]*?"5min": 78,/);
});

test("all classifiable bars retain their Strat number markers", () => {
  const snippet = source.slice(
    source.indexOf("  const stratTypeForBar"),
    source.indexOf("  const sessionBreakMarkersForPayload"),
  );
  const context = vm.createContext({
    asNum: (value) => (Number.isFinite(Number(value)) ? Number(value) : null),
    HERO_CHART_THEME: {
      stratUp: "white",
      stratDown: "blue",
      stratInside: "gray",
      stratOutside: "silver",
    },
    displayPrefs: {showMarkers: true},
    STRAT_MARKER_LIMIT: 800,
  });
  vm.runInContext(`${snippet}\n globalThis.markersFor = stratMarkersForPayload;`, context);

  const bars = Array.from({length: 78}, (_, index) => ({
    time: 1_700_000_000 + (index * 300),
    open: 100 + index,
    high: 101 + index,
    low: 99,
    close: 100.5 + index,
  }));
  const markers = context.markersFor({
    bars,
    previous_session_bar_count: 0,
    current_session_bar_count: 78,
  });
  const numbers = markers.filter((marker) => marker.shape === "text");

  assert.equal(numbers.length, 77);
  assert.equal(markers.length, 154);
  assert.equal(numbers[0].time, bars[1].time);
  assert.equal(numbers.at(-1).time, bars.at(-1).time);
});
