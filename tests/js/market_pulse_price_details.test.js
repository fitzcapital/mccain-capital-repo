const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

const source = fs.readFileSync(path.join(__dirname, "../../static/js/spx_hero_chart.js"), "utf8");
const snippet = source.slice(source.indexOf("  const clearCanonicalStrategyLines"),
  source.indexOf("  const handleCanonicalMarketPulseUpdate"));

function fixture() {
  const overlays = [
    {value: 7675, roles: ["ACTIVE"], title: "ACTIVE · NPW · +1.4", color: "blue", width: 2},
    {value: 7680, roles: ["BEAR INVALIDATION"], title: "BEAR INVALIDATION · PW · +6.4", color: "red", width: 2},
    {value: 7665, roles: ["REVERSAL TARGET"], title: "REVERSAL TARGET · CDL · -8.6", color: "green", width: 2},
  ];
  const lines = [];
  const context = vm.createContext({
    buildCanonicalStrategyOverlays: () => overlays,
    drawingState: {enabled: false},
    canvas: {getBoundingClientRect: () => ({left: 10, top: 20, width: 900, height: 600}),
      addEventListener: () => {}},
    chart: {priceScale: () => ({width: () => 100})},
    candleSeries: {
      removePriceLine: () => {},
      priceToCoordinate: (price) => ({7675: 200, 7680: 100, 7665: 400})[price] ?? null,
      createPriceLine: (options) => {
        const line = {options, applyOptions: (next) => Object.assign(options, next)};
        lines.push(line);
        return line;
      },
    },
  });
  vm.runInContext(`let canonicalStrategyLines = []; const expandedCanonicalRoles = new Set();
    let canonicalStrategyState = {overlays: []}; ${snippet}
    globalThis.apply = applyCanonicalStrategyOverlays;
    globalThis.toggle = toggleCanonicalPriceDetails;`, context);
  context.apply({});
  const click = (x, y) => context.toggle({clientX: x + 10, clientY: y + 20,
    preventDefault() {}, stopPropagation() {}});
  return {context, lines, click};
}

test("three price labels start compact and each independently expands and collapses", () => {
  const {lines, click} = fixture();
  assert.deepEqual(lines.map((line) => line.options.title), ["", "", ""]);
  [200, 100, 400].forEach((y, index) => {
    click(850, y);
    assert.notEqual(lines[index].options.title, "");
    click(850, y);
    assert.equal(lines[index].options.title, "");
  });
  assert.deepEqual(lines.map((line) => line.options.color), ["blue", "red", "green"]);
  assert.ok(lines.every((line) => line.options.axisLabelVisible));
});

test("details survive canonical refresh, while plot clicks and drawing mode do not toggle", () => {
  const {context, lines, click} = fixture();
  click(700, 200);
  assert.equal(lines[0].options.title, "");
  click(850, 200);
  context.apply({});
  assert.equal(lines[3].options.title, "ACTIVE · NPW · +1.4");
  context.drawingState.enabled = true;
  click(850, 200);
  assert.equal(lines[3].options.title, "ACTIVE · NPW · +1.4");
});
