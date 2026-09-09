const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(
  path.join(__dirname, "../../static/js/market_pulse_refresh_coordinator.js"),
  "utf8",
);

const loadCoordinator = () => {
  const events = [];
  const window = {
    setTimeout,
    clearTimeout,
    dispatchEvent: (event) => events.push(event),
  };
  const context = vm.createContext({
    window,
    CustomEvent: class CustomEvent {
      constructor(type, options = {}) {
        this.type = type;
        this.detail = options.detail;
      }
    },
  });
  vm.runInContext(source, context);
  return {coordinator: window.marketPulseRefreshCoordinator, events};
};

test("coordinator keeps each lane single-flight and cleans it up", async () => {
  const {coordinator} = loadCoordinator();
  let active = 0;
  let peak = 0;
  let calls = 0;

  coordinator.register("bars", {
    initialDelay: 0,
    defaultDelay: 2,
    getDelay: () => 2,
    run: async () => {
      calls += 1;
      active += 1;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 8));
      active -= 1;
    },
  });
  coordinator.trigger("bars", 0);
  coordinator.trigger("bars", 0);
  await new Promise((resolve) => setTimeout(resolve, 28));
  coordinator.unregister("bars");
  const settledCalls = calls;
  await new Promise((resolve) => setTimeout(resolve, 12));

  assert.equal(peak, 1);
  assert.ok(settledCalls >= 1);
  assert.equal(calls, settledCalls);
  assert.equal(coordinator.state().length, 0);
});

test("manual lanes retain the explicit deadline", async () => {
  const {coordinator} = loadCoordinator();
  let calls = 0;
  coordinator.register("canonical", {
    initialDelay: 1000,
    autoSchedule: false,
    run: () => { calls += 1; },
  });
  const deadline = coordinator.schedule("canonical", 5);

  assert.ok(deadline >= Date.now());
  await new Promise((resolve) => setTimeout(resolve, 15));
  assert.equal(calls, 1);
  assert.equal(coordinator.deadline("canonical"), null);
  coordinator.unregister("canonical");
});

test("coordinator reports truthful scheduled and refreshing lifecycle states", async () => {
  const {coordinator, events} = loadCoordinator();
  coordinator.register("canonical", {
    initialDelay: 1,
    autoSchedule: false,
    run: async () => new Promise((resolve) => setTimeout(resolve, 3)),
  });
  await new Promise((resolve) => setTimeout(resolve, 12));

  const states = events.map((event) => event.detail?.status).filter(Boolean);
  assert.ok(states.includes("scheduled"));
  assert.ok(states.includes("refreshing"));
  assert.ok(states.includes("idle"));
  coordinator.unregister("canonical");
});

test("a boundary scheduled during an in-flight refresh runs once after reconciliation", async () => {
  const {coordinator} = loadCoordinator();
  let calls = 0;
  let release;
  coordinator.register("canonical", {
    initialDelay: 0,
    autoSchedule: false,
    run: async () => {
      calls += 1;
      if (calls === 1) await new Promise((resolve) => { release = resolve; });
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 5));
  coordinator.schedule("canonical", 0);
  coordinator.schedule("canonical", 0);
  release();
  await new Promise((resolve) => setTimeout(resolve, 15));

  assert.equal(calls, 2);
  coordinator.unregister("canonical");
});
