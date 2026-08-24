const assert = require("node:assert/strict");
const test = require("node:test");

const monitor = require("../../static/js/market_pulse_live_setup.js");

test("rejects older revisions for the same setup", () => {
  assert.equal(monitor.shouldAcceptRevision("one", 3, "one", 2), false);
  assert.equal(monitor.shouldAcceptRevision("one", 3, "one", 3), true);
  assert.equal(monitor.shouldAcceptRevision("one", 3, "two", 1), true);
});

test("countdown never becomes negative", () => {
  const now = Date.parse("2026-08-20T11:30:00-04:00");
  assert.equal(monitor.secondsUntil("2026-08-20T11:30:15-04:00", now), 15);
  assert.equal(monitor.secondsUntil("2026-08-20T11:29:00-04:00", now), 0);
  assert.equal(monitor.secondsUntil("bad", now), null);
});

test("elapsed setup age stays compact beyond one minute", () => {
  assert.equal(monitor.formatElapsed(9), "9s");
  assert.equal(monitor.formatElapsed(60), "1m");
  assert.equal(monitor.formatElapsed(3599), "59m");
  assert.equal(monitor.formatElapsed(3600), "1h");
  assert.equal(monitor.formatElapsed(12117), "3h 21m");
  assert.equal(monitor.formatElapsed(90000), "1d 1h");
});

test("delivery requires a new server-authorized id", () => {
  const event = {id: "alert-1", deliver_now: true};
  assert.equal(monitor.shouldDeliver(event, []), true);
  assert.equal(monitor.shouldDeliver(event, ["alert-1"]), false);
  assert.equal(monitor.shouldDeliver({...event, deliver_now: false}, []), false);
});

test("secondary watches stay best to least", () => {
  const rows = monitor.rankSecondary([
    {state: "WATCHING", score: 95, distance: 1},
    {state: "ARMED", score: 76, distance: 3},
    {state: "ARMED", score: 90, distance: 8},
  ]);
  assert.deepEqual(rows.map((row) => row.score), [90, 76, 95]);
});
