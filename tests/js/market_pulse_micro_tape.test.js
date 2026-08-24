const assert = require("node:assert/strict");
const test = require("node:test");

const tape = require("../../static/js/market_pulse_micro_tape.js");

const liveSample = (overrides = {}) => ({
  price: 7700,
  asOf: "2026-08-21T10:00:02-04:00",
  now: "2026-08-21T10:00:03-04:00",
  marketPhase: "open",
  visible: true,
  ...overrides,
});

test("buckets fresh samples and replaces a duplicate bucket", () => {
  const state = tape.create({intervalSeconds: 5});
  assert.equal(tape.accept(state, liveSample()).status, "live");
  tape.accept(state, liveSample({price: 7701, asOf: "2026-08-21T10:00:04-04:00"}));
  assert.deepEqual(state.points, [{time: 1787320800, value: 7701}]);
});

test("bounds retained points", () => {
  const state = tape.create({maxPoints: 12});
  for (let index = 0; index < 15; index += 1) {
    const asOf = 1787320800 + (index * 5);
    tape.accept(state, liveSample({asOf, now: asOf, price: 7700 + index}));
  }
  assert.equal(state.points.length, 12);
  assert.equal(state.points.at(-1).value, 7714);
});

test("marks a feed gap and inserts whitespace without interpolation", () => {
  const state = tape.create({gapSeconds: 15});
  tape.accept(state, liveSample({asOf: 1787320800, now: 1787320800}));
  const result = tape.accept(state, liveSample({asOf: 1787320820, now: 1787320820}));
  assert.equal(result.status, "interrupted");
  assert.deepEqual(state.points, [
    {time: 1787320800, value: 7700},
    {time: 1787320805},
    {time: 1787320820, value: 7700},
  ]);
});

test("does not accept hidden, closed, stale, or malformed samples", () => {
  const state = tape.create({gapSeconds: 15});
  assert.equal(tape.accept(state, liveSample({visible: false})).status, "paused");
  assert.equal(tape.accept(state, liveSample({marketPhase: "closed"})).status, "closed");
  assert.equal(tape.accept(state, liveSample({now: "2026-08-21T10:01:00-04:00"})).status, "interrupted");
  assert.equal(tape.accept(state, liveSample({price: null})).status, "unavailable");
  assert.equal(state.points.length, 0);
});

test("resumes from a fresh point after interruption without backfill", () => {
  const state = tape.create({gapSeconds: 15});
  tape.accept(state, liveSample({asOf: 1787320800, now: 1787320800}));
  tape.accept(state, liveSample({asOf: 1787320820, now: 1787320820}));
  const result = tape.accept(state, liveSample({asOf: 1787320825, now: 1787320825}));
  assert.equal(result.status, "live");
  assert.equal(state.points.filter((point) => Object.hasOwn(point, "value")).length, 3);
});

test("tracks the active source and detects when stream fallback is required", () => {
  const state = tape.create({gapSeconds: 15});
  tape.accept(state, liveSample({asOf: 1787320800, now: 1787320800, source: "stream"}));
  assert.equal(state.activeSource, "stream");
  assert.equal(tape.sourceIsFresh(state, "stream", 1787320810, 15), true);
  assert.equal(tape.sourceIsFresh(state, "stream", 1787320816, 15), false);

  tape.accept(state, liveSample({asOf: 1787320820, now: 1787320820, source: "poll"}));
  assert.equal(state.activeSource, "poll");
});
