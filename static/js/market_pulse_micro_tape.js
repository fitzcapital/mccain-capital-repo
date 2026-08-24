(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.MarketPulseMicroTape = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  const numberOr = (value, fallback) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  };

  const timestampSeconds = (value) => {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value > 100000000000 ? Math.floor(value / 1000) : Math.floor(value);
    }
    const parsed = Date.parse(String(value || ""));
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : null;
  };

  const isOpenPhase = (value) => ["open", "regular", "live"].includes(
    String(value || "").trim().toLowerCase()
  );

  const create = (options) => ({
    intervalSeconds: numberOr(options?.intervalSeconds, 5),
    gapSeconds: numberOr(options?.gapSeconds, 15),
    maxPoints: Math.max(12, Math.floor(numberOr(options?.maxPoints, 72))),
    points: [],
    lastTimestamp: null,
    activeSource: "none",
    lastAcceptedAt: null,
    status: "pending",
  });

  const trim = (state) => {
    if (state.points.length > state.maxPoints) {
      state.points = state.points.slice(state.points.length - state.maxPoints);
    }
  };

  const accept = (state, sample) => {
    if (!state || !Array.isArray(state.points)) return {changed: false, status: "unavailable"};
    if (sample?.visible === false) {
      state.status = "paused";
      return {changed: false, status: state.status};
    }
    if (!isOpenPhase(sample?.marketPhase)) {
      state.status = "closed";
      return {changed: false, status: state.status};
    }

    const price = Number(sample?.price);
    const timestamp = timestampSeconds(sample?.asOf);
    const now = timestampSeconds(sample?.now) ?? Math.floor(Date.now() / 1000);
    if (!Number.isFinite(price) || price <= 0 || timestamp === null) {
      state.status = "unavailable";
      return {changed: false, status: state.status};
    }
    if (Math.max(0, now - timestamp) > state.gapSeconds) {
      state.status = "interrupted";
      return {changed: false, status: state.status};
    }
    if (state.lastTimestamp !== null && timestamp < state.lastTimestamp) {
      return {changed: false, status: state.status};
    }

    const bucket = Math.floor(timestamp / state.intervalSeconds) * state.intervalSeconds;
    const lastPoint = state.points[state.points.length - 1];
    let gap = false;
    if (state.lastTimestamp !== null && timestamp - state.lastTimestamp > state.gapSeconds) {
      const breakTime = Math.floor(
        (state.lastTimestamp + state.intervalSeconds) / state.intervalSeconds
      ) * state.intervalSeconds;
      if (!lastPoint || breakTime > Number(lastPoint.time)) {
        state.points.push({time: breakTime});
      }
      gap = true;
    }

    const tail = state.points[state.points.length - 1];
    if (tail && Number(tail.time) === bucket && Object.hasOwn(tail, "value")) {
      tail.value = price;
    } else if (!tail || bucket > Number(tail.time)) {
      state.points.push({time: bucket, value: price});
    } else {
      return {changed: false, status: state.status};
    }

    state.lastTimestamp = timestamp;
    state.activeSource = String(sample?.source || "unknown");
    state.lastAcceptedAt = now;
    state.status = gap ? "interrupted" : "live";
    trim(state);
    return {changed: true, status: state.status, gap, points: state.points};
  };

  const setLifecycle = (state, options) => {
    if (!state) return "unavailable";
    if (options?.visible === false) state.status = "paused";
    else if (!isOpenPhase(options?.marketPhase)) state.status = "closed";
    else if (state.status === "paused" || state.status === "closed") state.status = "pending";
    return state.status;
  };

  const sourceIsFresh = (state, source, now, staleSeconds) => {
    if (!state || state.activeSource !== source || state.lastAcceptedAt === null) return false;
    const current = timestampSeconds(now) ?? Math.floor(Date.now() / 1000);
    return Math.max(0, current - state.lastAcceptedAt) <= numberOr(staleSeconds, state.gapSeconds);
  };

  return {accept, create, isOpenPhase, setLifecycle, sourceIsFresh, timestampSeconds};
});
