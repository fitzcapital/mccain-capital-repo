(function marketPulseLiveSetupModule(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.MarketPulseLiveSetup = api;
})(typeof window !== "undefined" ? window : globalThis, function buildMarketPulseLiveSetup() {
  "use strict";

  const stateRank = {CONFIRMED: 0, ARMED: 1, WATCHING: 2};

  function shouldAcceptRevision(currentId, currentRevision, nextId, nextRevision) {
    if (!nextId || nextId !== currentId) return true;
    return Number(nextRevision || 0) >= Number(currentRevision || 0);
  }

  function secondsUntil(value, nowMs = Date.now()) {
    const target = Date.parse(String(value || ""));
    if (!Number.isFinite(target)) return null;
    return Math.max(0, Math.ceil((target - nowMs) / 1000));
  }

  function formatElapsed(seconds) {
    const total = Math.max(0, Math.floor(Number(seconds) || 0));
    if (total < 60) return `${total}s`;
    const minutes = Math.floor(total / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    if (hours < 24) return remainingMinutes ? `${hours}h ${remainingMinutes}m` : `${hours}h`;
    const days = Math.floor(hours / 24);
    const remainingHours = hours % 24;
    return remainingHours ? `${days}d ${remainingHours}h` : `${days}d`;
  }

  function shouldDeliver(event, deliveredIds) {
    return Boolean(
      event
      && event.deliver_now
      && event.id
      && !new Set(deliveredIds || []).has(event.id)
    );
  }

  function rankSecondary(rows) {
    return [...(rows || [])].sort((left, right) => {
      const state = (stateRank[String(left.state || "").toUpperCase()] ?? 3)
        - (stateRank[String(right.state || "").toUpperCase()] ?? 3);
      if (state) return state;
      const score = Number(right.score || 0) - Number(left.score || 0);
      if (score) return score;
      return Number(left.distance ?? Infinity) - Number(right.distance ?? Infinity);
    });
  }

  return {formatElapsed, rankSecondary, secondsUntil, shouldAcceptRevision, shouldDeliver};
});
