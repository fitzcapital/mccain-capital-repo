(() => {
  "use strict";

  if (window.marketPulseRefreshCoordinator) return;

  const lanes = new Map();
  let wakeTimer = null;
  let stopped = false;

  const emitState = (name, lane, status) => {
    window.dispatchEvent(new CustomEvent("market-pulse-refresh-state", {
      detail: {
        name,
        status,
        nextAt: Number.isFinite(lane?.nextAt) ? lane.nextAt : null,
        inFlight: !!lane?.inFlight,
      },
    }));
  };

  const normalizeDelay = (value, fallback = 1000) => {
    const numeric = Number(value);
    return Math.max(0, Number.isFinite(numeric) ? numeric : fallback);
  };

  const arm = () => {
    if (wakeTimer) window.clearTimeout(wakeTimer);
    wakeTimer = null;
    if (stopped || !lanes.size) return;

    const deadlines = Array.from(lanes.values())
      .filter((lane) => lane.enabled && !lane.inFlight && Number.isFinite(lane.nextAt))
      .map((lane) => lane.nextAt);
    if (!deadlines.length) return;
    const delay = Math.max(0, Math.min(...deadlines) - Date.now());
    wakeTimer = window.setTimeout(runDue, delay);
  };

  const runDue = () => {
    wakeTimer = null;
    if (stopped) return;
    const now = Date.now();
    Array.from(lanes.entries()).forEach(([name, lane]) => {
      if (!lane.enabled || lane.inFlight || lane.nextAt > now) return;
      lane.nextAt = Number.POSITIVE_INFINITY;
      lane.inFlight = true;
      emitState(name, lane, "refreshing");
      Promise.resolve()
        .then(() => lane.run())
        .catch((error) => {
          window.dispatchEvent(new CustomEvent("market-pulse-refresh-lane-error", {
            detail: {name, error},
          }));
          emitState(name, lane, "retrying");
        })
        .finally(() => {
          lane.inFlight = false;
          if (!lane.enabled) return arm();
          if (lane.autoSchedule) {
            const nextDelay = normalizeDelay(lane.getDelay?.(), lane.defaultDelay);
            lane.nextAt = Date.now() + nextDelay;
          }
          emitState(name, lane, Number.isFinite(lane.nextAt) ? "scheduled" : "idle");
          arm();
        });
    });
    arm();
  };

  const register = (name, options = {}) => {
    if (!name || typeof options.run !== "function") return null;
    const existing = lanes.get(name);
    const defaultDelay = normalizeDelay(options.defaultDelay, 1000);
    const lane = {
      run: options.run,
      getDelay: typeof options.getDelay === "function" ? options.getDelay : null,
      defaultDelay,
      autoSchedule: options.autoSchedule !== false,
      enabled: true,
      inFlight: existing?.inFlight || false,
      nextAt: Number.isFinite(existing?.nextAt)
        ? existing.nextAt
        : Date.now() + normalizeDelay(options.initialDelay, defaultDelay),
    };
    lanes.set(name, lane);
    emitState(name, lane, "scheduled");
    arm();
    return lane.nextAt;
  };

  const schedule = (name, delay = 0) => {
    const lane = lanes.get(name);
    if (!lane) return null;
    lane.enabled = true;
    lane.nextAt = Date.now() + normalizeDelay(delay, lane.defaultDelay);
    emitState(name, lane, "scheduled");
    arm();
    return lane.nextAt;
  };

  const unregister = (name) => {
    const lane = lanes.get(name);
    if (lane) lane.enabled = false;
    lanes.delete(name);
    arm();
  };

  const stop = () => {
    stopped = true;
    if (wakeTimer) window.clearTimeout(wakeTimer);
    wakeTimer = null;
  };

  const resume = () => {
    stopped = false;
    arm();
  };

  window.marketPulseRefreshCoordinator = {
    register,
    schedule,
    trigger: schedule,
    unregister,
    stop,
    resume,
    deadline: (name) => {
      const nextAt = lanes.get(name)?.nextAt;
      return Number.isFinite(nextAt) ? nextAt : null;
    },
    state: () => Array.from(lanes.entries()).map(([name, lane]) => ({
      name,
      inFlight: lane.inFlight,
      nextAt: Number.isFinite(lane.nextAt) ? lane.nextAt : null,
    })),
  };
})();
