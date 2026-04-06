(() => {
  "use strict";

  if (typeof document === "undefined") return;

  const stage = document.getElementById("spxExecutionChartStage");
  const ladder = document.getElementById("spxExecutionLadder");
  const ladderMeta = document.getElementById("spxExecutionLadderMeta");
  const widgetMeta = document.getElementById("spxExecutionWidgetMeta");
  const widgetHost = document.getElementById("spxExecutionTradingView");
  const gammaCard = document.getElementById("marketPulseHeaderGammaCard");
  const gammaLabel = document.getElementById("marketPulseHeaderGammaLabel");
  const gammaSub = document.getElementById("marketPulseHeaderGammaSub");
  const biasCard = document.getElementById("marketPulseHeaderBiasCard");
  const biasPrimary = document.getElementById("marketPulseHeaderBiasPrimary");
  const biasSecondary = document.getElementById("marketPulseHeaderBiasSecondary");
  const biasAbove = document.getElementById("marketPulseHeaderBiasAbove");
  const biasBelow = document.getElementById("marketPulseHeaderBiasBelow");
  const overlayPrice = document.getElementById("spxExecutionOverlayPrice");
  const overlayFlip = document.getElementById("spxExecutionOverlayFlip");
  const overlayCall = document.getElementById("spxExecutionOverlayCall");
  const overlayPut = document.getElementById("spxExecutionOverlayPut");
  const stratSummary = document.getElementById("marketPulseStratSummary");
  const stratInput = document.getElementById("marketPulseStratLevelsInput");
  const stratSave = document.getElementById("marketPulseStratSave");
  const stratClear = document.getElementById("marketPulseStratClear");
  if (!stage || !ladder || !ladderMeta || !widgetMeta || !widgetHost) return;
  const STRAT_STORAGE_KEY = "marketPulseStratLevels";

  const asNum = (value) => {
    if (value === null || value === undefined || value === "") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const getJson = (id) => {
    const node = document.getElementById(id);
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "null");
    } catch (_err) {
      return null;
    }
  };

  const etLabel = (stamp) => {
    if (!Number.isFinite(stamp)) return "";
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      weekday: "short",
      month: "short",
      day: "numeric",
    }).format(new Date(stamp));
  };

  const derivePhase = (iso) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) return "closed";
    const date = new Date(ts);
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).formatToParts(date);
    const weekday = String((parts.find((part) => part.type === "weekday") || {}).value || "");
    const hour = Number((parts.find((part) => part.type === "hour") || {}).value || "0");
    const minute = Number((parts.find((part) => part.type === "minute") || {}).value || "0");
    if (weekday === "Sat" || weekday === "Sun") return "closed";
    const mins = (hour * 60) + minute;
    if (mins >= 570 && mins < 960) return "open";
    if (mins >= 240 && mins < 570) return "premarket";
    if (mins >= 960 && mins < 1200) return "afterhours";
    return "closed";
  };

  const formatPrice = (value, digits = 2) => {
    const n = asNum(value);
    return n === null
      ? "—"
      : n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
  };

  const formatCompact = (value) => {
    const n = asNum(value);
    return n === null ? "—" : n.toLocaleString("en-US", { maximumFractionDigits: 0 });
  };

  const localFlipFromGamma = (gamma, fallback = null) => {
    const snapshot = gamma && typeof gamma === "object" ? gamma : {};
    if (Object.prototype.hasOwnProperty.call(snapshot, "local_flip_aggregated_gamma")) {
      return asNum(snapshot.local_flip_aggregated_gamma);
    }
    if (Object.prototype.hasOwnProperty.call(snapshot, "local_flip")) {
      return asNum(snapshot.local_flip);
    }
    return fallback;
  };

  const formatSigned = (value) => {
    const n = asNum(value);
    if (n === null) return "—";
    return `${n > 0 ? "+" : ""}${n.toFixed(1)} pts`;
  };

  const normalizePoints = (rows) => {
    const out = [];
    const seen = new Set();
    for (const row of Array.isArray(rows) ? rows : []) {
      if (!row || typeof row !== "object") continue;
      const ts = String(row.ts || "").trim();
      const price = asNum(row.price ?? row.v ?? row.close);
      if (!ts || price === null || seen.has(ts)) continue;
      const stamp = Date.parse(ts);
      if (!Number.isFinite(stamp)) continue;
      seen.add(ts);
      out.push({ ts, stamp, price });
    }
    return out.sort((a, b) => a.stamp - b.stamp);
  };

  const normalizeLevels = (rows) =>
    (Array.isArray(rows) ? rows : [])
      .map((row) => ({
        key: String((row && row.key) || "").trim(),
        value: asNum(row && row.value),
      }))
      .filter((row) => row.key && row.value !== null);

  const loadStratLevels = () => {
    try {
      const raw = window.localStorage ? window.localStorage.getItem(STRAT_STORAGE_KEY) : "";
      const parsed = raw ? JSON.parse(raw) : [];
      return (Array.isArray(parsed) ? parsed : [])
        .map((value) => asNum(value))
        .filter((value) => value !== null)
        .slice(0, 6);
    } catch (_err) {
      return [];
    }
  };

  const saveStratLevels = (levels) => {
    try {
      if (window.localStorage) window.localStorage.setItem(STRAT_STORAGE_KEY, JSON.stringify(levels));
    } catch (_err) {
      // ignore storage failures
    }
  };

  const parseStratInput = (raw) =>
    String(raw || "")
      .split(/[,\s]+/)
      .map((value) => asNum(value))
      .filter((value) => value !== null)
      .slice(0, 6);

  const sessionDayForPoints = (points) => {
    const rows = normalizePoints(points);
    if (!rows.length) return "";
    return etLabel(rows[rows.length - 1].stamp);
  };

  const resolveMode = (payload) => {
    if (payload && payload.mode) return String(payload.mode);
    const points = normalizePoints(payload && payload.points);
    const phase = String((payload && payload.phase) || "closed");
    if (points.length) return phase === "open" ? "live_session" : "last_session_replay";
    return "unavailable";
  };

  const getExecutionModel = (payload) => (
    payload && payload.execution_model && typeof payload.execution_model === "object"
      ? payload.execution_model
      : null
  );

  const updateHeaderCards = (model, fallbackRegime, fallbackEnvironment, currentPrice) => {
    const macro = (model && model.macro_regime) || {};
    const local = (model && model.local_bias) || {};
    const macroState = String(macro.state || "").toLowerCase();
    const fallbackRegimeText = String(fallbackRegime || "").toLowerCase();
    const positive = macroState === "positive" || (!macroState && fallbackRegimeText.includes("positive"));
    const neutral = macroState === "neutral" || (!macroState && fallbackRegimeText.includes("neutral"));
    const negative = macroState === "negative" || (!macroState && !positive && !neutral);
    const gammaTone = neutral ? "is-neutral" : positive ? "is-positive" : "is-negative";
    if (gammaCard) {
      gammaCard.classList.remove("is-positive", "is-negative", "is-neutral");
      gammaCard.classList.add(gammaTone);
    }
    if (biasCard) {
      biasCard.classList.remove("is-positive", "is-negative", "is-neutral");
      biasCard.classList.add(
        local.state === "above_local" ? "is-positive" : local.state === "below_local" ? "is-negative" : "is-neutral"
      );
    }
    if (gammaLabel) gammaLabel.textContent = String(macro.title || (positive ? "POSITIVE GAMMA" : "NEGATIVE GAMMA"));
    if (gammaSub) gammaSub.textContent = String(macro.subtitle || fallbackEnvironment || "—");

    if (biasPrimary) biasPrimary.textContent = String(local.context || "LOCAL FLIP UNKNOWN");
    if (biasSecondary) biasSecondary.textContent = String(local.label || (currentPrice !== null ? "WAIT" : "—"));
    if (biasAbove) biasAbove.classList.toggle("is-active", String(local.state || "") === "above_local");
    if (biasBelow) biasBelow.classList.toggle("is-active", String(local.state || "") === "below_local");
  };

  const levelMeta = (key) => {
    if (key === "price") return { short: "Price", tone: "price" };
    if (key === "gamma_flip") return { short: "Flip", tone: "flip" };
    if (key === "local_flip") return { short: "Local", tone: "local" };
    if (key === "call_wall") return { short: "CW", tone: "call" };
    if (key === "put_wall") return { short: "PW", tone: "put" };
    if (key === "vwap") return { short: "VWAP", tone: "vwap" };
    if (String(key).startsWith("strat_")) return { short: `S${String(key).split("_")[1] || ""}`, tone: "strat" };
    return { short: String(key || "").toUpperCase(), tone: "neutral" };
  };

  const inferRegime = (gammaMap, current) => {
    const regime = String((gammaMap && gammaMap.regime) || current.regime || "").trim();
    if (regime) {
      return {
        regime,
        environment: String(current.environment || (regime.toLowerCase().includes("positive") ? "Mean Reversion" : "Expansion")),
      };
    }
    const netGamma = asNum(gammaMap && gammaMap.net_gex);
    if (netGamma === null) return { regime: current.regime || "—", environment: current.environment || "—" };
    return {
      regime: netGamma >= 0 ? "Positive Gamma" : "Negative Gamma",
      environment: netGamma >= 0 ? "Mean Reversion" : "Expansion",
    };
  };

  const WIDGET_SYMBOL = "AMEX:SPY";
  let widgetSymbolLoaded = "";

  const loadTradingView = (symbol, note) => {
    widgetSymbolLoaded = symbol;
    widgetHost.innerHTML = "";
    const script = document.createElement("script");
    script.type = "text/javascript";
    script.async = true;
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js";
    script.text = JSON.stringify({
      autosize: true,
      symbol,
      interval: "5",
      timezone: "America/New_York",
      theme: "dark",
      style: "1",
      locale: "en",
      allow_symbol_change: false,
      calendar: false,
      details: false,
      hide_side_toolbar: true,
      hide_top_toolbar: true,
      hide_legend: true,
      save_image: false,
      studies: [],
      support_host: "https://www.tradingview.com",
      container_id: "spxExecutionTradingView",
      backgroundColor: "#0b1525",
      gridColor: "rgba(110, 146, 188, 0.10)",
      withdateranges: false,
    });
    widgetHost.appendChild(script);
    widgetMeta.textContent = note || "SPY 5m proxy";
  };

  const ensureTradingView = () => {
    if (widgetSymbolLoaded) return;
    loadTradingView(WIDGET_SYMBOL);
  };

  const mergeLivePoints = (currentPoints, streamPoints) => {
    const byTs = new Map(normalizePoints(currentPoints).map((point) => [point.ts, point]));
    normalizePoints(streamPoints).forEach((point) => {
      byTs.set(point.ts, point);
    });
    return Array.from(byTs.values()).sort((a, b) => a.stamp - b.stamp).slice(-240);
  };

  const mergePayload = (current, streamPayload) => {
    const next = { ...current };
    const gamma = (streamPayload && streamPayload.gamma_map) || {};
    const streamExecutionModel =
      streamPayload && streamPayload.execution_model && typeof streamPayload.execution_model === "object"
        ? streamPayload.execution_model
        : null;
    const streamPoints =
      (((streamPayload || {}).series_points || {}).SPX)
      || (((streamPayload || {}).series_points || {})["^GSPC"])
      || [];
    const serverTs = (streamPayload && streamPayload.server_ts) || null;
    const nextPhase = serverTs ? derivePhase(serverTs) : String(next.phase || "closed");
    const normalizedStreamPoints = normalizePoints(streamPoints);

    if (normalizedStreamPoints.length >= 2) {
      if (nextPhase === "open") {
        next.points = normalizedStreamPoints;
      } else if (!normalizePoints(next.points).length) {
        next.points = normalizedStreamPoints;
      } else if (String(current.mode || "") !== "last_session_replay") {
        const currentSessionDay = sessionDayForPoints(next.points);
        const streamSessionDay = sessionDayForPoints(normalizedStreamPoints);
        if (!currentSessionDay || !streamSessionDay || currentSessionDay === streamSessionDay) {
          next.points = mergeLivePoints(next.points, normalizedStreamPoints);
        }
      }
    }

    if (gamma && typeof gamma === "object") {
      const levels = new Map(normalizeLevels(next.levels).map((level) => [level.key, level]));
      const nextLocalFlip = localFlipFromGamma(gamma);
      const patch = {
        gamma_flip: gamma.gamma_flip_combined_basket,
        local_flip: nextLocalFlip,
        call_wall: gamma.call_wall_aggregated_gamma,
        put_wall: gamma.put_wall_aggregated_gamma,
      };
      Object.entries(patch).forEach(([key, value]) => {
        const n = asNum(value);
        if (n === null) {
          if (key === "local_flip") levels.delete(key);
          return;
        }
        levels.set(key, { key, value: n });
      });
      next.levels = Array.from(levels.values());
      const regimeState = inferRegime(gamma, current);
      next.regime = regimeState.regime;
      next.environment = regimeState.environment;
      if (streamExecutionModel) {
        next.execution_model = streamExecutionModel;
      } else if (next.execution_model && next.execution_model.levels) {
        next.execution_model = {
          ...next.execution_model,
          levels: {
            ...(next.execution_model.levels || {}),
            main_flip: asNum(gamma.gamma_flip_combined_basket) ?? (next.execution_model.levels || {}).main_flip,
            local_flip: localFlipFromGamma(gamma, (next.execution_model.levels || {}).local_flip),
            call_wall: asNum(gamma.call_wall_aggregated_gamma) ?? (next.execution_model.levels || {}).call_wall,
            put_wall: asNum(gamma.put_wall_aggregated_gamma) ?? (next.execution_model.levels || {}).put_wall,
          },
        };
      }
    }
    if (streamExecutionModel && !next.execution_model) {
      next.execution_model = streamExecutionModel;
    }

    if (serverTs) next.phase = derivePhase(serverTs);
    next.mode = resolveMode(next);
    const points = normalizePoints(next.points);
    const lastPoint = points.length ? points[points.length - 1] : null;
    const sessionLabel = lastPoint ? etLabel(lastPoint.stamp) : "";
    next.session_label = sessionLabel;
    if (!next.last_stored_replay_label && sessionLabel) next.last_stored_replay_label = sessionLabel;
    if (next.execution_model && next.execution_model.levels && lastPoint) {
      next.execution_model = {
        ...next.execution_model,
        levels: {
          ...(next.execution_model.levels || {}),
          spot: lastPoint.price,
        },
      };
    }
    return next;
  };

  const renderLadder = (payload) => {
    const model = getExecutionModel(payload);
    const levels = normalizeLevels(payload.levels);
    const stratLevels = loadStratLevels();
    const levelMap = new Map(levels.map((level) => [level.key, level]));
    const points = normalizePoints(payload.points);
    const lastPoint = points.length ? points[points.length - 1] : null;
    const currentPrice = lastPoint ? lastPoint.price : asNum(payload.latest_price);
    const flip = levelMap.get("gamma_flip");
    const localFlip = levelMap.get("local_flip");
    const callWall = levelMap.get("call_wall");
    const putWall = levelMap.get("put_wall");
    ladder.innerHTML = "";

    const stack = document.createElement("div");
    stack.className = "marketPulseExecutionLadderStack";
    const rows = Array.isArray(model && model.ladder_rows) && model.ladder_rows.length
      ? model.ladder_rows.map((row) => ({
          key: String(row.key || ""),
          value: asNum(row.value),
          label: String(row.label || row.short_label || ""),
          tone: String(row.tone || "neutral"),
          distance_points: asNum(row.distance_points),
        }))
      : [
          { key: "gamma_flip", value: flip && flip.value, label: "Main Flip", tone: "flip", distance_points: currentPrice !== null && flip ? flip.value - currentPrice : null },
          { key: "price", value: currentPrice, label: "Price", tone: "price", distance_points: null },
          { key: "local_flip", value: localFlip && localFlip.value, label: "Local Flip", tone: "local", distance_points: currentPrice !== null && localFlip ? localFlip.value - currentPrice : null },
          { key: "call_wall", value: callWall && callWall.value, label: "Call Wall", tone: "call", distance_points: currentPrice !== null && callWall ? callWall.value - currentPrice : null },
          { key: "put_wall", value: putWall && putWall.value, label: "Put Wall", tone: "put", distance_points: currentPrice !== null && putWall ? putWall.value - currentPrice : null },
        ];
    const diffs = rows
      .filter((row) => row.key !== "price" && currentPrice !== null && asNum(row.value) !== null)
      .map((row) => ({ key: row.key, diff: Math.abs(asNum(row.value) - currentPrice) }));
    const nearestKey = diffs.sort((a, b) => a.diff - b.diff)[0]?.key || "";

    rows.forEach((row) => {
      const numeric = asNum(row.value);
      if (numeric === null) return;
      const item = document.createElement("div");
      item.className = `marketPulseExecutionLadderMetric is-${row.tone}${nearestKey === row.key ? " is-nearest" : ""}`;

      const head = document.createElement("div");
      head.className = "marketPulseExecutionLadderMetricHead";

      const meta = document.createElement("div");
      meta.className = "marketPulseExecutionLadderMetricMeta";
      const dot = document.createElement("span");
      dot.className = "marketPulseExecutionLadderDot";
      const label = document.createElement("span");
      label.className = "marketPulseExecutionLadderMetricLabel";
      label.textContent = row.label;
      meta.append(dot, label);

      const value = document.createElement("strong");
      value.className = "marketPulseExecutionLadderMetricValue";
      value.textContent = formatPrice(numeric, row.key === "price" ? 2 : 0);
      head.append(meta, value);

      const dist = document.createElement("div");
      dist.className = "marketPulseExecutionLadderMetricDist";
      if (row.key === "price") {
        const localDistance = model && model.distances ? asNum(model.distances.to_local_flip) : null;
        dist.textContent = localDistance !== null ? `Δ Local ${formatSigned(localDistance)}` : flip ? `Δ Main ${formatSigned(numeric - flip.value)}` : "Live price";
      } else {
        const distance = asNum(row.distance_points);
        dist.textContent = currentPrice === null ? "Awaiting price" : distance !== null ? formatSigned(distance) : `${numeric >= currentPrice ? "+" : ""}${(numeric - currentPrice).toFixed(1)} pts`;
      }

      item.append(head, dist);
      stack.appendChild(item);
    });
    ladder.appendChild(stack);

    const spreadParts = [];
    if (currentPrice !== null) spreadParts.push(`SPX ${formatPrice(currentPrice)}`);
    if (model && model.levels && asNum(model.levels.main_flip) !== null) spreadParts.push(`Main ${formatCompact(model.levels.main_flip)}`);
    if (model && model.levels && asNum(model.levels.local_flip) !== null) spreadParts.push(`Local ${formatCompact(model.levels.local_flip)}`);
    ladderMeta.textContent = spreadParts.join(" • ") || "Price vs flip.";
    widgetMeta.textContent = "SPY 5m proxy";
    if (overlayPrice) overlayPrice.textContent = `Price ${formatPrice(currentPrice, 2)}`;
    if (overlayFlip) overlayFlip.textContent = `Flip ${formatCompact(flip && flip.value)}`;
    if (overlayCall) overlayCall.textContent = `CW ${formatCompact(callWall && callWall.value)}`;
    if (overlayPut) overlayPut.textContent = `PW ${formatCompact(putWall && putWall.value)}`;
    updateHeaderCards(model, payload.regime, payload.environment, currentPrice);

    if (stratSummary) {
      stratSummary.hidden = true;
      stratSummary.innerHTML = "";
    }
    if (stratInput && document.activeElement !== stratInput) {
      stratInput.value = stratLevels.map((level) => formatCompact(level)).join(", ");
    }
  };

  const render = (payload) => {
    stage.dataset.chartMode = resolveMode(payload);
    stage.dataset.phase = String(payload.phase || "closed");
    stage.dataset.regime = String(payload.regime || "").toLowerCase().includes("positive") ? "positive" : "negative";
    renderLadder(payload);
  };

  let current = getJson("spxExecutionChartPayload") || {};
  if (stratSave) {
    stratSave.addEventListener("click", () => {
      const levels = parseStratInput(stratInput ? stratInput.value : "");
      saveStratLevels(levels);
      render(current);
    });
  }
  if (stratClear) {
    stratClear.addEventListener("click", () => {
      saveStratLevels([]);
      if (stratInput) stratInput.value = "";
      render(current);
    });
  }
  ensureTradingView();
  render(current);
  window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));

  window.addEventListener("market-pulse-stream-payload", (event) => {
    current = mergePayload(current, (event && event.detail) || {});
    render(current);
  });
})();
