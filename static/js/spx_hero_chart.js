(() => {
  "use strict";

  window.__mcHeroApiDriven = true;

  const host = document.getElementById("spxExecutionHeroChart");
  const canvas = document.getElementById("spxExecutionHeroChartCanvas");
  const levelRail = document.getElementById("spxExecutionHeroLevelRail");
  const markersToggle = document.getElementById("marketPulseHeroToggleMarkers");
  const levelsToggle = document.getElementById("marketPulseHeroToggleLevels");
  const emptyState = document.getElementById("spxExecutionHeroChartEmpty");
  if (!host || !canvas) return;

  const LightweightCharts = window.LightweightCharts;
  if (!LightweightCharts || typeof LightweightCharts.createChart !== "function") {
    if (emptyState) emptyState.textContent = "Lightweight Charts failed to load.";
    return;
  }

  const barsUrl = String(host.dataset.barsUrl || "/api/hero/bars");
  const levelsUrl = String(host.dataset.levelsUrl || "/api/hero/levels");
  const streamUrl = String(host.dataset.streamUrl || "/api/hero/stream-session");
  const symbol = String(host.dataset.symbol || "QQQ").toUpperCase();
  const interval = String(host.dataset.interval || "5min");
  const HERO_CHART_TIMEZONE = "America/New_York";
  const HERO_TIME_AXIS_FORMATTER = new Intl.DateTimeFormat("en-US", {
    timeZone: HERO_CHART_TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const HERO_CHART_THEME = {
    background: "#06111F",
    panel: "#0B1D33",
    border: "#163250",
    textPrimary: "#EAF2FF",
    textSecondary: "#9CB3D1",
    gridMajor: "rgba(120, 160, 200, 0.16)",
    gridMinor: "rgba(120, 160, 200, 0.08)",
    axis: "rgba(120, 160, 200, 0.20)",
    bull: "#0FA37F",
    bullBorder: "#19C997",
    bullWick: "#22D3A6",
    bear: "#C23B57",
    bearBorder: "#E25574",
    bearWick: "#F06A88",
    bullMuted: "rgba(15, 163, 127, 0.56)",
    bullBorderMuted: "rgba(25, 201, 151, 0.62)",
    bullWickMuted: "rgba(34, 211, 166, 0.58)",
    bearMuted: "rgba(194, 59, 87, 0.52)",
    bearBorderMuted: "rgba(226, 85, 116, 0.60)",
    bearWickMuted: "rgba(240, 106, 136, 0.58)",
    spx: "#63B3FF",
    current: "#C23B57",
    cw: "#C85A72",
    ncw: "#E07186",
    lf: "#A88BFF",
    npw: "#4DD599",
    pw: "#218A5A",
    main: "rgba(140,155,180,0.38)",
    labelDark: "rgba(8, 14, 24, 0.88)",
    labelBorder: "rgba(255,255,255,0.08)",
    labelCwBg: "#C85A72",
    labelCwText: "#FFFFFF",
    labelNcwBg: "#E07186",
    labelNcwText: "#08111D",
    labelLfBg: "#A88BFF",
    labelLfText: "#08111D",
    labelBlueBg: "#63B3FF",
    labelBlueText: "#08111D",
    labelCurrentBg: "#C23B57",
    labelCurrentText: "#FFFFFF",
    labelMintBg: "#4DD599",
    labelMintText: "#08111D",
    labelGreenBg: "#218A5A",
    labelGreenText: "#EAF2FF",
    stratUp: "#00ff9f",
    stratDown: "#ff2d7a",
    stratInside: "#66dcff",
    stratOutside: "#f6c76b",
  };
  const DEFAULT_VISIBLE_BARS = 28;
  const LEFT_SCROLL_BUFFER_BARS = 8;
  const DEFAULT_RIGHT_OFFSET_BARS = 5;
  const HERO_CHART_HEIGHT = 548;
  const HERO_CHART_PREFS_KEY = "mc_hero_chart_display_prefs";

  const priceScaleWidth = 70;
  const chart = LightweightCharts.createChart(canvas, {
    autoSize: true,
    height: HERO_CHART_HEIGHT,
    layout: {
      background: { type: LightweightCharts.ColorType.Solid, color: HERO_CHART_THEME.background },
      textColor: HERO_CHART_THEME.textSecondary,
      fontFamily: '"Segoe UI", "Trebuchet MS", sans-serif',
      fontSize: 12,
    },
    grid: {
      vertLines: { color: HERO_CHART_THEME.gridMinor },
      horzLines: { color: HERO_CHART_THEME.gridMajor },
    },
    crosshair: {
      vertLine: { color: "rgba(99, 179, 255, 0.16)", width: 1, style: 2 },
      horzLine: { color: "rgba(46, 211, 198, 0.12)", width: 1, style: 2 },
    },
    rightPriceScale: {
      borderColor: HERO_CHART_THEME.axis,
      scaleMargins: { top: 0.08, bottom: 0.18 },
      minimumWidth: priceScaleWidth,
    },
    timeScale: {
      borderColor: HERO_CHART_THEME.axis,
      timeVisible: true,
      secondsVisible: false,
      rightOffset: DEFAULT_RIGHT_OFFSET_BARS,
      barSpacing: 10,
      fixLeftEdge: false,
      lockVisibleTimeRangeOnResize: true,
      tickMarkFormatter: (time) => HERO_TIME_AXIS_FORMATTER.format(new Date(Number(time) * 1000)),
    },
    localization: {
      locale: "en-US",
      priceFormatter: (value) => Number(value).toFixed(2),
      timeFormatter: (time) => HERO_TIME_AXIS_FORMATTER.format(new Date(Number(time) * 1000)),
    },
    handleScroll: true,
    handleScale: true,
  });

  const priorSessionSeries = chart.addCandlestickSeries({
    upColor: HERO_CHART_THEME.bullMuted,
    downColor: HERO_CHART_THEME.bearMuted,
    wickUpColor: HERO_CHART_THEME.bullWickMuted,
    wickDownColor: HERO_CHART_THEME.bearWickMuted,
    borderUpColor: HERO_CHART_THEME.bullBorderMuted,
    borderDownColor: HERO_CHART_THEME.bearBorderMuted,
    lastValueVisible: false,
    priceLineVisible: false,
  });

  const candleSeries = chart.addCandlestickSeries({
    upColor: HERO_CHART_THEME.bull,
    downColor: HERO_CHART_THEME.bear,
    wickUpColor: HERO_CHART_THEME.bullWick,
    wickDownColor: HERO_CHART_THEME.bearWick,
    borderUpColor: HERO_CHART_THEME.bullBorder,
    borderDownColor: HERO_CHART_THEME.bearBorder,
    lastValueVisible: false,
    priceLineVisible: false,
  });

  const volumeSeries = chart.addHistogramSeries({
    priceScaleId: "",
    priceFormat: { type: "volume" },
    color: "rgba(86, 116, 151, 0.22)",
    priceLineVisible: false,
    lastValueVisible: false,
  });

  volumeSeries.priceScale().applyOptions({
    scaleMargins: { top: 0.78, bottom: 0 },
  });

  const upperFrameSeries = chart.addLineSeries({
    color: "rgba(0,0,0,0)",
    lineWidth: 1,
    crosshairMarkerVisible: false,
    lastValueVisible: false,
    priceLineVisible: false,
  });

  const lowerFrameSeries = chart.addLineSeries({
    color: "rgba(0,0,0,0)",
    lineWidth: 1,
    crosshairMarkerVisible: false,
    lastValueVisible: false,
    priceLineVisible: false,
  });

  let priceLines = [];
  let polling = { bars_interval_ms: 45000, levels_interval_ms: 20000 };
  let barsTimer = null;
  let levelsTimer = null;
  let initialized = false;
  let lastBarsPayload = null;
  let lastLevelsPayload = null;
  let lastGoodLevelsPayload = null;
  let lastAppliedLevelsSignature = "";
  let lastBarsSignature = "";
  let pendingLevelsPayload = null;
  let levelsRenderTimer = null;
  let resizeTimer = null;
  let barsRequestInFlight = false;
  let levelsRequestInFlight = false;
  let pageVisible = document.visibilityState !== "hidden";
  let lastMeasuredWidth = 0;
  let levelRailRows = [];
  let displayPrefs = { showMarkers: true, showLevels: true };
  const LEVEL_RENDER_DEBOUNCE_MS = 120;
  const HIDDEN_BARS_INTERVAL_MS = 120000;
  const HIDDEN_LEVELS_INTERVAL_MS = 60000;
  const RESIZE_DEBOUNCE_MS = 140;
  const HERO_LEVEL_KEYS = [
    "main_flip",
    "local_flip",
    "call_wall",
    "put_wall",
    "next_call_wall",
    "next_put_wall",
  ];
  const HERO_REQUIRED_LEVEL_KEYS = ["local_flip", "call_wall", "put_wall"];
  const OFF_CHART_LEVEL_PCT = 0.018;

  const asNum = (value) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  };

  const fmt = (value, digits = 0) => {
    const numeric = asNum(value);
    return numeric === null ? "Unavailable" : numeric.toLocaleString("en-US", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  };

  const setText = (id, value) => {
    const node = document.getElementById(id);
    if (node) node.textContent = String(value ?? "");
  };

  const setToneVariant = (id, variants, active) => {
    const node = document.getElementById(id);
    if (!node) return;
    variants.forEach((variant) => node.classList.remove(variant));
    if (active) node.classList.add(active);
  };

  const fmtCompactLevel = (value, digits = 0, fallback = "—") => {
    const numeric = asNum(value);
    return numeric === null ? fallback : fmt(numeric, digits);
  };

  const loadDisplayPrefs = () => {
    try {
      const raw = window.localStorage.getItem(HERO_CHART_PREFS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (typeof parsed?.showMarkers === "boolean") displayPrefs.showMarkers = parsed.showMarkers;
      if (typeof parsed?.showLevels === "boolean") displayPrefs.showLevels = parsed.showLevels;
    } catch (_) {}
  };

  const saveDisplayPrefs = () => {
    try {
      window.localStorage.setItem(HERO_CHART_PREFS_KEY, JSON.stringify(displayPrefs));
    } catch (_) {}
  };

  const syncToggleButtons = () => {
    if (markersToggle) {
      markersToggle.setAttribute("aria-pressed", displayPrefs.showMarkers ? "true" : "false");
      markersToggle.classList.toggle("is-active", displayPrefs.showMarkers);
    }
    if (levelsToggle) {
      levelsToggle.setAttribute("aria-pressed", displayPrefs.showLevels ? "true" : "false");
      levelsToggle.classList.toggle("is-active", displayPrefs.showLevels);
    }
  };

  const stratTypeForBar = (previousBar, currentBar) => {
    if (!previousBar || !currentBar) return null;
    const prevHigh = asNum(previousBar.high);
    const prevLow = asNum(previousBar.low);
    const high = asNum(currentBar.high);
    const low = asNum(currentBar.low);
    if ([prevHigh, prevLow, high, low].some((value) => value === null)) return null;
    if (high <= prevHigh && low >= prevLow) return "1";
    if (high > prevHigh && low < prevLow) return "3";
    if (high > prevHigh) return "2U";
    if (low < prevLow) return "2D";
    return null;
  };

  const stratMarkersForType = (type, bar) => {
    if (!type || !bar) return null;
    if (type === "2U") {
      return [
        {
          time: bar.time,
          position: "aboveBar",
          shape: "arrowUp",
          color: HERO_CHART_THEME.stratUp,
        },
        {
          time: bar.time,
          position: "belowBar",
          shape: "text",
          text: "2",
          color: HERO_CHART_THEME.stratUp,
        },
      ];
    }
    if (type === "2D") {
      return [
        {
          time: bar.time,
          position: "aboveBar",
          shape: "arrowDown",
          color: HERO_CHART_THEME.stratDown,
        },
        {
          time: bar.time,
          position: "belowBar",
          shape: "text",
          text: "2",
          color: HERO_CHART_THEME.stratDown,
        },
      ];
    }
    if (type === "1") {
      return [
        {
          time: bar.time,
          position: "belowBar",
          shape: "text",
          text: "1",
          color: HERO_CHART_THEME.stratInside,
        },
      ];
    }
    if (type === "3") {
      return [
        {
          time: bar.time,
          position: "aboveBar",
          shape: "text",
          text: "3",
          color: HERO_CHART_THEME.stratOutside,
        },
      ];
    }
    return null;
  };

  const buildStratMarkers = (allCandles, startIndex = 0) => {
    if (!Array.isArray(allCandles) || allCandles.length < 2) return [];
    const markers = [];
    const firstIndex = Math.max(1, Number(startIndex) || 0);
    for (let index = firstIndex; index < allCandles.length; index += 1) {
      const type = stratTypeForBar(allCandles[index - 1], allCandles[index]);
      const nextMarkers = stratMarkersForType(type, allCandles[index]);
      if (Array.isArray(nextMarkers) && nextMarkers.length) markers.push(...nextMarkers);
    }
    return markers;
  };

  const stratMarkersForPayload = (payload) => {
    const bars = Array.isArray(payload?.bars) ? payload.bars : [];
    if (bars.length < 2 || !displayPrefs.showMarkers) return [];
    const previousSessionBarCount = Math.max(0, Number(payload?.previous_session_bar_count) || 0);
    const currentSessionBarCount = Math.max(0, Number(payload?.current_session_bar_count) || 0);
    const boundedPreviousCount = Math.min(previousSessionBarCount, bars.length);
    const boundedCurrentCount = Math.min(currentSessionBarCount, Math.max(0, bars.length - boundedPreviousCount));
    const splitIndex = boundedCurrentCount > 0 ? boundedPreviousCount : bars.length;
    return boundedCurrentCount > 0
      ? buildStratMarkers(bars, splitIndex)
      : buildStratMarkers(bars, 1);
  };

  const updateHeaderSummary = (levels) => {
    const gammaState = String(levels.gamma_regime || "").toLowerCase();
    const biasState = String(levels.bias_state || "").toLowerCase();
    const spot = fmt(levels.spot, 2);
    const snapshotLabel = String(levels.last_valid_snapshot_time_label || levels.snapshot_timestamp_label || "—");
    const spotSourceLabel = String(levels?.spot_meta?.source_label || "Last Valid Session");
    const localFlip = levels.local_flip === null || levels.local_flip === undefined
      ? "—"
      : fmtCompactLevel(levels.local_flip, 0);
    setText(
      "marketPulseHeaderSubline",
      `Main Flip ${fmtCompactLevel(levels.main_flip, 0)} | Local Flip ${localFlip} | CW ${fmtCompactLevel(levels.call_wall, 0)} | PW ${fmtCompactLevel(levels.put_wall, 0)}`
    );
    setText("marketPulseTitle", `${symbol} PLAYBOOK`);
    setText("marketPulseHeaderSnapshot", `${spotSourceLabel} ${snapshotLabel} • ${symbol} ${spot}`);
    setText("marketPulseHeaderGammaLabel", levels.gamma_regime_label || "REGIME UNAVAILABLE");
    setText("marketPulseHeaderGammaSub", levels.gamma_regime_subtitle || "Gamma snapshot unavailable");
    setText("marketPulseHeaderBiasPrimary", levels.bias_context || levels.planning_bias_label || "Awaiting valid structure");
    setText("marketPulseHeaderBiasSecondary", levels.bias_label || "WAIT");

    setToneVariant(
      "marketPulseHeaderGammaCard",
      ["is-positive", "is-negative", "is-neutral"],
      gammaState === "positive" ? "is-positive" : gammaState === "negative" ? "is-negative" : "is-neutral",
    );
    setToneVariant(
      "marketPulseHeaderBiasCard",
      ["is-positive", "is-negative", "is-neutral"],
      biasState === "above_local" ? "is-positive" : biasState === "below_local" ? "is-negative" : "is-neutral",
    );

    const aboveNode = document.getElementById("marketPulseHeaderBiasAbove");
    const belowNode = document.getElementById("marketPulseHeaderBiasBelow");
    if (aboveNode) aboveNode.classList.toggle("is-active", biasState === "above_local");
    if (belowNode) belowNode.classList.toggle("is-active", biasState === "below_local");
  };

  const setSpotTrendTone = (trend) => {
    const node = document.getElementById("marketPulseHeroSpotCard");
    if (!node) return;
    node.classList.remove("is-trend-up", "is-trend-down");
    if (trend === "up") node.classList.add("is-trend-up");
    if (trend === "down") node.classList.add("is-trend-down");
  };

  const detectShortTermTrend = (bars) => {
    if (!Array.isArray(bars) || bars.length < 4) return "neutral";
    const lastClose = asNum(bars[bars.length - 1]?.close);
    const anchorClose = asNum(bars[Math.max(0, bars.length - 4)]?.close);
    if (lastClose === null || anchorClose === null) return "neutral";
    const delta = lastClose - anchorClose;
    if (delta >= 1.25) return "up";
    if (delta <= -1.25) return "down";
    return "neutral";
  };

  const toneClass = (state) => {
    const normalized = String(state || "").toUpperCase();
    if (normalized === "READY") return "tone-positive";
    if (normalized === "NO_TRADE") return "tone-negative";
    return "tone-warn";
  };

  const setStateChip = (id, state) => {
    const node = document.getElementById(id);
    if (!node) return;
    node.textContent = String(state || "WATCH").replaceAll("_", " ");
    node.classList.remove("tone-positive", "tone-warn", "tone-negative");
    node.classList.add(toneClass(state));
  };

  const fetchJson = async (url) => {
    const response = await fetch(url, { credentials: "same-origin", cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  };

  const barsEndpoint = () =>
    `${barsUrl}?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}`;
  const levelsEndpoint = () => `${levelsUrl}?symbol=${encodeURIComponent(symbol)}`;

  const isValidHeroLevel = (value) => {
    const numeric = asNum(value);
    return numeric !== null && numeric > 0;
  };

  const levelsSignature = (payload) =>
    HERO_LEVEL_KEYS.map((key) => {
      const numeric = asNum(payload?.[key]);
      return `${key}:${numeric === null ? "na" : numeric.toFixed(4)}`;
    }).join("|");

  const barsSignature = (candles, payload) => {
    if (!Array.isArray(candles) || !candles.length) return "empty";
    const first = candles[0];
    const last = candles[candles.length - 1];
    return [
      candles.length,
      Number(first.time) || 0,
      Number(last.time) || 0,
      Number(last.close || 0).toFixed(2),
      Math.max(0, Number(payload?.previous_session_bar_count) || 0),
      Math.max(0, Number(payload?.current_session_bar_count) || 0),
      Boolean(payload?.opening_session_mode) ? "open" : "session",
    ].join("|");
  };

  const sanitizeLevelsPayload = (payload) => {
    if (!payload || typeof payload !== "object") return null;
    for (const key of HERO_REQUIRED_LEVEL_KEYS) {
      if (!isValidHeroLevel(payload[key])) {
        return null;
      }
    }
    return {
      ...(lastGoodLevelsPayload || {}),
      ...payload,
    };
  };

  const applyLevelsPayload = (payload) => {
    lastLevelsPayload = payload;
    lastGoodLevelsPayload = payload;
    lastAppliedLevelsSignature = levelsSignature(payload);
    renderSummary(payload);
    updateOverlayLines(payload);
    applyViewport();
    if (emptyState && initialized) emptyState.hidden = true;
  };

  const scheduleLevelsApply = (payload) => {
    pendingLevelsPayload = payload;
    window.clearTimeout(levelsRenderTimer);
    levelsRenderTimer = window.setTimeout(() => {
      if (!pendingLevelsPayload) return;
      applyLevelsPayload(pendingLevelsPayload);
      pendingLevelsPayload = null;
    }, LEVEL_RENDER_DEBOUNCE_MS);
  };

  const clearFrameBounds = () => {
    upperFrameSeries.setData([]);
    lowerFrameSeries.setData([]);
  };

  const applyFrameBounds = (bars, levels) => {
    if (!Array.isArray(bars) || !bars.length) {
      clearFrameBounds();
      return;
    }
    const prices = [];
    bars.forEach((bar) => {
      const high = asNum(bar.high);
      const low = asNum(bar.low);
      if (high !== null) prices.push(high);
      if (low !== null) prices.push(low);
    });
    const spot = asNum(levels?.spot);
    [
      levels?.spot,
      levels?.local_flip,
      levels?.call_wall,
      levels?.put_wall,
      levels?.next_call_wall,
      levels?.next_put_wall,
      levels?.main_flip,
    ].forEach((value) => {
      const numeric = asNum(value);
      if (numeric === null) return;
      if (spot !== null) {
        const pctDistance = Math.abs(numeric - spot) / Math.max(Math.abs(spot), 1);
        if (pctDistance > OFF_CHART_LEVEL_PCT) return;
      }
      prices.push(numeric);
    });
    if (!prices.length) {
      clearFrameBounds();
      return;
    }
    const low = Math.min(...prices);
    const high = Math.max(...prices);
    const padding = Math.max((high - low) * 0.14, 6);
    const firstTime = Number(bars[0].time);
    const lastTime = Number(bars[bars.length - 1].time);
    upperFrameSeries.setData([
      { time: firstTime, value: high + padding },
      { time: lastTime, value: high + padding },
    ]);
    lowerFrameSeries.setData([
      { time: firstTime, value: low - padding },
      { time: lastTime, value: low - padding },
    ]);
  };

  const applyViewport = ({ fitContent = false } = {}) => {
    if (!lastBarsPayload || !Array.isArray(lastBarsPayload.bars) || !lastBarsPayload.bars.length) return;
    const bars = lastBarsPayload.bars;
    const openingMode = Boolean(lastBarsPayload.opening_session_mode);
    const sessionTargetBarCount = Math.max(0, Number(lastBarsPayload.session_target_bar_count) || 0);
    if (openingMode) {
      const previousCount = Math.max(0, Number(lastBarsPayload.previous_session_bar_count) || 0);
      const currentCount = Math.max(0, Number(lastBarsPayload.current_session_bar_count) || 0);
      const rightOffsetBars = Math.max(4, Math.min(8, Number(lastBarsPayload.right_offset_bars) || 5));
      const liveWindowBars = currentCount > 0
        ? Math.max(18, Math.min(34, currentCount + 14))
        : Math.max(18, Math.min(30, bars.length));
      const from = currentCount > 0
        ? Math.max(0, previousCount + currentCount - liveWindowBars)
        : Math.max(0, bars.length - liveWindowBars);
      chart.timeScale().applyOptions({
        rightOffset: rightOffsetBars,
        barSpacing: 12,
      });
      chart.timeScale().setVisibleLogicalRange({
        from,
        to: (bars.length - 1) + rightOffsetBars,
      });
      applyFrameBounds(bars, lastLevelsPayload);
      renderLevelRail(lastLevelsPayload);
      return;
    }

    clearFrameBounds();
    chart.timeScale().applyOptions({
      rightOffset: DEFAULT_RIGHT_OFFSET_BARS,
      barSpacing: 10,
    });
    if (fitContent || !initialized) {
      const requestedVisibleBars = Math.max(
        Number(lastBarsPayload.visible_window_bars) || 0,
        Math.min(bars.length, DEFAULT_VISIBLE_BARS),
      );
      const visibleBars = Math.max(
        Math.min(bars.length, requestedVisibleBars, DEFAULT_VISIBLE_BARS) + LEFT_SCROLL_BUFFER_BARS,
        Math.min(bars.length, DEFAULT_VISIBLE_BARS),
      );
      const targetTo = sessionTargetBarCount > 0
        ? Math.max((bars.length - 1) + DEFAULT_RIGHT_OFFSET_BARS, sessionTargetBarCount - 1)
        : (bars.length - 1) + DEFAULT_RIGHT_OFFSET_BARS;
      chart.timeScale().setVisibleLogicalRange({
        from: Math.max(-LEFT_SCROLL_BUFFER_BARS, targetTo - visibleBars + 1 - LEFT_SCROLL_BUFFER_BARS),
        to: targetTo,
      });
    }
    renderLevelRail(lastLevelsPayload);
  };

  const clearPriceLines = () => {
    priceLines.forEach((line) => {
      try {
        candleSeries.removePriceLine(line);
      } catch (_) {}
    });
    priceLines = [];
  };

  const applyMarkerVisibility = () => {
      candleSeries.setMarkers(stratMarkersForPayload(lastBarsPayload));
  };

  const applyLevelVisibility = () => {
    host.classList.toggle("is-levels-hidden", !displayPrefs.showLevels);
    if (!displayPrefs.showLevels) {
      clearPriceLines();
      if (levelRail) levelRail.innerHTML = "";
      return;
    }
    if (lastLevelsPayload) updateOverlayLines(lastLevelsPayload);
  };

  const addLevelLine = ({
    title,
    value,
    color,
    width = 1,
    style = 0,
    axis = false,
    axisLabelColor,
    axisLabelTextColor,
  }) => {
    const numeric = asNum(value);
    if (numeric === null) return;
    priceLines.push(
      candleSeries.createPriceLine({
        price: numeric,
        color,
        lineWidth: width,
        lineStyle: style,
        axisLabelVisible: axis,
        axisLabelColor,
        axisLabelTextColor,
        title,
      })
    );
  };

  const updateOverlayLines = (levels) => {
    // Python is the source of truth for state; the frontend only renders levels and emphasis.
    clearPriceLines();
    if (!displayPrefs.showLevels) {
      levelRailRows = [];
      if (levelRail) levelRail.innerHTML = "";
      return;
    }
    const state = String(levels.state || "").toUpperCase();
    const price = asNum(levels.spot);
    const callWall = asNum(levels.call_wall);
    const putWall = asNum(levels.put_wall);
    const localFlip = asNum(levels.local_flip);
    const mainFlip = asNum(levels.main_flip);
    const nextCallWall = asNum(levels.next_call_wall);
    const nextPutWall = asNum(levels.next_put_wall);

    const nextRailRows = [];
    const pushRailRow = (kind, title, value, emphasis = "") => {
      const numeric = asNum(value);
      if (numeric === null) return;
      nextRailRows.push({ kind, title, value: numeric, emphasis });
    };

    addLevelLine({
      title: "Main",
      value: mainFlip,
      color: HERO_CHART_THEME.main,
      width: 1,
      style: 2,
      axisLabelColor: HERO_CHART_THEME.labelDark,
      axisLabelTextColor: HERO_CHART_THEME.textSecondary,
    });
    pushRailRow("main", "Main", mainFlip);
    addLevelLine({
      title: "NPW",
      value: nextPutWall,
      color: HERO_CHART_THEME.npw,
      width: 1,
      style: 1,
      axisLabelColor: HERO_CHART_THEME.labelMintBg,
      axisLabelTextColor: HERO_CHART_THEME.labelMintText,
    });
    pushRailRow("put-next", "NPW", nextPutWall);
    addLevelLine({
      title: "PW",
      value: putWall,
      color: HERO_CHART_THEME.pw,
      width: 2,
      style: 0,
      axisLabelColor: HERO_CHART_THEME.labelGreenBg,
      axisLabelTextColor: HERO_CHART_THEME.labelGreenText,
    });
    pushRailRow("put", "PW", putWall);
    addLevelLine({
      title: "LF",
      value: localFlip,
      color: HERO_CHART_THEME.lf,
      width: 2,
      style: 0,
      axisLabelColor: HERO_CHART_THEME.labelLfBg,
      axisLabelTextColor: HERO_CHART_THEME.labelLfText,
    });
    pushRailRow("local", "LF", localFlip, "support");
    addLevelLine({
      title: "CW",
      value: callWall,
      color: HERO_CHART_THEME.cw,
      width: state === "NO_TRADE" ? 3 : 2,
      style: 0,
      axisLabelColor: HERO_CHART_THEME.labelCwBg,
      axisLabelTextColor: HERO_CHART_THEME.labelCwText,
    });
    pushRailRow("call", "CW", callWall, state === "NO_TRADE" ? "danger" : "target");
    addLevelLine({
      title: "NCW",
      value: nextCallWall,
      color: HERO_CHART_THEME.ncw,
      width: 1,
      style: 1,
      axisLabelColor: HERO_CHART_THEME.labelNcwBg,
      axisLabelTextColor: HERO_CHART_THEME.labelNcwText,
    });
    pushRailRow("call-next", "NCW", nextCallWall);
    addLevelLine({
      title: "",
      value: price,
      color: HERO_CHART_THEME.current,
      width: 1,
      style: 1,
      axis: false,
    });
    addLevelLine({
      title: symbol,
      value: price,
      color: HERO_CHART_THEME.spx,
      width: 2,
      style: 2,
      axisLabelColor: HERO_CHART_THEME.labelBlueBg,
      axisLabelTextColor: HERO_CHART_THEME.labelBlueText,
    });
    pushRailRow("spot", symbol, price, "current");
    levelRailRows = nextRailRows;
    renderLevelRail(levels);
  };

  const renderLevelRail = (levels) => {
    if (!displayPrefs.showLevels) {
      if (levelRail) levelRail.innerHTML = "";
      return;
    }
    if (!levelRail || !levels || !Array.isArray(levelRailRows) || !levelRailRows.length) return;
    const plottedRows = levelRailRows
      .map((row) => ({ ...row, y: candleSeries.priceToCoordinate(row.value) }))
      .filter((row) => Number.isFinite(row.y))
      .sort((a, b) => a.y - b.y);
    if (!plottedRows.length) {
      levelRail.innerHTML = "";
      return;
    }

    const minGap = 27;
    let previousY = -Infinity;
    plottedRows.forEach((row) => {
      const y = Math.max(12, Number(row.y));
      row.displayY = Math.max(y, previousY + minGap);
      previousY = row.displayY;
    });

    const railHeight = levelRail.getBoundingClientRect().height || 320;
    for (let i = plottedRows.length - 1; i >= 0; i -= 1) {
      const row = plottedRows[i];
      row.displayY = Math.min(row.displayY, railHeight - 14);
      if (i < plottedRows.length - 1) {
        row.displayY = Math.min(row.displayY, plottedRows[i + 1].displayY - minGap);
      }
    }

    levelRail.innerHTML = plottedRows.map((row) => `
      <div class="marketPulseExecutionHeroLevelRailItem is-${row.kind} ${row.emphasis ? `is-${row.emphasis}` : ""}" style="top:${Math.max(12, row.displayY)}px">
        <span>${row.title}</span>
        <strong>${fmt(row.value, row.kind === "spot" ? 2 : 0)}</strong>
      </div>
    `).join("");
  };

  const renderSummary = (levels) => {
    // /api/hero/levels already derives read, pullback, destination, and trade state.
    const state = String(levels.trade_state_label || levels.state || "WATCH").replaceAll("_", " ");
    const currentRead = String(levels.current_read || "Await structure");
    const headline = `${state} - ${currentRead}`.toUpperCase();

    updateHeaderSummary(levels);
    setText("marketPulseHeroSpot", fmt(levels.spot, 2));
    setText("marketPulseHeroSpotLabel", levels?.spot_source_short_label || levels?.spot_meta?.source_label || `${symbol} Spot`);
    setText("marketPulseHeroGamma", levels.gamma_regime_label || "Regime Unavailable");
    setText("marketPulseHeroBias", levels.current_read || levels.bias_context || levels.bias_label || "Awaiting structure");
    const tradeability = String(
      levels.execution_regime_label || levels.tradeability || "Reduced confidence"
    ).replaceAll("_", " ");
    setText("marketPulseHeroTradeability", tradeability);
    setText("marketPulseHeroSession", levels.session || "Closed · No confidence");
    setText("marketPulseHeroMacroFlip", fmt(levels.main_flip, 0));

    setText("marketPulseHeroTopState", `READ: ${currentRead}`);
    setText("marketPulseHeroTopMode", `DECISION: ${state}`);
    setText("marketPulseHeroRailSummary", levels.current_read || "Awaiting valid structure");
    setText("marketPulseHeroChartBanner", headline);

    setText("marketPulseHeroRailFootState", currentRead);
    setText("marketPulseHeroPullbackLevel", levels.pullback_level || "Awaiting level");
    setText("marketPulseHeroDestinationInline", levels.next_destination || "Awaiting next test");

    setStateChip("marketPulseHeroStateContext", levels.state);
    setStateChip("marketPulseHeroStateChip", levels.state);
    setText("marketPulseHeroTradeState", state);
    setText("marketPulseHeroBestLook", levels.best_look || "Wait for cleaner structure");
    setText("marketPulseHeroRequiredTrigger", levels.required_trigger || "Confirmation required");
    setText("marketPulseHeroInvalidation", levels.invalidation || "Awaiting valid structure");
  };

  const updateBars = async ({ fitContent = false } = {}) => {
    if (barsRequestInFlight) return;
    barsRequestInFlight = true;
    const payload = await fetchJson(barsEndpoint());
    try {
      const bars = Array.isArray(payload.bars) ? payload.bars : [];
      if (!bars.length) {
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `Tradier returned no intraday bars for ${symbol}.`;
        }
        return;
      }

      const candles = bars
        .map((bar) => ({
          time: Number(bar.time),
          open: Number(bar.open),
          high: Number(bar.high),
          low: Number(bar.low),
          close: Number(bar.close),
        }))
        .filter((bar) => Number.isFinite(bar.time) && Number.isFinite(bar.open) && Number.isFinite(bar.high) && Number.isFinite(bar.low) && Number.isFinite(bar.close));

      const nextBarsSignature = barsSignature(candles, payload);
      const previousSessionBarCount = Math.max(0, Number(payload.previous_session_bar_count) || 0);
      const currentSessionBarCount = Math.max(0, Number(payload.current_session_bar_count) || 0);
      const boundedPreviousCount = Math.min(previousSessionBarCount, candles.length);
      const boundedCurrentCount = Math.min(currentSessionBarCount, Math.max(0, candles.length - boundedPreviousCount));
      const splitIndex = boundedCurrentCount > 0 ? boundedPreviousCount : candles.length;
      const priorCandles = candles.slice(0, splitIndex);
      const currentCandles = candles.slice(splitIndex);

      lastBarsPayload = {
        ...payload,
        bars: candles,
      };

      if (nextBarsSignature === lastBarsSignature) {
        if (fitContent) applyViewport({ fitContent: true });
        if (emptyState) emptyState.hidden = true;
        return;
      }

      const volume = bars
        .map((bar) => {
          const close = Number(bar.close);
          const open = Number(bar.open);
          const amount = Number(bar.volume);
          return {
            time: Number(bar.time),
            value: Number.isFinite(amount) ? amount : 0,
            color: close >= open ? "rgba(15, 163, 127, 0.18)" : "rgba(194, 59, 87, 0.16)",
          };
        })
        .filter((bar) => Number.isFinite(bar.time));

      priorSessionSeries.setData(currentCandles.length ? priorCandles : []);
      candleSeries.setData(currentCandles.length ? currentCandles : candles);
      candleSeries.setMarkers(stratMarkersForPayload(lastBarsPayload));
      volumeSeries.setData(volume);
      lastBarsSignature = nextBarsSignature;
      setSpotTrendTone(detectShortTermTrend(currentCandles.length ? currentCandles : candles));
      applyViewport({ fitContent });
      if (emptyState) emptyState.hidden = true;
      if (!initialized) {
        initialized = true;
        window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
      }
    } finally {
      barsRequestInFlight = false;
    }
  };

  const updateLevels = async () => {
    if (levelsRequestInFlight) return;
    levelsRequestInFlight = true;
    try {
      const payload = await fetchJson(levelsEndpoint());
      const nextLevels = sanitizeLevelsPayload(payload);
      if (!nextLevels) {
        console.warn(`${symbol} hero levels update skipped: invalid level payload`, payload);
        return;
      }
      const nextSignature = levelsSignature(nextLevels);
      if (nextSignature === lastAppliedLevelsSignature) {
        lastLevelsPayload = nextLevels;
        lastGoodLevelsPayload = nextLevels;
        return;
      }
      scheduleLevelsApply(nextLevels);
    } finally {
      levelsRequestInFlight = false;
    }
  };

  const clearPollTimers = () => {
    window.clearTimeout(barsTimer);
    window.clearTimeout(levelsTimer);
    barsTimer = null;
    levelsTimer = null;
  };

  const scheduleBarsPoll = (delay) => {
    window.clearTimeout(barsTimer);
    if (!pageVisible) return;
    const intervalMs = Math.max(15000, Number(polling.bars_interval_ms) || 45000);
    barsTimer = window.setTimeout(async () => {
      try {
        await updateBars();
      } catch (error) {
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `${symbol} chart refresh failed: ${error.message}`;
        }
      } finally {
        scheduleBarsPoll(intervalMs);
      }
    }, Math.max(0, Number(delay) || intervalMs));
  };

  const scheduleLevelsPoll = (delay) => {
    window.clearTimeout(levelsTimer);
    if (!pageVisible) return;
    const intervalMs = Math.max(10000, Number(polling.levels_interval_ms) || 20000);
    levelsTimer = window.setTimeout(async () => {
      try {
        await updateLevels();
      } catch (_) {
      } finally {
        scheduleLevelsPoll(intervalMs);
      }
    }, Math.max(0, Number(delay) || intervalMs));
  };

  const startPolling = () => {
    clearPollTimers();
    scheduleBarsPoll(Math.min(Math.max(15000, Number(polling.bars_interval_ms) || 45000), HIDDEN_BARS_INTERVAL_MS));
    scheduleLevelsPoll(Math.min(Math.max(10000, Number(polling.levels_interval_ms) || 20000), HIDDEN_LEVELS_INTERVAL_MS));
  };

  const boot = async () => {
    try {
      const streamPayload = await fetchJson(streamUrl);
      if (streamPayload && typeof streamPayload === "object") {
        polling = { ...polling, ...streamPayload };
      }
    } catch (_) {}

    try {
      await Promise.all([updateBars({ fitContent: true }), updateLevels()]);
      startPolling();
    } catch (error) {
      if (emptyState) {
        emptyState.hidden = false;
        emptyState.textContent = `${symbol} hero failed to initialize: ${error.message}`;
      }
      window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
    }
  };

  const resize = () => {
    const nextWidth = canvas.clientWidth;
    if (!nextWidth || nextWidth === lastMeasuredWidth) return;
    lastMeasuredWidth = nextWidth;
    try {
      chart.applyOptions({ width: nextWidth, height: HERO_CHART_HEIGHT });
    } catch (_) {}
    renderLevelRail(lastLevelsPayload);
  };

  const bindDisplayToggles = () => {
    if (markersToggle) {
      markersToggle.addEventListener("click", () => {
        displayPrefs.showMarkers = !displayPrefs.showMarkers;
        saveDisplayPrefs();
        syncToggleButtons();
        applyMarkerVisibility();
      });
    }
    if (levelsToggle) {
      levelsToggle.addEventListener("click", () => {
        displayPrefs.showLevels = !displayPrefs.showLevels;
        saveDisplayPrefs();
        syncToggleButtons();
        applyLevelVisibility();
      });
    }
  };

  loadDisplayPrefs();
  syncToggleButtons();
  bindDisplayToggles();
  applyLevelVisibility();
  window.addEventListener("resize", () => {
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(resize, RESIZE_DEBOUNCE_MS);
  });
  document.addEventListener("visibilitychange", () => {
    pageVisible = document.visibilityState !== "hidden";
    if (!pageVisible) {
      clearPollTimers();
      return;
    }
    resize();
    scheduleBarsPoll(0);
    scheduleLevelsPoll(0);
  });
  boot();
})();
