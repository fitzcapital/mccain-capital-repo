(() => {
  "use strict";

  window.__mcHeroApiDriven = true;

  const host = document.getElementById("spxExecutionHeroChart");
  const canvas = document.getElementById("spxExecutionHeroChartCanvas");
  const levelRail = document.getElementById("spxExecutionHeroLevelRail");
  const markersToggle = document.getElementById("marketPulseHeroToggleMarkers");
  const levelsToggle = document.getElementById("marketPulseHeroToggleLevels");
  const dayLevelsToggle = document.getElementById("marketPulseHeroToggleDayLevels");
  const intervalToggles = Array.from(document.querySelectorAll("[data-hero-chart-interval]"));
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
  const DEFAULT_INTERVAL = "5min";
  const INTERVAL_LABELS = {
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1h": "1h",
  };
  const INTERVAL_ALIASES = {
    "5": "5min",
    "5m": "5min",
    "5min": "5min",
    "15": "15min",
    "15m": "15min",
    "15min": "15min",
    "30": "30min",
    "30m": "30min",
    "30min": "30min",
    "1h": "1h",
    "60": "1h",
    "60m": "1h",
    "60min": "1h",
  };
  const normalizeInterval = (value) => (
    INTERVAL_ALIASES[String(value || DEFAULT_INTERVAL).trim().toLowerCase()] || DEFAULT_INTERVAL
  );
  let interval = normalizeInterval(host.dataset.interval || DEFAULT_INTERVAL);
  const HERO_CHART_TIMEZONE = "America/New_York";
  const HERO_TIME_AXIS_FORMATTER = new Intl.DateTimeFormat("en-US", {
    timeZone: HERO_CHART_TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const HERO_CHART_THEME = {
    background: "#071421",
    panel: "#0B1F3A",
    border: "#2F6BFF",
    textPrimary: "#F4F8FF",
    textSecondary: "#C7D0D9",
    gridMajor: "rgba(199, 208, 217, 0.18)",
    gridMinor: "rgba(199, 208, 217, 0.09)",
    axis: "rgba(199, 208, 217, 0.30)",
    bull: "#19C997",
    bullBorder: "#22C55E",
    bullWick: "#4ADE80",
    bear: "#D64D66",
    bearBorder: "#EF4444",
    bearWick: "#FB7185",
    bullMuted: "rgba(25, 201, 151, 0.86)",
    bullBorderMuted: "rgba(34, 197, 94, 0.92)",
    bullWickMuted: "rgba(74, 222, 128, 0.88)",
    bearMuted: "rgba(214, 77, 102, 0.84)",
    bearBorderMuted: "rgba(239, 68, 68, 0.90)",
    bearWickMuted: "rgba(251, 113, 133, 0.88)",
    spx: "#63B3FF",
    current: "#C23B57",
    cw: "#C85A72",
    ncw: "#E07186",
    lf: "#A88BFF",
    npw: "#4DD599",
    pw: "#218A5A",
    main: "rgba(199,208,217,0.56)",
    labelDark: "rgba(8, 14, 24, 0.88)",
    labelBorder: "rgba(255,255,255,0.08)",
    labelCwBg: "#ff4f79",
    labelCwText: "#FFFFFF",
    labelNcwBg: "#ff6c8d",
    labelNcwText: "#07111f",
    labelLfBg: "#7f5cff",
    labelLfText: "#f8fbff",
    labelBlueBg: "#42bfff",
    labelBlueText: "#08111D",
    labelCurrentBg: "#2f9cff",
    labelCurrentText: "#FFFFFF",
    labelMintBg: "#00ff9f",
    labelMintText: "#08111D",
    labelGreenBg: "#19c77e",
    labelGreenText: "#EAF2FF",
    stratUp: "#1dffad",
    stratDown: "#ff3b7f",
    stratInside: "#71ddff",
    stratOutside: "#ffd45f",
    pdh: "#f7d56f",
    pdl: "#b78cff",
    cdh: "#63f7d4",
    cdl: "#ff8aa3",
  };
  const DEFAULT_VISIBLE_BARS = 28;
  const LEFT_SCROLL_BUFFER_BARS = 8;
  const DEFAULT_RIGHT_OFFSET_BARS = 5;
  const HERO_CHART_HEIGHT = 600;
  const LEGACY_HERO_CHART_PREFS_KEY = "mc_hero_chart_display_prefs";
  const HERO_CHART_PREFS_KEY = `mc_hero_chart_display_prefs_${symbol}`;
  const STRAT_MARKER_LIMIT = 96;
  const LEVEL_RAIL_MIN_GAP = 38;

  const priceScaleWidth = 62;
  const chart = LightweightCharts.createChart(canvas, {
    autoSize: false,
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
      scaleMargins: { top: 0.14, bottom: 0.18 },
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
  let dayLevelLines = [];
  let polling = { bars_interval_ms: 45000, levels_interval_ms: 20000 };
  let barsTimer = null;
  let levelsTimer = null;
  let initialized = false;
  let lastBarsPayload = null;
  let lastLevelsPayload = null;
  let lastGoodLevelsPayload = null;
  let lastAppliedLevelsSignature = "";
  let lastBarsSignature = "";
  let lastLiveBarSignature = "";
  let lastMarkerSignature = "";
  let lastDayLevelSignature = "";
  let pendingLevelsPayload = null;
  let levelsRenderTimer = null;
  let resizeTimer = null;
  let resizeObserver = null;
  let barsRequestInFlight = false;
  let levelsRequestInFlight = false;
  let barsRequestSerial = 0;
  let pageVisible = document.visibilityState !== "hidden";
  let lastMeasuredWidth = 0;
  let lastMeasuredHeight = 0;
  let levelRailRows = [];
  let dayLevelRailRows = [];
  let displayPrefs = { showMarkers: true, showLevels: true, showDayLevels: true };
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

  const railDigitsForKind = (kind) =>
    ["spot", "pdh", "pdl", "cdh", "cdl"].includes(String(kind || "")) ? 2 : 0;

  const loadDisplayPrefs = () => {
    try {
      const raw =
        window.localStorage.getItem(HERO_CHART_PREFS_KEY) ||
        window.localStorage.getItem(LEGACY_HERO_CHART_PREFS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (typeof parsed?.showMarkers === "boolean") displayPrefs.showMarkers = parsed.showMarkers;
      if (typeof parsed?.showLevels === "boolean") displayPrefs.showLevels = parsed.showLevels;
      if (typeof parsed?.showDayLevels === "boolean") {
        displayPrefs.showDayLevels = parsed.showDayLevels;
      }
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
    if (dayLevelsToggle) {
      dayLevelsToggle.setAttribute("aria-pressed", displayPrefs.showDayLevels ? "true" : "false");
      dayLevelsToggle.classList.toggle("is-active", displayPrefs.showDayLevels);
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
          size: 0.72,
        },
        {
          time: bar.time,
          position: "belowBar",
          shape: "text",
          text: "2",
          color: HERO_CHART_THEME.stratUp,
          size: 0.9,
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
          size: 0.72,
        },
        {
          time: bar.time,
          position: "belowBar",
          shape: "text",
          text: "2",
          color: HERO_CHART_THEME.stratDown,
          size: 0.9,
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
          size: 0.9,
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
          size: 1,
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
      ? buildStratMarkers(bars, splitIndex).slice(-STRAT_MARKER_LIMIT)
      : buildStratMarkers(bars, 1).slice(-STRAT_MARKER_LIMIT);
  };

  const applyStratMarkers = ({ force = false } = {}) => {
    const markers = stratMarkersForPayload(lastBarsPayload);
    const nextSignature = markersSignature(markers);
    if (!force && nextSignature === lastMarkerSignature) return;
    candleSeries.setMarkers(markers);
    lastMarkerSignature = nextSignature;
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

  const volumeBarForSource = (bar) => {
    const close = Number(bar?.close);
    const open = Number(bar?.open);
    const amount = Number(bar?.volume);
    const time = Number(bar?.time);
    if (!Number.isFinite(time)) return null;
    return {
      time,
      value: Number.isFinite(amount) ? amount : 0,
      color: close >= open ? "rgba(15, 163, 127, 0.18)" : "rgba(194, 59, 87, 0.16)",
    };
  };

  const volumeBarsForSource = (bars) => (
    Array.isArray(bars)
      ? bars.map(volumeBarForSource).filter((bar) => bar && Number.isFinite(bar.time))
      : []
  );

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

  const syncIntervalControls = () => {
    const label = INTERVAL_LABELS[interval] || INTERVAL_LABELS[DEFAULT_INTERVAL];
    document.querySelectorAll(".marketPulseExecutionHeroTimeframe").forEach((node) => {
      node.textContent = `· ${label}`;
    });
    intervalToggles.forEach((button) => {
      const active = normalizeInterval(button.dataset.heroChartInterval) === interval;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
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
      Math.max(0, Number(payload?.previous_session_bar_count) || 0),
      Math.max(0, Number(payload?.current_session_bar_count) || 0),
      Boolean(payload?.opening_session_mode) ? "open" : "session",
    ].join("|");
  };

  const liveBarSignature = (candle, sourceBar) => {
    if (!candle) return "empty";
    return [
      Number(candle.time) || 0,
      Number(candle.open || 0).toFixed(4),
      Number(candle.high || 0).toFixed(4),
      Number(candle.low || 0).toFixed(4),
      Number(candle.close || 0).toFixed(4),
      Number(sourceBar?.volume || 0),
    ].join("|");
  };

  const markersSignature = (markers) => {
    if (!Array.isArray(markers) || !markers.length) return "empty";
    return markers
      .map((marker) => [
        Number(marker.time) || 0,
        marker.position || "",
        marker.shape || "",
        marker.text || "",
        marker.color || "",
      ].join(":"))
      .join("|");
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

  const currentChartPrice = () => {
    const bars = Array.isArray(lastBarsPayload?.bars) ? lastBarsPayload.bars : [];
    if (!bars.length) return null;
    return asNum(bars[bars.length - 1]?.close);
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
    const padding = Math.max((high - low) * 0.22, 8);
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

  const clearDayLevelLines = () => {
    dayLevelLines.forEach((line) => {
      try {
        candleSeries.removePriceLine(line);
      } catch (_) {}
    });
    dayLevelLines = [];
  };

  const applyMarkerVisibility = () => {
    applyStratMarkers({ force: true });
  };

  const applyLevelVisibility = () => {
    host.classList.toggle(
      "is-levels-hidden",
      !displayPrefs.showLevels && !displayPrefs.showDayLevels
    );
    if (!displayPrefs.showLevels) {
      clearPriceLines();
      levelRailRows = [];
    } else if (lastLevelsPayload) {
      updateOverlayLines(lastLevelsPayload);
    }
    renderLevelRail(lastLevelsPayload);
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

  const addDayLevelLine = ({ title, value, color }) => {
    const numeric = asNum(value);
    if (numeric === null) return;
    dayLevelLines.push(
      candleSeries.createPriceLine({
        price: numeric,
        color,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: false,
        title,
      })
    );
  };

  const highLowForCandles = (candles) => {
    if (!Array.isArray(candles) || !candles.length) return null;
    const highs = candles.map((bar) => asNum(bar.high)).filter((value) => value !== null);
    const lows = candles.map((bar) => asNum(bar.low)).filter((value) => value !== null);
    if (!highs.length || !lows.length) return null;
    return { high: Math.max(...highs), low: Math.min(...lows) };
  };

  const buildDayLevelRows = (payload) => {
    const bars = Array.isArray(payload?.bars) ? payload.bars : [];
    if (!bars.length) return [];
    const previousCount = Math.max(0, Number(payload.previous_session_bar_count) || 0);
    const currentCount = Math.max(0, Number(payload.current_session_bar_count) || 0);
    const boundedPreviousCount = Math.min(previousCount, bars.length);
    const boundedCurrentCount = Math.min(currentCount, Math.max(0, bars.length - boundedPreviousCount));
    const splitIndex = boundedCurrentCount > 0 ? boundedPreviousCount : bars.length;
    const priorRange = highLowForCandles(bars.slice(0, splitIndex));
    const currentRange = highLowForCandles(bars.slice(splitIndex));
    const rows = [];
    if (priorRange) {
      rows.push({ kind: "pdh", title: "PDH", value: priorRange.high });
      rows.push({ kind: "pdl", title: "PDL", value: priorRange.low });
    }
    if (currentRange) {
      rows.push({ kind: "cdh", title: "CDH", value: currentRange.high });
      rows.push({ kind: "cdl", title: "CDL", value: currentRange.low });
    }
    return rows;
  };

  const updateDayLevelLines = () => {
    host.classList.toggle(
      "is-levels-hidden",
      !displayPrefs.showLevels && !displayPrefs.showDayLevels
    );
    if (!displayPrefs.showDayLevels) {
      if (lastDayLevelSignature !== "hidden") {
        clearDayLevelLines();
        dayLevelRailRows = [];
        lastDayLevelSignature = "hidden";
      }
      renderLevelRail(lastLevelsPayload);
      return;
    }
    const nextRows = buildDayLevelRows(lastBarsPayload);
    const nextSignature = nextRows
      .map((row) => `${row.kind}:${Number(row.value || 0).toFixed(4)}`)
      .join("|") || "empty";
    if (nextSignature === lastDayLevelSignature) {
      renderLevelRail(lastLevelsPayload);
      return;
    }
    clearDayLevelLines();
    dayLevelRailRows = nextRows;
    dayLevelRailRows.forEach((row) => {
      addDayLevelLine({
        title: row.title,
        value: row.value,
        color: HERO_CHART_THEME[row.kind] || HERO_CHART_THEME.textSecondary,
      });
    });
    lastDayLevelSignature = nextSignature;
    renderLevelRail(lastLevelsPayload);
  };

  const updateOverlayLines = (levels) => {
    // Python is the source of truth for state; the frontend only renders levels and emphasis.
    clearPriceLines();
    if (!displayPrefs.showLevels) {
      levelRailRows = [];
      renderLevelRail(levels);
      return;
    }
    const state = String(levels.state || "").toUpperCase();
    const quotePrice = asNum(levels.spot);
    const chartPrice = currentChartPrice();
    const price = chartPrice ?? quotePrice;
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
      title: "P",
      value: price,
      color: HERO_CHART_THEME.spx,
      width: 2,
      style: 2,
      axisLabelColor: HERO_CHART_THEME.labelBlueBg,
      axisLabelTextColor: HERO_CHART_THEME.labelBlueText,
    });
    pushRailRow("spot", "P", price, "current");
    levelRailRows = nextRailRows;
    renderLevelRail(levels);
  };

  const renderLevelRail = (levels) => {
    const railRows = [
      ...(displayPrefs.showLevels ? levelRailRows : []),
      ...(displayPrefs.showDayLevels ? dayLevelRailRows : []),
    ];
    if (!displayPrefs.showLevels && !displayPrefs.showDayLevels) {
      if (levelRail) levelRail.innerHTML = "";
      return;
    }
    if (!levelRail || !Array.isArray(railRows) || !railRows.length) {
      if (levelRail) levelRail.innerHTML = "";
      return;
    }
    const railHeight = levelRail.getBoundingClientRect().height || 320;
    const plottedRows = railRows
      .map((row) => ({ ...row, y: candleSeries.priceToCoordinate(row.value) }))
      .filter((row) => Number.isFinite(row.y))
      .sort((a, b) => a.y - b.y);
    if (!plottedRows.length) {
      levelRail.innerHTML = "";
      return;
    }

    const topLimit = 18;
    const bottomLimit = Math.max(topLimit, railHeight - 18);
    plottedRows.forEach((row) => {
      row.displayY = Math.min(Math.max(Number(row.y), topLimit), bottomLimit);
    });
    for (let index = 1; index < plottedRows.length; index += 1) {
      plottedRows[index].displayY = Math.max(
        plottedRows[index].displayY,
        plottedRows[index - 1].displayY + LEVEL_RAIL_MIN_GAP,
      );
    }
    const overflow = plottedRows[plottedRows.length - 1].displayY - bottomLimit;
    if (overflow > 0) {
      plottedRows[plottedRows.length - 1].displayY = bottomLimit;
      for (let index = plottedRows.length - 2; index >= 0; index -= 1) {
        plottedRows[index].displayY = Math.min(
          plottedRows[index].displayY,
          plottedRows[index + 1].displayY - LEVEL_RAIL_MIN_GAP,
        );
      }
    }

    levelRail.innerHTML = plottedRows.map((row) => `
      <div class="marketPulseExecutionHeroLevelRailItem is-${row.kind} ${row.emphasis ? `is-${row.emphasis}` : ""}" title="${row.title} ${fmt(row.value, railDigitsForKind(row.kind))}" style="top:${Math.max(12, row.displayY)}px">
        <span>${row.title}</span>
        <strong>${fmt(row.value, railDigitsForKind(row.kind))}</strong>
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
    const requestSerial = barsRequestSerial;
    const requestInterval = interval;
    try {
      const payload = await fetchJson(barsEndpoint());
      if (requestSerial !== barsRequestSerial || requestInterval !== interval) return;
      const bars = Array.isArray(payload.bars) ? payload.bars : [];
      if (!bars.length) {
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `Tradier returned no intraday bars for ${symbol}.`;
        }
        return;
      }

      const normalizedBars = bars
        .map((bar) => ({
          source: bar,
          candle: {
            time: Number(bar.time),
            open: Number(bar.open),
            high: Number(bar.high),
            low: Number(bar.low),
            close: Number(bar.close),
          },
        }))
        .filter(({ candle }) => Number.isFinite(candle.time) && Number.isFinite(candle.open) && Number.isFinite(candle.high) && Number.isFinite(candle.low) && Number.isFinite(candle.close));
      const candles = normalizedBars.map(({ candle }) => candle);
      const sourceBars = normalizedBars.map(({ source }) => source);
      if (!candles.length) {
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `Tradier returned invalid intraday bars for ${symbol}.`;
        }
        return;
      }

      const nextBarsSignature = barsSignature(candles, payload);
      const previousSessionBarCount = Math.max(0, Number(payload.previous_session_bar_count) || 0);
      const currentSessionBarCount = Math.max(0, Number(payload.current_session_bar_count) || 0);
      const boundedPreviousCount = Math.min(previousSessionBarCount, candles.length);
      const boundedCurrentCount = Math.min(currentSessionBarCount, Math.max(0, candles.length - boundedPreviousCount));
      const splitIndex = boundedCurrentCount > 0 ? boundedPreviousCount : candles.length;
      const priorCandles = candles.slice(0, splitIndex);
      const currentCandles = candles.slice(splitIndex);
      const activeCandles = currentCandles.length ? currentCandles : candles;
      const activeSourceBars = currentCandles.length ? sourceBars.slice(splitIndex) : sourceBars;
      const latestCandle = activeCandles[activeCandles.length - 1];
      const latestSourceBar = activeSourceBars[activeSourceBars.length - 1];
      const nextLiveBarSignature = liveBarSignature(latestCandle, latestSourceBar);

      lastBarsPayload = {
        ...payload,
        bars: candles,
      };

      if (initialized && !fitContent && nextBarsSignature === lastBarsSignature) {
        if (nextLiveBarSignature !== lastLiveBarSignature && latestCandle) {
          candleSeries.update(latestCandle);
          const latestVolume = volumeBarForSource(latestSourceBar);
          if (latestVolume) volumeSeries.update(latestVolume);
          lastLiveBarSignature = nextLiveBarSignature;
        }
        applyStratMarkers();
        updateDayLevelLines();
        setSpotTrendTone(detectShortTermTrend(activeCandles));
        if (emptyState) emptyState.hidden = true;
        return;
      }

      if (nextBarsSignature === lastBarsSignature) {
        if (fitContent) applyViewport({ fitContent: true });
        applyStratMarkers();
        updateDayLevelLines();
        if (emptyState) emptyState.hidden = true;
        return;
      }

      const volume = volumeBarsForSource(activeSourceBars);

      priorSessionSeries.setData(currentCandles.length ? priorCandles : []);
      candleSeries.setData(activeCandles);
      applyStratMarkers({ force: true });
      volumeSeries.setData(volume);
      updateDayLevelLines();
      lastBarsSignature = nextBarsSignature;
      lastLiveBarSignature = nextLiveBarSignature;
      setSpotTrendTone(detectShortTermTrend(activeCandles));
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
    const nextHeight = Math.max(360, Math.round(canvas.clientHeight || HERO_CHART_HEIGHT));
    if (!nextWidth || (nextWidth === lastMeasuredWidth && nextHeight === lastMeasuredHeight)) return;
    lastMeasuredWidth = nextWidth;
    lastMeasuredHeight = nextHeight;
    try {
      chart.applyOptions({ width: nextWidth, height: nextHeight });
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
    if (dayLevelsToggle) {
      dayLevelsToggle.addEventListener("click", () => {
        displayPrefs.showDayLevels = !displayPrefs.showDayLevels;
        saveDisplayPrefs();
        syncToggleButtons();
        lastDayLevelSignature = "";
        updateDayLevelLines();
      });
    }
  };

  const bindIntervalToggles = () => {
    intervalToggles.forEach((button) => {
      button.addEventListener("click", async () => {
        const nextInterval = normalizeInterval(button.dataset.heroChartInterval);
        if (nextInterval === interval) return;

        interval = nextInterval;
        host.dataset.interval = nextInterval;
        barsRequestSerial += 1;
        barsRequestInFlight = false;
        lastBarsPayload = null;
        lastBarsSignature = "";
        lastLiveBarSignature = "";
        lastMarkerSignature = "";
        lastDayLevelSignature = "";
        syncIntervalControls();
        clearPollTimers();
        priorSessionSeries.setData([]);
        candleSeries.setData([]);
        volumeSeries.setData([]);
        candleSeries.setMarkers([]);
        renderLevelRail(lastLevelsPayload);
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `Loading ${symbol} ${INTERVAL_LABELS[interval]} chart...`;
        }
        try {
          await updateBars({ fitContent: true });
        } catch (error) {
          if (emptyState) {
            emptyState.hidden = false;
            emptyState.textContent = `${symbol} ${INTERVAL_LABELS[interval]} chart failed: ${error.message}`;
          }
        } finally {
          startPolling();
        }
      });
    });
  };

  loadDisplayPrefs();
  syncToggleButtons();
  syncIntervalControls();
  bindDisplayToggles();
  bindIntervalToggles();
  applyLevelVisibility();
  window.addEventListener("resize", () => {
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(resize, RESIZE_DEBOUNCE_MS);
  });
  if ("ResizeObserver" in window) {
    resizeObserver = new ResizeObserver(() => {
      window.clearTimeout(resizeTimer);
      resizeTimer = window.setTimeout(resize, RESIZE_DEBOUNCE_MS);
    });
    resizeObserver.observe(canvas);
  }
  try {
    chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
      renderLevelRail(lastLevelsPayload);
    });
  } catch (_) {}
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
  resize();
  boot();
})();
