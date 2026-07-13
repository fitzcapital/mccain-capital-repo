(() => {
  "use strict";

  window.__mcHeroApiDriven = true;

  const host = document.getElementById("spxExecutionHeroChart");
  const canvas = document.getElementById("spxExecutionHeroChartCanvas");
  const levelRail = document.getElementById("spxExecutionHeroLevelRail");
  const drawToggle = document.getElementById("marketPulseHeroToggleDraw");
  const undoDrawButton = document.getElementById("marketPulseHeroUndoDraw");
  const clearDrawButton = document.getElementById("marketPulseHeroClearDraw");
  const markersToggle = document.getElementById("marketPulseHeroToggleMarkers");
  const levelsToggle = document.getElementById("marketPulseHeroToggleLevels");
  const dayLevelsToggle = document.getElementById("marketPulseHeroToggleDayLevels");
  const intervalToggles = Array.from(document.querySelectorAll("[data-hero-chart-interval]"));
  const emptyState = document.getElementById("spxExecutionHeroChartEmpty");
  const pollStatusNode = document.getElementById("marketPulseHeroPollStatus");
  const sessionBreakLabel = document.getElementById("spxExecutionHeroSessionBreak");
  if (!host || !canvas) return;

  const LightweightCharts = window.LightweightCharts;
  if (!LightweightCharts || typeof LightweightCharts.createChart !== "function") {
    if (emptyState) emptyState.textContent = "Lightweight Charts failed to load.";
    return;
  }

  const barsUrl = String(host.dataset.barsUrl || "/api/hero/bars");
  const levelsUrl = String(host.dataset.levelsUrl || "/api/hero/levels");
  const quoteUrl = String(host.dataset.quoteUrl || "/api/hero/quote");
  const streamUrl = String(host.dataset.streamUrl || "/api/hero/stream-session");
  const symbol = String(host.dataset.symbol || "SPY").toUpperCase();
  const DEFAULT_INTERVAL = "5min";
  const INTERVAL_LABELS = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "1h": "1h",
    "4h": "4H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1mo": "1M",
  };
  const INTERVAL_ALIASES = {
    "1": "1min",
    "1m": "1min",
    "1min": "1min",
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
    "4h": "4h",
    "240": "4h",
    "240m": "4h",
    "240min": "4h",
    "12h": "12h",
    "720": "12h",
    "720m": "12h",
    "720min": "12h",
    "1d": "1d",
    "1day": "1d",
    "1w": "1w",
    "1wk": "1w",
    "1week": "1w",
    "1mo": "1mo",
    "1mon": "1mo",
    "1month": "1mo",
  };
  const normalizeInterval = (value) => (
    INTERVAL_ALIASES[String(value || DEFAULT_INTERVAL).trim().toLowerCase()] || DEFAULT_INTERVAL
  );
  const INTERVAL_MINUTES = {
    "1min": 1,
    "5min": 5,
    "15min": 15,
    "30min": 30,
    "1h": 60,
    "4h": 240,
    "12h": 720,
    "1d": 1440,
    "1w": 10080,
    "1mo": 43200,
  };
  const INTERVAL_VISIBLE_BARS = {
    "1min": 90,
    "5min": 60,
    "15min": 48,
    "30min": 40,
    "1h": 32,
    "4h": 28,
    "12h": 24,
    "1d": 30,
    "1w": 26,
    "1mo": 24,
  };
  let interval = normalizeInterval(host.dataset.interval || DEFAULT_INTERVAL);
  const HERO_CHART_TIMEZONE = "America/New_York";
  const HERO_TIME_AXIS_FORMATTER = new Intl.DateTimeFormat("en-US", {
    timeZone: HERO_CHART_TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const HERO_DATE_AXIS_FORMATTER = new Intl.DateTimeFormat("en-US", {
    timeZone: HERO_CHART_TIMEZONE,
    month: "short",
    day: "numeric",
  });
  let heroAxisShowsDates = false;
  const formatAxisTime = (time) => {
    const date = new Date(Number(time) * 1000);
    const clock = HERO_TIME_AXIS_FORMATTER.format(date);
    if (!heroAxisShowsDates) return clock;
    if (clock === "09:30" || clock === "10:00" || clock === "16:00") {
      return HERO_DATE_AXIS_FORMATTER.format(date);
    }
    return clock;
  };
  const HERO_CHART_THEME = {
    background: "#071421",
    panel: "#0B1F3A",
    border: "#2F6BFF",
    textPrimary: "#F4F8FF",
    textSecondary: "#C7D0D9",
    gridMajor: "rgba(199, 208, 217, 0.18)",
    gridMinor: "rgba(199, 208, 217, 0.09)",
    axis: "rgba(199, 208, 217, 0.30)",
    bull: "#F4F8FF",
    bullBorder: "#E6EEF9",
    bullWick: "#F4F8FF",
    bear: "#4988FF",
    bearBorder: "#3C79F2",
    bearWick: "#4988FF",
    bullMuted: "rgba(244, 248, 255, 0.72)",
    bullBorderMuted: "rgba(230, 238, 249, 0.78)",
    bullWickMuted: "rgba(244, 248, 255, 0.72)",
    bearMuted: "rgba(73, 136, 255, 0.68)",
    bearBorderMuted: "rgba(60, 121, 242, 0.76)",
    bearWickMuted: "rgba(73, 136, 255, 0.70)",
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
    stratUp: "#F4F8FF",
    stratDown: "#4988FF",
    stratInside: "#AFC5E8",
    stratOutside: "#E6EEF9",
    pdh: "#f7d56f",
    pdl: "#b78cff",
    cdh: "#63f7d4",
    cdl: "#ff8aa3",
  };
  const DEFAULT_VISIBLE_BARS = 20;
  const LEFT_SCROLL_BUFFER_BARS = 3;
  const DEFAULT_RIGHT_OFFSET_BARS = 4;
  const HERO_CHART_HEIGHT = 600;
  const LEGACY_HERO_CHART_PREFS_KEY = "mc_hero_chart_display_prefs";
  const HERO_CHART_PREFS_KEY = `mc_hero_chart_display_prefs_${symbol}`;
  const HERO_CHART_DRAWINGS_KEY = `mc_hero_chart_drawings_${symbol}`;
  const STRAT_MARKER_LIMIT = 96;
  const LEVEL_RAIL_MIN_GAP = 38;

  const priceScaleWidth = 96;
  const chart = LightweightCharts.createChart(canvas, {
    autoSize: false,
    height: HERO_CHART_HEIGHT,
    layout: {
      background: { type: LightweightCharts.ColorType.Solid, color: HERO_CHART_THEME.background },
      textColor: HERO_CHART_THEME.textSecondary,
      fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", sans-serif',
      fontSize: 10,
    },
    grid: {
      vertLines: { color: HERO_CHART_THEME.gridMinor },
      horzLines: { color: HERO_CHART_THEME.gridMajor },
    },
    crosshair: {
      vertLine: { color: "rgba(99, 179, 255, 0.16)", width: 1, style: 2 },
      horzLine: {
        color: "rgba(46, 211, 198, 0.12)",
        width: 1,
        style: 2,
        labelVisible: false,
      },
    },
    rightPriceScale: {
      borderColor: HERO_CHART_THEME.axis,
      scaleMargins: { top: 0.18, bottom: 0.22 },
      minimumWidth: priceScaleWidth,
      entireTextOnly: true,
    },
    timeScale: {
      borderColor: HERO_CHART_THEME.axis,
      timeVisible: true,
      secondsVisible: false,
      rightOffset: DEFAULT_RIGHT_OFFSET_BARS,
      barSpacing: 10,
      fixLeftEdge: false,
      lockVisibleTimeRangeOnResize: true,
      tickMarkFormatter: formatAxisTime,
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
  let spotPriceLines = [];
  let dayLevelLines = [];
  let polling = {
    session_phase: "open",
    bars_interval_ms: 10000,
    quote_interval_ms: 3000,
    levels_interval_ms: 45000,
    closed_bars_interval_ms: 180000,
    closed_quote_interval_ms: 60000,
    closed_levels_interval_ms: 600000,
    hidden_bars_interval_ms: 120000,
    hidden_quote_interval_ms: 15000,
    hidden_levels_interval_ms: 300000,
    bar_boundary_grace_ms: 3000,
  };
  let barsTimer = null;
  let levelsTimer = null;
  let quoteTimer = null;
  let initialized = false;
  let lastBarsPayload = null;
  let lastLevelsPayload = null;
  let lastGoodLevelsPayload = null;
  let lastAppliedLevelsSignature = "";
  let lastBarsSignature = "";
  let lastLiveBarSignature = "";
  let lastQuotePatchSignature = "";
  let lastMarkerSignature = "";
  let lastDayLevelSignature = "";
  let pendingLevelsPayload = null;
  let levelsRenderTimer = null;
  let resizeTimer = null;
  let resizeObserver = null;
  let barsRequestInFlight = false;
  let levelsRequestInFlight = false;
  let quoteRequestInFlight = false;
  let barsRequestSerial = 0;
  let pageVisible = document.visibilityState !== "hidden";
  let lastMeasuredWidth = 0;
  let lastMeasuredHeight = 0;
  let lastDataShapeSignature = "";
  let levelRailRows = [];
  let dayLevelRailRows = [];
  let pollStatus = {
    bars: { label: "Bars", state: "pending", text: "pending" },
    quote: { label: "Quote", state: "pending", text: "pending" },
    levels: { label: "Levels", state: "pending", text: "pending" },
  };
  let displayPrefs = { showMarkers: true, showLevels: true, showDayLevels: true };
  let drawingState = { enabled: false, lines: [], series: [] };
  const LEVEL_RENDER_DEBOUNCE_MS = 120;
  const HIDDEN_BARS_INTERVAL_MS = 120000;
  const HIDDEN_LEVELS_INTERVAL_MS = 300000;
  const HIDDEN_QUOTE_INTERVAL_MS = 15000;
  const RESIZE_DEBOUNCE_MS = 140;
  const HERO_LEVEL_KEYS = [
    "main_flip",
    "local_flip",
    "call_wall",
    "put_wall",
    "next_call_wall",
    "next_put_wall",
  ];
  const OFF_CHART_LEVEL_PCT = 0.018;

  const asNum = (value) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  };

  const asChartTime = (value) => {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (value && typeof value === "object") {
      const year = Number(value.year);
      const month = Number(value.month);
      const day = Number(value.day);
      if ([year, month, day].every(Number.isFinite)) {
        return Math.floor(Date.UTC(year, month - 1, day) / 1000);
      }
    }
    return null;
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

  const LOCAL_FLIP_NONE_LABEL = "No local flip in band";

  const renderHeaderLevels = (items) => {
    const node = document.getElementById("marketPulseHeaderSubline");
    if (!node) return;
    node.innerHTML = (Array.isArray(items) ? items : []).map((item) => {
      const label = String(item.label || "");
      const value = String(item.value ?? "—");
      const muted = label === "Local Flip" && value === LOCAL_FLIP_NONE_LABEL ? " is-muted" : "";
      return `<div class="marketPulseHeaderLevelItem${muted}"><span>${label}</span><strong>${value}</strong></div>`;
    }).join("");
  };

  const parseDateValue = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return null;
    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  };

  const formatClock = (value) => {
    const date = value instanceof Date ? value : parseDateValue(value);
    return date ? HERO_TIME_AXIS_FORMATTER.format(date) : "pending";
  };

  const formatAge = (value) => {
    const date = value instanceof Date ? value : parseDateValue(value);
    if (!date) return "pending";
    const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    return "stale";
  };

  const renderPollStatus = () => {
    if (!pollStatusNode) return;
    const parts = ["bars", "quote", "levels"].map((key) => {
      const entry = pollStatus[key] || {};
      return `${entry.label || key} ${entry.text || "pending"}`;
    });
    const state = Object.values(pollStatus).some((entry) => entry.state === "error")
      ? "error"
      : Object.values(pollStatus).some((entry) => entry.state === "stale")
        ? "stale"
        : Object.values(pollStatus).some((entry) => entry.state === "pending")
          ? "pending"
          : "fresh";
    pollStatusNode.textContent = parts.join(" · ");
    pollStatusNode.classList.remove("is-pending", "is-fresh", "is-stale", "is-error");
    pollStatusNode.classList.add(`is-${state}`);
  };

  const setPollStatus = (key, state, text) => {
    pollStatus = {
      ...pollStatus,
      [key]: {
        ...(pollStatus[key] || {}),
        state,
        text,
      },
    };
    renderPollStatus();
  };

  const setToneVariant = (id, variants, active) => {
    const node = document.getElementById(id);
    if (!node) return;
    variants.forEach((variant) => node.classList.remove(variant));
    if (active) node.classList.add(active);
  };

  const renderSimpleBadges = (id, labels) => {
    const root = document.getElementById(id);
    if (!root) return;
    const normalized = (Array.isArray(labels) ? labels : [])
      .map((label) => String(label || "").trim())
      .filter(Boolean);
    root.innerHTML = normalized.map((label) => `<span class="marketPulseGammaBadge">${label}</span>`).join("");
  };

  const regimeDisplayLabel = (state, fallback = "REGIME UNAVAILABLE") => {
    if (state === "positive") return "Positive Ⲅ";
    if (state === "negative") return "Negative Ⲅ";
    return fallback;
  };

  const gammaStateMeta = (state) => {
    const normalized = String(state || "").toLowerCase();
    if (normalized === "positive") {
      return {
        cardClass: "gamma-card--positive",
        pillClass: "state-pill--positive",
        pillLabel: "POSITIVE Ⲅ",
        badges: ["PINNING", "MEAN REVERSION"],
      };
    }
    if (normalized === "negative") {
      return {
        cardClass: "gamma-card--negative",
        pillClass: "state-pill--negative",
        pillLabel: "NEGATIVE Ⲅ",
        badges: ["EXPANSION RISK", "TREND CONTINUATION"],
      };
    }
    if (normalized === "unconfirmed") {
      return {
        cardClass: "gamma-card--unconfirmed",
        pillClass: "state-pill--wait",
        pillLabel: "WAIT FOR CONFIRMATION",
        badges: ["WAIT FOR CONFIRMATION"],
      };
    }
    return {
      cardClass: "gamma-card--neutral",
      pillClass: "state-pill--wait",
      pillLabel: "NEUTRAL / DATA",
      badges: ["WAIT FOR CONFIRMATION"],
    };
  };

  const decisionStateMeta = (label) => {
    const normalized = String(label || "").trim().toLowerCase();
    if (normalized === "actionable") {
      return { pillClass: "state-pill--execute", pillLabel: "EXECUTE" };
    }
    if (normalized === "planning only") {
      return { pillClass: "state-pill--wait", pillLabel: "PLANNING ONLY" };
    }
    if (normalized === "no trade") {
      return { pillClass: "state-pill--negative", pillLabel: "NO TRADE" };
    }
    return { pillClass: "state-pill--wait", pillLabel: "WAIT" };
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

  const drawingRayEndTime = (startTime) => {
    const intervalMinutes = INTERVAL_MINUTES[interval] || INTERVAL_MINUTES[DEFAULT_INTERVAL] || 5;
    const extensionSeconds = intervalMinutes * 60 * 240;
    const bars = Array.isArray(lastBarsPayload?.bars) ? lastBarsPayload.bars : [];
    const lastBarTime = bars.length ? asChartTime(bars[bars.length - 1]?.time) : null;
    const anchorTime = Math.max(
      Number.isFinite(lastBarTime) ? lastBarTime : 0,
      Number.isFinite(startTime) ? startTime : 0,
    );
    return anchorTime + extensionSeconds;
  };

  const loadDrawings = () => {
    try {
      const raw = window.localStorage.getItem(HERO_CHART_DRAWINGS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      drawingState.lines = parsed
        .map((line) => ({
          time: asChartTime(line?.time ?? line?.startTime),
          value: asNum(line?.value ?? line?.startValue),
        }))
        .filter((line) => (
          Number.isFinite(line.time)
          && line.value !== null
        ));
    } catch (_) {
      drawingState.lines = [];
    }
  };

  const saveDrawings = () => {
    try {
      window.localStorage.setItem(HERO_CHART_DRAWINGS_KEY, JSON.stringify(drawingState.lines));
    } catch (_) {}
  };

  const clearDrawingSeries = () => {
    drawingState.series.forEach((series) => {
      try {
        chart.removeSeries(series);
      } catch (_) {}
    });
    drawingState.series = [];
  };

  const drawingLineData = (line) => {
    if (!line) return null;
    const startTime = asChartTime(line.time);
    const value = asNum(line.value);
    const endTime = drawingRayEndTime(startTime);
    if (
      !Number.isFinite(startTime)
      || !Number.isFinite(endTime)
      || value === null
      || endTime <= startTime
    ) {
      return null;
    }
    return [
      { time: startTime, value },
      { time: endTime, value },
    ];
  };

  const renderDrawings = () => {
    clearDrawingSeries();
    drawingState.lines.forEach((line) => {
      const data = drawingLineData(line);
      if (!data) return;
      const series = chart.addLineSeries({
        color: "rgba(99, 179, 255, 0.95)",
        lineWidth: 2,
        lineStyle: 0,
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      series.setData(data);
      drawingState.series.push(series);
    });
  };

  const syncDrawingButtons = () => {
    if (drawToggle) {
      drawToggle.setAttribute("aria-pressed", drawingState.enabled ? "true" : "false");
      drawToggle.classList.toggle("is-active", drawingState.enabled);
      drawToggle.title = "Place horizontal rays";
    }
    if (undoDrawButton) {
      const hasLines = drawingState.lines.length > 0;
      undoDrawButton.disabled = !hasLines;
      undoDrawButton.setAttribute("aria-disabled", hasLines ? "false" : "true");
    }
    if (clearDrawButton) {
      const hasLines = drawingState.lines.length > 0;
      clearDrawButton.disabled = !hasLines;
      clearDrawButton.setAttribute("aria-disabled", hasLines ? "false" : "true");
    }
    canvas.style.cursor = drawingState.enabled ? "crosshair" : "";
  };

  const removeLastDrawing = () => {
    if (!drawingState.lines.length) return;
    drawingState.lines = drawingState.lines.slice(0, -1);
    saveDrawings();
    renderDrawings();
    syncDrawingButtons();
  };

  const clearAllDrawings = () => {
    if (!drawingState.lines.length) return;
    drawingState.lines = [];
    saveDrawings();
    renderDrawings();
    syncDrawingButtons();
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

  const sessionBreakMarkersForPayload = (payload) => {
    const bars = Array.isArray(payload?.bars) ? payload.bars : [];
    const previousCount = Math.max(0, Number(payload?.previous_session_bar_count) || 0);
    const currentCount = Math.max(0, Number(payload?.current_session_bar_count) || 0);
    if (!bars.length || previousCount <= 0 || currentCount <= 0 || previousCount >= bars.length) {
      return [];
    }
    const firstCurrent = bars[previousCount];
    const time = Number(firstCurrent?.time);
    if (!Number.isFinite(time)) return [];
    return [{ time }];
  };

  const renderSessionBreakLabel = () => {
    if (!sessionBreakLabel) return;
    const marker = sessionBreakMarkersForPayload(lastBarsPayload)[0];
    if (!marker) {
      sessionBreakLabel.hidden = true;
      host.classList.remove("has-session-break");
      return;
    }
    const x = chart.timeScale().timeToCoordinate(marker.time);
    if (!Number.isFinite(x)) {
      sessionBreakLabel.hidden = true;
      host.classList.remove("has-session-break");
      return;
    }
    const width = canvas.clientWidth || host.clientWidth || 0;
    const left = Math.min(Math.max(Number(x), 18), Math.max(18, width - 54));
    sessionBreakLabel.textContent = "Today";
    sessionBreakLabel.style.left = `${left}px`;
    sessionBreakLabel.hidden = false;
    host.classList.add("has-session-break");
  };

  const markersForPayload = (payload) => stratMarkersForPayload(payload);

  const applyStratMarkers = ({ force = false } = {}) => {
    const markers = markersForPayload(lastBarsPayload);
    const nextSignature = markersSignature(markers);
    if (!force && nextSignature === lastMarkerSignature) return;
    candleSeries.setMarkers(markers);
    lastMarkerSignature = nextSignature;
    renderSessionBreakLabel();
  };

  const updateHeaderSummary = (levels) => {
    const gammaState = String(levels.gamma_regime || "").toLowerCase();
    const biasState = String(levels.bias_state || "").toLowerCase();
    const gammaMeta = gammaStateMeta(gammaState);
    const decisionMeta = decisionStateMeta(levels.decision_label || "Not actionable yet");
    const snapshotLabel = String(levels.last_valid_snapshot_time_label || levels.snapshot_timestamp_label || "—");
    const spotSourceLabel = String(levels?.spot_meta?.source_label || "Last Valid Session");
    const localFlip = levels.local_flip === null || levels.local_flip === undefined
      ? (levels.local_flip_found === false ? LOCAL_FLIP_NONE_LABEL : "—")
      : fmtCompactLevel(levels.local_flip, 0);
    renderHeaderLevels([
      { label: "Spot", value: fmt(levels.spot, 2) },
      { label: "Main Flip", value: fmtCompactLevel(levels.main_flip, 0) },
      { label: "Local Flip", value: localFlip },
      { label: "CW", value: fmtCompactLevel(levels.call_wall, 0) },
      { label: "PW", value: fmtCompactLevel(levels.put_wall, 0) },
    ]);
    setText("marketPulseTitle", `${symbol} PLAYBOOK`);
    setText("marketPulseHeaderSnapshot", `${spotSourceLabel} ${snapshotLabel}`);
    const gammaDisplayLabel = regimeDisplayLabel(gammaState, levels.gamma_regime_label || "REGIME UNAVAILABLE");
    setText("marketPulseHeaderGammaLabel", gammaDisplayLabel);
    setText("marketPulseHeaderGammaSummary", levels.hero_summary || `${gammaDisplayLabel || "Regime unavailable"}, ${(levels.bias_summary_label || levels.bias_label || "wait for cleaner structure").toLowerCase()}.`);
    setText("marketPulseHeaderGammaSub", levels.gamma_regime_subtitle || "Gamma snapshot unavailable");
    setText("marketPulseHeaderDecision", levels.decision_label || "Not actionable yet");
    setText("marketPulseHeaderBiasPrimary", levels.bias_summary_label || levels.bias_label || "Wait for cleaner structure");
    setText("marketPulseHeaderBiasSecondary", levels.bias_label || levels.planning_bias_label || "Awaiting valid structure");
    setText("marketPulseHeaderTradeability", levels.tradeability_display_label || String(levels.execution_regime_label || levels.tradeability || "Trigger required").replaceAll("_", " "));
    setText("marketPulseHeaderDecisionStatePill", decisionMeta.pillLabel);
    renderSimpleBadges("marketPulseHeaderBadgeRow", gammaMeta.badges);

    setToneVariant(
      "marketPulseHeaderGammaCard",
      ["is-positive", "is-negative", "is-neutral"],
      gammaState === "positive" ? "is-positive" : gammaState === "negative" ? "is-negative" : "is-neutral",
    );
    setToneVariant(
      "marketPulseHeaderGammaCard",
      ["gamma-card--positive", "gamma-card--negative", "gamma-card--neutral", "gamma-card--unconfirmed"],
      gammaMeta.cardClass,
    );
    setToneVariant(
      "marketPulseHeaderBiasCard",
      ["is-positive", "is-negative", "is-neutral"],
      biasState === "above_local" ? "is-positive" : biasState === "below_local" ? "is-negative" : "is-neutral",
    );
    setToneVariant(
      "marketPulseHeaderDecisionStatePill",
      ["state-pill--positive", "state-pill--negative", "state-pill--wait", "state-pill--execute"],
      decisionMeta.pillClass,
    );
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
      color: close >= open ? "rgba(244, 248, 255, 0.18)" : "rgba(73, 136, 255, 0.18)",
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
  const quoteEndpoint = () => `${quoteUrl}?symbol=${encodeURIComponent(symbol)}`;

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

  const barsShapeSignature = (candles, payload) => {
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
    const merged = {
      ...(lastGoodLevelsPayload || {}),
      ...payload,
    };
    const hasActionableLevel = HERO_LEVEL_KEYS.some((key) => isValidHeroLevel(merged[key]));
    if (!hasActionableLevel) {
      return null;
    }
    return merged;
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

  const activeBarsForPayload = (payload) => {
    const bars = Array.isArray(payload?.bars) ? payload.bars : [];
    if (!bars.length) return [];
    const previousCount = Math.max(0, Number(payload?.previous_session_bar_count) || 0);
    const currentCount = Math.max(0, Number(payload?.current_session_bar_count) || 0);
    if (currentCount > 0 && previousCount < bars.length) return bars.slice(previousCount);
    return bars;
  };

  const intervalShowsDates = () => ["4h", "12h", "1d", "1w", "1mo"].includes(interval);

  const preferredVisibleBars = () => (
    INTERVAL_VISIBLE_BARS[interval] || INTERVAL_VISIBLE_BARS[DEFAULT_INTERVAL] || DEFAULT_VISIBLE_BARS
  );

  const syncAxisSessionMode = (payload) => {
    const previousDay = String(payload?.previous_session_day || "");
    const currentDay = String(payload?.current_session_day || "");
    heroAxisShowsDates = intervalShowsDates() || Boolean(previousDay && currentDay && previousDay !== currentDay);
    try {
      chart.timeScale().applyOptions({ tickMarkFormatter: formatAxisTime });
    } catch (_) {}
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
        ? Math.max(14, Math.min(20, currentCount + 8))
        : Math.max(14, Math.min(20, bars.length));
      const from = currentCount > 0
        ? Math.max(0, previousCount + currentCount - liveWindowBars)
        : Math.max(0, bars.length - liveWindowBars);
      chart.timeScale().applyOptions({
        rightOffset: rightOffsetBars,
        barSpacing: 16,
      });
      chart.timeScale().setVisibleLogicalRange({
        from,
        to: (bars.length - 1) + rightOffsetBars,
      });
      applyFrameBounds(activeBarsForPayload(lastBarsPayload), lastLevelsPayload);
      renderLevelRail(lastLevelsPayload);
      return;
    }

    clearFrameBounds();
    chart.timeScale().applyOptions({
      rightOffset: DEFAULT_RIGHT_OFFSET_BARS,
      barSpacing: 15,
    });
    if (fitContent || !initialized) {
      const intervalVisibleBars = preferredVisibleBars();
      const requestedVisibleBars = Math.max(
        Number(lastBarsPayload.visible_window_bars) || 0,
        Math.min(bars.length, intervalVisibleBars),
      );
      const visibleBars = Math.max(
        Math.min(bars.length, requestedVisibleBars, intervalVisibleBars) + LEFT_SCROLL_BUFFER_BARS,
        Math.min(bars.length, intervalVisibleBars),
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

  const clearSpotPriceLines = () => {
    spotPriceLines.forEach((line) => {
      try {
        candleSeries.removePriceLine(line);
      } catch (_) {}
    });
    spotPriceLines = [];
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
      clearSpotPriceLines();
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

  const addSpotPriceLine = ({
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
    spotPriceLines.push(
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
    clearSpotPriceLines();
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
    addSpotPriceLine({
      title: "",
      value: price,
      color: HERO_CHART_THEME.current,
      width: 1,
      style: 1,
      axis: false,
    });
    addSpotPriceLine({
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

  const updateSpotOverlayPrice = (price) => {
    const numeric = asNum(price);
    if (numeric === null) return;
    clearSpotPriceLines();
    levelRailRows = [
      ...levelRailRows.filter((row) => row.kind !== "spot"),
      { kind: "spot", title: "P", value: numeric, emphasis: "current" },
    ];
    if (displayPrefs.showLevels) {
      addSpotPriceLine({
        title: "",
        value: numeric,
        color: HERO_CHART_THEME.current,
        width: 1,
        style: 1,
        axis: false,
      });
      addSpotPriceLine({
        title: "P",
        value: numeric,
        color: HERO_CHART_THEME.spx,
        width: 2,
        style: 2,
        axisLabelColor: HERO_CHART_THEME.labelBlueBg,
        axisLabelTextColor: HERO_CHART_THEME.labelBlueText,
      });
    }
    renderLevelRail(lastLevelsPayload);
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
    const state = String(levels.decision_label || levels.trade_state_label || levels.state || "WATCH").replaceAll("_", " ");
    const currentRead = String(levels.current_read || "Await structure");
    const headline = `${state} - ${currentRead}`.toUpperCase();

    updateHeaderSummary(levels);
    setText("marketPulseHeaderSpot", fmt(levels.spot, 2));
    setText("marketPulseHeroSpot", fmt(levels.spot, 2));
    setText("marketPulseHeroSpotLabel", levels?.spot_source_short_label || levels?.spot_meta?.source_label || `${symbol} Spot`);
    setText("marketPulseHeroGamma", regimeDisplayLabel(String(levels.gamma_regime || "").toLowerCase(), levels.gamma_regime_label || "Regime Unavailable"));
    setText("marketPulseHeroBias", levels.bias_summary_label || levels.current_read || levels.bias_context || levels.bias_label || "Awaiting structure");
    const tradeability = String(
      levels.tradeability_display_label || levels.execution_regime_label || levels.tradeability || "Reduced confidence"
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
        setPollStatus("bars", "error", "empty");
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
        setPollStatus("bars", "error", "invalid");
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `Tradier returned invalid intraday bars for ${symbol}.`;
        }
        return;
      }

      const nextBarsSignature = barsSignature(candles, payload);
      const nextDataShapeSignature = barsShapeSignature(candles, payload);
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
      syncAxisSessionMode(lastBarsPayload);
      setPollStatus("bars", "fresh", formatClock(payload.latest_bar_time || payload.fetched_at));

      if (initialized && !fitContent && nextBarsSignature === lastBarsSignature) {
        if (nextLiveBarSignature !== lastLiveBarSignature && latestCandle) {
          candleSeries.update(latestCandle);
          const latestVolume = volumeBarForSource(latestSourceBar);
          if (latestVolume) volumeSeries.update(latestVolume);
          lastLiveBarSignature = nextLiveBarSignature;
        }
        applyStratMarkers();
        updateDayLevelLines();
        renderDrawings();
        setSpotTrendTone(detectShortTermTrend(activeCandles));
        if (emptyState) emptyState.hidden = true;
        return;
      }

      if (nextBarsSignature === lastBarsSignature) {
        if (fitContent) applyViewport({ fitContent: true });
        applyStratMarkers();
        updateDayLevelLines();
        renderDrawings();
        if (emptyState) emptyState.hidden = true;
        return;
      }

      const volume = volumeBarsForSource(activeSourceBars);
      const canPatchLatestBar = (
        initialized
        && !fitContent
        && nextDataShapeSignature === lastDataShapeSignature
        && latestCandle
      );

      if (canPatchLatestBar) {
        candleSeries.update(latestCandle);
        const latestVolume = volumeBarForSource(latestSourceBar);
        if (latestVolume) volumeSeries.update(latestVolume);
        applyStratMarkers();
        updateDayLevelLines();
        renderDrawings();
        lastBarsSignature = nextBarsSignature;
        lastDataShapeSignature = nextDataShapeSignature;
        lastLiveBarSignature = nextLiveBarSignature;
        setSpotTrendTone(detectShortTermTrend(activeCandles));
        if (emptyState) emptyState.hidden = true;
        return;
      }

      priorSessionSeries.setData(currentCandles.length ? priorCandles : []);
      candleSeries.setData(activeCandles);
      applyStratMarkers({ force: true });
      volumeSeries.setData(volume);
      updateDayLevelLines();
      renderDrawings();
      lastBarsSignature = nextBarsSignature;
      lastDataShapeSignature = nextDataShapeSignature;
      lastLiveBarSignature = nextLiveBarSignature;
      setSpotTrendTone(detectShortTermTrend(activeCandles));
      if (fitContent || !initialized) applyViewport({ fitContent });
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
        setPollStatus("levels", "error", "invalid");
        console.warn(`${symbol} hero levels update skipped: invalid level payload`, payload);
        return;
      }
      setPollStatus("levels", "fresh", formatClock(nextLevels.as_of || nextLevels.snapshot_timestamp));
      const nextSignature = levelsSignature(nextLevels);
      if (nextSignature === lastAppliedLevelsSignature) {
        lastLevelsPayload = nextLevels;
        lastGoodLevelsPayload = nextLevels;
        return;
      }
      scheduleLevelsApply(nextLevels);
    } catch (error) {
      setPollStatus("levels", "error", "error");
      console.warn(`${symbol} hero levels update failed`, error);
    } finally {
      levelsRequestInFlight = false;
    }
  };

  const updateQuotePatch = async () => {
    if (!initialized || barsRequestInFlight || quoteRequestInFlight) return;
    const bars = Array.isArray(lastBarsPayload?.bars) ? lastBarsPayload.bars : [];
    if (!bars.length) return;

    quoteRequestInFlight = true;
    try {
      const payload = await fetchJson(quoteEndpoint());
      const price = asNum(payload?.price);
      if (price === null || price <= 0) {
        setPollStatus("quote", "error", "unavailable");
        return;
      }
      setPollStatus("quote", "fresh", formatAge(payload?.as_of || new Date()));

      const signature = [
        Number(price).toFixed(4),
        String(payload?.as_of || ""),
        String(payload?.provider || ""),
      ].join("|");
      if (signature === lastQuotePatchSignature) return;

      lastQuotePatchSignature = signature;
      setText("marketPulseHeaderSpot", fmt(price, 2));
      setText("marketPulseHeroSpot", fmt(price, 2));
      updateSpotOverlayPrice(price);
    } finally {
      quoteRequestInFlight = false;
    }
  };

  const clearPollTimers = () => {
    window.clearTimeout(barsTimer);
    window.clearTimeout(levelsTimer);
    window.clearTimeout(quoteTimer);
    barsTimer = null;
    levelsTimer = null;
    quoteTimer = null;
  };

  const pollingNumber = (key, fallback) => {
    const value = Number(polling?.[key]);
    return Number.isFinite(value) && value > 0 ? value : fallback;
  };

  const marketIsOpenForPolling = () => {
    const phase = String(polling?.session_phase || "").trim().toLowerCase();
    return phase === "open" || phase === "regular" || phase === "live";
  };

  const barsBaseIntervalMs = () => {
    if (!pageVisible) {
      return Math.max(60000, pollingNumber("hidden_bars_interval_ms", HIDDEN_BARS_INTERVAL_MS));
    }
    if (!marketIsOpenForPolling()) {
      return Math.max(120000, pollingNumber("closed_bars_interval_ms", 180000));
    }
    return Math.max(8000, pollingNumber("bars_interval_ms", 10000));
  };

  const quoteBaseIntervalMs = () => {
    if (!pageVisible) {
      return Math.max(15000, pollingNumber("hidden_quote_interval_ms", HIDDEN_QUOTE_INTERVAL_MS));
    }
    if (!marketIsOpenForPolling()) {
      return Math.max(30000, pollingNumber("closed_quote_interval_ms", 60000));
    }
    return Math.max(3000, pollingNumber("quote_interval_ms", 3000));
  };

  const levelsBaseIntervalMs = () => {
    if (!pageVisible) {
      return Math.max(120000, pollingNumber("hidden_levels_interval_ms", HIDDEN_LEVELS_INTERVAL_MS));
    }
    if (!marketIsOpenForPolling()) {
      return Math.max(300000, pollingNumber("closed_levels_interval_ms", 600000));
    }
    return Math.max(30000, pollingNumber("levels_interval_ms", 45000));
  };

  const nextBarBoundaryDelayMs = () => {
    if (!marketIsOpenForPolling()) return null;
    const minutes = INTERVAL_MINUTES[interval] || INTERVAL_MINUTES[DEFAULT_INTERVAL];
    const boundaryMs = Math.max(60000, minutes * 60 * 1000);
    const graceMs = Math.max(1000, pollingNumber("bar_boundary_grace_ms", 3000));
    const nowMs = Date.now();
    const nextBoundaryMs = Math.ceil(nowMs / boundaryMs) * boundaryMs;
    return Math.max(1000, (nextBoundaryMs + graceMs) - nowMs);
  };

  const nextBarsPollDelayMs = () => {
    const baseMs = barsBaseIntervalMs();
    const boundaryMs = nextBarBoundaryDelayMs();
    if (boundaryMs === null) return baseMs;
    return Math.min(baseMs, boundaryMs);
  };

  const scheduleBarsPoll = (delay) => {
    window.clearTimeout(barsTimer);
    if (!pageVisible) return;
    const fallbackDelay = nextBarsPollDelayMs();
    barsTimer = window.setTimeout(async () => {
      try {
        await updateBars();
      } catch (error) {
        setPollStatus("bars", "error", "error");
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `${symbol} chart refresh failed: ${error.message}`;
        }
      } finally {
        scheduleBarsPoll(nextBarsPollDelayMs());
      }
    }, Math.max(0, Number(delay) || fallbackDelay));
  };

  const scheduleLevelsPoll = (delay) => {
    window.clearTimeout(levelsTimer);
    if (!pageVisible) return;
    const intervalMs = levelsBaseIntervalMs();
    levelsTimer = window.setTimeout(async () => {
      try {
        await updateLevels();
      } catch (_) {
        setPollStatus("levels", "error", "error");
      } finally {
        scheduleLevelsPoll(intervalMs);
      }
    }, Math.max(0, Number(delay) || intervalMs));
  };

  const scheduleQuotePoll = (delay) => {
    window.clearTimeout(quoteTimer);
    if (!pageVisible) return;
    const intervalMs = quoteBaseIntervalMs();
    quoteTimer = window.setTimeout(async () => {
      try {
        await updateQuotePatch();
      } catch (_) {
        setPollStatus("quote", "error", "error");
      } finally {
        scheduleQuotePoll(intervalMs);
      }
    }, Math.max(0, Number(delay) || intervalMs));
  };

  const startPolling = () => {
    clearPollTimers();
    scheduleBarsPoll(nextBarsPollDelayMs());
    scheduleLevelsPoll(levelsBaseIntervalMs());
    scheduleQuotePoll(quoteBaseIntervalMs());
  };

  const boot = async () => {
    try {
      const streamPayload = await fetchJson(streamUrl);
      if (streamPayload && typeof streamPayload === "object") {
        polling = { ...polling, ...streamPayload };
      }
    } catch (_) {}

    try {
      await updateBars({ fitContent: true });
      if (!initialized) {
        window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
      }
      startPolling();
      updateLevels().catch((error) => {
        setPollStatus("levels", "error", "error");
        console.warn(`${symbol} hero levels bootstrap failed`, error);
      });
    } catch (error) {
      if (emptyState) {
        emptyState.hidden = false;
        emptyState.textContent = `${symbol} hero failed to initialize: ${error.message}`;
      }
      window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
      startPolling();
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
    renderSessionBreakLabel();
  };

  const bindDisplayToggles = () => {
    if (drawToggle) {
      drawToggle.addEventListener("click", () => {
        drawingState.enabled = !drawingState.enabled;
        syncDrawingButtons();
      });
    }
    if (undoDrawButton) {
      undoDrawButton.addEventListener("click", () => {
        removeLastDrawing();
      });
    }
    if (clearDrawButton) {
      clearDrawButton.addEventListener("click", () => {
        clearAllDrawings();
      });
    }
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
        lastDataShapeSignature = "";
        lastLiveBarSignature = "";
        lastQuotePatchSignature = "";
        lastMarkerSignature = "";
        lastDayLevelSignature = "";
        setPollStatus("bars", "pending", "loading");
        setPollStatus("quote", "pending", "pending");
        setPollStatus("levels", "pending", "pending");
        syncIntervalControls();
        clearPollTimers();
        priorSessionSeries.setData([]);
        candleSeries.setData([]);
        volumeSeries.setData([]);
        candleSeries.setMarkers([]);
        renderSessionBreakLabel();
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
  loadDrawings();
  renderPollStatus();
  syncToggleButtons();
  syncDrawingButtons();
  syncIntervalControls();
  bindDisplayToggles();
  bindIntervalToggles();
  applyLevelVisibility();
  renderDrawings();
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
      renderSessionBreakLabel();
    });
  } catch (_) {}
  try {
    chart.subscribeClick((param) => {
      if (!drawingState.enabled || !param?.point) return;
      const time = asChartTime(param.time);
      const value = asNum(candleSeries.coordinateToPrice(param.point.y));
      if (!Number.isFinite(time) || value === null) return;
      drawingState.lines = [
        ...drawingState.lines,
        {
          time,
          value,
        },
      ];
      saveDrawings();
      renderDrawings();
      syncDrawingButtons();
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
    scheduleQuotePoll(0);
  });
  resize();
  boot();
})();
