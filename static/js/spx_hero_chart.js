(() => {
  "use strict";

  window.__mcHeroApiDriven = true;

  const host = document.getElementById("spxExecutionHeroChart");
  const canvas = document.getElementById("spxExecutionHeroChartCanvas");
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
  const symbol = String(host.dataset.symbol || "SPX");
  const interval = String(host.dataset.interval || "5min");
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
  };
  const DEFAULT_VISIBLE_BARS = 140;
  const LEFT_SCROLL_BUFFER_BARS = 18;
  const DEFAULT_RIGHT_OFFSET_BARS = 10;

  const priceScaleWidth = 88;
  const chart = LightweightCharts.createChart(canvas, {
    autoSize: true,
    height: 358,
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
      barSpacing: 7.5,
      fixLeftEdge: false,
      lockVisibleTimeRangeOnResize: true,
    },
    localization: {
      locale: "en-US",
      priceFormatter: (value) => Number(value).toFixed(2),
    },
    handleScroll: true,
    handleScale: true,
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
  let polling = { bars_interval_ms: 30000, levels_interval_ms: 10000 };
  let barsTimer = null;
  let levelsTimer = null;
  let initialized = false;
  let lastBarsPayload = null;
  let lastLevelsPayload = null;
  let lastGoodLevelsPayload = null;
  let lastAppliedLevelsSignature = "";
  let pendingLevelsPayload = null;
  let levelsRenderTimer = null;
  const LEVEL_RENDER_DEBOUNCE_MS = 120;
  const HERO_LEVEL_KEYS = [
    "main_flip",
    "local_flip",
    "call_wall",
    "put_wall",
    "next_call_wall",
    "next_put_wall",
  ];

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

  const sanitizeLevelsPayload = (payload) => {
    if (!payload || typeof payload !== "object") return null;
    for (const key of HERO_LEVEL_KEYS) {
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
    [
      levels?.spot,
      levels?.local_flip,
      levels?.call_wall,
      levels?.put_wall,
      levels?.next_call_wall,
      levels?.next_put_wall,
    ].forEach((value) => {
      const numeric = asNum(value);
      if (numeric !== null) prices.push(numeric);
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
    if (openingMode) {
      // Hold a stable opening frame with prior-session carryover and right-side
      // breathing room instead of zooming the hero into the first live candle.
      const rightOffsetBars = Math.max(4, Number(lastBarsPayload.right_offset_bars) || 6);
      const desiredWindowBars = Math.max(
        bars.length,
        Number(lastBarsPayload.visible_window_bars) || 0,
      );
      chart.timeScale().applyOptions({
        rightOffset: rightOffsetBars,
        barSpacing: 11,
      });
      chart.timeScale().setVisibleLogicalRange({
        from: Math.max(-1, bars.length - desiredWindowBars),
        to: (bars.length - 1) + rightOffsetBars,
      });
      applyFrameBounds(bars, lastLevelsPayload);
      return;
    }

    clearFrameBounds();
    chart.timeScale().applyOptions({
      rightOffset: DEFAULT_RIGHT_OFFSET_BARS,
      barSpacing: 7.5,
    });
    if (fitContent || !initialized) {
      const visibleBars = Math.max(
        Math.min(bars.length + LEFT_SCROLL_BUFFER_BARS, DEFAULT_VISIBLE_BARS),
        Math.min(bars.length, 90),
      );
      chart.timeScale().setVisibleLogicalRange({
        from: Math.max(-LEFT_SCROLL_BUFFER_BARS, bars.length - visibleBars - LEFT_SCROLL_BUFFER_BARS),
        to: (bars.length - 1) + DEFAULT_RIGHT_OFFSET_BARS,
      });
    }
  };

  const clearPriceLines = () => {
    priceLines.forEach((line) => {
      try {
        candleSeries.removePriceLine(line);
      } catch (_) {}
    });
    priceLines = [];
  };

  const addLevelLine = ({
    title,
    value,
    color,
    width = 1,
    style = 0,
    axis = true,
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
    const state = String(levels.state || "").toUpperCase();
    const price = asNum(levels.spot);
    const callWall = asNum(levels.call_wall);
    const putWall = asNum(levels.put_wall);
    const localFlip = asNum(levels.local_flip);
    const mainFlip = asNum(levels.main_flip);
    const nextCallWall = asNum(levels.next_call_wall);
    const nextPutWall = asNum(levels.next_put_wall);

    addLevelLine({
      title: "Main",
      value: mainFlip,
      color: HERO_CHART_THEME.main,
      width: 1,
      style: 2,
      axisLabelColor: HERO_CHART_THEME.labelDark,
      axisLabelTextColor: HERO_CHART_THEME.textSecondary,
    });
    addLevelLine({
      title: "NPW",
      value: nextPutWall,
      color: HERO_CHART_THEME.npw,
      width: 1,
      style: 1,
      axisLabelColor: HERO_CHART_THEME.labelMintBg,
      axisLabelTextColor: HERO_CHART_THEME.labelMintText,
    });
    addLevelLine({
      title: "PW",
      value: putWall,
      color: HERO_CHART_THEME.pw,
      width: 2,
      style: 0,
      axisLabelColor: HERO_CHART_THEME.labelGreenBg,
      axisLabelTextColor: HERO_CHART_THEME.labelGreenText,
    });
    addLevelLine({
      title: "LF",
      value: localFlip,
      color: HERO_CHART_THEME.lf,
      width: 3,
      style: 0,
      axisLabelColor: HERO_CHART_THEME.labelLfBg,
      axisLabelTextColor: HERO_CHART_THEME.labelLfText,
    });
    addLevelLine({
      title: "CW",
      value: callWall,
      color: HERO_CHART_THEME.cw,
      width: state === "NO_TRADE" ? 3 : 2,
      style: 0,
      axisLabelColor: HERO_CHART_THEME.labelCwBg,
      axisLabelTextColor: HERO_CHART_THEME.labelCwText,
    });
    addLevelLine({
      title: "NCW",
      value: nextCallWall,
      color: HERO_CHART_THEME.ncw,
      width: 1,
      style: 1,
      axisLabelColor: HERO_CHART_THEME.labelNcwBg,
      axisLabelTextColor: HERO_CHART_THEME.labelNcwText,
    });
    addLevelLine({
      title: "",
      value: price,
      color: HERO_CHART_THEME.current,
      width: 1,
      style: 1,
      axis: false,
    });
    addLevelLine({
      title: "SPX",
      value: price,
      color: HERO_CHART_THEME.spx,
      width: 2,
      style: 2,
      axisLabelColor: HERO_CHART_THEME.labelBlueBg,
      axisLabelTextColor: HERO_CHART_THEME.labelBlueText,
    });
  };

  const renderSummary = (levels) => {
    // /api/hero/levels already derives read, pullback, destination, and trade state.
    const state = String(levels.state || "WATCH").replaceAll("_", " ");
    const currentRead = String(levels.current_read || "Await structure");
    const stateNote = String(levels.state_note || levels.plan_note || "Wait for clean structure.");
    const headline = `${state} — ${currentRead}`.toUpperCase();

    setText("marketPulseHeroSpot", fmt(levels.spot, 2));
    setText("marketPulseHeroGamma", levels.gamma_regime_label || "Unavailable");
    setText("marketPulseHeroBias", levels.bias || "Unavailable");
    setText("marketPulseHeroTradeability", levels.tradeability || "Unavailable");
    setText("marketPulseHeroSession", levels.session || "Unavailable");
    setText("marketPulseHeroMacroFlip", fmt(levels.main_flip, 0));

    setText("marketPulseHeroRailContext", levels.plan_note || "Execution map pending.");
    setText("marketPulseHeroRailSummary", levels.current_read || "Local Flip is the intraday decision line.");
    setText("marketPulseHeroChartStateRead", currentRead);
    setText("marketPulseHeroChartStateAction", stateNote);
    setText("marketPulseHeroChartBanner", headline);
    setText("marketPulseHeroChartBannerSub", stateNote);

    setText("marketPulseHeroRailFootState", currentRead);
    setText("marketPulseHeroPullbackLevel", levels.pullback_level || "Awaiting level");
    setText("marketPulseHeroDestinationInline", levels.next_destination || "Awaiting next test");

    setStateChip("marketPulseHeroStateContext", levels.state);
    setStateChip("marketPulseHeroChartStateChip", levels.state);
    setStateChip("marketPulseHeroStateChip", levels.state);
    setText("marketPulseHeroTradeState", state);
    setText("marketPulseHeroBestLook", levels.best_look || "Wait for cleaner structure");
    setText("marketPulseHeroRequiredTrigger", levels.required_trigger || "Confirmation required");
    setText("marketPulseHeroInvalidation", levels.invalidation || "Wait for live structure");
    setText("marketPulseHeroStateNote", levels.plan_note || "Awaiting market posture.");
  };

  const updateBars = async ({ fitContent = false } = {}) => {
    const payload = await fetchJson(barsEndpoint());
    const bars = Array.isArray(payload.bars) ? payload.bars : [];
    if (!bars.length) {
      if (emptyState) {
        emptyState.hidden = false;
        emptyState.textContent = "Tradier returned no intraday bars for SPX.";
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

    const volume = bars
      .map((bar, index) => {
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

    candleSeries.setData(candles);
    volumeSeries.setData(volume);
    lastBarsPayload = {
      ...payload,
      bars: candles,
    };
    applyViewport({ fitContent });
    if (emptyState) emptyState.hidden = true;
    if (!initialized) {
      initialized = true;
      window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
    }
  };

  const updateLevels = async () => {
    const payload = await fetchJson(levelsEndpoint());
    const nextLevels = sanitizeLevelsPayload(payload);
    if (!nextLevels) {
      console.warn("SPX hero levels update skipped: invalid level payload", payload);
      return;
    }
    const nextSignature = levelsSignature(nextLevels);
    if (nextSignature === lastAppliedLevelsSignature) {
      lastLevelsPayload = nextLevels;
      lastGoodLevelsPayload = nextLevels;
      return;
    }
    scheduleLevelsApply(nextLevels);
  };

  const startPolling = () => {
    // Phase 1 keeps live updates backend-driven via polling rather than direct browser streaming.
    window.clearInterval(barsTimer);
    window.clearInterval(levelsTimer);
    barsTimer = window.setInterval(() => {
      updateBars().catch((error) => {
        if (emptyState) {
          emptyState.hidden = false;
          emptyState.textContent = `SPX chart refresh failed: ${error.message}`;
        }
      });
    }, Math.max(5000, Number(polling.bars_interval_ms) || 30000));
    levelsTimer = window.setInterval(() => {
      updateLevels().catch(() => {});
    }, Math.max(3000, Number(polling.levels_interval_ms) || 10000));
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
        emptyState.textContent = `SPX hero failed to initialize: ${error.message}`;
      }
      window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
    }
  };

  const resize = () => {
    try {
      chart.applyOptions({ width: canvas.clientWidth, height: 358 });
    } catch (_) {}
  };

  window.addEventListener("resize", resize);
  boot();
})();
