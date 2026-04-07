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

  const priceScaleWidth = 88;
  const chart = LightweightCharts.createChart(canvas, {
    autoSize: true,
    height: 358,
    layout: {
      background: { type: LightweightCharts.ColorType.Solid, color: "#07111f" },
      textColor: "rgba(229, 239, 252, 0.84)",
      fontFamily: '"Segoe UI", "Trebuchet MS", sans-serif',
      fontSize: 12,
    },
    grid: {
      vertLines: { color: "rgba(92, 126, 170, 0.12)" },
      horzLines: { color: "rgba(112, 150, 196, 0.18)" },
    },
    crosshair: {
      vertLine: { color: "rgba(144, 188, 235, 0.26)", width: 1, style: 2 },
      horzLine: { color: "rgba(144, 188, 235, 0.22)", width: 1, style: 2 },
    },
    rightPriceScale: {
      borderColor: "rgba(102, 137, 181, 0.18)",
      scaleMargins: { top: 0.08, bottom: 0.18 },
      minimumWidth: priceScaleWidth,
    },
    timeScale: {
      borderColor: "rgba(102, 137, 181, 0.14)",
      timeVisible: true,
      secondsVisible: false,
      rightOffset: 8,
      barSpacing: 9,
      fixLeftEdge: true,
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
    upColor: "#38d6c4",
    downColor: "#ff6c8a",
    wickUpColor: "#65ead7",
    wickDownColor: "#ff86a1",
    borderUpColor: "#38d6c4",
    borderDownColor: "#ff6c8a",
    lastValueVisible: true,
    priceLineVisible: false,
  });

  const volumeSeries = chart.addHistogramSeries({
    priceScaleId: "",
    priceFormat: { type: "volume" },
    color: "rgba(111, 171, 236, 0.32)",
    priceLineVisible: false,
    lastValueVisible: false,
  });

  volumeSeries.priceScale().applyOptions({
    scaleMargins: { top: 0.78, bottom: 0 },
  });

  let priceLines = [];
  let polling = { bars_interval_ms: 30000, levels_interval_ms: 10000 };
  let barsTimer = null;
  let levelsTimer = null;
  let initialized = false;

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

  const clearPriceLines = () => {
    priceLines.forEach((line) => {
      try {
        candleSeries.removePriceLine(line);
      } catch (_) {}
    });
    priceLines = [];
  };

  const addLevelLine = ({ title, value, color, width = 1, style = 0, axis = true }) => {
    const numeric = asNum(value);
    if (numeric === null) return;
    priceLines.push(
      candleSeries.createPriceLine({
        price: numeric,
        color,
        lineWidth: width,
        lineStyle: style,
        axisLabelVisible: axis,
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

    addLevelLine({ title: "Main", value: mainFlip, color: "rgba(214, 223, 236, 0.34)", width: 1, style: 2 });
    addLevelLine({ title: "NPW", value: nextPutWall, color: "rgba(130, 233, 191, 0.52)", width: 1, style: 1 });
    addLevelLine({ title: "PW", value: putWall, color: "rgba(102, 233, 173, 0.86)", width: 2, style: 0 });
    addLevelLine({ title: "LF", value: localFlip, color: "rgba(255, 210, 116, 0.96)", width: 3, style: 0 });
    addLevelLine({
      title: "CW",
      value: callWall,
      color: state === "NO_TRADE" || (price !== null && callWall !== null && price >= callWall)
        ? "rgba(255, 172, 106, 0.98)"
        : "rgba(255, 128, 162, 0.88)",
      width: state === "NO_TRADE" ? 3 : 2,
      style: 0,
    });
    addLevelLine({ title: "NCW", value: nextCallWall, color: "rgba(255, 214, 144, 0.52)", width: 1, style: 1 });
    addLevelLine({ title: "SPX", value: price, color: "rgba(127, 208, 255, 0.92)", width: 2, style: 2 });
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
          color: close >= open ? "rgba(56, 214, 196, 0.28)" : "rgba(255, 108, 138, 0.24)",
        };
      })
      .filter((bar) => Number.isFinite(bar.time));

    candleSeries.setData(candles);
    volumeSeries.setData(volume);
    if (fitContent || !initialized) chart.timeScale().fitContent();
    if (emptyState) emptyState.hidden = true;
    if (!initialized) {
      initialized = true;
      window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
    }
  };

  const updateLevels = async () => {
    const payload = await fetchJson(levelsEndpoint());
    renderSummary(payload);
    updateOverlayLines(payload);
    if (emptyState && initialized) emptyState.hidden = true;
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
