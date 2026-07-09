function initCalendarPreview(root = document) {
  if (!root || root.dataset?.calendarPreviewBound === "1") return;
  const preview = root.querySelector("#calendarPreview");
  const title = root.querySelector("#calendarPreviewTitle");
  const status = root.querySelector("#calendarPreviewStatus");
  const net = root.querySelector("#previewNet");
  const trades = root.querySelector("#previewTrades");
  const record = root.querySelector("#previewRecord");
  const balance = root.querySelector("#previewBalance");
  const open = root.querySelector("#calendarPreviewOpen");
  const close = root.querySelector("#calendarPreviewClose");
  if (!preview || !title || !status || !net || !trades || !record || !balance || !open || !close) return;

  const buttons = Array.from(root.querySelectorAll(".dayPreviewButton"));
  if (!buttons.length) return;

  const clearActive = () => {
    buttons.forEach((node) => {
      node.setAttribute("aria-pressed", "false");
      node.closest(".dayCard")?.classList.remove("is-active");
    });
  };

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const d = button.dataset;
      clearActive();
      button.setAttribute("aria-pressed", "true");
      button.closest(".dayCard")?.classList.add("is-active");
      title.textContent = `${d.weekday} ${d.daynum} • ${d.iso}`;
      status.textContent = d.status || "Session preview";
      net.textContent = d.net || "—";
      trades.textContent = d.tradeCount || "0";
      const winRate = d.winRate || "0";
      record.textContent = `${d.wins || "0"}W / ${d.losses || "0"}L (${winRate}%)`;
      balance.textContent = d.balance || "—";
      open.href = d.openUrl || (d.iso ? `/trades?d=${encodeURIComponent(d.iso)}` : "/trades");
      preview.hidden = false;
    });
  });

  close.addEventListener("click", () => {
    clearActive();
    preview.hidden = true;
  });

  if (root.dataset) {
    root.dataset.calendarPreviewBound = "1";
  }
}

const dashboardUIFX = (() => {
  const uiFX = typeof window !== "undefined" ? window.mcUIFX : null;

  const asNumericText = (value) => {
    const text = String(value ?? "").replace(/[^0-9+\-.]/g, "");
    if (!text) return null;
    const numeric = Number(text);
    return Number.isFinite(numeric) ? numeric : null;
  };

  const directionFor = (previousText, nextValue, fallback = "neutral") => {
    const prev = asNumericText(previousText);
    const next = typeof nextValue === "number" ? nextValue : asNumericText(nextValue);
    if (prev === null || next === null) return fallback;
    if (next > prev) return "up";
    if (next < prev) return "down";
    return "neutral";
  };

  const setText = (node, value, { live = false, direction = "neutral", pulse = false, tone = "neutral", essential = false } = {}) => {
    if (!node) return false;
    const next = String(value ?? "—");
    const previous = String(node.textContent || "");
    if (previous === next) return false;
    node.textContent = next;
    if (live) {
      uiFX?.flashValue?.(node, directionFor(previous, value, direction), { essential: true });
    } else if (pulse) {
      uiFX?.pulseNode?.(node, tone, { essential });
    }
    return true;
  };

  const pulse = (node, tone = "neutral", options = {}) => {
    if (!node) return;
    uiFX?.pulseNode?.(node, tone, options);
  };

  return { pulse, setText };
})();

const tapeSessionClock = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  weekday: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const marketSessionState = (now = new Date()) => {
  const parts = tapeSessionClock.formatToParts(now);
  const weekday = String((parts.find((part) => part.type === "weekday") || {}).value || "").toLowerCase();
  const hour = Number((parts.find((part) => part.type === "hour") || {}).value || "0");
  const minute = Number((parts.find((part) => part.type === "minute") || {}).value || "0");
  const isWeekend = weekday === "sat" || weekday === "sun";
  const minutes = hour * 60 + minute;
  const isRegularSession = !isWeekend && minutes >= 570 && minutes < 960;
  const isAfterClose = isWeekend || minutes >= 960;
  const isClosed = !isRegularSession;
  return {
    isRegularSession,
    isAfterClose,
    isClosed,
  };
};

(function () {
  const shell = document.getElementById("dashboardModeShell");
  if (!shell) return;

  const storageKey = String(shell.dataset.playbookTickerStorageKey || "mc_playbook_ticker");
  const selectedTicker = String(shell.dataset.selectedTicker || "SPY").toUpperCase();
  const supportedTickers = new Set(["QQQ", "SPY", "SPX"]);
  const switchLinks = Array.from(document.querySelectorAll("[data-dashboard-ticker-switch]"));

  const normalizeTicker = (value) => {
    const ticker = String(value || "").trim().toUpperCase();
    return supportedTickers.has(ticker) ? ticker : "";
  };

  const storageGet = (key) => {
    try {
      return window.localStorage ? window.localStorage.getItem(key) : null;
    } catch (_err) {
      return null;
    }
  };

  const storageSet = (key, value) => {
    try {
      if (window.localStorage) window.localStorage.setItem(key, value);
    } catch (_err) {
      // Ignore storage failures in strict/private contexts.
    }
  };

  const url = new URL(window.location.href);
  const queryTicker = normalizeTicker(url.searchParams.get("ticker"));
  const storedTicker = normalizeTicker(storageGet(storageKey));
  const staleLegacyDefault = selectedTicker === "SPY" && ["SPX", "QQQ"].includes(storedTicker);
  if (!queryTicker && storedTicker && storedTicker !== selectedTicker && !staleLegacyDefault) {
    url.searchParams.set("ticker", storedTicker);
    window.location.replace(url.toString());
    return;
  }

  storageSet(storageKey, queryTicker || selectedTicker || "SPY");
  switchLinks.forEach((link) => {
    link.addEventListener("click", (event) => {
      const nextTicker = normalizeTicker(link.dataset.dashboardTickerSwitch);
      if (!nextTicker || nextTicker === selectedTicker) {
        event.preventDefault();
        return;
      }
      storageSet(storageKey, nextTicker);
      if (typeof window.showDashboardLoading === "function") {
        window.showDashboardLoading(`${nextTicker} dashboard`, `Loading ${nextTicker} context…`);
      }
    });
  });
})();

(function () {
  const button = document.getElementById("dashboardWakeLockBtn");
  const details = document.getElementById("dashboardHealthSurface");
  if (!button || !details) return;

  const storageKey = "mc_dashboard_fold_support_health";

  const storageGet = () => {
    try {
      const value = window.localStorage?.getItem(storageKey);
      return value === "1" ? true : value === "0" ? false : null;
    } catch (_err) {
      return null;
    }
  };

  const storageSet = (isOpen) => {
    try {
      if (window.localStorage) {
        window.localStorage.setItem(storageKey, isOpen ? "1" : "0");
      }
    } catch (_err) {
      // Ignore storage failures.
    }
  };

  const renderState = (isOpen) => {
    const nextTitle = isOpen ? "Collapse Support & Health" : "Keep Support & Health open";
    button.classList.toggle("is-active", !!isOpen);
    button.setAttribute("aria-pressed", isOpen ? "true" : "false");
    button.setAttribute("aria-expanded", isOpen ? "true" : "false");
    button.setAttribute("aria-label", nextTitle);
    button.title = nextTitle;
  };

  const setOpenState = (isOpen, persist = true) => {
    details.open = !!isOpen;
    renderState(details.open);
    if (persist) {
      storageSet(details.open);
    }
  };

  const storedOpen = storageGet();
  if (storedOpen !== null) {
    setOpenState(storedOpen, false);
  } else {
    renderState(details.open);
  }

  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    setOpenState(!details.open);
  });

  details.addEventListener("toggle", () => {
    renderState(details.open);
    storageSet(details.open);
  });
})();

(function () {
  const details = document.getElementById("advancedDashboardWidgets");
  const lazyShell = document.getElementById("dashboardCalendarLazy");
  if (!details || !lazyShell) return;

  let loading = false;

  const bindCalendarPreview = () => {
    if (lazyShell.dataset) {
      delete lazyShell.dataset.calendarPreviewBound;
    }
    initCalendarPreview(lazyShell);
  };

  const setPlaceholder = (message, tone = "info") => {
    lazyShell.innerHTML = `
      <div class="dashboardCalendarLazyPlaceholder is-${tone}">
        <div class="tiny stack8 line15">${message}</div>
      </div>
    `;
  };

  const loadCalendar = async () => {
    if (loading || lazyShell.dataset.loaded === "1") return;
    const endpoint = details.dataset.calendarEndpoint;
    if (!endpoint) return;
    loading = true;
    lazyShell.dataset.loading = "1";
    setPlaceholder("Loading calendar…", "loading");
    try {
      const response = await fetch(endpoint, {
        credentials: "same-origin",
        headers: { "X-Requested-With": "XMLHttpRequest" },
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      lazyShell.innerHTML = await response.text();
      lazyShell.dataset.loaded = "1";
      bindCalendarPreview();
    } catch (_error) {
      setPlaceholder("Calendar could not load. Collapse and reopen to retry.", "error");
    } finally {
      loading = false;
      delete lazyShell.dataset.loading;
    }
  };

  details.addEventListener("toggle", () => {
    if (details.open) {
      void loadCalendar();
    }
  });

  if (details.open) {
    void loadCalendar();
  }

  bindCalendarPreview();
})();

(function () {
  const rows = Array.from(document.querySelectorAll(".liveTapeWatchRow[data-watch-symbol]"));
  const detailPanels = new Map(
    Array.from(document.querySelectorAll(".liveTapeWatchDetail[data-detail-symbol]")).map((node) => [
      String(node.dataset.detailSymbol || "").toUpperCase(),
      node,
    ])
  );
  const statusNode = document.getElementById("dashboardTapeStreamStatus");
  const statusTextNode = document.getElementById("dashboardTapeStreamStatusText");
  const updatedNode = document.getElementById("dashboardTapeUpdatedAt");
  const tapeCard = document.querySelector(".dashboardCoreTapeCard");
  const tapeRefreshBtn = document.getElementById("dashboardTapeRefreshBtn");
  const tapeWindowToggle = tapeCard?.querySelector('[data-role="tape-window-toggle"]') || null;
  const tapeWindowLabel = tapeCard?.querySelector('[data-role="tape-window-label"]') || null;
  const tapeWindowMenu = tapeCard?.querySelector('[data-role="tape-window-menu"]') || null;
  const tapeWindowOptions = Array.from(tapeCard?.querySelectorAll("[data-tape-window]") || []);
  const gammaStrip = document.getElementById("dashboardGammaStrip");
  const gammaMeta = document.getElementById("dashboardGammaMeta");
  const decisionCard = document.querySelector(".dashboardDecisionCard");
  const decisionRefreshBtn = document.getElementById("dashboardPlanningRefreshBtn");
  const decisionStatusChip = document.getElementById("dashboardDecisionStatusChip");
  const decisionLead = document.getElementById("dashboardDecisionLead");
  const decisionBiasValue = document.getElementById("dashboardDecisionBiasValue");
  const decisionRiskValue = document.getElementById("dashboardDecisionRiskValue");
  const decisionPlanValue = document.getElementById("dashboardDecisionPlanValue");
  const decisionTradeGateValue = document.getElementById("dashboardDecisionTradeGateValue");
  const briefCardShell = document.getElementById("dashboardBriefCardShell");
  if (!rows.length || !statusNode || !updatedNode) return;

  const TAPE_WINDOWS = ["15M", "30M", "1H", "6H", "24H"];
  const TAPE_WINDOW_STORAGE_KEY = "mccain.dashboard.tapeWindow";
  const TAPE_SYMBOL_STORAGE_KEY = "mccain.dashboard.tapeChartSymbols";
  const DEFAULT_TAPE_SYMBOLS = { top: "SPX", bottom: "VIX" };
  const normalizeTapeWindow = (value) => {
    const key = String(value || "").trim().toUpperCase();
    return TAPE_WINDOWS.includes(key) ? key : "1H";
  };
  const normalizeTapeSymbol = (value, fallback = "SPX") => {
    const key = String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9.^-]/g, "");
    if (!key) return fallback;
    return key === "^VIX" ? "VIX" : key;
  };
  const readStoredTapeWindow = () => {
    try {
      return window.localStorage?.getItem(TAPE_WINDOW_STORAGE_KEY);
    } catch (_error) {
      return "";
    }
  };
  const readStoredTapeSymbols = () => {
    try {
      const parsed = JSON.parse(window.localStorage?.getItem(TAPE_SYMBOL_STORAGE_KEY) || "{}");
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (_error) {
      return {};
    }
  };
  const writeStoredTapeSymbols = () => {
    try {
      const payload = {};
      rows.forEach((row) => {
        const lane = String(row.dataset.tapeLane || "").toLowerCase();
        if (!lane) return;
        payload[lane] = normalizeTapeSymbol(
          row.dataset.watchSymbol,
          DEFAULT_TAPE_SYMBOLS[lane] || "SPX"
        );
      });
      window.localStorage?.setItem(TAPE_SYMBOL_STORAGE_KEY, JSON.stringify(payload));
    } catch (_error) {
      // Local storage is optional; the active page state still updates.
    }
  };
  let activeTapeWindow = normalizeTapeWindow(readStoredTapeWindow());
  const latestTimeframes = {};
  const parseTimeframes = (node) => {
    const raw = String(node?.dataset?.timeframes || "").trim();
    if (!raw) return {};
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (_error) {
      return {};
    }
  };
  const shell = document.getElementById("dashboardModeShell");
  const activeTicker = String(shell?.dataset.selectedTicker || "SPY").toUpperCase();

  // Hybrid (Option 1+2): remove any skeleton state after paint for initial content to show
  if (tapeCard) {
    const hydrate = () => {
      tapeCard.classList.remove("is-skeleton");
      rows.forEach(r => r.classList.remove("is-skeleton"));
      rows.forEach((row) => {
        const spark = row.querySelector('[data-role="sparkline"]');
        if (spark) spark.classList.remove("is-skeleton");
      });
    };
    if (typeof requestIdleCallback === "function") {
      requestIdleCallback(hydrate, { timeout: 300 });
    } else {
      setTimeout(hydrate, 80);
    }
  }

  let freshTimer = null;
  let openSymbol = "";
  let stream = null;
  let reconnectTimer = null;
  let pendingPayload = null;
  let pendingPayloadTimer = null;
  let pageVisible = document.visibilityState !== "hidden";
  let lastPayloadAppliedAt = 0;
  const STREAM_FLUSH_MS = 250;

  const asNum = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const formatSigned = (value, digits = 2) => {
    const n = asNum(value);
    if (n === null) return "—";
    return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}`;
  };

  const formatValue = (value, digits = 2) => {
    const n = asNum(value);
    if (n === null) return "—";
    return n.toLocaleString("en-US", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  };

  const seriesValues = (points) => (Array.isArray(points) ? points : [])
    .map((row) => (row && typeof row === "object" ? asNum(row.v ?? row.close) : asNum(row)))
    .filter((value) => value !== null);

  const computeSparkPoints = (points) => {
    const values = seriesValues(points);
    if (values.length < 2) return null;
    const recent = values.slice(-40);
    const width = 138;
    const height = 60;
    const plotStart = 5;
    const plotEnd = width - 5;
    const plotWidth = plotEnd - plotStart;
    let minV = Math.min(...recent);
    let maxV = Math.max(...recent);
    const centerV = recent[recent.length - 1] || 0;
    const visualFloor = Math.max(Math.abs(centerV) * 0.0012, 0.18);
    if (Math.abs(maxV - minV) < visualFloor) {
      const midV = (maxV + minV) / 2;
      minV = midV - (visualFloor / 2);
      maxV = midV + (visualFloor / 2);
    }
    const yFor = (value) => (((maxV - value) / (maxV - minV)) * (height - 14)) + 7;
    const coords = recent.map((value, index) => [
      plotStart + ((index * plotWidth) / Math.max(recent.length - 1, 1)),
      yFor(value),
    ]);
    const first = recent[0];
    const last = recent[recent.length - 1];
    return {
      coords,
      baselineY: yFor(first),
      className: last > first ? "up" : last < first ? "down" : "flat",
      height,
      plotStart,
      plotEnd,
    };
  };

  const symbolSeed = (symbol) => {
    const text = String(symbol || "TAPE").toUpperCase();
    let hash = 2166136261;
    for (let index = 0; index < text.length; index += 1) {
      hash ^= text.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  };

  const seededUnit = (seed, offset) => {
    let value = (seed + Math.imul(offset + 1, 0x9e3779b9)) >>> 0;
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    return ((value >>> 0) % 1000) / 1000;
  };

  const buildAmbientLayers = (symbol, width, height, { softGlow = false } = {}) => {
    const seed = symbolSeed(symbol);
    const bandCount = 3 + (seed % 3);
    const bands = [];
    const centerY = height / 2;
    for (let index = 0; index < bandCount; index += 1) {
      const unit = seededUnit(seed, index);
      const opacity = 0.08 + (seededUnit(seed, index + 23) * 0.08);
      if (softGlow) {
        const cx = 28 + (seededUnit(seed, index + 7) * (width - 56));
        const cy = centerY - 6 + (index * 4) + ((seededUnit(seed, index + 13) - 0.5) * 5);
        const rx = 16 + (unit * 14);
        const ry = 2.6 + (seededUnit(seed, index + 17) * 1.8);
        bands.push(
          `<ellipse class="marketMiniSparkAmbientGlow marketMiniSparkAmbientGlow--${index + 1}" cx="${cx.toFixed(2)}" cy="${cy.toFixed(2)}" rx="${rx.toFixed(2)}" ry="${ry.toFixed(2)}" style="--spark-glow-opacity:${opacity.toFixed(3)}" />`
        );
      } else {
        const bandWidth = 74 + (unit * 48);
        const x = 8 + (seededUnit(seed, index + 7) * Math.max(1, width - bandWidth - 16));
        const y = centerY - 11 + (index * 5.2) + ((seededUnit(seed, index + 13) - 0.5) * 3.6);
        bands.push(
          `<rect class="marketMiniSparkAmbientBand marketMiniSparkAmbientBand--${index + 1}" x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${bandWidth.toFixed(2)}" height="${(5.8 + (unit * 2.6)).toFixed(2)}" rx="6" style="--spark-band-opacity:${opacity.toFixed(3)}" />`
        );
      }
    }
    const particles = [];
    for (let index = 0; index < 4; index += 1) {
      const x = 12 + (seededUnit(seed, index + 31) * (width - 24));
      const y = 14 + (seededUnit(seed, index + 41) * (height - 28));
      const radius = 0.55 + (seededUnit(seed, index + 51) * 0.55);
      const opacity = 0.10 + (seededUnit(seed, index + 61) * 0.08);
      particles.push(
        `<circle class="marketMiniSparkParticle" cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="${radius.toFixed(2)}" style="--spark-particle-opacity:${opacity.toFixed(3)}" />`
      );
    }
    return `<g class="marketMiniSparkAmbient" data-ambient-symbol="${String(symbol || "TAPE").toUpperCase()}">${bands.join("")}${particles.join("")}</g>`;
  };

  const buildSparklineSvg = (points, tone, symbol = "") => {
    const spark = computeSparkPoints(points);
    if (!spark) {
      return '<div class="marketMiniSparkEmpty">No trend</div>';
    }
    const width = 138;
    const height = 60;
    const linePath = spark.coords.reduce((path, point, index) => {
          if (index === 0) return `M ${point[0].toFixed(2)} ${point[1].toFixed(2)}`;
      const prev = spark.coords[index - 1];
          const midX = (prev[0] + point[0]) / 2;
      return `${path} C ${midX.toFixed(2)} ${prev[1].toFixed(2)} ${midX.toFixed(2)} ${point[1].toFixed(2)} ${point[0].toFixed(2)} ${point[1].toFixed(2)}`;
        }, "")
    const firstPoint = spark.coords[0];
    const lastPoint = spark.coords[spark.coords.length - 1];
    const areaPath = `${linePath} L ${lastPoint[0].toFixed(2)} ${(height - 6).toFixed(2)} L ${firstPoint[0].toFixed(2)} ${(height - 6).toFixed(2)} Z`;
    const lastCloseClass = spark.className;
    return (
      `<svg viewBox="0 0 138 60" class="marketMiniSpark marketMiniSpark--line" aria-hidden="true">`
      + `<defs><linearGradient id="dashboardTapeAmbientGradient" x1="0%" y1="50%" x2="100%" y2="50%"><stop offset="0%" stop-color="#7e5cff" /><stop offset="52%" stop-color="#5484ff" /><stop offset="100%" stop-color="#54f6eb" /></linearGradient></defs>`
      + buildAmbientLayers(symbol, width, height)
      + `<line class="marketMiniSparkGuide marketMiniSparkBaseline" x1="${spark.plotStart.toFixed(2)}" y1="${spark.baselineY.toFixed(2)}" x2="${spark.plotEnd.toFixed(2)}" y2="${spark.baselineY.toFixed(2)}" />`
      + `<path class="marketMiniSparkArea ${lastCloseClass}" d="${areaPath}" />`
      + `<path class="marketMiniSparkLine ${lastCloseClass}" d="${linePath}" />`
      + `<circle class="marketMiniSparkCurrentGlow ${lastCloseClass}" cx="${lastPoint[0].toFixed(2)}" cy="${lastPoint[1].toFixed(2)}" r="10.5" />`
      + `<line class="marketMiniSparkPriceMarker ${lastCloseClass}" x1="${Math.max(spark.plotStart, lastPoint[0] - 5).toFixed(2)}" y1="${lastPoint[1].toFixed(2)}" x2="${spark.plotEnd.toFixed(2)}" y2="${lastPoint[1].toFixed(2)}" />`
      + `<circle class="marketMiniSparkPoint ${lastCloseClass}" cx="${lastPoint[0].toFixed(2)}" cy="${lastPoint[1].toFixed(2)}" r="2.7" />`
      + `</svg>`
    );
  };

  const updateSparkNode = (node, points, tone) => {
    if (!node) return false;
    const spark = computeSparkPoints(points);
    if (!spark) return false;

    const sparkClass = tone === "up" ? "spark-pos" : tone === "down" ? "spark-neg" : "spark-flat";
    node.classList.remove("spark-pos", "spark-neg", "spark-flat", "is-skeleton");
    node.classList.add(sparkClass);

    const parentRow = node.closest(".liveTapeWatchRow");
    if (parentRow) parentRow.classList.remove("is-skeleton");
    const card = document.querySelector(".dashboardCoreTapeCard");
    if (card) card.classList.remove("is-skeleton");

    const existingSvg = node.querySelector("svg.marketMiniSpark");
    const symbol = node.closest("[data-watch-symbol]")?.dataset.watchSymbol || "";

    if (!existingSvg || !existingSvg.classList.contains("marketMiniSpark--line")) {
      node.innerHTML = buildSparklineSvg(points, tone, symbol);
      return true;
    }

    try {
      const linePath = spark.coords.reduce((path, point, index) => {
        if (index === 0) return `M ${point[0].toFixed(2)} ${point[1].toFixed(2)}`;
        const prev = spark.coords[index - 1];
        const midX = (prev[0] + point[0]) / 2;
        return `${path} C ${midX.toFixed(2)} ${prev[1].toFixed(2)} ${midX.toFixed(2)} ${point[1].toFixed(2)} ${point[0].toFixed(2)} ${point[1].toFixed(2)}`;
      }, "");
      const firstPoint = spark.coords[0];
      const lastPoint = spark.coords[spark.coords.length - 1];
      const areaPath = `${linePath} L ${lastPoint[0].toFixed(2)} ${(spark.height - 6).toFixed(2)} L ${firstPoint[0].toFixed(2)} ${(spark.height - 6).toFixed(2)} Z`;
      const cls = spark.className;
      existingSvg.querySelector(".marketMiniSparkLine")?.setAttribute("d", linePath);
      existingSvg.querySelector(".marketMiniSparkLine")?.setAttribute("class", `marketMiniSparkLine ${cls}`);
      existingSvg.querySelector(".marketMiniSparkArea")?.setAttribute("d", areaPath);
      existingSvg.querySelector(".marketMiniSparkArea")?.setAttribute("class", `marketMiniSparkArea ${cls}`);
      const baseline = existingSvg.querySelector(".marketMiniSparkBaseline");
      if (baseline) {
        baseline.setAttribute("y1", spark.baselineY.toFixed(2));
        baseline.setAttribute("y2", spark.baselineY.toFixed(2));
      }
      const glow = existingSvg.querySelector(".marketMiniSparkCurrentGlow");
      if (glow) {
        glow.setAttribute("cx", lastPoint[0].toFixed(2));
        glow.setAttribute("cy", lastPoint[1].toFixed(2));
        glow.setAttribute("class", `marketMiniSparkCurrentGlow ${cls}`);
      }
      const marker = existingSvg.querySelector(".marketMiniSparkPriceMarker");
      if (marker) {
        marker.setAttribute("x1", Math.max(spark.plotStart, lastPoint[0] - 5).toFixed(2));
        marker.setAttribute("y1", lastPoint[1].toFixed(2));
        marker.setAttribute("x2", spark.plotEnd.toFixed(2));
        marker.setAttribute("y2", lastPoint[1].toFixed(2));
        marker.setAttribute("class", `marketMiniSparkPriceMarker ${cls}`);
      }
      const point = existingSvg.querySelector(".marketMiniSparkPoint");
      if (point) {
        point.setAttribute("cx", lastPoint[0].toFixed(2));
        point.setAttribute("cy", lastPoint[1].toFixed(2));
        point.setAttribute("class", `marketMiniSparkPoint ${cls}`);
      }
      return true;
    } catch (e) {
      node.innerHTML = buildSparklineSvg(points, tone, symbol);
      return true;
    }
  };

  const hasMeaningfulText = (node) => {
    if (!node) return false;
    const text = String(node.textContent || "").trim().toLowerCase();
    return Boolean(text) && text !== "loading..." && text !== "—" && text !== "--";
  };

  const inferAbsoluteChange = (price, pctChange) => {
    const p = asNum(price);
    const pct = asNum(pctChange);
    if (p === null || pct === null) return null;
    const prior = p / (1 + (pct / 100));
    if (!Number.isFinite(prior)) return null;
    return p - prior;
  };

  const quotePctChange = (quote) => {
    const raw = quote || {};
    const explicit = asNum(
      raw.pct_change
        ?? raw.change_pct
        ?? raw.percent_change
        ?? raw.change_percent
        ?? raw.day_change_pct
    );
    if (explicit !== null) return explicit;
    const price = asNum(raw.price);
    const prior = asNum(raw.prev_close ?? raw.prior_close ?? raw.previous_close ?? raw.close);
    if (price === null || prior === null || prior <= 0) return null;
    return ((price - prior) / prior) * 100.0;
  };

  const inferPreviousClose = (quote) => {
    const explicit = asNum((quote || {}).prev_close ?? (quote || {}).prior_close ?? (quote || {}).previous_close);
    if (explicit !== null) return explicit;
    const price = asNum((quote || {}).price);
    const pct = quotePctChange(quote);
    if (price === null || pct === null || Math.abs(100.0 + pct) < 1e-9) return null;
    const prior = price / (1 + (pct / 100.0));
    return Number.isFinite(prior) ? prior : null;
  };

  const applyMixedToneToSparkline = (row, tapeTone, pct) => {
    const sparkNode = row.querySelector('[data-role="sparkline"]');
    if (!sparkNode) return;
    const trendTone = pct !== null && pct > 0 ? "positive" : pct !== null && pct < 0 ? "negative" : "neutral";
    const sparkClass = trendTone === "positive" ? "spark-pos" : trendTone === "negative" ? "spark-neg" : "spark-flat";
    sparkNode.classList.remove("tone-positive", "tone-negative", "tone-neutral", "spark-pos", "spark-neg", "spark-flat");
    sparkNode.classList.add(`tone-${tapeTone}`, sparkClass);
    sparkNode.querySelectorAll(".marketMiniSparkLine, .marketMiniSparkArea, .marketMiniSparkPoint").forEach((node) => {
      node.classList.remove("up", "down", "flat");
      node.classList.add(trendTone === "positive" ? "up" : trendTone === "negative" ? "down" : "flat");
    });
  };

  const formatClock = (iso) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) return "—";
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
    }).format(new Date(ts));
  };

  const compactAgeLabel = (ageS) => {
    const seconds = Math.max(0, Math.floor(Number(ageS) || 0));
    if (seconds >= 72 * 3600) return "72h+ old";
    if (seconds >= 3600) return `${Math.floor(seconds / 3600)}h old`;
    if (seconds >= 60) return `${Math.floor(seconds / 60)}m old`;
    return "Fresh";
  };

  const formatFreshness = (iso, state, hasPrice = false) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) {
      if (hasPrice) {
        const tone = stateClass(state);
        return {
          full: "Quote Loaded",
          compact: "quote",
          band: state,
          status: state === "Live" ? "" : state,
          tone,
        };
      }
      return {
        full: "Awaiting Tick",
        compact: "wait",
        band: "Wait",
        status: "Wait",
        tone: "missing",
      };
    }
    const ageS = Math.max(0, (Date.now() - ts) / 1000);
    const seconds = Math.floor(ageS);
    const compact = compactAgeLabel(seconds).replace(" old", "");
    const band = seconds >= 900 ? "Critical" : state;
    return {
      full: seconds < 60 ? "Data Fresh" : compactAgeLabel(seconds),
      compact,
      band,
      status: band === "Live" ? "" : band,
      tone: stateClass(band === "Critical" ? band : state),
    };
  };

  const freshnessGlyph = (tone) => {
    if (tone === "live") return "✓";
    if (tone === "delayed") return "◷";
    return "!";
  };

  const setFreshnessIndicator = (node, label, tone) => {
    if (!node) return;
    const cleanLabel = String(label || "Awaiting Tick").trim() || "Awaiting Tick";
    const cleanTone = String(tone || "missing").trim() || "missing";
    node.dataset.freshnessLabel = cleanLabel;
    node.dataset.freshnessTone = cleanTone;
    node.title = cleanLabel;
    node.setAttribute("aria-label", cleanLabel);
    node.textContent = "";
    const glyph = document.createElement("span");
    glyph.className = "dashboardTapeFreshnessGlyph";
    glyph.setAttribute("aria-hidden", "true");
    glyph.textContent = freshnessGlyph(cleanTone);
    const sr = document.createElement("span");
    sr.className = "srOnly";
    sr.textContent = cleanLabel;
    node.append(glyph, sr);
  };

  const deriveState = (quote) => {
    const price = asNum((quote || {}).price);
    if (price === null) return "Wait";
    const provider = String((quote || {}).provider || "").toLowerCase();
    const reason = String((quote || {}).reason || "").toLowerCase();
    if (provider === "tradier" && reason.startsWith("tradier_")) return "Live";
    if (reason.includes("cached")) return "Cached";
    if (
      reason.includes("fallback")
      || reason.includes("close")
      || reason.includes("snapshot")
      || reason.includes("intraday")
      || reason.includes("prev_close")
    ) {
      return "Delayed";
    }
    return provider === "tradier" ? "Live" : "Delayed";
  };

  const stateClass = (state) => {
    if (state === "Live") return "live";
    if (state === "Delayed" || state === "Cached" || state === "Wait") return "delayed";
    if (state === "Critical") return "critical";
    return "missing";
  };

  const quoteSeriesPoints = (quote) => {
    const raw = quote || {};
    const candidates = [raw.mini_series, raw.series, raw.prior_session_series];
    for (const source of candidates) {
      const values = seriesValues(source);
      if (values.length >= 2) return values;
    }
    return [];
  };

  const displayRangeFromPoints = (points) => {
    const values = seriesValues(points);
    if (values.length < 2) return "";
    const low = Math.min(...values);
    const high = Math.max(...values);
    if (!Number.isFinite(low) || !Number.isFinite(high)) return "";
    return Math.abs(high - low) < 0.01
      ? formatValue(high, 2)
      : `${formatValue(low, 2)}-${formatValue(high, 2)}`;
  };

  const normalizeHourTone = (tone, change = null) => {
    const raw = String(tone || "").toLowerCase();
    if (raw === "up" || raw === "positive") return "up";
    if (raw === "down" || raw === "negative") return "down";
    const numeric = asNum(change);
    if (numeric !== null && numeric > 0) return "up";
    if (numeric !== null && numeric < 0) return "down";
    return "flat";
  };

  const HOUR_CHART_WIDTH = 360;
  const HOUR_CHART_HEIGHT = 128;
  const HOUR_CHART_PAD_X = 16;
  const HOUR_CHART_TOP = 12;
  const HOUR_CHART_BOTTOM = 112;

  const hourScale = (values) => {
    const clean = values.map((value) => asNum(value)).filter((value) => value !== null);
    if (clean.length < 2) return null;
    let minV = Math.min(...clean);
    let maxV = Math.max(...clean);
    const centerV = clean[clean.length - 1] || 0;
    const visualFloor = Math.max(Math.abs(centerV) * 0.0009, 0.12);
    if (Math.abs(maxV - minV) < visualFloor) {
      const mid = (minV + maxV) / 2;
      minV = mid - (visualFloor / 2);
      maxV = mid + (visualFloor / 2);
    }
    const plotHeight = HOUR_CHART_BOTTOM - HOUR_CHART_TOP;
    const y = (value) => (
      (((maxV - value) / (maxV - minV)) * plotHeight) + HOUR_CHART_TOP
    );
    return {
      y,
      minV,
      maxV,
      height: HOUR_CHART_HEIGHT,
      plotStart: HOUR_CHART_PAD_X,
      plotEnd: HOUR_CHART_WIDTH - HOUR_CHART_PAD_X,
    };
  };

  const buildLastHourChart = (lastHour, symbol = "") => {
    const payload = lastHour && typeof lastHour === "object" ? lastHour : {};
    const candles = Array.isArray(payload.candles) ? payload.candles : [];
    const linePoints = Array.isArray(payload.line_points) ? payload.line_points : [];
    const tone = normalizeHourTone(payload.tone, payload.change);
    const ohlc = candles
      .map((row) => ({
        open: asNum(row.open),
        high: asNum(row.high),
        low: asNum(row.low),
        close: asNum(row.close),
      }))
      .filter((row) => row.open !== null && row.high !== null && row.low !== null && row.close !== null);
    const closeOnly = linePoints.map((value) => asNum(value)).filter((value) => value !== null);
    const values = ohlc.length >= 2
      ? ohlc.flatMap((row) => [row.high, row.low, row.close])
      : closeOnly;
    const scale = hourScale(values);
    if (!scale) return '<div class="dashboardTapeHourEmpty">No trend</div>';
    const baselineValues = ohlc.length >= 2 ? ohlc.map((row) => row.close) : closeOnly;
    const baselineY = scale.y(baselineValues[0]);
    const parts = [
      `<svg viewBox="0 0 ${HOUR_CHART_WIDTH} ${HOUR_CHART_HEIGHT}" class="dashboardTapeHourChart${ohlc.length >= 2 ? " is-ohlc" : " is-close-only"}" aria-hidden="true">`,
      `<line class="dashboardTapeHourBaseline" x1="${scale.plotStart.toFixed(2)}" y1="${baselineY.toFixed(2)}" x2="${scale.plotEnd.toFixed(2)}" y2="${baselineY.toFixed(2)}" />`,
    ];
    let lastX = scale.plotEnd;
    let lastY = baselineY;
    if (ohlc.length >= 2) {
      const slot = (scale.plotEnd - scale.plotStart) / Math.max(ohlc.length, 1);
      const candleWidth = Math.min(17, Math.max(8.5, slot * 0.58));
      ohlc.forEach((row, index) => {
        const centerX = scale.plotStart + ((index + 0.5) * slot);
        const openY = scale.y(row.open);
        const closeY = scale.y(row.close);
        const highY = scale.y(Math.max(row.open, row.high, row.low, row.close));
        const lowY = scale.y(Math.min(row.open, row.high, row.low, row.close));
        const cls = row.close > row.open ? "up" : row.close < row.open ? "down" : "flat";
        const current = index === ohlc.length - 1 ? " current" : "";
        parts.push(`<line class="dashboardTapeHourWick ${cls}${current}" x1="${centerX.toFixed(2)}" y1="${highY.toFixed(2)}" x2="${centerX.toFixed(2)}" y2="${lowY.toFixed(2)}" />`);
        parts.push(`<rect class="dashboardTapeHourBody ${cls}${current}" x="${(centerX - (candleWidth / 2)).toFixed(2)}" y="${Math.min(openY, closeY).toFixed(2)}" width="${candleWidth.toFixed(2)}" height="${Math.max(5.5, Math.abs(closeY - openY)).toFixed(2)}" rx=".45" />`);
        lastX = centerX;
        lastY = closeY;
      });
    } else {
      const coords = closeOnly.map((value, index) => [
        scale.plotStart + ((index * (scale.plotEnd - scale.plotStart)) / Math.max(closeOnly.length - 1, 1)),
        scale.y(value),
      ]);
      const path = coords.reduce((memo, point, index) => {
        if (index === 0) return `M ${point[0].toFixed(2)} ${point[1].toFixed(2)}`;
        const prev = coords[index - 1];
        const midX = (prev[0] + point[0]) / 2;
        return `${memo} C ${midX.toFixed(2)} ${prev[1].toFixed(2)} ${midX.toFixed(2)} ${point[1].toFixed(2)} ${point[0].toFixed(2)} ${point[1].toFixed(2)}`;
      }, "");
      parts.push(`<path class="dashboardTapeHourLine ${tone}" d="${path}" />`);
      [lastX, lastY] = coords[coords.length - 1];
    }
    parts.push(`<circle class="dashboardTapeHourPoint ${tone}" cx="${lastX.toFixed(2)}" cy="${lastY.toFixed(2)}" r="3.4" />`);
    parts.push("</svg>");
    return parts.join("");
  };

  const updateLastHourNode = (row, lastHour) => {
    const module = row.querySelector('[data-role="last-hour"]');
    if (!module || !lastHour || typeof lastHour !== "object") return;
    const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
    const tone = normalizeHourTone(lastHour.tone, lastHour.change);
    module.classList.remove("is-up", "is-down", "is-flat");
    module.classList.add(`is-${tone}`);
    module.dataset.hourStatus = String(lastHour.status || "missing");
    const labelNode = module.querySelector('[data-role="last-hour-label"]');
    const changeNode = module.querySelector('[data-role="last-hour-change"]');
    const pctNode = module.querySelector('[data-role="last-hour-pct"]');
    const chartNode = module.querySelector('[data-role="last-hour-chart"]');
    if (labelNode) dashboardUIFX.setText(labelNode, String(lastHour.label || activeTapeWindow || "1H"));
    if (changeNode) dashboardUIFX.setText(changeNode, String(lastHour.change_display || "—"));
    if (pctNode) dashboardUIFX.setText(pctNode, String(lastHour.pct_display || "—"));
    if (chartNode) chartNode.innerHTML = buildLastHourChart(lastHour, symbol);
  };

  const setWindowMenuOpen = (open) => {
    if (!tapeWindowMenu || !tapeWindowToggle) return;
    tapeWindowMenu.hidden = !open;
    tapeWindowToggle.setAttribute("aria-expanded", open ? "true" : "false");
  };

  const timeframePayloadForRow = (row, legacyLastHour = null) => {
    const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
    const frames = latestTimeframes[symbol] || {};
    return (
      frames[activeTapeWindow]
      || frames["1H"]
      || (legacyLastHour && typeof legacyLastHour === "object" ? legacyLastHour : null)
    );
  };

  const updateTapeWindowControls = () => {
    if (tapeWindowLabel) {
      dashboardUIFX.setText(tapeWindowLabel, activeTapeWindow);
    }
    tapeWindowOptions.forEach((option) => {
      const selected = normalizeTapeWindow(option.dataset.tapeWindow) === activeTapeWindow;
      option.setAttribute("aria-checked", selected ? "true" : "false");
      option.classList.toggle("is-active", selected);
    });
  };

  const renderActiveTimeframe = () => {
    rows.forEach((row) => {
      const payload = timeframePayloadForRow(row);
      if (payload) updateLastHourNode(row, payload);
    });
  };

  const setActiveTapeWindow = (value, { persist = true } = {}) => {
    activeTapeWindow = normalizeTapeWindow(value);
    if (persist) {
      try {
        window.localStorage?.setItem(TAPE_WINDOW_STORAGE_KEY, activeTapeWindow);
      } catch (_error) {
        // Local storage is an enhancement; switching still works without it.
      }
    }
    updateTapeWindowControls();
    renderActiveTimeframe();
  };

  const tapeStateFor = (symbol, pct) => {
    if (["SPY", "QQQ", "IWM"].includes(symbol)) {
      if (pct !== null && pct >= 0.35) {
        return {
          label: "RISK-ON",
          tone: "positive",
          title: "Broad tape supports long risk.",
        };
      }
      if (pct !== null && pct <= -0.35) {
        return {
          label: "RISK-OFF",
          tone: "negative",
          title: "Broad tape is defensive; long risk needs extra confirmation.",
        };
      }
    }
    if (pct !== null && pct >= 0.75) {
      return {
        label: "STRONG",
        tone: "positive",
        title: "Symbol is leading or showing strong upside pressure.",
      };
    }
    if (pct !== null && pct <= -0.75) {
      return {
        label: "WEAK",
        tone: "negative",
        title: "Symbol is lagging or under downside pressure.",
      };
    }
    return {
      label: "MIXED",
      tone: pct === null || pct >= 0 ? "positive" : "negative",
      title: "No clean tape edge yet.",
    };
  };

  const setTapeRowSymbol = (row, value) => {
    if (!row) return "";
    const lane = String(row.dataset.tapeLane || "").toLowerCase();
    const fallback = DEFAULT_TAPE_SYMBOLS[lane] || String(row.dataset.defaultSymbol || "SPX");
    const symbol = normalizeTapeSymbol(value, fallback);
    const previous = String(row.dataset.watchSymbol || "").toUpperCase();
    row.dataset.watchSymbol = symbol;
    row.setAttribute("aria-label", `${lane ? `${lane} ` : ""}${symbol} market tape chart`);
    const symbolNode = row.querySelector('[data-role="lane-symbol"]');
    const inputNode = row.querySelector('[data-role="tape-symbol-input"]');
    const lastNode = row.querySelector('[data-role="last"]');
    const stateNode = row.querySelector('[data-role="state"]');
    const rangeNode = row.querySelector('[data-role="detail-range"] .dashboardTapeRangeValue');
    if (symbolNode) dashboardUIFX.setText(symbolNode, symbol);
    if (inputNode) inputNode.value = symbol;
    if (previous && previous !== symbol) {
      row.dataset.pendingSymbolRefresh = "true";
      delete latestTimeframes[previous];
      if (lastNode) lastNode.textContent = "Standby";
      if (stateNode) {
        stateNode.textContent = "MIXED";
        stateNode.dataset.stateLabel = "MIXED";
        stateNode.classList.remove("tone-positive", "tone-negative", "tone-neutral");
        stateNode.classList.add("tone-neutral");
      }
      if (rangeNode) rangeNode.textContent = "—";
    }
    return symbol;
  };

  const setTapeSearchOpen = (row, open) => {
    if (!row) return;
    const toggle = row.querySelector('[data-role="tape-symbol-toggle"]');
    const popover = row.querySelector('[data-role="tape-symbol-popover"]');
    const input = row.querySelector('[data-role="tape-symbol-input"]');
    if (!toggle || !popover) return;
    popover.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    row.classList.toggle("is-search-open", !!open);
    if (open && input) {
      input.value = normalizeTapeSymbol(row.dataset.watchSymbol, row.dataset.defaultSymbol || "SPX");
      window.setTimeout(() => {
        input.focus();
        input.select();
      }, 0);
    }
  };

  const closeTapeSearches = (exceptRow = null) => {
    rows.forEach((row) => {
      if (exceptRow && row === exceptRow) return;
      setTapeSearchOpen(row, false);
    });
  };

  const setExpandedSymbol = (symbol) => {
    openSymbol = String(symbol || "").toUpperCase();
    rows.forEach((row) => {
      const rowSymbol = String(row.dataset.watchSymbol || "").toUpperCase();
      const isOpen = rowSymbol === openSymbol;
      row.setAttribute("aria-expanded", String(isOpen));
      detailPanels.get(rowSymbol)?.toggleAttribute("hidden", !isOpen);
    });
  };

  rows.forEach((row) => {
    row.addEventListener("click", (event) => {
      if (row.dataset.tapeLane || event.target.closest('[data-role="tape-symbol-form"]')) return;
      const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
      setExpandedSymbol(symbol === openSymbol ? "" : symbol);
    });
  });

  const sanitizeTapePlaceholder = (row) => {
    if (!row) return;
    const lastNode = row.querySelector('[data-role="last"]');
    const marketStateNode = row.querySelector('[data-role="market-state"]');
    const liveNode = row.querySelector('[data-role="row-live"]');
    const detailNode = detailPanels.get(String(row.dataset.watchSymbol || "").toUpperCase());
    const detailChgNode = detailNode?.querySelector('[data-role="detail-chg"]');
    const detailLiveNode = detailNode?.querySelector('[data-role="detail-live"]');

    const lower = (node) => String(node?.textContent || "").trim().toLowerCase();
    if (lastNode && (lower(lastNode) === "loading..." || lower(lastNode) === "unavailable")) {
      lastNode.textContent = "Standby";
    }
    if (marketStateNode && lower(marketStateNode) === "unavailable") {
      marketStateNode.textContent = "Wait";
      marketStateNode.classList.remove("is-missing");
      marketStateNode.classList.add("is-delayed");
    }
    if (liveNode && (lower(liveNode) === "unavailable" || lower(liveNode) === "loading...")) {
      liveNode.classList.remove("is-missing");
      liveNode.classList.add("is-delayed");
      setFreshnessIndicator(liveNode, "Awaiting Tick", "delayed");
    }
    if (detailChgNode && (lower(detailChgNode) === "loading..." || lower(detailChgNode) === "unavailable")) {
      detailChgNode.textContent = "—";
    }
    if (detailLiveNode && (lower(detailLiveNode) === "loading..." || lower(detailLiveNode) === "unavailable")) {
      detailLiveNode.textContent = "wait";
    }
  };

  rows.forEach((row) => sanitizeTapePlaceholder(row));

  const updateRow = (row, quote, points, lastHour = null) => {
    const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
    const detailNode = detailPanels.get(symbol);
    const price = asNum((quote || {}).price);
    const pct = quotePctChange(quote);
    const absChange = inferAbsoluteChange(price, pct);
    const state = deriveState(quote);
    const lastNode = row.querySelector('[data-role="last"]');
    const stateNode = row.querySelector('[data-role="state"]');
    const rowMarketStateNode = row.querySelector('[data-role="market-state"]');
    const rowLiveNode = row.querySelector('[data-role="row-live"]');
    const rowRangeNode = row.querySelector('[data-role="detail-range"]');
    const rowRangeValueNode = rowRangeNode?.querySelector(".dashboardTapeRangeValue") || rowRangeNode;
    const detailChgNode = detailNode?.querySelector('[data-role="detail-chg"]');
    const detailOpenNode = detailNode?.querySelector('[data-role="detail-open"]');
    const detailPrevNode = detailNode?.querySelector('[data-role="detail-prev"]');
    const detailLiveNode = detailNode?.querySelector('[data-role="detail-live"]');
    const openValue = asNum((quote || {}).day_open ?? (quote || {}).open);
    const prevCloseValue = inferPreviousClose(quote);
    const freshness = formatFreshness((quote || {}).as_of || (quote || {}).asof, state, price !== null);
    const usablePoints = seriesValues(points).length >= 2 ? points : quoteSeriesPoints(quote);
    const tapeState = tapeStateFor(symbol, pct);
    const tapeLabel = tapeState.label;
    const tapeTone = tapeState.tone;
    const hasSeededRowValues = (
      price === null
      && !String((quote || {}).as_of || "").trim()
      && !String((quote || {}).provider || "").trim()
      && (
        hasMeaningfulText(lastNode)
      )
    );

    if (hasSeededRowValues) {
      return;
    }

    if (lastNode) {
      if (price !== null) {
        dashboardUIFX.setText(lastNode, formatValue(price, 2), { live: true, direction: tapeTone === "positive" ? "up" : tapeTone === "negative" ? "down" : "neutral" });
      } else if (!hasMeaningfulText(lastNode)) {
        lastNode.textContent = "Standby";
      }
    }
    row.classList.remove(
      "is-up",
      "is-down",
      "is-flat",
      "is-live",
      "is-delayed",
      "is-critical",
      "is-missing",
      "tone-positive",
      "tone-negative",
      "tone-neutral"
    );
    row.classList.add(`is-${freshness.tone}`);
    row.classList.add(tapeTone === "positive" ? "is-up" : tapeTone === "negative" ? "is-down" : "is-flat");
    row.classList.add(`tone-${tapeTone}`);
    row.dataset.tapeTone = tapeTone;
    updateLastHourNode(row, lastHour);
    if (stateNode) {
      dashboardUIFX.setText(stateNode, tapeLabel);
      stateNode.title = tapeState.title;
      stateNode.dataset.stateLabel = tapeLabel;
      stateNode.classList.remove("tone-positive", "tone-negative", "tone-neutral");
      stateNode.classList.add(`tone-${tapeTone}`);
    }
    [rowMarketStateNode, rowLiveNode, detailLiveNode].forEach((node) => {
      if (!node) return;
      node.classList.remove("is-live", "is-delayed", "is-critical", "is-missing");
      node.classList.add(`is-${freshness.tone}`);
    });
    if (rowMarketStateNode) {
      dashboardUIFX.setText(rowMarketStateNode, freshness.status);
    }
    if (rowLiveNode) {
      setFreshnessIndicator(rowLiveNode, freshness.full, freshness.tone);
    }
    if (rowRangeValueNode && ((quote || {}).day_low !== undefined || (quote || {}).day_high !== undefined)) {
      const dayLow = asNum((quote || {}).day_low);
      const dayHigh = asNum((quote || {}).day_high);
      if (dayLow !== null && dayHigh !== null) {
        dashboardUIFX.setText(rowRangeValueNode, `${formatValue(dayLow, 2)} to ${formatValue(dayHigh, 2)}`);
      }
    } else if (rowRangeValueNode) {
      const derivedRange = displayRangeFromPoints(usablePoints);
      if (derivedRange) dashboardUIFX.setText(rowRangeValueNode, derivedRange);
    }

    if (detailChgNode) {
      if (absChange !== null) {
        dashboardUIFX.setText(detailChgNode, formatSigned(absChange, 2), { live: true, direction: absChange > 0 ? "up" : absChange < 0 ? "down" : "neutral" });
      } else if (!hasMeaningfulText(detailChgNode)) {
        detailChgNode.textContent = "—";
      }
    }
    if (detailOpenNode && openValue !== null) dashboardUIFX.setText(detailOpenNode, formatValue(openValue, 2));
    if (detailPrevNode && prevCloseValue !== null) dashboardUIFX.setText(detailPrevNode, formatValue(prevCloseValue, 2));
    if (detailLiveNode && String((quote || {}).as_of || "").trim()) dashboardUIFX.setText(detailLiveNode, freshness.compact);
  };

  const setStreamStatus = (label, detail) => {
    const normalized = String(label || "").toLowerCase();
    const sessionState = marketSessionState();
    const tone = (
      normalized.includes("retry")
      || normalized.includes("delayed")
      || normalized.includes("cached")
      || normalized.includes("paused")
    )
      ? "delayed"
      : normalized.includes("connect")
      ? "off"
      : normalized.includes("unavailable") || normalized.includes("error")
      ? "missing"
      : "live";
    const displayLabel = sessionState.isClosed
      ? "Closed"
      : tone === "live" && sessionState.isRegularSession
      ? "Pulse"
      : tone === "delayed"
        ? "Lag"
        : tone === "missing"
          ? "Dark"
          : "Booting";
    const badgeTone = sessionState.isClosed
      ? "closed"
      : tone === "live" && sessionState.isRegularSession
        ? "live"
        : tone;
    const previousLabel = String(statusNode?.getAttribute("aria-label") || "");
    const statusChanged = previousLabel !== displayLabel;
    if (statusTextNode) {
      dashboardUIFX.setText(statusTextNode, displayLabel, { pulse: true, tone: normalized.includes("retry") ? "warning" : "info" });
    }
    if (statusNode) {
      statusNode.dataset.tone = badgeTone;
      statusNode.setAttribute("aria-label", displayLabel);
      statusNode.setAttribute("title", displayLabel);
    }
    dashboardUIFX.setText(updatedNode, detail, { pulse: statusChanged, tone: "info" });
  };

  const dispatchTapeState = () => {
    let liveCards = 0;
    let delayedCards = 0;
    rows.forEach((row) => {
      if (row.classList.contains("is-live")) liveCards += 1;
      if (row.classList.contains("is-delayed")) delayedCards += 1;
    });
    document.dispatchEvent(new CustomEvent("dashboard:tape-state", {
      detail: {
        hasLive: liveCards > 0,
        hasDelayed: delayedCards > 0,
      },
    }));
  };

  const tapeHasRenderableValues = () => rows.some((row) => {
    const lastNode = row.querySelector('[data-role="last"]');
    return hasMeaningfulText(lastNode) && String(lastNode.textContent || "").trim().toLowerCase() !== "loading...";
  });

  const applyTapeSnapshot = (quotes, updatedLabel, seriesPoints = {}, lastHour = {}, timeframes = {}) => {
    const payload = quotes && typeof quotes === "object" ? quotes : {};
    const series = seriesPoints && typeof seriesPoints === "object" ? seriesPoints : {};
    const hourPayload = lastHour && typeof lastHour === "object" ? lastHour : {};
    const timeframePayload = timeframes && typeof timeframes === "object" ? timeframes : {};
    rows.forEach((row) => {
      const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
      if (timeframePayload[symbol] && typeof timeframePayload[symbol] === "object") {
        latestTimeframes[symbol] = timeframePayload[symbol];
        delete row.dataset.pendingSymbolRefresh;
      } else if (!latestTimeframes[symbol] && hourPayload[symbol]) {
        latestTimeframes[symbol] = { "1H": hourPayload[symbol] };
        delete row.dataset.pendingSymbolRefresh;
      }
      updateRow(row, payload[symbol] || {}, series[symbol] || [], timeframePayloadForRow(row, hourPayload[symbol] || null));
    });
    if (updatedLabel && updatedNode) {
      updatedNode.textContent = updatedLabel;
    }
  };

  const selectedTapeSymbols = () => {
    const symbols = [];
    rows.forEach((row) => {
      const lane = String(row.dataset.tapeLane || "").toLowerCase();
      if (!lane) return;
      const symbol = normalizeTapeSymbol(
        row.dataset.watchSymbol,
        DEFAULT_TAPE_SYMBOLS[lane] || "SPX"
      );
      if (symbol && !symbols.includes(symbol)) symbols.push(symbol);
    });
    return symbols;
  };

  const storedTapeSymbols = readStoredTapeSymbols();
  rows.forEach((row) => {
    const lane = String(row.dataset.tapeLane || "").toLowerCase();
    if (!lane || !storedTapeSymbols[lane]) return;
    setTapeRowSymbol(row, storedTapeSymbols[lane]);
  });
  rows.forEach((row) => {
    if (row.dataset.pendingSymbolRefresh === "true") return;
    const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
    const frames = parseTimeframes(row.querySelector('[data-role="last-hour"]'));
    if (Object.keys(frames).length) {
      latestTimeframes[symbol] = frames;
    }
  });
  updateTapeWindowControls();
  renderActiveTimeframe();

  if (tapeWindowToggle && tapeWindowMenu) {
    tapeWindowToggle.addEventListener("click", (event) => {
      event.stopPropagation();
      setWindowMenuOpen(tapeWindowMenu.hidden);
    });
    tapeWindowOptions.forEach((option) => {
      option.addEventListener("click", (event) => {
        event.stopPropagation();
        setActiveTapeWindow(option.dataset.tapeWindow);
        setWindowMenuOpen(false);
      });
    });
    document.addEventListener("click", (event) => {
      if (!tapeCard?.contains(event.target)) {
        setWindowMenuOpen(false);
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        setWindowMenuOpen(false);
      }
    });
  }

  const gammaCardByKey = (key) => document.getElementById(`dashboardGammaChip-${key}`);
  const gammaIconByKey = (key) => document.getElementById(`dashboardGammaIcon-${key}`);
  const gammaValueByKey = (key) => document.getElementById(`dashboardGammaValue-${key}`);
  const gammaDetailByKey = (key) => document.getElementById(`dashboardGammaDetail-${key}`);
  const gammaPopoverByKey = (key) => document.getElementById(`dashboardGammaPopover-${key}`);
  let openGammaPopoverKey = null;

  const formatLevel = (value) => {
    const numeric = asNum(value);
    return numeric === null ? "--" : String(Math.round(numeric));
  };

  const compactGammaRegimeLabel = (value) => {
    const normalized = String(value || "").trim().toLowerCase();
    if (normalized.includes("negative")) return "Negative Gamma";
    if (normalized.includes("positive")) return "Positive Gamma";
    if (normalized.includes("neutral")) return "Neutral Gamma";
    return String(value || "").trim();
  };

  const structureToneForKey = (key, structure) => {
    if (key === "regime") {
      const regime = String((structure && (structure.gamma_regime || structure.gamma_regime_label)) || "").toLowerCase();
      if (regime.includes("positive")) return "positive";
      if (regime.includes("negative")) return "negative";
      if (regime.includes("neutral") || regime.includes("unconfirmed")) return "info";
      return "";
    }
    if (key === "next_call_wall" || key === "call_wall") return "negative";
    if (key === "next_put_wall" || key === "put_wall") return "positive";
    if (key === "main_flip") return "info";
    return "";
  };

  const structureGlowForKey = (key, structure) => {
    if (key === "regime") {
      const tone = structureToneForKey(key, structure);
      return tone === "positive" || tone === "negative";
    }
    return key === "local_flip" || key === "next_call_wall" || key === "next_put_wall";
  };

  const structureValueForKey = (key, structure) => {
    if (!structure || typeof structure !== "object") return "--";
    if (key === "regime") {
      const regimeLabel = compactGammaRegimeLabel(
        structure.gamma_regime_label || structure.gamma_regime || "Unavailable"
      );
      const normalized = regimeLabel.toLowerCase();
      if (normalized === "positive gamma" || normalized === "positive") return "Positive Ⲅ";
      if (normalized === "negative gamma" || normalized === "negative") return "Negative Ⲅ";
      if (normalized === "neutral gamma" || normalized === "neutral") return "Neutral Ⲅ";
      if (normalized === "unconfirmed") return "Unconfirmed Ⲅ";
      if (normalized === "regime unavailable" || normalized === "unavailable") return "Unavailable Ⲅ";
      return `${regimeLabel} Ⲅ`;
    }
    if (key === "local_flip" && structure.local_flip === null && structure.local_flip_found === false) {
      return "--";
    }
    const fieldMap = {
      local_flip: "local_flip",
      next_call_wall: "next_call_wall",
      next_put_wall: "next_put_wall",
      main_flip: "main_flip",
      call_wall: "call_wall",
      put_wall: "put_wall",
    };
    return formatLevel(structure[fieldMap[key]]);
  };

  const structureDetailForKey = (key, structure) => {
    if (!structure || typeof structure !== "object") return "";
    if (key === "regime") {
      return "";
    }
    if (key === "local_flip" && structure.local_flip === null && structure.local_flip_found === false) {
      return "No flip in wall band";
    }
    return "";
  };

  const structureHelperForKey = (key, structure) => {
    if (!structure || typeof structure !== "object") return "";
    if (key === "regime") {
      if (structure.gamma_regime === "unconfirmed" || structure.gamma_regime === "unavailable") {
        return String(structure.gamma_regime_reason_label || "");
      }
      return String(structure.gamma_regime_subtitle || structure.gamma_regime_reason_label || "");
    }
    return "";
  };

  const closeGammaPopover = () => {
    if (!openGammaPopoverKey) return;
    const button = gammaIconByKey(openGammaPopoverKey);
    const popover = gammaPopoverByKey(openGammaPopoverKey);
    if (button) button.setAttribute("aria-expanded", "false");
    if (popover) popover.hidden = true;
    openGammaPopoverKey = null;
  };

  const toggleGammaPopover = (key) => {
    const button = gammaIconByKey(key);
    const popover = gammaPopoverByKey(key);
    if (!button || !popover || popover.classList.contains("is-empty")) return;
    if (openGammaPopoverKey === key) {
      closeGammaPopover();
      return;
    }
    closeGammaPopover();
    button.setAttribute("aria-expanded", "true");
    popover.hidden = false;
    openGammaPopoverKey = key;
  };

  const applyGammaTone = (node, tone, glow) => {
    if (!node) return;
    node.classList.remove("is-positive", "is-negative", "is-info", "has-glow");
    if (tone === "positive") node.classList.add("is-positive");
    if (tone === "negative") node.classList.add("is-negative");
    if (tone === "info") node.classList.add("is-info");
    if (glow) node.classList.add("has-glow");
    node.dataset.gammaTone = tone || "";
  };

  const truncateText = (value, max) => {
    const text = String(value || "").trim();
    if (!text) return "";
    return text.length > max ? `${text.slice(0, Math.max(0, max - 1)).trim()}…` : text;
  };

  const setPlanningHydrationState = (loading) => {
    if (decisionCard) {
      decisionCard.classList.toggle("is-hydrating", !!loading);
    }
    const briefCard = document.getElementById("daily-brief-card");
    if (briefCard) {
      briefCard.classList.toggle("is-hydrating", !!loading);
    }
  };

  const updateDecisionPanel = (panel) => {
    if (!panel || typeof panel !== "object") return;
    if (decisionLead) {
      dashboardUIFX.setText(decisionLead, String(
        panel.posture_summary || panel.plan || "Wait for aligned planning context."
      ), { pulse: true, tone: "info" });
    }
    if (decisionBiasValue) dashboardUIFX.setText(decisionBiasValue, String(panel.bias || "Unavailable"), { pulse: true, tone: "info" });
    if (decisionRiskValue) dashboardUIFX.setText(decisionRiskValue, String(panel.risk_size || "Unavailable"), { pulse: true, tone: "warning" });
    if (decisionPlanValue) {
      const fullPlan = String(panel.plan || "Wait");
      dashboardUIFX.setText(decisionPlanValue, truncateText(fullPlan, 88), { pulse: true, tone: "info" });
      decisionPlanValue.title = fullPlan;
    }
    if (decisionTradeGateValue) {
      const fullGate = String(panel.trade_gate || "Wait");
      dashboardUIFX.setText(decisionTradeGateValue, truncateText(fullGate, 74), { pulse: true, tone: "warning" });
      decisionTradeGateValue.title = fullGate;
    }
    if (decisionStatusChip) {
      const tone = String(panel.status_tone || "").toLowerCase();
      const changed = dashboardUIFX.setText(decisionStatusChip, String(panel.status || "Unavailable"));
      decisionStatusChip.classList.remove("positive", "negative", "warning", "info");
      if (["positive", "negative", "warning", "info"].includes(tone)) {
        decisionStatusChip.classList.add(tone);
      }
      if (changed) {
        dashboardUIFX.pulse(decisionStatusChip, tone || "info");
      }
    }
    setPlanningHydrationState(false);
  };

  const updateGammaStrip = (structure, metaPayload) => {
    if (!gammaStrip || !structure || typeof structure !== "object") return;
    const payload = metaPayload && typeof metaPayload === "object" ? metaPayload : {};
    const state = String(payload.state || (
      String(structure.levels_source || "").toLowerCase() === "unavailable"
        ? "unavailable"
        : ["partial", "stale_but_usable", "fallback_valid"].includes(String(structure.gamma_data_status || "").toLowerCase())
        || String(structure.levels_source || "").toLowerCase() === "last_valid_snapshot"
        ? "stale"
        : "live"
    ));
    gammaStrip.dataset.gammaState = state;
    gammaStrip.classList.remove("is-live", "is-stale", "is-loading", "is-unavailable");
    gammaStrip.classList.add(`is-${state}`);
    if (gammaMeta) {
      const gammaMetaChanged = dashboardUIFX.setText(gammaMeta, String(
        payload.status_text
          || structure.gamma_regime_reason_label
          || structure.secondary_structure_degraded_reason
          || "Gamma context unavailable"
      ), { pulse: true, tone: state === "stale" ? "warning" : state === "unavailable" ? "negative" : "info" });
      gammaMeta.classList.remove("is-stale", "is-loading", "is-unavailable");
      if (state === "stale" || state === "loading" || state === "unavailable") {
        gammaMeta.classList.add(`is-${state}`);
      }
      if (gammaMetaChanged && state === "stale") {
        dashboardUIFX.pulse(gammaMeta, "warning");
      }
    }
    [
      "regime",
      "local_flip",
      "next_call_wall",
      "next_put_wall",
      "main_flip",
      "call_wall",
      "put_wall",
    ].forEach((key) => {
      const card = gammaCardByKey(key);
      const iconNode = gammaIconByKey(key);
      const valueNode = gammaValueByKey(key);
      const detailNode = gammaDetailByKey(key);
      const tone = structureToneForKey(key, structure);
      const glow = structureGlowForKey(key, structure);
      const valueChanged = valueNode
        ? dashboardUIFX.setText(valueNode, structureValueForKey(key, structure), {
            pulse: true,
            tone: tone || "info",
          })
        : false;
      if (detailNode) {
        const detailText = structureDetailForKey(key, structure);
        dashboardUIFX.setText(detailNode, detailText);
        detailNode.classList.toggle("is-empty", !detailText);
      }
      if (iconNode) {
        const helperText = structureHelperForKey(key, structure);
        const keyLabel = String(key).replace(/_/g, " ");
        const popoverNode = gammaPopoverByKey(key);
        if (key === "regime" && helperText) {
          iconNode.setAttribute("aria-label", `${keyLabel} · ${helperText}`);
          if (popoverNode) {
            dashboardUIFX.setText(popoverNode, helperText);
            popoverNode.classList.remove("is-empty");
          }
        } else {
          iconNode.setAttribute("aria-label", keyLabel);
          if (key === "regime" && popoverNode) {
            dashboardUIFX.setText(popoverNode, "");
            popoverNode.classList.add("is-empty");
            if (openGammaPopoverKey === key) {
              closeGammaPopover();
            } else {
              popoverNode.hidden = true;
              iconNode.setAttribute("aria-expanded", "false");
            }
          }
        }
      }
      applyGammaTone(card, tone, glow);
      applyGammaTone(iconNode, tone, glow);
      if (valueChanged) {
        dashboardUIFX.pulse(card, tone || "info");
      }
    });
  };

  const applyStreamPayload = (payload) => {
    if (!payload || typeof payload !== "object") return;
    const prices = payload.prices || {};
    const seriesPoints = payload.series_points || {};
    updateGammaStrip(payload.market_structure_snapshot || null, payload.dashboard_gamma || null);
    rows.forEach((row) => {
      const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
      if (row.dataset.tapeLane && !prices[symbol] && !seriesPoints[symbol]) return;
      updateRow(row, prices[symbol] || {}, seriesPoints[symbol] || [], timeframePayloadForRow(row));
    });
    dispatchTapeState();
    setStreamStatus("Live", formatClock(payload.updated_at || payload.server_ts || new Date().toISOString()));
    updatedNode.classList.remove("is-fresh");
    window.requestAnimationFrame(() => updatedNode.classList.add("is-fresh"));
    if (freshTimer) window.clearTimeout(freshTimer);
    freshTimer = window.setTimeout(() => {
      updatedNode.classList.remove("is-fresh");
    }, 1400);
    lastPayloadAppliedAt = Date.now();
  };

  const regimeIconButton = gammaIconByKey("regime");
  if (regimeIconButton) {
    regimeIconButton.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      toggleGammaPopover("regime");
    });
  }

  document.addEventListener("click", (event) => {
    if (!openGammaPopoverKey) return;
    const button = gammaIconByKey(openGammaPopoverKey);
    const popover = gammaPopoverByKey(openGammaPopoverKey);
    if ((button && button.contains(event.target)) || (popover && popover.contains(event.target))) {
      return;
    }
    closeGammaPopover();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeGammaPopover();
    }
  });

  const flushPendingPayload = () => {
    pendingPayloadTimer = null;
    if (!pageVisible || !pendingPayload) return;
    const nextPayload = pendingPayload;
    pendingPayload = null;
    applyStreamPayload(nextPayload);
  };

  const queuePayload = (payload) => {
    pendingPayload = payload;
    if (!pageVisible) return;
    const elapsed = Date.now() - lastPayloadAppliedAt;
    if (elapsed >= STREAM_FLUSH_MS && pendingPayloadTimer === null) {
      flushPendingPayload();
      return;
    }
    if (pendingPayloadTimer !== null) return;
    pendingPayloadTimer = window.setTimeout(flushPendingPayload, Math.max(0, STREAM_FLUSH_MS - elapsed));
  };

  const cleanupStream = () => {
    if (reconnectTimer) {
      window.clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (pendingPayloadTimer) {
      window.clearTimeout(pendingPayloadTimer);
      pendingPayloadTimer = null;
    }
    if (!stream) return;
    try {
      stream.close();
    } catch (_err) {
      // Ignore stream close failures.
    }
    stream = null;
  };

  const connect = () => {
    cleanupStream();
    if (!pageVisible) return;
    const streamUrl = new URL("/stream/market", window.location.origin);
    streamUrl.searchParams.set("ticker", activeTicker);
    stream = new EventSource(streamUrl.toString());
    stream.onopen = () => {
      setStreamStatus("Live", "just now");
    };
    stream.onmessage = (event) => {
      if (!event || !event.data) return;
      let payload = null;
      try {
        payload = JSON.parse(event.data);
      } catch (_err) {
        return;
      }
      queuePayload(payload);
    };
    stream.onerror = () => {
      cleanupStream();
      if (!pageVisible) return;
      setStreamStatus("Retrying", "reconnecting…");
      reconnectTimer = window.setTimeout(connect, 3000);
    };
  };

  setStreamStatus("Connecting", "waiting for first tick…");
  connect();
  document.addEventListener("visibilitychange", () => {
    pageVisible = document.visibilityState !== "hidden";
    if (!pageVisible) {
      cleanupStream();
      setStreamStatus("Paused", "background tab");
      return;
    }
    if (pendingPayload) flushPendingPayload();
    setStreamStatus("Connecting", "restoring live feed…");
    connect();
  });

  if (tapeCard && tapeRefreshBtn) {
    const endpoint = String(tapeCard.dataset.refreshEndpoint || "").trim();
    const TAPE_SLOW_POLL_MS = 5 * 60 * 1000;
    let tapePollTimer = 0;
    const buildEndpoint = () => {
      if (!endpoint) return "";
      const url = new URL(endpoint, window.location.origin);
      const pageParams = new URLSearchParams(window.location.search);
      pageParams.forEach((value, key) => {
        if (!url.searchParams.has(key)) {
          url.searchParams.set(key, value);
        }
      });
      const selectedSymbols = selectedTapeSymbols();
      if (selectedSymbols.length) {
        url.searchParams.set("symbols", selectedSymbols.join(","));
      }
      return url.toString();
    };
    const setTapeRefreshState = (loading) => {
      tapeRefreshBtn.disabled = !!loading;
      tapeRefreshBtn.classList.toggle("is-loading", !!loading);
      tapeRefreshBtn.setAttribute("aria-busy", loading ? "true" : "false");
    };
    let tapeRefreshInFlight = false;
    const clearTapePoll = () => {
      if (tapePollTimer) {
        window.clearTimeout(tapePollTimer);
        tapePollTimer = 0;
      }
    };
    const scheduleTapePoll = () => {
      clearTapePoll();
      if (!endpoint || document.visibilityState === "hidden") return;
      tapePollTimer = window.setTimeout(() => {
        void refreshTape({ showLoading: false, fromPoll: true });
      }, TAPE_SLOW_POLL_MS);
    };
    const refreshTape = async ({ showLoading = true, fromPoll = false } = {}) => {
      if (!endpoint || tapeRefreshInFlight) return;
      if (!fromPoll) clearTapePoll();
      tapeRefreshInFlight = true;
      setTapeRefreshState(true);
      if (showLoading && typeof window.showDashboardLoading === "function") {
        window.showDashboardLoading("Refreshing dashboard tape", "Updating live tape state.");
      }
      try {
        const response = await fetch(buildEndpoint(), {
          credentials: "same-origin",
          cache: "no-store",
          headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        const payload = await response.json();
        if (!response.ok || !payload || payload.ok === false) {
          throw new Error("tape_refresh_failed");
        }
        applyTapeSnapshot(
          payload.quotes || {},
          payload.updated_label || "",
          payload.series_points || {},
          payload.last_hour || {},
          payload.timeframes || {}
        );
        if (tapeHasRenderableValues()) {
          setStreamStatus("Live", payload.updated_label || "just now");
        }
      } catch (_error) {
        // Keep existing rows/status if the manual refresh fails.
      } finally {
        if (showLoading && typeof window.completeDashboardLoading === "function") {
          window.completeDashboardLoading();
        }
        tapeRefreshInFlight = false;
        setTapeRefreshState(false);
        scheduleTapePoll();
      }
    };
    tapeRefreshBtn.addEventListener("click", () => {
      void refreshTape({ showLoading: true });
    });
    rows.forEach((row) => {
      const toggle = row.querySelector('[data-role="tape-symbol-toggle"]');
      const form = row.querySelector('[data-role="tape-symbol-form"]');
      const input = row.querySelector('[data-role="tape-symbol-input"]');
      if (!form || !input) return;
      if (toggle) {
        toggle.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          const shouldOpen = row.querySelector('[data-role="tape-symbol-popover"]')?.hidden !== false;
          closeTapeSearches(row);
          setTapeSearchOpen(row, shouldOpen);
        });
      }
      form.addEventListener("submit", (event) => {
        event.preventDefault();
        const previous = String(row.dataset.watchSymbol || "").toUpperCase();
        const next = setTapeRowSymbol(row, input.value);
        writeStoredTapeSymbols();
        setTapeSearchOpen(row, false);
        if (next && next !== previous) {
          void refreshTape({ showLoading: false });
        }
      });
      input.addEventListener("blur", () => {
        input.value = normalizeTapeSymbol(input.value, row.dataset.watchSymbol || "SPX");
      });
    });
    document.addEventListener("click", (event) => {
      if (event.target.closest(".dashboardTapeSymbolSearchPopover") || event.target.closest('[data-role="tape-symbol-toggle"]')) return;
      closeTapeSearches();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeTapeSearches();
      }
    });
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "hidden") {
        clearTapePoll();
        return;
      }
      scheduleTapePoll();
    });
    window.setTimeout(() => {
      void refreshTape({ showLoading: false });
    }, tapeHasRenderableValues() ? 220 : 650);
  }

  if (decisionCard && decisionRefreshBtn) {
    const endpoint = String(decisionCard.dataset.refreshEndpoint || "").trim();
    const readinessCard = document.querySelector("[data-dashboard-readiness]");
    const readinessPctLabel = readinessCard?.querySelector("[data-readiness-pct-label]") || null;
    const readinessMeter = readinessCard?.querySelector("[data-readiness-meter]") || null;
    const readinessCount = readinessCard?.querySelector("[data-readiness-count]") || null;
    const readinessState = readinessCard?.querySelector("[data-readiness-state]") || null;
    const readinessDetail = readinessCard?.querySelector("[data-readiness-detail]") || null;
    const readinessBlockers = readinessCard?.querySelector("[data-readiness-blockers]") || null;
    const readinessCommandSummary = document.querySelector("[data-readiness-command-summary]");
    const readinessItems = Array.from(document.querySelectorAll("[data-readiness-item]"));
    const mindsetItem = document.querySelector('[data-readiness-key="mindset-anchored"]');
    const mindsetStatus = mindsetItem?.querySelector("[data-readiness-item-status]") || null;
    const mindsetDetail = mindsetItem?.querySelector("[data-readiness-item-detail]") || null;
    const mindsetAction = mindsetItem?.querySelector("[data-readiness-item-action]") || null;
    const mindsetDefaultDetail = String(mindsetDetail?.textContent || "").trim();
    const mindsetDefaultStatus = String(mindsetStatus?.textContent || "").trim() || "Loading";
    const mindsetDefaultAction = String(mindsetAction?.textContent || "").trim() || "Load";
    const mindsetSuccessDetail = "Planning, gamma, and brief context are loaded for this session view.";
    const hasObjectContent = (value) => !!value && typeof value === "object" && Object.keys(value).length > 0;
    const hasText = (value) => String(value || "").trim().length > 0;
    const getReadinessState = (done, total) => {
      if (total <= 0) {
        return { pct: 0, state: "Needs attention", detail: "Clear the missing blockers before adding risk." };
      }
      const pct = Math.round((100 * done) / total);
      if (done >= total) {
        return { pct, state: "Ready to trade", detail: "All core checks are locked." };
      }
      if (done >= Math.max(1, total - 1)) {
        return { pct, state: "Almost ready", detail: "Clear the missing blockers before adding risk." };
      }
      return { pct, state: "Needs attention", detail: "Clear the missing blockers before adding risk." };
    };
    const syncReadinessFromChecklist = () => {
      if (!readinessCard || !readinessItems.length) return;
      const blockers = [];
      let done = 0;
      readinessItems.forEach((item) => {
        const label = String(item.querySelector(".dashboardChecklistTitle")?.textContent || "").trim();
        const isDone = item.classList.contains("is-done");
        if (isDone) {
          done += 1;
        } else if (label) {
          blockers.push(label);
        }
      });
      const total = readinessItems.length;
      const { pct, state, detail } = getReadinessState(done, total);
      readinessCard.dataset.readinessDone = String(done);
      readinessCard.dataset.readinessTotal = String(total);
      if (readinessPctLabel) readinessPctLabel.textContent = `${pct}%`;
      if (readinessMeter) readinessMeter.style.setProperty("--readiness-pct", `${pct.toFixed(1)}%`);
      if (readinessCount) readinessCount.textContent = `${done}/${total} checks complete`;
      if (readinessState) readinessState.textContent = state;
      if (readinessDetail) readinessDetail.textContent = detail;
      if (readinessCommandSummary) readinessCommandSummary.textContent = `${done}/${total} ready`;
      if (readinessBlockers) {
        readinessBlockers.innerHTML = blockers.slice(0, 3).map((label) => (
          `<span class="trendChip negative">${label}</span>`
        )).join("");
        readinessBlockers.hidden = blockers.length === 0;
      }
    };
    const setMindsetAnchoredState = (done) => {
      if (!mindsetItem) return;
      mindsetItem.classList.toggle("is-done", done);
      mindsetItem.classList.toggle("is-missing", !done);
      mindsetItem.title = done ? mindsetSuccessDetail : mindsetDefaultDetail;
      if (mindsetStatus) mindsetStatus.textContent = done ? "Loaded" : mindsetDefaultStatus;
      if (mindsetDetail) mindsetDetail.textContent = done ? mindsetSuccessDetail : mindsetDefaultDetail;
      if (mindsetAction) mindsetAction.textContent = done ? "Ready" : mindsetDefaultAction;
      syncReadinessFromChecklist();
    };
    const planningHydrationComplete = (payload) => {
      const panel = payload?.decision_panel || {};
      const hasDecision = [
        panel.status,
        panel.lead,
        panel.bias,
        panel.risk_size,
        panel.plan_primary,
        panel.trade_gate,
      ].some(hasText);
      const hasGamma = hasObjectContent(payload?.market_structure_snapshot) || hasObjectContent(payload?.dashboard_gamma);
      const hasBrief = hasText(payload?.brief_html);
      return hasDecision && hasGamma && hasBrief;
    };
    const planningDomHydrated = () => {
      const leadText = String(decisionLead?.textContent || "").trim();
      const biasText = String(decisionBiasValue?.textContent || "").trim();
      const planText = String(decisionPlanValue?.textContent || "").trim();
      const gammaState = String(gammaStrip?.dataset.gammaState || "").trim().toLowerCase();
      const briefText = String(briefCardShell?.textContent || "").trim();
      const decisionReady = (
        hasText(leadText)
        && leadText.toLowerCase() !== "loading planning context."
        && hasText(biasText)
        && biasText.toLowerCase() !== "loading"
        && hasText(planText)
        && !planText.toLowerCase().startsWith("loading")
      );
      const gammaReady = !!gammaState && gammaState !== "loading" && gammaState !== "unavailable";
      const briefReady = (
        hasText(briefText)
        && !briefText.toLowerCase().includes("building the next-session brief")
        && !briefText.toLowerCase().includes("preparing the brief shell")
      );
      return decisionReady && gammaReady && briefReady;
    };
    const syncMindsetAnchoredFromPlanning = (payload = null) => {
      const hydrated = planningHydrationComplete(payload) || planningDomHydrated();
      setMindsetAnchoredState(hydrated);
    };
    syncReadinessFromChecklist();
    const setRefreshState = (loading) => {
      decisionRefreshBtn.disabled = !!loading;
      decisionRefreshBtn.classList.toggle("is-loading", !!loading);
      decisionRefreshBtn.setAttribute("aria-busy", loading ? "true" : "false");
    };
    const applyBriefHtml = (html) => {
      if (!briefCardShell || typeof html !== "string" || !html.trim()) return;
      briefCardShell.innerHTML = html;
    };
    const buildEndpoint = (force) => {
      if (!endpoint) return "";
      const url = new URL(endpoint, window.location.origin);
      const pageParams = new URLSearchParams(window.location.search);
      pageParams.forEach((value, key) => {
        if (!url.searchParams.has(key)) {
          url.searchParams.set(key, value);
        }
      });
      if (force) {
        url.searchParams.set("force", "1");
      }
      return url.toString();
    };
    const refreshPlanning = async ({ force = false, showLoading = false } = {}) => {
      if (!endpoint || decisionRefreshBtn.disabled) return;
      setRefreshState(true);
      setPlanningHydrationState(true);
      if (showLoading && typeof window.showDashboardLoading === "function") {
        window.showDashboardLoading("Refreshing dashboard plan", "Updating planning and gamma context.");
      }
      try {
        const response = await fetch(buildEndpoint(force), {
          credentials: "same-origin",
          cache: "no-store",
          headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        const payload = await response.json();
        if (!response.ok || !payload || payload.ok === false) {
          throw new Error("planning_refresh_failed");
        }
        updateDecisionPanel(payload.decision_panel || {});
        updateGammaStrip(payload.market_structure_snapshot || null, payload.dashboard_gamma || null);
        applyBriefHtml(payload.brief_html || "");
        window.setTimeout(() => {
          syncMindsetAnchoredFromPlanning(payload);
        }, 0);
      } catch (_error) {
        // Keep the current planning state visible if the manual refresh fails.
      } finally {
        if (showLoading && typeof window.completeDashboardLoading === "function") {
          window.completeDashboardLoading();
        }
        if (decisionLead && String(decisionLead.textContent || "").trim().length > 0) {
          setPlanningHydrationState(false);
        }
        window.setTimeout(() => {
          syncMindsetAnchoredFromPlanning();
        }, 80);
        setRefreshState(false);
      }
    };
    decisionRefreshBtn.addEventListener("click", async () => {
      void refreshPlanning({ force: true, showLoading: true });
    });
    window.setTimeout(() => {
      void refreshPlanning({ force: false, showLoading: false });
    }, 60);
  }
})();

(function () {
  const form = document.getElementById("dashboardDriftRefreshForm");
  const button = document.getElementById("dashboardDriftRefreshBtn");
  if (!form || !button) return;

  const setLoading = (loading) => {
    button.disabled = !!loading;
    button.classList.toggle("is-loading", !!loading);
    button.setAttribute("aria-busy", loading ? "true" : "false");
  };

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (button.disabled) return;
    setLoading(true);
    if (typeof window.showDashboardLoading === "function") {
      window.showDashboardLoading("Rebuilding ledger", "Refreshing canonical ledger balances.");
    }
    try {
      const response = await fetch(form.action, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: new FormData(form),
      });
      const payload = await response.json();
      if (!response.ok || !payload || payload.ok === false) {
        throw new Error("dashboard_drift_refresh_failed");
      }
      window.location.assign(payload.redirect_url || window.location.href);
      return;
    } catch (_error) {
      window.location.reload();
      return;
    } finally {
      if (typeof window.completeDashboardLoading === "function") {
        window.completeDashboardLoading();
      }
      setLoading(false);
    }
  });
})();

(function () {
  const shell = document.getElementById("dashboardModeShell");
  if (!shell) return;

  const stateButtons = Array.from(document.querySelectorAll("[data-discipline-state]"));
  const modeButtons = Array.from(document.querySelectorAll("[data-discipline-mode]"));
  const disciplineRail = document.querySelector(".dashboardDisciplineRail");
  const disciplineMobileToggle = document.getElementById("dashboardDisciplineMobileToggle");
  const mobileStateSummary = document.getElementById("dashboardMobileStateSummary");
  const mobileModeSummary = document.getElementById("dashboardMobileModeSummary");
  const disciplineToggleCue = document.getElementById("dashboardDisciplineToggleCue");
  const gateButtons = Array.from(document.querySelectorAll("[data-trade-gate-toggle]"));
  const gateStatus = document.getElementById("dashboardTradeGateStatus");
  const gateNote = document.getElementById("dashboardTradeGateNote");
  const planningSection = document.getElementById("dashboardPlanningSection");
  const resetTriggers = Array.from(document.querySelectorAll("[data-urgency-trigger], #dashboardResetTrigger"));
  const resetModal = document.getElementById("dashboardResetModal");
  const resetFeedback = document.getElementById("dashboardResetFeedback");
  const resetCloseButtons = Array.from(document.querySelectorAll("[data-reset-close]"));
  const resetActionButtons = Array.from(document.querySelectorAll("[data-reset-action]"));
  const tradeActionLinks = Array.from(document.querySelectorAll("[data-discipline-trade-action]"));
  const storageKey = "mc_dashboard_discipline_layer";
  let disciplineTouchedAt = "";

  const readState = () => {
    try {
      const raw = window.localStorage.getItem(storageKey);
      const parsed = raw ? JSON.parse(raw) : {};
      return {
        disciplineState: String(parsed.disciplineState || "locked-in"),
        disciplineMode: String(parsed.disciplineMode || "a-plus-only"),
        tradeGate: {
          structure: !!parsed.tradeGate?.structure,
          trigger: !!parsed.tradeGate?.trigger,
          risk: !!parsed.tradeGate?.risk,
        },
      };
    } catch (_err) {
      return {
        disciplineState: "locked-in",
        disciplineMode: "a-plus-only",
        tradeGate: { structure: false, trigger: false, risk: false },
      };
    }
  };

  const persistState = (nextState) => {
    try {
      window.localStorage.setItem(storageKey, JSON.stringify(nextState));
    } catch (_err) {
      // Ignore storage failures.
    }
  };

  const uiState = readState();
  const gateLabels = ["No Trade", "Wait", "Still Wait", "Eligible"];
  const gateNotes = {
    "0": "No trade. Protect capital until structure, trigger, and risk all agree.",
    "1": "Wait. One checkbox is not enough to earn risk.",
    "2": "Still wait. Do not anticipate the last condition.",
    "3": "Eligible. Only proceed if the setup still matches plan.",
    manage: "Manage only. Defend the winner, but do not add new risk.",
    done: "Stand down. The session is closed to new risk.",
  };

  const syncButtonGroup = (buttons, key, value) => {
    buttons.forEach((button) => {
      const isActive = button.dataset[key] === value;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
  };

  const gateCount = () => Object.values(uiState.tradeGate).filter(Boolean).length;

  const setResetFeedback = (message = "") => {
    if (!resetFeedback) return;
    resetFeedback.hidden = !message;
    resetFeedback.textContent = message;
  };

  const clearGate = () => {
    Object.keys(uiState.tradeGate).forEach((key) => {
      uiState.tradeGate[key] = false;
    });
  };

  const modeBlocksNewRisk = () => (
    uiState.disciplineMode === "manage-winner" || uiState.disciplineMode === "done-for-day"
  );

  const syncTradeActions = () => {
    const blocked = modeBlocksNewRisk();
    tradeActionLinks.forEach((link) => {
      let targetPath = "";
      try {
        targetPath = new URL(link.getAttribute("href") || "", window.location.href).pathname;
      } catch (_err) {
        targetPath = "";
      }
      const isMarketPulseNavigation = targetPath === "/market-pulse";
      const shouldGuard = blocked && !isMarketPulseNavigation;
      link.setAttribute("aria-disabled", shouldGuard ? "true" : "false");
      link.classList.toggle("isDisabled", shouldGuard);
      link.classList.toggle("is-guarded", shouldGuard);
      link.title = shouldGuard
        ? (
          uiState.disciplineMode === "done-for-day"
            ? "Done for Day blocks new risk."
            : "Manage Winner mode blocks new entries."
        )
        : "";
    });
  };

  const syncGateStatus = () => {
    const count = gateCount();
    if (!gateStatus) return;
    if (uiState.disciplineMode === "done-for-day") {
      gateStatus.textContent = "Stand Down";
      gateStatus.dataset.tradeGateStatus = "done";
      if (gateNote) gateNote.textContent = gateNotes.done;
      return;
    }
    if (uiState.disciplineMode === "manage-winner") {
      gateStatus.textContent = "Manage Only";
      gateStatus.dataset.tradeGateStatus = "manage";
      if (gateNote) gateNote.textContent = gateNotes.manage;
      return;
    }
    gateStatus.textContent = gateLabels[count] || "No Trade";
    gateStatus.dataset.tradeGateStatus = String(count);
    if (gateNote) gateNote.textContent = gateNotes[String(count)] || gateNotes["0"];
  };

  const emitDisciplineState = () => {
    document.dispatchEvent(new CustomEvent("dashboard:discipline-state", {
      detail: {
        disciplineState: uiState.disciplineState,
        disciplineMode: uiState.disciplineMode,
        gateCount: gateCount(),
        gateLabel: String(gateStatus?.textContent || gateLabels[gateCount()] || "No Trade"),
        gateNote: String(gateNote?.textContent || gateNotes[String(gateCount())] || gateNotes["0"]),
        touchedAt: disciplineTouchedAt,
      },
    }));
  };

  const syncGateButtons = () => {
    const disabled = modeBlocksNewRisk();
    gateButtons.forEach((button) => {
      const key = String(button.dataset.tradeGateToggle || "");
      const isActive = !!uiState.tradeGate[key];
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
      button.disabled = disabled;
    });
    syncGateStatus();
  };

  const syncShellState = () => {
    shell.dataset.disciplineState = uiState.disciplineState;
    shell.dataset.disciplineMode = uiState.disciplineMode;
  };

  const formatDisciplineLabel = (value) => String(value || "")
    .replace(/^a-plus-only$/, "A+ Only")
    .split("-")
    .filter(Boolean)
    .map((word) => word === "a" ? "A+" : word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");

  const syncMobileDisciplineSummary = () => {
    if (mobileStateSummary) {
      mobileStateSummary.textContent = formatDisciplineLabel(uiState.disciplineState);
    }
    if (mobileModeSummary) {
      mobileModeSummary.textContent = formatDisciplineLabel(uiState.disciplineMode);
    }
    if (disciplineMobileToggle && disciplineRail) {
      const expanded = disciplineRail.classList.contains("is-mobile-expanded");
      disciplineMobileToggle.setAttribute("aria-expanded", String(expanded));
      if (disciplineToggleCue) disciplineToggleCue.textContent = expanded ? "Collapse" : "Expand";
    }
  };

  const applyUiState = () => {
    syncShellState();
    syncButtonGroup(stateButtons, "disciplineState", uiState.disciplineState);
    syncButtonGroup(modeButtons, "disciplineMode", uiState.disciplineMode);
    syncGateButtons();
    syncTradeActions();
    syncMobileDisciplineSummary();
    emitDisciplineState();
  };

  const openResetModal = () => {
    if (!resetModal) return;
    setResetFeedback("");
    resetModal.hidden = false;
    document.body.classList.add("modalOpen");
    document.dispatchEvent(new CustomEvent("dashboard:urgency-check", {
      detail: { openedAt: new Date().toISOString() },
    }));
  };

  const closeResetModal = () => {
    if (!resetModal) return;
    resetModal.hidden = true;
    document.body.classList.remove("modalOpen");
  };

  applyUiState();

  if (disciplineMobileToggle && disciplineRail) {
    disciplineMobileToggle.addEventListener("click", () => {
      disciplineRail.classList.toggle("is-mobile-expanded");
      syncMobileDisciplineSummary();
    });
  }

  stateButtons.forEach((button) => {
    button.addEventListener("click", () => {
      uiState.disciplineState = String(button.dataset.disciplineState || "locked-in");
      disciplineTouchedAt = new Date().toISOString();
      applyUiState();
      persistState(uiState);
    });
  });

  modeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      uiState.disciplineMode = String(button.dataset.disciplineMode || "a-plus-only");
      disciplineTouchedAt = new Date().toISOString();
      if (modeBlocksNewRisk()) {
        clearGate();
      }
      applyUiState();
      persistState(uiState);
    });
  });

  gateButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const key = String(button.dataset.tradeGateToggle || "");
      if (!key) return;
      uiState.tradeGate[key] = !uiState.tradeGate[key];
      disciplineTouchedAt = new Date().toISOString();
      applyUiState();
      persistState(uiState);
    });
  });

  resetTriggers.forEach((button) => {
    button.addEventListener("click", openResetModal);
  });
  resetCloseButtons.forEach((button) => {
    button.addEventListener("click", closeResetModal);
  });

  resetActionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const action = String(button.dataset.resetAction || "");
      if (action === "plan") {
        uiState.disciplineState = "locked-in";
        uiState.disciplineMode = "a-plus-only";
        clearGate();
        disciplineTouchedAt = new Date().toISOString();
        setResetFeedback("");
        applyUiState();
        persistState(uiState);
        closeResetModal();
        planningSection?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      } else if (action === "stand-down") {
        uiState.disciplineState = "neutral";
        uiState.disciplineMode = "done-for-day";
        clearGate();
        disciplineTouchedAt = new Date().toISOString();
        setResetFeedback("");
        applyUiState();
        persistState(uiState);
        closeResetModal();
        planningSection?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      } else if (action === "proceed") {
        if (uiState.disciplineMode !== "a-plus-only") {
          setResetFeedback("Mode is not set for new risk. Stand down or return to plan before acting.");
          return;
        }
        if (gateCount() < 3) {
          setResetFeedback("Gate incomplete. No conviction = no trade until structure, trigger, and risk all agree.");
          planningSection?.scrollIntoView({ behavior: "smooth", block: "start" });
          return;
        }
        uiState.disciplineState = "locked-in";
        disciplineTouchedAt = new Date().toISOString();
        setResetFeedback("");
        applyUiState();
        persistState(uiState);
        closeResetModal();
        planningSection?.scrollIntoView({ behavior: "smooth", block: "start" });
        gateStatus?.focus?.();
        return;
      }
      applyUiState();
      persistState(uiState);
      closeResetModal();
    });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && resetModal && !resetModal.hidden) {
      closeResetModal();
    }
  });
})();

(function () {
  const foundationCard = document.getElementById("dashboardFoundationCard");
  if (!foundationCard) return;

  const storageKey = "mc_dashboard_foundation_system_v1";
  const intentionButtons = Array.from(document.querySelectorAll("[data-intention-preset]"));
  const intentionInput = document.getElementById("dashboardIntentionCustom");
  const intentionStatus = document.getElementById("dashboardIntentionStatus");
  const foundationSummary = document.getElementById("dashboardFoundationSummary");
  const foundationSummaryChip = document.getElementById("dashboardFoundationSummaryChip");
  const routineButtons = Array.from(document.querySelectorAll("[data-routine-check]"));
  const routineProgress = document.getElementById("dashboardRoutineProgressValue");
  const alignmentButtons = Array.from(document.querySelectorAll("[data-alignment-check]"));
  const alignmentStatusChip = document.getElementById("dashboardAlignmentStatusChip");
  const alignmentScoreDial = document.getElementById("dashboardAlignmentScoreDial");
  const alignmentScoreValue = document.getElementById("dashboardAlignmentScoreValue");
  const alignmentScoreLabel = document.getElementById("dashboardAlignmentScoreLabel");
  const reflectionCard = document.getElementById("dashboardReflectionCard");
  const reflectionEndpoint = String(reflectionCard?.dataset.reflectionEndpoint || "").trim();
  const reflectionDay = String(reflectionCard?.dataset.reflectionDay || "").trim();
  const reflectionStatus = document.getElementById("dashboardReflectionStatus");
  const reflectionButtons = Array.from(document.querySelectorAll("[data-reflection-answer]"));
  const reflectionDriftFields = document.getElementById("dashboardReflectionDriftFields");
  const reflectionBreak = document.getElementById("dashboardReflectionBreak");
  const reflectionUrgency = document.getElementById("dashboardReflectionUrgency");
  const reflectionObey = document.getElementById("dashboardReflectionObey");

  const defaults = {
    intentionPreset: "",
    intentionCustom: "",
    routine: {},
    alignment: {},
    lastIntentionAt: "",
    lastRoutineAt: "",
    lastAlignmentAt: "",
  };

  const presetLabels = Object.fromEntries(
    intentionButtons.map((button) => [
      String(button.dataset.intentionPreset || ""),
      String(button.textContent || "").trim(),
    ])
  );
  const routineKeys = routineButtons.map((button) => String(button.dataset.routineCheck || "")).filter(Boolean);
  const alignmentKeys = alignmentButtons.map((button) => String(button.dataset.alignmentCheck || "")).filter(Boolean);

  const readState = () => {
    try {
      const raw = window.localStorage.getItem(storageKey);
      const parsed = raw ? JSON.parse(raw) : {};
      return {
        intentionPreset: String(parsed.intentionPreset || defaults.intentionPreset),
        intentionCustom: String(parsed.intentionCustom || defaults.intentionCustom),
        routine: { ...defaults.routine, ...(parsed.routine || {}) },
        alignment: { ...defaults.alignment, ...(parsed.alignment || {}) },
        lastIntentionAt: String(parsed.lastIntentionAt || defaults.lastIntentionAt),
        lastRoutineAt: String(parsed.lastRoutineAt || defaults.lastRoutineAt),
        lastAlignmentAt: String(parsed.lastAlignmentAt || defaults.lastAlignmentAt),
      };
    } catch (_error) {
      return typeof window.structuredClone === "function"
        ? window.structuredClone(defaults)
        : JSON.parse(JSON.stringify(defaults));
    }
  };

  const persistState = (nextState) => {
    try {
      window.localStorage.setItem(storageKey, JSON.stringify(nextState));
    } catch (_error) {
      // Ignore storage failures.
    }
  };

  const state = readState();
  const reflectionState = {
    answer: reflectionButtons.find((button) => button.classList.contains("is-active"))?.dataset.reflectionAnswer || "",
    breakAlignment: String(reflectionBreak?.value || ""),
    urgencyTrigger: String(reflectionUrgency?.value || ""),
    obeyTomorrow: String(reflectionObey?.value || ""),
  };
  let reflectionSaveTimer = null;
  let reflectionRequestSeq = 0;

  const countDone = (collection, keys) => keys.reduce((sum, key) => sum + (collection[key] ? 1 : 0), 0);

  const activeIntentionText = () => {
    const custom = String(state.intentionCustom || "").trim();
    if (custom) return custom;
    return presetLabels[state.intentionPreset] || "Choose patience over pressure.";
  };

  const syncIntentions = () => {
    intentionButtons.forEach((button) => {
      const active = String(button.dataset.intentionPreset || "") === state.intentionPreset;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    if (intentionInput && intentionInput.value !== state.intentionCustom) {
      intentionInput.value = state.intentionCustom;
    }
    const nextText = activeIntentionText();
    if (intentionStatus) intentionStatus.textContent = nextText;
  };

  const syncRoutine = () => {
    routineButtons.forEach((button) => {
      const key = String(button.dataset.routineCheck || "");
      const active = !!state.routine[key];
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    if (routineProgress) {
      routineProgress.textContent = `${countDone(state.routine, routineKeys)}/${routineKeys.length}`;
    }
  };

  const alignmentStatusForPct = (pct) => {
    if (pct >= 80) {
      return {
        label: "Aligned",
        detail: "Locked on process",
        tone: "positive",
      };
    }
    if (pct >= 40) {
      return {
        label: "Warning",
        detail: "Guard against drift",
        tone: "warning",
      };
    }
    return {
      label: "Out of Alignment",
      detail: "Stand down and reset",
      tone: "negative",
    };
  };

  const syncAlignment = () => {
    alignmentButtons.forEach((button) => {
      const key = String(button.dataset.alignmentCheck || "");
      const active = !!state.alignment[key];
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    const completed = countDone(state.alignment, alignmentKeys);
    const pct = alignmentKeys.length ? Math.round((completed / alignmentKeys.length) * 100) : 0;
    const status = alignmentStatusForPct(pct);
    if (alignmentStatusChip) {
      const changed = dashboardUIFX.setText(alignmentStatusChip, status.label);
      alignmentStatusChip.classList.remove("positive", "negative", "warning", "info");
      alignmentStatusChip.classList.add(status.tone);
      if (changed) dashboardUIFX.pulse(alignmentStatusChip, status.tone);
    }
    if (alignmentScoreDial) {
      alignmentScoreDial.style.setProperty("--alignment-pct", `${pct}%`);
      alignmentScoreDial.dataset.alignmentTone = status.tone;
    }
    if (alignmentScoreValue) dashboardUIFX.setText(alignmentScoreValue, `${pct}%`, { pulse: true, tone: status.tone });
    if (alignmentScoreLabel) dashboardUIFX.setText(alignmentScoreLabel, status.detail, { pulse: true, tone: status.tone });
    if (foundationSummary) {
      dashboardUIFX.setText(foundationSummary, pct >= 80
        ? `Aligned to execute. Intention: ${activeIntentionText()}`
        : pct >= 40
        ? `Guard the session. Intention: ${activeIntentionText()}`
        : "Pressure is rising. Re-anchor before taking risk.", { pulse: true, tone: status.tone });
    }
    if (foundationSummaryChip) {
      const changed = dashboardUIFX.setText(foundationSummaryChip, status.label);
      foundationSummaryChip.classList.remove("positive", "negative", "warning", "info");
      foundationSummaryChip.classList.add(status.tone);
      if (changed) dashboardUIFX.pulse(foundationSummaryChip, status.tone);
    }
  };

  const syncReflection = () => {
    reflectionButtons.forEach((button) => {
      const value = String(button.dataset.reflectionAnswer || "");
      const active = value === reflectionState.answer;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    const showDrift = reflectionState.answer === "no";
    if (reflectionDriftFields) {
      reflectionDriftFields.hidden = !showDrift;
      reflectionDriftFields.style.display = showDrift ? "grid" : "none";
    }
    if (reflectionBreak && reflectionBreak.value !== reflectionState.breakAlignment) {
      reflectionBreak.value = reflectionState.breakAlignment;
    }
    if (reflectionUrgency && reflectionUrgency.value !== reflectionState.urgencyTrigger) {
      reflectionUrgency.value = reflectionState.urgencyTrigger;
    }
    if (reflectionObey && reflectionObey.value !== reflectionState.obeyTomorrow) {
      reflectionObey.value = reflectionState.obeyTomorrow;
    }
  };

  const syncUi = () => {
    syncIntentions();
    syncRoutine();
    syncAlignment();
    syncReflection();
    document.dispatchEvent(new CustomEvent("dashboard:foundation-state", {
      detail: {
        intention: activeIntentionText(),
        routineDone: countDone(state.routine, routineKeys),
        routineTotal: routineKeys.length,
        alignmentDone: countDone(state.alignment, alignmentKeys),
        alignmentTotal: alignmentKeys.length,
        alignmentPct: alignmentKeys.length ? Math.round((countDone(state.alignment, alignmentKeys) / alignmentKeys.length) * 100) : 0,
        alignmentLabel: String(alignmentStatusChip?.textContent || "Foundation live"),
        alignmentDetail: String(alignmentScoreLabel?.textContent || ""),
        reflectionAnswer: reflectionState.answer,
        reflectionStatus: String(reflectionStatus?.textContent || ""),
        lastTouchedAt: state.lastAlignmentAt || state.lastRoutineAt || state.lastIntentionAt || "",
      },
    }));
  };

  const setReflectionStatus = (message) => {
    if (reflectionStatus) reflectionStatus.textContent = String(message || "");
  };

  const applyReflectionPayload = (payload) => {
    const reflection = payload && typeof payload === "object" ? payload : {};
    reflectionState.answer = String(reflection.answer || "");
    reflectionState.breakAlignment = String(reflection.break_alignment || "");
    reflectionState.urgencyTrigger = String(reflection.urgency_trigger || "");
    reflectionState.obeyTomorrow = String(reflection.obey_tomorrow || "");
    setReflectionStatus(reflection.status_label || "Saved to day");
    syncUi();
  };

  const saveReflection = async () => {
    if (!reflectionEndpoint || !reflectionDay) return;
    const seq = ++reflectionRequestSeq;
    setReflectionStatus("Saving...");
    const body = new URLSearchParams({
      reflection_day: reflectionDay,
      reflection_answer: reflectionState.answer,
      reflection_break_alignment: reflectionState.answer === "no" ? reflectionState.breakAlignment : "",
      reflection_urgency_trigger: reflectionState.answer === "no" ? reflectionState.urgencyTrigger : "",
      reflection_obey_tomorrow: reflectionState.answer === "no" ? reflectionState.obeyTomorrow : "",
    });
    try {
      const response = await fetch(reflectionEndpoint, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: body.toString(),
      });
      const payload = await response.json();
      if (!response.ok || !payload || payload.ok === false) {
        throw new Error("reflection_save_failed");
      }
      if (seq !== reflectionRequestSeq) return;
      applyReflectionPayload(payload.reflection || {});
    } catch (_error) {
      if (seq !== reflectionRequestSeq) return;
      setReflectionStatus("Save failed");
    }
  };

  const queueReflectionSave = () => {
    if (reflectionSaveTimer) {
      window.clearTimeout(reflectionSaveTimer);
    }
    reflectionSaveTimer = window.setTimeout(() => {
      reflectionSaveTimer = null;
      void saveReflection();
    }, 280);
  };

  intentionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const value = String(button.dataset.intentionPreset || "");
      state.intentionPreset = state.intentionPreset === value ? "" : value;
      if (state.intentionPreset) {
        state.intentionCustom = "";
      }
      state.lastIntentionAt = new Date().toISOString();
      syncUi();
      persistState(state);
    });
  });

  intentionInput?.addEventListener("input", () => {
    state.intentionCustom = String(intentionInput.value || "").trimStart();
    if (state.intentionCustom) {
      state.intentionPreset = "";
    }
    state.lastIntentionAt = new Date().toISOString();
    syncUi();
    persistState(state);
  });

  routineButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const key = String(button.dataset.routineCheck || "");
      if (!key) return;
      state.routine[key] = !state.routine[key];
      state.lastRoutineAt = new Date().toISOString();
      syncUi();
      persistState(state);
    });
  });

  alignmentButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const key = String(button.dataset.alignmentCheck || "");
      if (!key) return;
      state.alignment[key] = !state.alignment[key];
      state.lastAlignmentAt = new Date().toISOString();
      syncUi();
      persistState(state);
    });
  });

  reflectionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const value = String(button.dataset.reflectionAnswer || "");
      reflectionState.answer = reflectionState.answer === value ? "" : value;
      if (reflectionState.answer !== "no") {
        reflectionState.breakAlignment = "";
        reflectionState.urgencyTrigger = "";
        reflectionState.obeyTomorrow = "";
      }
      syncUi();
      queueReflectionSave();
    });
  });

  [
    [reflectionBreak, "breakAlignment"],
    [reflectionUrgency, "urgencyTrigger"],
    [reflectionObey, "obeyTomorrow"],
  ].forEach(([node, key]) => {
    node?.addEventListener("input", () => {
      reflectionState[key] = String(node.value || "").trimStart();
      queueReflectionSave();
    });
  });

  syncUi();
})();

(function () {
  const shell = document.getElementById("dashboardModeShell");
  const wrap = document.getElementById("dashboardCommandDeckWrap");
  const stateValue = document.getElementById("dashboardCommandStateValue");
  const stateMeta = document.getElementById("dashboardCommandStateMeta");
  const permissionValue = document.getElementById("dashboardCommandPermissionValue");
  const permissionMeta = document.getElementById("dashboardCommandPermissionMeta");
  const alignmentValue = document.getElementById("dashboardCommandAlignmentValue");
  const alignmentMeta = document.getElementById("dashboardCommandAlignmentMeta");
  const nextValue = document.getElementById("dashboardCommandNextValue");
  const nextMeta = document.getElementById("dashboardCommandNextMeta");
  const trendGrid = document.getElementById("dashboardBehaviorTrendGrid");
  const alignedDaysNode = document.getElementById("dashboardBehaviorAlignedDays");
  const doerStreakNode = document.getElementById("dashboardBehaviorDoerStreak");
  const urgencyTotalNode = document.getElementById("dashboardBehaviorUrgencyTotal");
  const alignmentDoerStreakNode = document.getElementById("dashboardAlignmentDoerStreakValue");
  const alignmentUrgencyNode = document.getElementById("dashboardAlignmentUrgencyValue");
  const cards = {
    session: document.getElementById("dashboardCommandCardSession"),
    permission: document.getElementById("dashboardCommandCardPermission"),
    alignment: document.getElementById("dashboardCommandCardAlignment"),
    next: document.getElementById("dashboardCommandCardNext"),
  };
  if (!shell || !stateValue || !stateMeta || !permissionValue || !permissionMeta || !alignmentValue || !alignmentMeta || !nextValue || !nextMeta) {
    return;
  }
  const behaviorEndpoint = String(wrap?.dataset.behaviorEndpoint || "").trim();
  const behaviorDay = String(wrap?.dataset.behaviorDay || "").trim();
  let saveTimer = null;
  let requestSeq = 0;
  let pendingUrgencyIncrements = 0;

  const discipline = {
    disciplineState: String(shell.dataset.disciplineState || "locked-in"),
    disciplineMode: String(shell.dataset.disciplineMode || "a-plus-only"),
    gateCount: 0,
    gateLabel: String(permissionValue.textContent || "No Trade"),
    gateNote: String(permissionMeta.textContent || "Finish the gate before adding risk."),
    touchedAt: "",
  };
  const foundation = {
    intention: "Choose patience over pressure.",
    routineDone: 0,
    routineTotal: 16,
    alignmentPct: 0,
    alignmentLabel: "Foundation live",
    alignmentDetail: "Routine before pressure.",
    reflectionAnswer: "",
    reflectionStatus: "",
    lastTouchedAt: "",
  };
  const trend = {
    entries: [],
    alignedDays: 0,
    window: 5,
    doerStreak: 0,
    urgencyTotal: 0,
  };

  const formatClock = (iso) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) return "";
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }).format(new Date(ts));
  };

  const setTone = (node, tone) => {
    if (!node) return;
    node.dataset.commandTone = tone;
  };

  const escapeHtml = (value) => String(value || "").replace(/[&<>"]/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
  }[char] || char));

  const renderTrend = () => {
    if (!Array.isArray(trend.entries) || !trend.entries.length) {
      return;
    }
    if (alignedDaysNode) {
      alignedDaysNode.textContent = `${trend.alignedDays}/${trend.window} aligned`;
    }
    if (doerStreakNode) {
      doerStreakNode.textContent = `${trend.doerStreak} day doer streak`;
    }
    if (urgencyTotalNode) {
      urgencyTotalNode.textContent = `${trend.urgencyTotal} urgency checks`;
    }
    if (alignmentDoerStreakNode) {
      alignmentDoerStreakNode.textContent = `${trend.doerStreak} day doer streak`;
    }
    if (alignmentUrgencyNode) {
      alignmentUrgencyNode.textContent = `${trend.urgencyTotal} urgency checks / 5d`;
    }
    if (!trendGrid) return;
    trendGrid.innerHTML = trend.entries.map((entry) => {
      const tone = escapeHtml(entry.status_tone || "info");
      const day = escapeHtml(entry.day || "");
      const dow = escapeHtml(entry.dow || "");
      const pct = escapeHtml(entry.alignment_pct ?? 0);
      const statusLabel = escapeHtml(entry.status_label || "Pending");
      const meta = entry.doer_answer === "yes"
        ? "Doer kept"
        : entry.doer_answer === "no"
        ? "Review logged"
        : "No close logged";
      return `
        <article class="dashboardBehaviorTrendDay is-${tone}${entry.is_today ? " is-today" : ""}" data-trend-day="${day}">
          <span class="dashboardBehaviorTrendDow">${dow}</span>
          <strong class="dashboardBehaviorTrendPct">${pct}%</strong>
          <span class="dashboardBehaviorTrendStatus">${statusLabel}</span>
          <span class="dashboardBehaviorTrendMeta">${escapeHtml(meta)}</span>
        </article>
      `;
    }).join("");
  };

  const pushBehavior = async ({ incrementUrgency = false } = {}) => {
    if (!behaviorEndpoint || !behaviorDay) return;
    const seq = ++requestSeq;
    const body = new URLSearchParams({
      behavior_day: behaviorDay,
      discipline_state: discipline.disciplineState,
      discipline_mode: discipline.disciplineMode,
      gate_count: String(discipline.gateCount || 0),
      routine_done: String(foundation.routineDone || 0),
      routine_total: String(foundation.routineTotal || 0),
      alignment_pct: String(foundation.alignmentPct || 0),
      intention: foundation.intention || "",
      reflection_answer: foundation.reflectionAnswer || "",
      updated_at: new Date().toISOString(),
    });
    if (incrementUrgency || pendingUrgencyIncrements > 0) {
      body.set("increment_urgency", "1");
      body.set("urgency_increment_count", String(Math.max(1, pendingUrgencyIncrements)));
    }
    try {
      const response = await fetch(behaviorEndpoint, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: body.toString(),
      });
      const payload = await response.json();
      if (!response.ok || !payload || payload.ok === false) {
        throw new Error("behavior_save_failed");
      }
      if (seq !== requestSeq) return;
      pendingUrgencyIncrements = 0;
      if (payload.trend && typeof payload.trend === "object") {
        trend.entries = Array.isArray(payload.trend.entries) ? payload.trend.entries : [];
        trend.alignedDays = Number(payload.trend.aligned_days || 0);
        trend.window = Number(payload.trend.window || 5);
        trend.doerStreak = Number(payload.trend.doer_streak || 0);
        trend.urgencyTotal = Number(payload.trend.urgency_total || 0);
        renderTrend();
      }
    } catch (_error) {
      // Ignore intermittent dashboard history save failures.
    }
  };

  const queueBehaviorSave = ({ incrementUrgency = false } = {}) => {
    if (incrementUrgency) {
      pendingUrgencyIncrements += 1;
    }
    if (saveTimer) {
      window.clearTimeout(saveTimer);
    }
    saveTimer = window.setTimeout(() => {
      const shouldIncrementUrgency = pendingUrgencyIncrements > 0;
      saveTimer = null;
      void pushBehavior({ incrementUrgency: shouldIncrementUrgency });
    }, incrementUrgency ? 80 : 260);
  };

  const render = () => {
    let sessionTone = "info";
    let sessionLabel = "Planning Only";
    let sessionDetail = "Alignment before risk. Let the brief lead.";

    if (discipline.disciplineMode === "done-for-day") {
      sessionTone = "negative";
      sessionLabel = "Stand Down";
      sessionDetail = "Day is closed to new risk. Review and close intentionally.";
    } else if (discipline.disciplineMode === "manage-winner") {
      sessionTone = "warning";
      sessionLabel = "Manage Only";
      sessionDetail = "Defend the win. No new entries until mode changes.";
    } else if (discipline.disciplineState === "urgent" || discipline.disciplineState === "frustrated" || foundation.reflectionAnswer === "no" || foundation.alignmentPct < 40) {
      sessionTone = "negative";
      sessionLabel = "Out of Alignment";
      sessionDetail = "Pressure is rising. Reset before taking risk.";
    } else if (discipline.gateCount === 3 && foundation.alignmentPct >= 80 && discipline.disciplineState === "locked-in") {
      sessionTone = "positive";
      sessionLabel = "Cleared";
      sessionDetail = "Structure, trigger, and risk agree. Execute only if plan still matches.";
    } else if (discipline.gateCount > 0 || foundation.alignmentPct >= 40) {
      sessionTone = "warning";
      sessionLabel = "Planning Only";
      sessionDetail = "Build confirmation. Do not anticipate.";
    }

    stateValue.textContent = sessionLabel;
    stateMeta.textContent = sessionDetail;
    setTone(cards.session, sessionTone);

    let permissionTone = "info";
    if (discipline.disciplineMode === "done-for-day") {
      permissionTone = "negative";
    } else if (discipline.disciplineMode === "manage-winner" || discipline.gateCount === 1 || discipline.gateCount === 2) {
      permissionTone = "warning";
    } else if (discipline.gateCount === 3) {
      permissionTone = "positive";
    }
    permissionValue.textContent = discipline.gateLabel || "No Trade";
    permissionMeta.textContent = discipline.gateNote || "Finish the gate before adding risk.";
    setTone(cards.permission, permissionTone);

    const alignmentTone = foundation.alignmentPct >= 80 ? "positive" : foundation.alignmentPct >= 40 ? "warning" : "negative";
    alignmentValue.textContent = `${foundation.alignmentLabel || "Foundation live"} ${foundation.alignmentPct}%`;
    alignmentMeta.textContent = `Routine ${foundation.routineDone}/${foundation.routineTotal}. ${foundation.intention || "Choose patience over pressure."}`;
    setTone(cards.alignment, alignmentTone);

    let nextLabel = "Finish trade gate";
    let nextDetail = "Complete structure, trigger, and risk before action.";
    let nextTone = "info";
    if (discipline.disciplineMode === "done-for-day") {
      nextLabel = "Close the session";
      nextDetail = foundation.reflectionAnswer === "yes"
        ? "Log the close and leave the day intact."
        : "Finish the review and name tomorrow's instruction.";
      nextTone = "negative";
    } else if (discipline.disciplineState === "urgent" || discipline.disciplineState === "frustrated" || foundation.reflectionAnswer === "no" || foundation.alignmentPct < 40) {
      nextLabel = "Run pressure check";
      nextDetail = "Interrupt emotion before the next decision.";
      nextTone = "negative";
    } else if (discipline.gateCount < 3) {
      nextLabel = "Finish trade gate";
      nextDetail = "No conviction means no trade until all three agree.";
      nextTone = "warning";
    } else if (foundation.alignmentPct < 80) {
      nextLabel = "Tighten alignment";
      nextDetail = foundation.alignmentDetail || "Honor the process before risk.";
      nextTone = "warning";
    } else {
      nextLabel = "Execute the brief";
      nextDetail = "Take the clean setup only.";
      nextTone = "positive";
    }
    nextValue.textContent = nextLabel;
    nextMeta.textContent = nextDetail;
    setTone(cards.next, nextTone);
  };

  document.addEventListener("dashboard:discipline-state", (event) => {
    Object.assign(discipline, event?.detail || {});
    render();
    queueBehaviorSave();
  });

  document.addEventListener("dashboard:foundation-state", (event) => {
    Object.assign(foundation, event?.detail || {});
    render();
    queueBehaviorSave();
  });

  document.addEventListener("dashboard:urgency-check", () => {
    queueBehaviorSave({ incrementUrgency: true });
  });

  renderTrend();
  render();
})();

(function () {
  const shell = document.getElementById("dashboardModeShell");
  const toggle = document.getElementById("dashboardModeToggle");
  if (!shell || !toggle) return;

  const storageKey = "mc_dashboard_attention_mode";
  const buttons = Array.from(toggle.querySelectorAll("[data-dashboard-mode-target]"));
  const optionalDetails = Array.from(document.querySelectorAll("[data-dashboard-optional]"));
  const defaultMode = String(shell.dataset.dashboardDefaultMode || shell.dataset.dashboardMode || "pre");

  const validMode = (value) => (value === "live" ? "live" : "pre");
  const getStoredMode = () => {
    try {
      const value = window.localStorage.getItem(storageKey);
      return value === "live" || value === "pre" ? value : null;
    } catch (_err) {
      return null;
    }
  };

  const syncButtons = (mode) => {
    buttons.forEach((button) => {
      const isActive = button.dataset.dashboardModeTarget === mode;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
  };

  const collapseOptionalDetails = () => {
    optionalDetails.forEach((node) => {
      if (
        node.matches(".dashboardProjectionFold[open]")
        || node.matches(".dashboardSupportFold[open]")
        || node.matches(".dataTrustDetails[open]")
      ) {
        return;
      }
      node.removeAttribute("open");
    });
  };

  const applyMode = (mode, options = {}) => {
    const nextMode = validMode(mode);
    shell.dataset.dashboardMode = nextMode;
    syncButtons(nextMode);
    if (options.persist) {
      try {
        window.localStorage.setItem(storageKey, nextMode);
      } catch (_err) {
        // Ignore storage failures.
      }
    }
    if (nextMode === "live") {
      collapseOptionalDetails();
    }
  };

  let manualOverride = getStoredMode();
  applyMode(manualOverride || defaultMode);

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      manualOverride = validMode(button.dataset.dashboardModeTarget);
      applyMode(button.dataset.dashboardModeTarget, { persist: true });
    });
  });

  document.addEventListener("dashboard:tape-state", (event) => {
    if (manualOverride) return;
    const detail = event && event.detail ? event.detail : {};
    if (detail.hasLive) {
      applyMode("live");
    } else if (detail.hasDelayed) {
      applyMode("pre");
    }
  });
})();

(function () {
  const buttons = Array.from(document.querySelectorAll(".dashboardStatusOrb[data-status-key]"));
  const sectionNode = document.getElementById("dashboardStatusDetailSection");
  const panelNode = document.getElementById("dashboardStatusDetailCard");
  const titleNode = document.getElementById("dashboardStatusDetailTitle");
  const primaryNode = document.getElementById("dashboardStatusDetailPrimary");
  const linesNode = document.getElementById("dashboardStatusDetailLines");
  const tagsNode = document.getElementById("dashboardStatusDetailTags");
  if (!buttons.length || !sectionNode || !panelNode || !titleNode || !primaryNode || !linesNode || !tagsNode) return;

  const renderButton = (button) => {
    const data = button.dataset || {};
    sectionNode.textContent = data.statusSection || "Status";
    titleNode.textContent = data.statusTitle || "—";
    primaryNode.textContent = data.statusPrimary || "—";

    const lines = [
      data.statusLine1,
      data.statusLine2,
      data.statusLine3,
      data.statusLine4,
    ].filter((value) => typeof value === "string" && value.trim());
    linesNode.innerHTML = lines
      .map((line) => `<div class="dashboardStatusDetailLine">${line}</div>`)
      .join("");

    const tags = String(data.statusTags || "")
      .split("||")
      .map((value) => value.trim())
      .filter(Boolean);
    tagsNode.innerHTML = tags
      .map((tag) => `<span class="trendChip">${tag}</span>`)
      .join("");
    tagsNode.hidden = tags.length === 0;
  };

  const activate = (button) => {
    buttons.forEach((node) => {
      const active = node === button;
      node.classList.toggle("is-active", active);
      node.setAttribute("aria-selected", String(active));
      node.tabIndex = active ? 0 : -1;
    });
    panelNode.setAttribute("aria-labelledby", button.id || "");
    renderButton(button);
  };

  buttons.forEach((button) => {
    button.addEventListener("click", () => activate(button));
    button.addEventListener("keydown", (event) => {
      const currentIndex = buttons.indexOf(button);
      if (currentIndex < 0) return;
      let nextIndex = null;
      if (event.key === "ArrowRight" || event.key === "ArrowDown") {
        nextIndex = (currentIndex + 1) % buttons.length;
      } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
        nextIndex = (currentIndex - 1 + buttons.length) % buttons.length;
      } else if (event.key === "Home") {
        nextIndex = 0;
      } else if (event.key === "End") {
        nextIndex = buttons.length - 1;
      }
      if (nextIndex === null) return;
      event.preventDefault();
      activate(buttons[nextIndex]);
      buttons[nextIndex].focus();
    });
  });

  const defaultButton = buttons.find((button) => button.classList.contains("is-active")) || buttons[0];
  activate(defaultButton);
})();

(function () {
  const syncPanel = document.querySelector("[data-dashboard-live-sync]");
  if (!syncPanel) return;

  const runBtn = syncPanel.querySelector("[data-dashboard-sync-run]");
  const runLabel = syncPanel.querySelector("[data-dashboard-sync-run-label]");
  const stateNode = syncPanel.querySelector("[data-dashboard-sync-state]");
  const detailNode = syncPanel.querySelector("[data-dashboard-sync-detail]");
  const metaNode = syncPanel.querySelector("[data-dashboard-sync-meta]");
  const todayNode = syncPanel.querySelector("[data-dashboard-sync-today]");
  const noteNode = syncPanel.querySelector("[data-dashboard-sync-note]");
  const runEndpoint = String(syncPanel.dataset.runEndpoint || "").trim();
  const jobEndpointTemplate = String(syncPanel.dataset.jobEndpointTemplate || "").trim();
  const finalStates = new Set(["success", "failed", "debug_only", "cancelled"]);
  let activeJobId = String(syncPanel.dataset.activeJobId || "").trim();
  let pollTimer = 0;

  const hasLastRequest = () => syncPanel.dataset.hasLastRequest === "1";
  const credentialsReady = () => syncPanel.dataset.credentialsReady === "1";
  const disabledReason = () => String(syncPanel.dataset.disabledReason || "").trim();
  const stageLabel = (value) => {
    const label = String(value || "").trim().replace(/_/g, " ");
    return label ? label.replace(/\b\w/g, (char) => char.toUpperCase()) : "Standby";
  };
  const stopPolling = () => {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = 0;
    }
  };
  const parseUpdatedAt = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return null;
    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  };
  const etDateLabel = (value = new Date()) => {
    const parsed = value instanceof Date ? value : parseUpdatedAt(value);
    if (!parsed) return "";
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(parsed);
  };
  const formatAbsoluteTimestamp = (raw) => {
    const parsed = parseUpdatedAt(raw);
    if (!parsed) return "";
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }).formatToParts(parsed);
    const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${byType.month} ${byType.day}, ${byType.year} · ${byType.hour}:${byType.minute} ${byType.dayPeriod} ET`;
  };
  const ranToday = (raw) => {
    const day = etDateLabel(raw);
    return !!day && day === etDateLabel(new Date());
  };
  const todayBadge = (statusToday) => {
    switch (String(statusToday || "").trim().toLowerCase()) {
      case "running":
        return "Today: Running";
      case "completed":
        return "Today: Ran";
      case "failed":
        return "Today: Failed";
      default:
        return "Today: Pending";
    }
  };
  const setTone = (tone) => {
    const tones = ["is-ready", "is-running", "is-success", "is-warning", "is-idle"];
    tones.forEach((name) => syncPanel.querySelector("[data-dashboard-sync-card]")?.classList.remove(name));
    const normalized = String(tone || "").trim().toLowerCase();
    const className = (
      normalized === "running" ? "is-running"
        : normalized === "success" ? "is-success"
          : normalized === "warning" ? "is-warning"
            : normalized === "ready" ? "is-ready"
              : "is-idle"
    );
    syncPanel.querySelector("[data-dashboard-sync-card]")?.classList.add(className);
  };
  const setRunDisabled = (disabled, reason = "") => {
    if (!runBtn) return;
    runBtn.disabled = !!disabled;
    if (reason) {
      runBtn.title = reason;
    } else {
      runBtn.removeAttribute("title");
    }
  };
  const setRunLoading = (loading, label = "") => {
    if (!runBtn) return;
    runBtn.classList.toggle("is-loading", !!loading);
    runBtn.setAttribute("aria-busy", loading ? "true" : "false");
    if (runLabel && label) {
      runLabel.textContent = label;
    } else if (runLabel && !loading) {
      runLabel.textContent = "Run Last Sync";
    }
  };
  const refreshAvailability = () => {
    if (!hasLastRequest()) {
      syncPanel.dataset.canRun = "0";
      setRunLoading(false);
      setRunDisabled(true, disabledReason() || "No previous live-sync request is available yet.");
      return false;
    }
    if (!credentialsReady()) {
      syncPanel.dataset.canRun = "0";
      setRunLoading(false);
      setRunDisabled(true, disabledReason() || "Saved live-sync credentials are missing.");
      return false;
    }
    syncPanel.dataset.canRun = "1";
    syncPanel.dataset.disabledReason = "";
    setRunLoading(false);
    setRunDisabled(false);
    return true;
  };
  const updateStatusCard = ({
    tone,
    state,
    detail,
    meta,
    updatedAtRaw,
    todayStatus,
    note,
    running = false,
  }) => {
    setTone(tone);
    if (stateNode && state) stateNode.textContent = state;
    if (detailNode && detail) detailNode.textContent = detail;
    if (metaNode && meta) metaNode.textContent = meta;
    if (metaNode && typeof updatedAtRaw !== "undefined") {
      metaNode.dataset.dashboardSyncUpdatedRaw = String(updatedAtRaw || "").trim();
    }
    if (todayNode) todayNode.textContent = todayBadge(todayStatus);
    if (noteNode && note) noteNode.textContent = note;
    setRunLoading(running, running ? "Sync Running" : "");
  };
  const applyIdleState = () => {
    activeJobId = "";
    stopPolling();
    syncPanel.dataset.activeJobId = "";
    const updatedAtRaw = String(metaNode?.dataset.dashboardSyncUpdatedRaw || "").trim();
    const lastRunMeta = formatAbsoluteTimestamp(updatedAtRaw);
    const syncedToday = ranToday(updatedAtRaw);
    refreshAvailability();
    updateStatusCard({
      tone: syncedToday ? "success" : "idle",
      state: syncedToday ? "COMPLETED TODAY" : "NOT RUN TODAY",
      detail: syncedToday ? "Import complete" : "No sync today",
      meta: lastRunMeta ? `Last run: ${lastRunMeta}` : "Last run: None yet",
      updatedAtRaw,
      todayStatus: syncedToday ? "completed" : "pending",
      note: syncedToday
        ? "Upload sync completed for today."
        : "No sync has run today yet.",
    });
  };
  const applyJobState = (job) => {
    const status = String(job?.status || "").trim().toLowerCase();
    const stage = String(job?.stage || "").trim();
    const message = String(job?.message || "").trim();
    const updatedAtRaw = String(job?.updated_at || "").trim();
    const isRunning = status === "queued" || status === "running";
    const meta = formatAbsoluteTimestamp(updatedAtRaw);
    const todayRan = ranToday(updatedAtRaw);
    if (isRunning) {
      syncPanel.dataset.canRun = "0";
      syncPanel.dataset.disabledReason = "A live sync is already running.";
      setRunDisabled(true, "A live sync is already running.");
      updateStatusCard({
        tone: "running",
        state: "SYNC RUNNING",
        detail: stageLabel(stage) || message || "Opening statement dialog",
        meta: meta ? `Last run: ${meta}` : "Last run: None yet",
        updatedAtRaw,
        todayStatus: "running",
        note: "Running now. Controls unlock when complete.",
        running: true,
      });
      return;
    }
    activeJobId = "";
    syncPanel.dataset.activeJobId = "";
    refreshAvailability();
    if (status === "success" || status === "debug_only") {
      updateStatusCard({
        tone: "success",
        state: todayRan ? "COMPLETED TODAY" : "NOT RUN TODAY",
        detail: stageLabel(stage) || "Import complete",
        meta: meta ? `Last run: ${meta}` : "Last run: None yet",
        updatedAtRaw,
        todayStatus: todayRan ? "completed" : "pending",
        note: todayRan ? "Upload sync completed for today." : "No sync has run today yet.",
      });
    } else if (status === "failed" || status === "cancelled") {
      updateStatusCard({
        tone: "warning",
        state: todayRan ? "FAILED TODAY" : "NOT RUN TODAY",
        detail: stageLabel(stage) || "Sync failed",
        meta: meta ? `Last run: ${meta}` : "Last run: None yet",
        updatedAtRaw,
        todayStatus: todayRan ? "failed" : "pending",
        note: todayRan ? "Last sync failed. Review logs and retry." : "No sync has run today yet.",
      });
    }
  };
  const pollJob = async () => {
    if (!activeJobId || !jobEndpointTemplate) return;
    try {
      const response = await fetch(
        jobEndpointTemplate.replace("__JOB_ID__", encodeURIComponent(activeJobId)),
        {
          credentials: "same-origin",
          cache: "no-store",
          headers: { Accept: "application/json" },
        },
      );
      const payload = await response.json();
      if (!response.ok || !payload?.ok || !payload.job) {
        throw new Error("dashboard_sync_poll_failed");
      }
      applyJobState(payload.job);
      const status = String(payload.job.status || "").trim().toLowerCase();
      if (!finalStates.has(status)) {
        pollTimer = window.setTimeout(() => {
          void pollJob();
        }, 2000);
      }
    } catch (_error) {
      pollTimer = window.setTimeout(() => {
        void pollJob();
      }, 3000);
    }
  };

  if (runBtn && runEndpoint) {
    runBtn.addEventListener("click", async () => {
      if (runBtn.disabled) return;
      setRunDisabled(true, "Starting live sync...");
      setRunLoading(true, "Starting Sync");
      updateStatusCard({
        tone: "running",
        state: "SYNC RUNNING",
        detail: "Opening statement dialog",
        meta: "Last run: None yet",
        updatedAtRaw: "",
        todayStatus: "running",
        note: "Running now. Controls unlock when complete.",
        running: true,
      });
      try {
        const response = await fetch(runEndpoint, {
          method: "POST",
          credentials: "same-origin",
          headers: {
            Accept: "application/json",
            "X-Requested-With": "XMLHttpRequest",
          },
        });
        const payload = await response.json().catch(() => null);
        if (!payload) {
          throw new Error("dashboard_sync_start_failed");
        }
        if (!response.ok && payload.job) {
          activeJobId = String(payload.job?.id || "").trim();
          syncPanel.dataset.activeJobId = activeJobId;
          applyJobState(payload.job);
          stopPolling();
          if (activeJobId) {
            pollTimer = window.setTimeout(() => {
              void pollJob();
            }, 1200);
          }
          return;
        }
        if (!payload.ok) {
          const message = String(payload.message || "Live sync could not start.").trim();
          syncPanel.dataset.disabledReason = message;
          updateStatusCard({
            tone: "warning",
            state: "FAILED TODAY",
            detail: message || "Sync failed",
            meta: "Last run: None yet",
            updatedAtRaw: "",
            todayStatus: "failed",
            note: "Last sync failed. Review logs and retry.",
            running: false,
          });
          refreshAvailability();
          return;
        }
        activeJobId = String(payload.job?.id || "").trim();
        syncPanel.dataset.activeJobId = activeJobId;
        if (payload.job) {
          applyJobState(payload.job);
        }
        stopPolling();
        if (activeJobId) {
          pollTimer = window.setTimeout(() => {
            void pollJob();
          }, 1200);
        }
      } catch (_error) {
        updateStatusCard({
          tone: "warning",
          state: "FAILED TODAY",
          detail: "Sync failed",
          meta: "Last run: None yet",
          updatedAtRaw: "",
          todayStatus: "failed",
          note: "Last sync failed. Review logs and retry.",
        });
        refreshAvailability();
      }
    });
  }

  if (activeJobId) {
    applyJobState({
      status: "running",
      stage: detailNode?.textContent || "running",
      message: detailNode?.textContent || "Opening statement dialog",
      updated_at: String(metaNode?.dataset.dashboardSyncUpdatedRaw || "").trim(),
    });
    pollTimer = window.setTimeout(() => {
      void pollJob();
    }, 1200);
  } else {
    applyIdleState();
  }
})();

(function () {
  const mindsetItem = document.querySelector('[data-readiness-key="mindset-anchored"]');
  const readinessCard = document.querySelector("[data-dashboard-readiness]");
  const readinessCommandSummary = document.querySelector("[data-readiness-command-summary]");
  const readinessPctLabel = readinessCard?.querySelector("[data-readiness-pct-label]") || null;
  const readinessMeter = readinessCard?.querySelector("[data-readiness-meter]") || null;
  const readinessCount = readinessCard?.querySelector("[data-readiness-count]") || null;
  const readinessState = readinessCard?.querySelector("[data-readiness-state]") || null;
  const readinessDetail = readinessCard?.querySelector("[data-readiness-detail]") || null;
  const readinessBlockers = readinessCard?.querySelector("[data-readiness-blockers]") || null;
  const readinessItems = Array.from(document.querySelectorAll("[data-readiness-item]"));
  const mindsetStatus = mindsetItem?.querySelector("[data-readiness-item-status]") || null;
  const mindsetDetail = mindsetItem?.querySelector("[data-readiness-item-detail]") || null;
  const mindsetAction = mindsetItem?.querySelector("[data-readiness-item-action]") || null;
  const decisionLead = document.getElementById("dashboardDecisionLead");
  const decisionBiasValue = document.getElementById("dashboardDecisionBiasValue");
  const decisionPlanValue = document.getElementById("dashboardDecisionPlanValue");
  const gammaStrip = document.getElementById("dashboardGammaStrip");
  const briefCardShell = document.getElementById("dashboardBriefCardShell");
  if (!mindsetItem || !readinessCard || !readinessItems.length) return;

  const defaultDetail = String(mindsetDetail?.textContent || "").trim();
  const defaultStatus = String(mindsetStatus?.textContent || "").trim() || "Loading";
  const defaultAction = String(mindsetAction?.textContent || "").trim() || "Load";
  const successDetail = "Planning, gamma, and brief context are loaded for this session view.";

  const hasText = (value) => String(value || "").trim().length > 0;

  const readinessSummary = (done, total) => {
    const pct = total > 0 ? Math.round((100 * done) / total) : 0;
    if (done >= total && total > 0) {
      return { pct, state: "Ready to trade", detail: "All core checks are locked." };
    }
    if (done >= Math.max(1, total - 1)) {
      return { pct, state: "Almost ready", detail: "Clear the missing blockers before adding risk." };
    }
    return { pct, state: "Needs attention", detail: "Clear the missing blockers before adding risk." };
  };

  const syncReadiness = () => {
    const blockers = [];
    let done = 0;
    readinessItems.forEach((item) => {
      const label = String(item.querySelector(".dashboardChecklistTitle")?.textContent || "").trim();
      if (item.classList.contains("is-done")) {
        done += 1;
      } else if (label) {
        blockers.push(label);
      }
    });
    const total = readinessItems.length;
    const summary = readinessSummary(done, total);
    if (readinessPctLabel) readinessPctLabel.textContent = `${summary.pct}%`;
    if (readinessMeter) readinessMeter.style.setProperty("--readiness-pct", `${summary.pct.toFixed(1)}%`);
    if (readinessCount) readinessCount.textContent = `${done}/${total} checks complete`;
    if (readinessState) readinessState.textContent = summary.state;
    if (readinessDetail) readinessDetail.textContent = summary.detail;
    if (readinessCommandSummary) readinessCommandSummary.textContent = `${done}/${total} ready`;
    if (readinessBlockers) {
      readinessBlockers.innerHTML = blockers.slice(0, 3).map((label) => (
        `<span class="trendChip negative">${label}</span>`
      )).join("");
      readinessBlockers.hidden = blockers.length === 0;
    }
  };

  const planningHydrated = () => {
    const leadText = String(decisionLead?.textContent || "").trim().toLowerCase();
    const biasText = String(decisionBiasValue?.textContent || "").trim().toLowerCase();
    const planText = String(decisionPlanValue?.textContent || "").trim().toLowerCase();
    const gammaState = String(gammaStrip?.dataset.gammaState || "").trim().toLowerCase();
    const briefText = String(briefCardShell?.textContent || "").trim().toLowerCase();
    const decisionReady = (
      hasText(leadText)
      && leadText !== "loading planning context."
      && hasText(biasText)
      && biasText !== "loading"
      && hasText(planText)
      && !planText.startsWith("loading")
    );
    const gammaReady = !!gammaState && gammaState !== "loading" && gammaState !== "unavailable";
    const briefReady = (
      hasText(briefText)
      && !briefText.includes("building the next-session brief")
      && !briefText.includes("preparing the brief shell")
    );
    return decisionReady && gammaReady && briefReady;
  };

  const syncMindsetAnchored = () => {
    const done = planningHydrated();
    mindsetItem.classList.toggle("is-done", done);
    mindsetItem.classList.toggle("is-missing", !done);
    mindsetItem.title = done ? successDetail : defaultDetail;
    if (mindsetStatus) mindsetStatus.textContent = done ? "Loaded" : defaultStatus;
    if (mindsetDetail) mindsetDetail.textContent = done ? successDetail : defaultDetail;
    if (mindsetAction) mindsetAction.textContent = done ? "Ready" : defaultAction;
    syncReadiness();
  };

  const observedNodes = [decisionLead, decisionBiasValue, decisionPlanValue, gammaStrip, briefCardShell]
    .filter(Boolean);
  const observer = new MutationObserver(() => {
    window.setTimeout(syncMindsetAnchored, 0);
  });
  observedNodes.forEach((node) => {
    observer.observe(node, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: node === gammaStrip,
      attributeFilter: node === gammaStrip ? ["data-gamma-state"] : undefined,
    });
  });

  syncMindsetAnchored();
  window.setTimeout(syncMindsetAnchored, 120);
  window.setTimeout(syncMindsetAnchored, 600);
})();

(() => {
  const form = document.querySelector("[data-dashboard-account-bulk-form]");
  if (!form) return;
  const checks = Array.from(document.querySelectorAll("[data-dashboard-account-check]"));
  const selectAll = document.querySelector("[data-dashboard-account-select-all]");
  const clear = document.querySelector("[data-dashboard-account-clear]");
  const countLabel = document.querySelector("[data-dashboard-account-selected-count]");

  const updateCount = () => {
    const selected = checks.filter((check) => check.checked).length;
    if (countLabel) countLabel.textContent = `${selected} selected`;
    form.classList.toggle("has-selection", selected > 0);
  };

  checks.forEach((check) => {
    check.addEventListener("change", updateCount);
    check.addEventListener("click", (event) => {
      event.stopPropagation();
    });
  });

  selectAll?.addEventListener("click", () => {
    checks.forEach((check) => {
      check.checked = true;
    });
    updateCount();
  });

  clear?.addEventListener("click", () => {
    checks.forEach((check) => {
      check.checked = false;
    });
    updateCount();
  });

  form.addEventListener("submit", (event) => {
    if (!checks.some((check) => check.checked)) {
      event.preventDefault();
      updateCount();
    }
  });

  updateCount();
})();
