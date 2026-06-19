(function () {
  const root = document.querySelector("[data-gamma-ladder]");
  if (!root) return;

  const defaultSymbol = String(root.dataset.defaultSymbol || "SPX").toUpperCase();
  const defaultWindowPreset = String(root.dataset.defaultWindow || "standard").toLowerCase();
  const defaultDtePreset = String(root.dataset.defaultDte || "0").toLowerCase();
  const supportedSymbols = (() => {
    try {
      const values = JSON.parse(root.dataset.supportedSymbols || "[]");
      return Array.isArray(values) ? values.map((value) => String(value).toUpperCase()) : [];
    } catch (_err) {
      return [];
    }
  })();
  const apiUrl = String(root.dataset.apiUrl || "/api/gamma-ladder");
  const pills = Array.from(root.querySelectorAll("[data-gamma-symbol-pill]"));
  const searchForm = root.querySelector("[data-gamma-symbol-search]");
  const searchInput = root.querySelector("[data-gamma-symbol-input]");
  const searchQuickButtons = Array.from(root.querySelectorAll("[data-gamma-symbol-quick]"));
  const windowPills = Array.from(root.querySelectorAll("[data-gamma-window-pill]"));
  const dtePills = Array.from(root.querySelectorAll("[data-gamma-dte-pill]"));
  const decisionPanels = root.querySelector("[data-gamma-decision-panels]");
  const structureSummary = root.querySelector("[data-gamma-structure-summary]");
  const topLevelsHost = root.querySelector("[data-gamma-top-levels]");
  const board = root.querySelector("[data-gamma-board]");
  const rowsHost = root.querySelector("[data-gamma-rows]");
  const loading = root.querySelector("[data-gamma-loading]");
  const errorNode = root.querySelector("[data-gamma-error]");
  const tooltip = root.querySelector("[data-gamma-tooltip]");
  const headerSymbol = root.querySelector("[data-gamma-symbol]");
  const headerSpot = root.querySelector("[data-gamma-spot]");
  const headerRegime = root.querySelector("[data-gamma-regime]");
  const headerExpiration = root.querySelector("[data-gamma-expiration]");
  const headerUpdated = root.querySelector("[data-gamma-updated]");
  const refreshButton = root.querySelector("[data-gamma-refresh]");
  const summaryNode = root.querySelector("[data-gamma-summary]");
  const legendItems = Array.from(root.querySelectorAll("[data-gamma-legend]"));

  let currentSymbol = defaultSymbol;
  let currentWindowPreset = defaultWindowPreset;
  let currentDtePreset = defaultDtePreset;
  let refreshTimer = null;
  let switchTimer = null;
  let controller = null;
  let requestSequence = 0;
  let inFlightSymbol = "";
  let hasLoadedData = false;
  let lastSummaryText = "Loading focused window…";
  let symbolSearchControl = null;
  let activeTooltipAnchor = null;
  let activeDetailRow = null;

  const symbolThemeClass = (symbol) => `gamma-theme-${String(symbol || "").toLowerCase()}`;
  const titleCase = (value) => {
    const text = String(value || "").trim().toLowerCase();
    return text ? `${text.charAt(0).toUpperCase()}${text.slice(1)}` : "Standard";
  };
  const formatNumber = (value, digits = 2) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "—";
    return numeric.toLocaleString("en-US", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  };
  const formatCompact = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "—";
    return new Intl.NumberFormat("en-US", {
      notation: "compact",
      maximumFractionDigits: 1,
      signDisplay: "always",
    }).format(numeric);
  };
  const escapeHtml = (value) =>
    String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const rgba = (red, green, blue, alpha) => `rgba(${red}, ${green}, ${blue}, ${alpha.toFixed(3)})`;
  const GAMMA_STRENGTH_THRESHOLDS = {
    moderate: 0.18,
    strong: 0.45,
    extreme: 0.72,
  };
  const LEVEL_TOLERANCE_POINTS = {
    SPX: 2,
    SPY: 0.25,
    QQQ: 0.35,
  };
  const CONFIRMATION_CLOSES = 2;
  const colorSets = {
    strongPositive: { base: [97, 243, 213], accent: [97, 243, 213] },
    positive: { base: [76, 201, 240], accent: [76, 201, 240] },
    neutral: { base: [148, 175, 199], accent: [148, 175, 199] },
    negative: { base: [178, 107, 255], accent: [178, 107, 255] },
    strongNegative: { base: [255, 77, 141], accent: [255, 77, 141] },
    strongest: { base: [255, 209, 102], accent: [255, 209, 102] },
  };
  const gammaBehavior = {
    "strong-positive": "Strong positive gamma. Market may stabilize, compress volatility, and pin near this strike.",
    positive: "Positive gamma. Price often mean reverts and directional follow-through is softer here.",
    neutral: "Neutral gamma. This is a transition zone where balance can break in either direction.",
    negative: "Negative gamma. Expansion risk rises here and moves can accelerate faster than usual.",
    "strong-negative":
      "Strong negative gamma. Volatility expansion and momentum amplification are more likely here.",
  };
  const gammaPolarity = (state) =>
    state.includes("positive") ? "positive" : state.includes("negative") ? "negative" : "neutral";
  const distanceFromSpot = (strike, spot) => Number(strike || 0) - Number(spot || 0);
  const classifyGammaStrength = (netGex, maxAbsGex, thresholds = GAMMA_STRENGTH_THRESHOLDS) => {
    const ratio = maxAbsGex > 0 ? Math.abs(Number(netGex) || 0) / maxAbsGex : 0;
    if (ratio >= thresholds.extreme) return { label: "Extreme", key: "extreme", ratio };
    if (ratio >= thresholds.strong) return { label: "Strong", key: "strong", ratio };
    if (ratio >= thresholds.moderate) return { label: "Moderate", key: "moderate", ratio };
    return { label: "Weak", key: "weak", ratio };
  };
  const classifyGammaLevel = ({ row, spot, regime, strength }) => {
    const distance = distanceFromSpot(row.strike, spot);
    const isAboveSpot = distance > 0;
    const isBelowSpot = distance < 0;
    if (row.is_strongest) return { label: "Dealer Magnet", type: "magnet" };
    if (row.is_spot_nearest) return { label: "Current market", type: "current" };
    if (row.is_flip) return { label: "Acceleration zone", type: "acceleration" };
    const negativeRegime = String(regime || "").includes("negative");
    if (negativeRegime && ["strong", "extreme"].includes(strength.key)) {
      return { label: "Acceleration zone", type: "acceleration" };
    }
    if (isAboveSpot) return { label: "Resistance Above", type: "resistance" };
    if (isBelowSpot) return { label: "Support Below", type: "support" };
    return { label: "Current market", type: "current" };
  };
  const getTolerancePoints = (symbol) =>
    LEVEL_TOLERANCE_POINTS[String(symbol || "").toUpperCase()] || 0.5;
  const numericCloses = (values) =>
    (Array.isArray(values) ? values : [])
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
  const getLevelStatus = ({
    level,
    spot,
    previousSpot,
    recentHigh,
    recentLow,
    recentCloses,
    levelType,
    tolerancePoints,
    confirmationCloses = CONFIRMATION_CLOSES,
  }) => {
    const levelValue = Number(level);
    const currentSpot = Number(spot);
    const priorSpot = Number(previousSpot);
    const high = Number(recentHigh);
    const low = Number(recentLow);
    const closes = numericCloses(recentCloses);
    const tolerance = Math.max(0, Number(tolerancePoints) || 0);
    const confirmCount = Math.max(1, Number(confirmationCloses) || CONFIRMATION_CLOSES);
    const confirmed = closes.slice(-confirmCount);
    const nearLevel =
      Number.isFinite(levelValue) &&
      Number.isFinite(currentSpot) &&
      Math.abs(currentSpot - levelValue) <= tolerance;
    const confirmedAbove =
      confirmed.length >= confirmCount && confirmed.every((close) => close > levelValue + tolerance);
    const confirmedBelow =
      confirmed.length >= confirmCount && confirmed.every((close) => close < levelValue - tolerance);
    const closesBackBelow =
      closes.length >= confirmCount && closes.slice(-confirmCount).every((close) => close < levelValue);
    const closesBackAbove =
      closes.length >= confirmCount && closes.slice(-confirmCount).every((close) => close > levelValue);
    const retestHoldAbove =
      Number.isFinite(low) &&
      low <= levelValue + tolerance &&
      closesBackAbove &&
      currentSpot >= levelValue;
    const retestRejectBelow =
      Number.isFinite(high) &&
      high >= levelValue - tolerance &&
      closesBackBelow &&
      currentSpot <= levelValue;

    if (!Number.isFinite(levelValue) || !Number.isFinite(currentSpot)) return "Approaching";

    if (levelType === "Resistance Above") {
      if (retestHoldAbove) return "Flipped to Support";
      if (confirmedAbove) return "Broken";
      if (Number.isFinite(high) && high >= levelValue && closesBackBelow) return "Holding";
      if (nearLevel) return "Testing";
      return "Approaching";
    }

    if (levelType === "Support Below") {
      if (retestRejectBelow) return "Flipped to Resistance";
      if (confirmedBelow) return "Broken";
      if (Number.isFinite(low) && low <= levelValue && closesBackAbove) return "Holding";
      if (nearLevel) return "Testing";
      return "Approaching";
    }

    if (levelType === "Magnet") {
      if (nearLevel) return "Orbiting";
      if (Number.isFinite(priorSpot)) {
        const wasFarther = Math.abs(priorSpot - levelValue) > Math.abs(currentSpot - levelValue);
        const awayConfirmed =
          Math.abs(currentSpot - levelValue) > tolerance &&
          confirmed.length >= confirmCount &&
          confirmed.every((close) => Math.abs(close - levelValue) > tolerance);
        if (!wasFarther && awayConfirmed) return "Escaping";
        if (wasFarther) return "Attracting";
      }
      return "Attracting";
    }

    if (levelType === "Acceleration Zone") {
      if (nearLevel) return "Testing";
      if (confirmedAbove || confirmedBelow) return "Broken";
      return "Approaching";
    }

    return nearLevel ? "Testing" : "Approaching";
  };
  const generateExpectedBehavior = (level) => {
    if (!level) return "Monitor the nearest high-gamma levels before acting.";
    if (level.type === "magnet") {
      return "Price may continue rotating around this level unless momentum expands.";
    }
    if (level.type === "resistance") {
      return "Price may stall or reject near this level.";
    }
    if (level.type === "support") {
      return "Price may bounce or stabilize near this level.";
    }
    if (level.type === "acceleration") {
      return "If price breaks through this level, movement may expand.";
    }
    if (level.type === "current") {
      return "Current market location anchors the nearby ladder.";
    }
    return "Monitor this level for changes in dealer pressure.";
  };
  const levelTooltip = (type, status = "") => {
    if (type === "resistance" && status === "Broken") {
      return {
        title: "Broken Resistance",
        body: "Price has accepted above this level. Do not treat it as resistance unless price loses it again.",
      };
    }
    if (type === "resistance" && status === "Flipped to Support") {
      return {
        title: "Flipped to Support",
        body: "Former resistance is now acting as support after a successful retest.",
      };
    }
    if (type === "support" && status === "Broken") {
      return {
        title: "Broken Support",
        body: "Price has accepted below this level. Do not treat it as support unless price regains it.",
      };
    }
    if (type === "support" && status === "Flipped to Resistance") {
      return {
        title: "Flipped to Resistance",
        body: "Former support is now acting as resistance after a failed reclaim.",
      };
    }
    if (type === "magnet") {
      return {
        title: "Dealer Magnet",
        body: "Strongest nearby gamma level that price may rotate toward.",
      };
    }
    if (type === "resistance") {
      return {
        title: "Resistance Above",
        body: "A strong gamma level above spot. It is not confirmed resistance until price tests and rejects.",
      };
    }
    if (type === "support") {
      return {
        title: "Support Below",
        body: "A strong gamma level below spot. It is not confirmed support until price tests and bounces.",
      };
    }
    if (type === "acceleration") {
      return {
        title: "Acceleration Zone",
        body: "Strong negative gamma level where a break may expand movement.",
      };
    }
    return {
      title: "Current Market",
      body: "Current price location relative to nearby gamma levels.",
    };
  };
  const tooltipData = (type, status = "") => escapeHtml(JSON.stringify(levelTooltip(type, status)));
  const getTopGammaLevels = (rows, limit = 3) =>
    rows
      .slice()
      .sort((a, b) => Math.abs(Number(b.net_gex) || 0) - Math.abs(Number(a.net_gex) || 0))
      .slice(0, limit);
  const roleBaseLabel = (row) => {
    if (!row || !row.level) return "Level";
    if (row.level.type === "magnet") return "Magnet";
    if (row.level.type === "resistance") return "Resistance";
    if (row.level.type === "support") return "Support";
    if (row.level.type === "acceleration") return "Acceleration Zone";
    return row.level.label || "Level";
  };
  const roleLabel = (row) => {
    if (!row) return "Level";
    const rank = row.importance?.label || "";
    if (row.level?.type === "magnet") return "Magnet";
    if (rank === "PRIMARY") return `Primary ${roleBaseLabel(row)}`;
    if (rank === "SECONDARY") return `Secondary ${roleBaseLabel(row)}`;
    if (rank === "MINOR") return `Minor ${roleBaseLabel(row)}`;
    return roleBaseLabel(row);
  };
  const focusDistanceLabel = (distance, symbol) => {
    const points = Math.abs(Number(distance) || 0);
    return `${formatNumber(points, String(symbol || "").toUpperCase() === "SPX" ? 1 : 2)} points away`;
  };
  const gammaSignal = ({ row, gammaState, strengthRatio, zoneMeta }) => {
    if (row.is_strongest) {
      return { label: "MAGNET", type: "strongest" };
    }
    if (row.is_spot_nearest) {
      return { label: "CURRENT", type: "spot" };
    }
    if (row.is_flip) {
      return { label: "FLIP", type: "flip" };
    }
    if (zoneMeta?.type === "compression") {
      return { label: "COMPRESSION", type: "compression" };
    }
    if (zoneMeta?.type === "expansion") {
      return { label: "EXPANSION", type: "expansion" };
    }
    if (strengthRatio < 0.24 || gammaState === "neutral") {
      return { label: "LIGHT", type: "light" };
    }
    if (gammaState.includes("positive")) {
      return { label: "COMPRESSION", type: "compression" };
    }
    return { label: "EXPANSION", type: "expansion" };
  };
  const formatDistance = (distance, symbol) => {
    const digits = String(symbol || "").toUpperCase() === "SPX" ? 1 : 2;
    const numeric = Number(distance) || 0;
    const prefix = numeric > 0 ? "+" : "";
    return `${prefix}${formatNumber(numeric, digits)}`;
  };
  const dteDisplayLabel = (dtePreset) => {
    const value = String(dtePreset || "3").toLowerCase();
    return value === "all" ? "All" : `${value}DTE`;
  };
  const regimeShortLabel = (payload) => {
    const regime = String(payload.regime || "");
    if (regime.includes("positive")) return "Positive";
    if (regime.includes("negative")) return "Negative";
    return "Neutral";
  };
  const statusLevelType = (level) => {
    if (!level) return "";
    if (level.type === "magnet") return "Magnet";
    if (level.type === "resistance") return "Resistance Above";
    if (level.type === "support") return "Support Below";
    if (level.type === "acceleration") return "Acceleration Zone";
    return level.label || "";
  };
  const buildGammaDecisionModel = (payload, rowMeta) => {
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const spot = Number(payload.spot) || 0;
    const regime = String(payload.regime || "mixed_gamma");
    const decoratedRows = rows.map((row, index) => {
      const meta = rowMeta[index] || {};
      const strength = meta.strength || { label: "Weak", key: "weak", ratio: 0 };
      const level = classifyGammaLevel({ row, spot, regime, strength });
      const status = getLevelStatus({
        level: row.strike,
        spot,
        previousSpot: payload.previous_spot,
        recentHigh: payload.recent_high,
        recentLow: payload.recent_low,
        recentCloses: payload.recent_closes,
        levelType: statusLevelType(level),
        tolerancePoints: getTolerancePoints(payload.symbol),
        confirmationCloses: payload.confirmation_closes || CONFIRMATION_CLOSES,
      });
      return {
        ...row,
        strength,
        level,
        status,
        distance: distanceFromSpot(row.strike, spot),
        behavior: generateExpectedBehavior(level),
      };
    });
    const magnet =
      decoratedRows.find((row) => row.level.type === "magnet") ||
      getTopGammaLevels(decoratedRows, 1)[0] ||
      null;
    const byAbsGex = (a, b) => Math.abs(Number(b.net_gex) || 0) - Math.abs(Number(a.net_gex) || 0);
    const support = decoratedRows.filter((row) => row.distance < 0).sort(byAbsGex)[0] || null;
    const resistance = decoratedRows.filter((row) => row.distance > 0).sort(byAbsGex)[0] || null;
    const primaryCandidates = [support, resistance].filter((row) => row && row !== magnet).sort(byAbsGex);
    const primaryLevel = primaryCandidates[0] || null;
    const secondaryLevel =
      primaryCandidates.find((row) => row !== primaryLevel) ||
      decoratedRows
        .filter((row) => row !== magnet && row !== primaryLevel)
        .sort(byAbsGex)[0] ||
      null;
    const meaningfulRows = decoratedRows.filter((row) => {
      const ratio = Number(row.strength?.ratio) || 0;
      return row === magnet || row === primaryLevel || row === secondaryLevel || ratio >= 0.18;
    });
    decoratedRows.forEach((row) => {
      let importance = { label: "MINOR", key: "minor" };
      if (row === magnet) importance = { label: "MAGNET", key: "magnet" };
      else if (row === primaryLevel) importance = { label: "PRIMARY", key: "primary" };
      else if (row === secondaryLevel) importance = { label: "SECONDARY", key: "secondary" };
      else if (!meaningfulRows.includes(row)) importance = { label: "MINOR", key: "minor" };
      row.importance = importance;
    });
    const focusLevel =
      meaningfulRows
        .slice()
        .filter((row) => row !== magnet || Math.abs(Number(row.distance) || 0) <= getTolerancePoints(payload.symbol) * 2)
        .sort((a, b) => Math.abs(Number(a.distance) || 0) - Math.abs(Number(b.distance) || 0))[0] ||
      meaningfulRows.slice().sort((a, b) => Math.abs(Number(a.distance) || 0) - Math.abs(Number(b.distance) || 0))[0] ||
      null;
    const behavior = magnet
      ? `Price is attracted to ${formatNumber(Number(magnet.strike), Number(magnet.strike) >= 1000 ? 0 : 2)}, but breaks away from the magnet may accelerate.`
      : "Monitor the nearest high-gamma levels before acting.";
    return {
      rows: decoratedRows,
      topLevels: getTopGammaLevels(meaningfulRows, 3),
      magnet,
      support,
      resistance,
      focusLevel,
      behavior,
    };
  };
  const sanitizeSymbol = (value) =>
    String(value || "")
      .toUpperCase()
      .replace(/[^A-Z.-]/g, "")
      .slice(0, 12);

  const applyTheme = (symbol, regime) => {
    root.dataset.symbol = symbol;
    root.dataset.regime = regime;
    Array.from(root.classList)
      .filter((className) => className.indexOf("gamma-theme-") === 0)
      .forEach((className) => root.classList.remove(className));
    root.classList.add(symbolThemeClass(symbol));
  };

  const setVisualState = (state) => {
    root.classList.remove("is-initial-loading", "is-refreshing", "is-error", "has-data");
    if (state) root.classList.add(state);
    if (hasLoadedData) root.classList.add("has-data");
  };

  const setLoadingState = ({ initial = false, refreshing = false } = {}) => {
    setVisualState(initial ? "is-initial-loading" : refreshing ? "is-refreshing" : "");
    if (loading) loading.hidden = !initial;
    if (board) board.hidden = initial && !hasLoadedData;
    if (errorNode) errorNode.hidden = true;
    if (refreshButton) refreshButton.disabled = initial || refreshing;
    if (initial && decisionPanels) decisionPanels.hidden = true;
  };

  const renderError = (message) => {
    if (errorNode) {
      errorNode.hidden = false;
      errorNode.textContent = message || "Gamma ladder unavailable.";
    }
    if (board) board.hidden = !hasLoadedData;
    if (loading) loading.hidden = true;
    if (refreshButton) refreshButton.disabled = false;
    setVisualState("is-error");
    if (summaryNode) {
      summaryNode.textContent = hasLoadedData
        ? "Refresh failed. Showing the last successful gamma ladder."
        : "Gamma ladder unavailable.";
    }
    if (!hasLoadedData && decisionPanels) decisionPanels.hidden = true;
  };

  const submitSymbolSearch = (symbol) => {
    const nextSymbol = sanitizeSymbol(symbol) || defaultSymbol;
    if (searchInput) searchInput.value = nextSymbol;
    if (nextSymbol === currentSymbol && hasLoadedData) return;
    window.clearTimeout(switchTimer);
    switchTimer = window.setTimeout(() => {
      setActiveSymbol(nextSymbol);
      fetchGammaLadder(nextSymbol);
    }, 120);
  };

  const setActiveSymbol = (symbol) => {
    currentSymbol = sanitizeSymbol(symbol) || defaultSymbol;
    if (searchInput) searchInput.value = currentSymbol;
    if (symbolSearchControl && symbolSearchControl.selectedSymbol !== currentSymbol) {
      symbolSearchControl.setSelected(currentSymbol);
    }
    pills.forEach((pill) => {
      const pillSymbol = String(pill.dataset.gammaSymbolPill || "").toUpperCase();
      const isActive = pillSymbol === currentSymbol;
      pill.classList.toggle("active", isActive);
      pill.setAttribute("aria-selected", isActive ? "true" : "false");
    });
  };

  const setActiveWindowPreset = (windowPreset) => {
    currentWindowPreset = String(windowPreset || defaultWindowPreset).toLowerCase();
    windowPills.forEach((pill) => {
      const pillWindow = String(pill.dataset.gammaWindowPill || "").toLowerCase();
      const isActive = pillWindow === currentWindowPreset;
      pill.classList.toggle("active", isActive);
      pill.setAttribute("aria-selected", isActive ? "true" : "false");
    });
  };

  const setActiveDtePreset = (dtePreset) => {
    currentDtePreset = String(dtePreset || defaultDtePreset).toLowerCase();
    dtePills.forEach((pill) => {
      const pillDte = String(pill.dataset.gammaDtePill || "").toLowerCase();
      const isActive = pillDte === currentDtePreset;
      pill.classList.toggle("active", isActive);
      pill.setAttribute("aria-selected", isActive ? "true" : "false");
    });
  };

  const applyAvailableDteOptions = (payload) => {
    const available = Array.isArray(payload.available_dte_options)
      ? payload.available_dte_options.map((value) => String(value).toLowerCase())
      : [];
    if (!available.length) return;
    dtePills.forEach((pill) => {
      const pillDte = String(pill.dataset.gammaDtePill || "").toLowerCase();
      const isAvailable = available.includes(pillDte);
      pill.disabled = !isAvailable;
      pill.setAttribute("aria-disabled", isAvailable ? "false" : "true");
      pill.title = isAvailable ? "" : "Only 3DTE data available";
    });
    root.classList.toggle("has-limited-dte", available.length === 1 && available[0] === "3");
  };

  const renderGammaHeader = (payload) => {
    const symbol = String(payload.symbol || currentSymbol).toUpperCase();
    const regime = String(payload.regime || "mixed_gamma");
    const regimeLabel = payload.regime_label || "Mixed Gamma Regime";
    if (headerSymbol) headerSymbol.textContent = symbol;
    if (headerSpot) headerSpot.textContent = `· ${formatNumber(payload.spot, symbol === "SPX" ? 2 : 2)}`;
    if (headerRegime) {
      headerRegime.textContent = regimeLabel;
      headerRegime.className = `gamma-ladder-regime gamma-ladder-regime--${regime}`;
    }
    if (headerExpiration) headerExpiration.textContent = `Data: ${dteDisplayLabel(currentDtePreset)}`;
    if (headerUpdated) {
      headerUpdated.textContent = payload.updated_label ? `· Updated ${payload.updated_label}` : "· Updated —";
    }
    if (summaryNode) {
      const visible = Number(payload.rows_visible) || 0;
      const total = Number(payload.rows_total) || visible;
      const windowPreset = titleCase(payload.window_preset || currentWindowPreset);
      const dteLabel = dteDisplayLabel(currentDtePreset);
      const updatedAt = Date.parse(String(payload.updated_at || ""));
      const isStale = Number.isFinite(updatedAt) && Date.now() - updatedAt > 5 * 60 * 1000;
      lastSummaryText =
        total > visible
          ? `Showing ${visible} of ${total} strikes near spot · ${windowPreset} window · ${dteLabel}.`
          : `Showing ${visible} focused strike${visible === 1 ? "" : "s"} · ${windowPreset} window · ${dteLabel}.`;
      summaryNode.textContent = isStale ? `${lastSummaryText} Stale data.` : lastSummaryText;
      root.classList.toggle("is-stale", isStale);
    }
    applyAvailableDteOptions(payload);
    setActiveWindowPreset(payload.window_preset || currentWindowPreset);
    setActiveDtePreset(currentDtePreset);
    setActiveSymbol(symbol);
    applyTheme(symbol, regime);
  };

  const MarketStructureSummary = (payload, decisionModel) => {
    if (!structureSummary) return;
    const symbol = String(payload.symbol || currentSymbol).toUpperCase();
    const spot = Number(payload.spot) || 0;
    const valueLabel = (row) =>
      row ? formatNumber(Number(row.strike), Number(row.strike) >= 1000 ? 0 : 2) : "None nearby";
    const statusLabel = (row) => (row ? row.status || "Approaching" : "Unavailable");
    const focus = decisionModel.focusLevel;
    const summaryLevel = (title, row, type) => `
      <span data-tooltip="${tooltipData(type, row?.status || "")}">
        <strong>${title}</strong>
        <em>${escapeHtml(valueLabel(row))}</em>
        <small>${escapeHtml(statusLabel(row))}</small>
      </span>
    `;
    structureSummary.innerHTML = `
      <div class="gamma-structure-card__lead">
        <span class="gamma-structure-card__symbol">${escapeHtml(symbol)}</span>
        <span class="gamma-structure-card__spot">${escapeHtml(formatNumber(spot, 2))}</span>
      </div>
      <div class="gamma-structure-card__grid">
        <span><strong>Regime</strong><em>${escapeHtml(regimeShortLabel(payload))}</em><small>Structure</small></span>
        ${summaryLevel("Primary Magnet", decisionModel.magnet, "magnet")}
        ${summaryLevel("Nearest Support Below", decisionModel.support, "support")}
        ${summaryLevel("Nearest Resistance Above", decisionModel.resistance, "resistance")}
      </div>
      <div class="gamma-focus-level">
        <strong>Focus Level</strong>
        <em>${escapeHtml(valueLabel(focus))}</em>
        <span>${escapeHtml(focus ? roleLabel(focus) : "None nearby")}</span>
        <small>${escapeHtml(focus ? focusDistanceLabel(focus.distance, symbol) : "No significant level")}</small>
      </div>
      <div class="gamma-structure-card__behavior">
        ${escapeHtml(decisionModel.behavior)}
      </div>
      <div class="gamma-structure-card__helper">
        Support is a strong level below spot. Resistance is a strong level above spot. Magnet is the strongest nearby GEX level. Acceleration zones are strong negative-gamma break areas.
      </div>
    `;
  };

  const TopLevelsPanel = (decisionModel) => {
    if (!topLevelsHost) return;
    const rows = Array.isArray(decisionModel.topLevels) ? decisionModel.topLevels : [];
    topLevelsHost.innerHTML = `
      <div class="gamma-top-levels__title">Top Levels</div>
      <div class="gamma-top-levels__header">
        <span>Level</span>
        <span>Role</span>
        <span>Status</span>
      </div>
      <div class="gamma-top-levels__list">
        ${
          rows.length
            ? rows
                .map((row) => {
                  const strike = Number(row.strike);
                  const strikeDigits = strike >= 1000 ? 0 : 2;
                  return `
                    <div class="gamma-top-levels__item gamma-top-levels__item--${escapeHtml(row.level.type)}" data-tooltip="${tooltipData(row.level.type, row.status)}">
                      <strong>${escapeHtml(formatNumber(strike, strikeDigits))}</strong>
                      <em>${escapeHtml(roleLabel(row))}</em>
                      <small>${escapeHtml(row.status || row.strength.label)}</small>
                    </div>
                  `;
                })
                .join("")
            : '<div class="gamma-top-levels__empty">No ranked levels.</div>'
        }
      </div>
    `;
  };

  const GammaRow = ({ row, payload, index, maxSide, meta }) => {
    const strike = Number(row.strike);
    const spot = Number(payload.spot) || 0;
    const callGex = Number(row.call_gex) || 0;
    const putGex = Math.abs(Number(row.put_gex) || 0);
    const netGex = Number(row.net_gex) || 0;
    const { strengthRatio, gammaState, strength, level } = meta;
    const zoneMeta = meta.zoneMeta;
    const colorSet =
      gammaState === "strong-positive"
        ? colorSets.strongPositive
        : gammaState === "positive"
          ? colorSets.positive
          : gammaState === "strong-negative"
            ? colorSets.strongNegative
            : gammaState === "negative"
              ? colorSets.negative
              : colorSets.neutral;
    const intensityClass =
      strength.key === "extreme" || row.is_strongest
        ? " is-dominant"
        : strength.key === "strong"
          ? " is-strong"
          : strength.key === "moderate"
            ? " is-medium"
            : " is-light";
    const callWidth = Math.max(0, Math.min(46, (Math.abs(callGex) / maxSide) * 46));
    const putWidth = Math.max(0, Math.min(46, (putGex / maxSide) * 46));
    const netRatio = Math.max(-1, Math.min(1, netGex / maxSide));
    const netX = 50 + netRatio * 44;
    const netBarWidth = Math.max(2, Math.min(44, (Math.abs(netGex) / maxSide) * 44));
    const netBarX = netGex >= 0 ? 50 : 50 - netBarWidth;
    const strikeDigits = strike >= 1000 ? 0 : 2;
    const symbol = String(payload.symbol || currentSymbol).toLowerCase();
    const distance = distanceFromSpot(strike, spot);
    const isAtSpot = Math.abs(distance) < 0.01;
    const isAboveSpot = distance > 0;
    const flipClass = row.is_flip ? " is-flip" : "";
    const strongestClass = row.is_strongest ? " is-strongest" : "";
    const spotClass = row.is_spot_nearest ? " is-spot" : "";
    const sideClass = isAtSpot ? " is-at-spot" : isAboveSpot ? " is-above-spot" : " is-below-spot";
    const zoneClass = zoneMeta
      ? ` is-${zoneMeta.type}-zone is-zone-${zoneMeta.position}${zoneMeta.isFocal ? " is-zone-focal" : ""}`
      : "";
    const stateClass = ` gamma-${gammaState}`;
    const levelClass = ` gamma-level-${level.type}`;
    const distanceLabel = formatDistance(distance, symbol);
    const [baseRed, baseGreen, baseBlue] = colorSet.base;
    const [accentRed, accentGreen, accentBlue] = colorSet.accent;
    const putStart = rgba(255, 77, 141, 0.12 + strengthRatio * 0.10);
    const putEnd = rgba(178, 107, 255, 0.36 + strengthRatio * 0.22);
    const callStart = rgba(76, 201, 240, 0.13 + strengthRatio * 0.10);
    const callEnd = rgba(97, 243, 213, 0.34 + strengthRatio * 0.22);
    const netStart = rgba(baseRed, baseGreen, baseBlue, 0.58 + strengthRatio * 0.18);
    const netEnd = rgba(accentRed, accentGreen, accentBlue, 0.98 - strengthRatio * 0.02);
    const overlayFill = row.is_strongest ? ' fill="rgba(255, 209, 102, 0.30)"' : "";
    const levelLabel = roleLabel({ level, importance: meta.importance });
    const statusLabel = meta.status || "Approaching";
    const importance = meta.importance || { label: "MINOR", key: "minor" };
    const detailId = `gamma-ladder-detail-${symbol}-${index}`;
    const tags = `
      <span class="gamma-ladder-tag gamma-ladder-tag--importance-${importance.key}" data-tooltip="${tooltipData(level.type, statusLabel)}">
        <span>${escapeHtml(importance.label)}</span>
      </span>
    `;
    const tooltipPayload = escapeHtml(
      JSON.stringify({
        strike: formatNumber(strike, strikeDigits),
        distance: distanceLabel,
        call: formatCompact(callGex),
        put: formatCompact(-putGex),
        net: formatCompact(netGex),
        behavior: `${levelLabel}: ${statusLabel}. ${generateExpectedBehavior(level)}`,
      })
    );
    const detailPayload = {
      strike: formatNumber(strike, strikeDigits),
      distance: distanceLabel,
      call: formatCompact(callGex),
      put: formatCompact(-putGex),
      net: formatCompact(netGex),
      role: levelLabel,
      status: statusLabel,
      behavior: generateExpectedBehavior(level),
    };
    return `
      <div class="gamma-ladder-rowWrap" data-gamma-row-wrap>
        <button
          class="gamma-ladder-row${flipClass}${strongestClass}${spotClass}${sideClass}${intensityClass}${stateClass}${zoneClass}${levelClass}"
          type="button"
          data-gamma-row
          data-tooltip="${tooltipPayload}"
          aria-expanded="false"
          aria-controls="${detailId}"
          aria-label="Strike ${formatNumber(strike, strikeDigits)} net gamma ${formatCompact(netGex)} ${importance.label} ${levelLabel} ${statusLabel} strength ${strength.label}"
        >
          <span class="gamma-ladder-row__strikeWrap">
            <span class="gamma-ladder-row__strike">${formatNumber(strike, strikeDigits)}</span>
            <span class="gamma-ladder-row__tone">${isAtSpot ? "At spot" : isAboveSpot ? "Above spot" : "Below spot"}</span>
            <span class="gamma-ladder-row__distance">${distanceLabel} ${isAtSpot ? "at spot" : isAboveSpot ? "above spot" : "below spot"}</span>
          </span>
          <span class="gamma-ladder-row__viz">
            <span class="gamma-ladder-row__laneLabel gamma-ladder-row__laneLabel--negative">NEG</span>
            <span class="gamma-ladder-row__laneLabel gamma-ladder-row__laneLabel--positive">POS</span>
            <svg viewBox="0 0 100 24" preserveAspectRatio="none" aria-hidden="true">
              <defs>
                <linearGradient id="gamma-put-${symbol}-${index}" x1="0%" y1="0%" x2="100%" y2="0%">
                  <stop offset="0%" stop-color="${putStart}"></stop>
                  <stop offset="100%" stop-color="${putEnd}"></stop>
                </linearGradient>
                <linearGradient id="gamma-call-${symbol}-${index}" x1="0%" y1="0%" x2="100%" y2="0%">
                  <stop offset="0%" stop-color="${callStart}"></stop>
                  <stop offset="100%" stop-color="${callEnd}"></stop>
                </linearGradient>
                <linearGradient id="gamma-net-${symbol}-${index}" x1="0%" y1="0%" x2="100%" y2="0%">
                  <stop offset="0%" stop-color="${netStart}"></stop>
                  <stop offset="100%" stop-color="${netEnd}"></stop>
                </linearGradient>
              </defs>
              <line x1="50" y1="2" x2="50" y2="22" class="gamma-ladder-row__axis"></line>
              <rect x="${50 - putWidth}" y="9" width="${putWidth}" height="6" rx="3" fill="url(#gamma-put-${symbol}-${index})" class="gamma-ladder-row__putBar"></rect>
              <rect x="50" y="9" width="${callWidth}" height="6" rx="3" fill="url(#gamma-call-${symbol}-${index})" class="gamma-ladder-row__callBar"></rect>
              <rect x="${netBarX}" y="6" width="${netBarWidth}" height="12" rx="5" fill="url(#gamma-net-${symbol}-${index})" class="gamma-ladder-row__netBar"></rect>
              ${row.is_strongest ? `<rect x="${netBarX}" y="4.5" width="${netBarWidth}" height="15" rx="6"${overlayFill} class="gamma-ladder-row__strongestOverlay"></rect>` : ""}
              <circle cx="${netX}" cy="12" r="${row.is_strongest ? 4.2 : row.is_spot_nearest ? 3.6 : 2.8}" class="gamma-ladder-row__netDot"></circle>
            </svg>
            ${row.is_flip ? '<span class="gamma-ladder-row__flipLine" aria-hidden="true"></span>' : ""}
            ${row.is_spot_nearest ? '<span class="gamma-ladder-row__spotMarker" aria-hidden="true"></span>' : ""}
          </span>
          <span class="gamma-ladder-row__netWrap">
            <span class="gamma-ladder-row__net">${formatCompact(netGex)}</span>
            <span class="gamma-ladder-row__strength gamma-ladder-row__strength--${strength.key}">${strength.label}</span>
          </span>
          <span class="gamma-ladder-row__micro">${tags}</span>
        </button>
        <div class="gamma-ladder-row-detail" id="${detailId}" data-gamma-row-detail hidden>
          <div class="gamma-ladder-row-detail__label">Details</div>
          <div class="gamma-ladder-row-detail__grid">
            <span><strong>Strike</strong><em>${escapeHtml(detailPayload.strike)}</em></span>
            <span><strong>Net GEX</strong><em>${escapeHtml(detailPayload.net)}</em></span>
            <span><strong>Call GEX</strong><em>${escapeHtml(detailPayload.call)}</em></span>
            <span><strong>Put GEX</strong><em>${escapeHtml(detailPayload.put)}</em></span>
            <span><strong>Distance</strong><em>${escapeHtml(detailPayload.distance)} from spot</em></span>
            <span><strong>Status</strong><em>${escapeHtml(detailPayload.role)} · ${escapeHtml(detailPayload.status)}</em></span>
          </div>
          <div class="gamma-ladder-row-detail__behavior">${escapeHtml(detailPayload.behavior)}</div>
        </div>
      </div>
    `;
  };

  const renderGammaLadderRows = (payload) => {
    if (!rowsHost) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!rows.length) {
      rowsHost.innerHTML = '<div class="gamma-ladder-emptyState">No ladder rows available.</div>';
      if (structureSummary) {
        structureSummary.innerHTML = '<div class="gamma-structure-card__empty">No strikes returned for this symbol and DTE selection.</div>';
      }
      if (topLevelsHost) {
        topLevelsHost.innerHTML = '<div class="gamma-top-levels__empty">No ranked levels.</div>';
      }
      if (decisionPanels) decisionPanels.hidden = false;
      root.style.setProperty("--gamma-ladder-visible-rows", "1");
      hasLoadedData = true;
      setVisualState("");
      if (loading) loading.hidden = true;
      if (board) board.hidden = false;
      return;
    }
    const maxSide = rows.reduce((max, row) => {
      const callAbs = Math.abs(Number(row.call_gex) || 0);
      const putAbs = Math.abs(Number(row.put_gex) || 0);
      const netAbs = Math.abs(Number(row.net_gex) || 0);
      return Math.max(max, callAbs, putAbs, netAbs);
    }, 0) || 1;
    const rowStateMeta = rows.map((row) => {
      const netGex = Number(row.net_gex) || 0;
      const strengthRatio = clamp(Math.abs(netGex) / maxSide, 0, 1);
      const isNeutral = row.is_flip || Math.abs(netGex) <= maxSide * 0.045;
      const gammaState = isNeutral
        ? "neutral"
        : netGex > 0
          ? strengthRatio >= 0.72
            ? "strong-positive"
            : "positive"
          : strengthRatio >= 0.72
            ? "strong-negative"
            : "negative";
      return {
        gammaState,
        polarity: gammaPolarity(gammaState),
        strengthRatio,
      };
    });
    const zoneMetaByIndex = new Map();
    let cursor = 0;
    while (cursor < rowStateMeta.length) {
      const current = rowStateMeta[cursor];
      if (current.polarity === "neutral" || current.strengthRatio < 0.42) {
        cursor += 1;
        continue;
      }
      const start = cursor;
      let end = cursor;
      while (
        end + 1 < rowStateMeta.length &&
        rowStateMeta[end + 1].polarity === current.polarity &&
        rowStateMeta[end + 1].strengthRatio >= 0.42
      ) {
        end += 1;
      }
      const length = end - start + 1;
      if (length >= 2) {
        let focalIndex = start;
        for (let idx = start + 1; idx <= end; idx += 1) {
          if (rowStateMeta[idx].strengthRatio > rowStateMeta[focalIndex].strengthRatio) {
            focalIndex = idx;
          }
        }
        for (let idx = start; idx <= end; idx += 1) {
          zoneMetaByIndex.set(idx, {
            type: current.polarity === "positive" ? "compression" : "expansion",
            position: idx === start ? "start" : idx === end ? "end" : "mid",
            isFocal: idx === focalIndex,
          });
        }
      }
      cursor = end + 1;
    }

    const rowMeta = rows.map((row, index) => {
      const base = rowStateMeta[index] || {};
      const strength = classifyGammaStrength(row.net_gex, maxSide);
      const zoneMeta = zoneMetaByIndex.get(index);
      const level = classifyGammaLevel({
        row,
        spot: Number(payload.spot) || 0,
        regime: payload.regime,
        strength,
      });
      return { ...base, strength, zoneMeta, level };
    });
    const decisionModel = buildGammaDecisionModel(payload, rowMeta);
    const metaByStrike = new Map(
      (decisionModel.rows || []).map((row) => [String(Number(row.strike)), row])
    );
    rowMeta.forEach((meta, index) => {
      const decorated = metaByStrike.get(String(Number(rows[index]?.strike)));
      if (decorated) {
        meta.importance = decorated.importance;
        meta.status = decorated.status;
        meta.level = decorated.level;
        meta.strength = decorated.strength;
      }
    });
    if (decisionPanels) decisionPanels.hidden = false;
    MarketStructureSummary(payload, decisionModel);
    TopLevelsPanel(decisionModel);
    activeDetailRow = null;
    rowsHost.innerHTML = rows
      .map((row, index) => GammaRow({ row, payload, index, maxSide, meta: rowMeta[index] }))
      .join("");
    root.style.setProperty(
      "--gamma-ladder-visible-rows",
      String(Math.max(1, Math.min(rows.length, 12)))
    );
    hasLoadedData = true;
    setVisualState("");
    if (loading) loading.hidden = true;
    if (board) board.hidden = false;
    if (refreshButton) refreshButton.disabled = false;
  };

  const hideTooltip = () => {
    if (!tooltip) return;
    activeTooltipAnchor = null;
    tooltip.hidden = true;
    tooltip.textContent = "";
    tooltip.classList.remove("is-floating");
  };

  const clampTooltipValue = (value, min, max) => Math.max(min, Math.min(max, value));

  const positionTooltipNearPoint = (event) => {
    if (!tooltip || tooltip.hidden) return;
    const rect = root.getBoundingClientRect();
    const margin = 10;
    const width = tooltip.offsetWidth || 220;
    const height = tooltip.offsetHeight || 96;
    const left = clampTooltipValue(event.clientX - rect.left + 14, margin, rect.width - width - margin);
    const top = clampTooltipValue(event.clientY - rect.top + 14, margin, rect.height - height - margin);
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };

  const renderTooltip = (payload, { isRow = false } = {}) => {
    if (!tooltip || !payload) return false;
    if (isRow) {
      tooltip.innerHTML = `
        <strong>Strike ${escapeHtml(payload.strike)}</strong>
        <span>Net GEX ${escapeHtml(payload.net)}</span>
        <span>Call GEX ${escapeHtml(payload.call)}</span>
        <span>Put GEX ${escapeHtml(payload.put)}</span>
        <span>Distance ${escapeHtml(payload.distance)} from spot</span>
        <span>${escapeHtml(payload.behavior)}</span>
      `;
    } else {
      tooltip.innerHTML = `
        <strong>${escapeHtml(payload.title)}</strong>
        <span>${escapeHtml(payload.body)}</span>
      `;
    }
    tooltip.hidden = false;
    tooltip.classList.toggle("is-floating", !isRow);
    return true;
  };

  const payloadForTooltipNode = (node) => {
    if (!node) return null;
    try {
      return JSON.parse(node.dataset.tooltip || "{}");
    } catch (_err) {
      return null;
    }
  };

  const detailById = (id) => {
    if (!id) return null;
    if (window.CSS && typeof window.CSS.escape === "function") {
      return root.querySelector(`#${window.CSS.escape(id)}`);
    }
    return root.querySelector(`[id="${String(id).replace(/"/g, '\\"')}"]`);
  };

  const showTooltip = (event) => {
    if (!tooltip) return;
    const row = event.target.closest("[data-gamma-row]");
    const legendItem = event.target.closest("[data-gamma-legend]");
    const tooltipItem = event.target.closest("[data-tooltip]");
    const anchor = row ? null : legendItem || tooltipItem;
    if (!anchor) {
      hideTooltip();
      return;
    }
    const payload = payloadForTooltipNode(anchor);
    if (!payload) {
      hideTooltip();
      return;
    }
    if (activeTooltipAnchor !== anchor || tooltip.hidden) {
      activeTooltipAnchor = anchor;
      if (!renderTooltip(payload, { isRow: false })) return;
    }
    if (event.clientX && event.clientY) {
      positionTooltipNearPoint(event);
    }
  };

  const closeRowDetail = (row) => {
    const target = row || activeDetailRow;
    if (!target) return;
    const detailId = target.getAttribute("aria-controls");
    const detail = detailById(detailId);
    target.classList.remove("is-detail-open");
    target.setAttribute("aria-expanded", "false");
    if (detail) detail.hidden = true;
    if (!row || activeDetailRow === target) {
      activeDetailRow = null;
    }
  };

  const openRowDetail = (row) => {
    if (!row) return;
    const detailId = row.getAttribute("aria-controls");
    const detail = detailById(detailId);
    if (!detail) return;
    const isOpen = activeDetailRow === row && row.getAttribute("aria-expanded") === "true";
    if (activeDetailRow && activeDetailRow !== row) {
      closeRowDetail(activeDetailRow);
    }
    if (isOpen) {
      closeRowDetail(row);
      return;
    }
    hideTooltip();
    activeDetailRow = row;
    row.classList.add("is-detail-open");
    row.setAttribute("aria-expanded", "true");
    detail.hidden = false;
  };

  const handleGammaClick = (event) => {
    const row = event.target.closest("[data-gamma-row]");
    if (row) {
      event.preventDefault();
      openRowDetail(row);
      return;
    }
    if (tooltip && !tooltip.hidden && !event.target.closest("[data-tooltip], [data-gamma-legend]")) {
      hideTooltip();
    }
  };

  const refreshLoop = () => {
    if (refreshTimer) window.clearInterval(refreshTimer);
    refreshTimer = window.setInterval(() => {
      fetchGammaLadder(currentSymbol, { force: true });
    }, 60000);
  };

  const fetchGammaLadder = async (symbol, { force = false } = {}) => {
    const normalized = sanitizeSymbol(symbol) || defaultSymbol;
    if (inFlightSymbol === normalized && !force) return;
    if (controller) controller.abort();
    controller = new AbortController();
    inFlightSymbol = normalized;
    requestSequence += 1;
    const requestId = requestSequence;
    const refreshing = hasLoadedData;
    setLoadingState({ initial: !refreshing, refreshing });
    if (refreshing && summaryNode) summaryNode.textContent = `${lastSummaryText} Refreshing…`;
    try {
      const url = new URL(apiUrl, window.location.origin);
      url.searchParams.set("symbol", normalized);
      url.searchParams.set("window", currentWindowPreset);
      url.searchParams.set("dte", currentDtePreset);
      console.log("Fetching ladder with DTE:", currentDtePreset);
      if (force) url.searchParams.set("_", String(Date.now()));
      const response = await fetch(url.toString(), {
        signal: controller.signal,
        cache: "no-store",
        credentials: "same-origin",
        headers: { "X-Requested-With": "XMLHttpRequest" },
      });
      const payload = await response.json();
      if (requestId !== requestSequence) return;
      if (!response.ok || !payload || payload.ok === false) {
        throw new Error((payload && payload.message) || "Gamma ladder unavailable.");
      }
      renderGammaHeader(payload);
      renderGammaLadderRows(payload);
      refreshLoop();
    } catch (err) {
      if (err && err.name === "AbortError") return;
      renderError((err && err.message) || "Gamma ladder unavailable.");
    } finally {
      if (requestId === requestSequence) {
        inFlightSymbol = "";
      }
    }
  };

  if (window.SymbolSearchControl) {
    symbolSearchControl = new window.SymbolSearchControl({
      root,
      selectedSymbol: defaultSymbol,
      allowedSymbols: supportedSymbols,
      onSymbolChange: submitSymbolSearch,
      showQuickButtons: true,
      placeholder: "SPX",
      unsupportedMessage: "Only QQQ, SPY, and SPX are supported.",
      activeClass: "active",
      quickButtonSelector: "[data-gamma-symbol-pill]",
      popoverQuickButtonSelector: "[data-gamma-symbol-quick]",
      toggleSelector: "[data-gamma-symbol-search-toggle]",
      popoverSelector: "[data-gamma-symbol-search-popover]",
      formSelector: "[data-gamma-symbol-search]",
      inputSelector: "[data-gamma-symbol-input]",
    });
  } else {
    pills.forEach((pill) => {
      pill.addEventListener("click", () => {
        submitSymbolSearch(pill.dataset.gammaSymbolPill);
      });
    });
    if (searchInput) {
      searchInput.addEventListener("input", () => {
        const nextValue = sanitizeSymbol(searchInput.value);
        if (searchInput.value !== nextValue) searchInput.value = nextValue;
      });
    }
    if (searchForm) {
      searchForm.addEventListener("submit", (event) => {
        event.preventDefault();
        submitSymbolSearch(searchInput ? searchInput.value : "");
      });
    }
    searchQuickButtons.forEach((button) => {
      button.addEventListener("click", () => submitSymbolSearch(button.dataset.gammaSymbolQuick));
    });
  }

  windowPills.forEach((pill) => {
    pill.addEventListener("click", () => {
      const nextWindow = String(pill.dataset.gammaWindowPill || "").toLowerCase();
      if (!nextWindow || nextWindow === currentWindowPreset) return;
      window.clearTimeout(switchTimer);
      switchTimer = window.setTimeout(() => {
        setActiveWindowPreset(nextWindow);
        fetchGammaLadder(currentSymbol);
      }, 120);
    });
  });

  dtePills.forEach((pill) => {
    pill.addEventListener("click", () => {
      if (pill.disabled) return;
      const nextDte = String(pill.dataset.gammaDtePill || "").toLowerCase();
      if (!nextDte || nextDte === currentDtePreset) return;
      window.clearTimeout(switchTimer);
      switchTimer = window.setTimeout(() => {
        console.log("Selected DTE:", nextDte);
        setActiveDtePreset(nextDte);
        fetchGammaLadder(currentSymbol);
      }, 120);
    });
  });

  if (refreshButton) {
    refreshButton.addEventListener("click", () => {
      fetchGammaLadder(currentSymbol, { force: true });
    });
  }

  root.addEventListener("click", handleGammaClick);
  root.addEventListener("mouseover", (event) => {
    if (event.target.closest("[data-gamma-row]")) return;
    showTooltip(event);
  });
  root.addEventListener("mousemove", (event) => {
    if (!tooltip || tooltip.hidden) return;
    const row = event.target.closest("[data-gamma-row]");
    if (row) return;
    const tooltipItem = event.target.closest("[data-tooltip], [data-gamma-legend]");
    if (!tooltipItem) return;
    showTooltip(event);
  });
  root.addEventListener("mouseleave", hideTooltip);
  root.addEventListener("focusin", (event) => {
    const row = event.target.closest("[data-gamma-row]");
    if (row) return;
    showTooltip(event);
  });
  root.addEventListener("focusout", (event) => {
    if (!root.contains(event.relatedTarget)) hideTooltip();
  });
  root.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeRowDetail();
      hideTooltip();
    }
  });
  document.addEventListener("click", (event) => {
    if (!root.contains(event.target)) {
      closeRowDetail();
      hideTooltip();
    }
  });

  setActiveSymbol(defaultSymbol);
  setActiveWindowPreset(defaultWindowPreset);
  setActiveDtePreset(defaultDtePreset);
  fetchGammaLadder(defaultSymbol);
})();
