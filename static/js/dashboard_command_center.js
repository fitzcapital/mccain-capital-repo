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
      record.textContent = `${d.wins || "0"}W / ${d.losses || "0"}L • ${d.winRate || "0"}%`;
      balance.textContent = d.balance || "—";
      open.href = d.openUrl || "/trades";
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

(function () {
  const shell = document.getElementById("dashboardModeShell");
  if (!shell) return;

  const storageKey = String(shell.dataset.playbookTickerStorageKey || "mc_playbook_ticker");
  const selectedTicker = String(shell.dataset.selectedTicker || "QQQ").toUpperCase();
  const supportedTickers = new Set(["QQQ", "SPY"]);
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
  if (!queryTicker && storedTicker && storedTicker !== selectedTicker) {
    url.searchParams.set("ticker", storedTicker);
    window.location.replace(url.toString());
    return;
  }

  storageSet(storageKey, queryTicker || selectedTicker || "QQQ");
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
  const details = document.getElementById("advancedDashboardWidgets");
  const lazyShell = document.getElementById("dashboardCalendarLazy");
  if (!details || !lazyShell) return;

  let loading = false;

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
      initCalendarPreview(lazyShell);
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
  const updatedNode = document.getElementById("dashboardTapeUpdatedAt");
  const tapeCard = document.querySelector(".dashboardCoreTapeCard");
  const tapeRefreshBtn = document.getElementById("dashboardTapeRefreshBtn");
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
  const shell = document.getElementById("dashboardModeShell");
  const activeTicker = String(shell?.dataset.selectedTicker || "QQQ").toUpperCase();

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
    sparkNode.querySelectorAll(".marketMiniSparkLine, .marketMiniSparkArea").forEach((node) => {
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
    return `${seconds}s old`;
  };

  const formatFreshness = (iso, state) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) return { full: `${state} · unavailable`, compact: state };
    const ageS = Math.max(0, (Date.now() - ts) / 1000);
    const seconds = Math.floor(ageS);
    const compact = compactAgeLabel(seconds).replace(" old", "");
    const band = seconds >= 900 ? "Critical" : state;
    return {
      full: `${band} · ${compactAgeLabel(seconds)}`,
      compact,
      state,
    };
  };

  const deriveState = (quote) => {
    const price = asNum((quote || {}).price);
    if (price === null) return "Unavailable";
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
    if (state === "Delayed") return "delayed";
    return "missing";
  };

  const tapeStateLabel = (symbol, pct) => {
    if (symbol === "VIX") {
      if (pct !== null && pct <= -0.35) return "WEAK";
      if (pct !== null && pct >= 0.35) return "STRONG";
      return "MIXED";
    }
    if (pct !== null && pct >= 0.35) return "RISK-ON";
    if (pct !== null && pct <= -0.35) return "RISK-OFF";
    return "MIXED";
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
    row.addEventListener("click", () => {
      const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
      setExpandedSymbol(symbol === openSymbol ? "" : symbol);
    });
  });

  const updateRow = (row, quote, points) => {
    const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
    const detailNode = detailPanels.get(symbol);
    const price = asNum((quote || {}).price);
    const pct = quotePctChange(quote);
    const absChange = inferAbsoluteChange(price, pct);
    const state = deriveState(quote);
    const lastNode = row.querySelector('[data-role="last"]');
    const chgpNode = row.querySelector('[data-role="chgp"]');
    const stateNode = row.querySelector('[data-role="state"]');
    const rowMarketStateNode = row.querySelector('[data-role="market-state"]');
    const rowLiveNode = row.querySelector('[data-role="row-live"]');
    const rowRangeNode = row.querySelector('[data-role="detail-range"]');
    const detailChgNode = detailNode?.querySelector('[data-role="detail-chg"]');
    const detailOpenNode = detailNode?.querySelector('[data-role="detail-open"]');
    const detailPrevNode = detailNode?.querySelector('[data-role="detail-prev"]');
    const detailLiveNode = detailNode?.querySelector('[data-role="detail-live"]');
    const openValue = asNum((quote || {}).day_open ?? (quote || {}).open);
    const prevCloseValue = inferPreviousClose(quote);
    const freshness = formatFreshness((quote || {}).as_of, state);
    const tapeLabel = tapeStateLabel(symbol, pct);
    const tapeTone = pct !== null && pct > 0 ? "positive" : pct !== null && pct < 0 ? "negative" : "neutral";
    const hasSeededRowValues = (
      price === null
      && !String((quote || {}).as_of || "").trim()
      && !String((quote || {}).provider || "").trim()
      && (
        hasMeaningfulText(lastNode)
        || hasMeaningfulText(chgpNode)
      )
    );

    if (hasSeededRowValues) {
      return;
    }

    if (lastNode) {
      if (price !== null) {
        dashboardUIFX.setText(lastNode, formatValue(price, 2), { live: true, direction: tapeTone === "positive" ? "up" : tapeTone === "negative" ? "down" : "neutral" });
      } else if (!hasMeaningfulText(lastNode)) {
        lastNode.textContent = "loading...";
      }
    }
    if (chgpNode) {
      if (pct !== null) {
        dashboardUIFX.setText(chgpNode, `${formatSigned(pct, 2)}%`, { live: true, direction: tapeTone === "positive" ? "up" : tapeTone === "negative" ? "down" : "neutral" });
      } else if (!hasMeaningfulText(chgpNode)) {
        chgpNode.textContent = "loading...";
      }
      chgpNode.classList.remove("tone-positive", "tone-negative", "tone-neutral");
      chgpNode.classList.add(`tone-${tapeTone}`);
    }

    row.classList.remove(
      "is-up",
      "is-down",
      "is-flat",
      "is-live",
      "is-delayed",
      "is-missing",
      "tone-positive",
      "tone-negative",
      "tone-neutral"
    );
    row.classList.add(`is-${stateClass(state)}`);
    row.classList.add(tapeTone === "positive" ? "is-up" : tapeTone === "negative" ? "is-down" : "is-flat");
    row.classList.add(`tone-${tapeTone}`);
    row.dataset.tapeTone = tapeTone;
    applyMixedToneToSparkline(row, tapeTone, pct);
    if (stateNode) {
      dashboardUIFX.setText(stateNode, tapeLabel);
      stateNode.classList.remove("tone-positive", "tone-negative", "tone-neutral");
      stateNode.classList.add(`tone-${tapeTone}`);
    }
    if (rowMarketStateNode) dashboardUIFX.setText(rowMarketStateNode, freshness.state);
    if (rowLiveNode && String((quote || {}).as_of || "").trim()) {
      dashboardUIFX.setText(rowLiveNode, freshness.full);
    }
    if (rowRangeNode && ((quote || {}).day_low !== undefined || (quote || {}).day_high !== undefined)) {
      const dayLow = asNum((quote || {}).day_low);
      const dayHigh = asNum((quote || {}).day_high);
      if (dayLow !== null && dayHigh !== null) {
        dashboardUIFX.setText(rowRangeNode, `${formatValue(dayLow, 2)} to ${formatValue(dayHigh, 2)}`);
      }
    }

    if (detailChgNode) {
      if (absChange !== null) {
        dashboardUIFX.setText(detailChgNode, formatSigned(absChange, 2), { live: true, direction: absChange > 0 ? "up" : absChange < 0 ? "down" : "neutral" });
      } else if (!hasMeaningfulText(detailChgNode)) {
        detailChgNode.textContent = "loading...";
      }
    }
    if (detailOpenNode && openValue !== null) dashboardUIFX.setText(detailOpenNode, formatValue(openValue, 2));
    if (detailPrevNode && prevCloseValue !== null) dashboardUIFX.setText(detailPrevNode, formatValue(prevCloseValue, 2));
    if (detailLiveNode && String((quote || {}).as_of || "").trim()) dashboardUIFX.setText(detailLiveNode, freshness.compact);
  };

  const setStreamStatus = (label, detail) => {
    const statusChanged = dashboardUIFX.setText(statusNode, label, { pulse: true, tone: label.toLowerCase().includes("retry") ? "warning" : "info" });
    statusNode.dataset.tone = label.toLowerCase().includes("retry")
      ? "delayed"
      : label.toLowerCase().includes("connect")
      ? "off"
      : "live";
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

  const applyTapeSnapshot = (quotes, updatedLabel) => {
    const payload = quotes && typeof quotes === "object" ? quotes : {};
    rows.forEach((row) => {
      const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
      updateRow(row, payload[symbol] || {}, []);
    });
    if (updatedLabel && updatedNode) {
      updatedNode.textContent = updatedLabel;
    }
  };

  const gammaCardByKey = (key) => document.getElementById(`dashboardGammaChip-${key}`);
  const gammaValueByKey = (key) => document.getElementById(`dashboardGammaValue-${key}`);
  const gammaDetailByKey = (key) => document.getElementById(`dashboardGammaDetail-${key}`);

  const formatLevel = (value) => {
    const numeric = asNum(value);
    return numeric === null ? "--" : String(Math.round(numeric));
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
      return String(structure.gamma_regime_label || structure.gamma_regime || "UNAVAILABLE").toUpperCase();
    }
    if (key === "local_flip" && structure.local_flip === null && structure.local_flip_found === false) {
      return "No Local Flip between Put Wall and Call Wall";
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

  const applyGammaTone = (node, tone, glow) => {
    if (!node) return;
    node.classList.remove("is-positive", "is-negative", "is-info", "has-glow");
    if (tone === "positive") node.classList.add("is-positive");
    if (tone === "negative") node.classList.add("is-negative");
    if (tone === "info") node.classList.add("is-info");
    if (glow) node.classList.add("has-glow");
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
      const valueNode = gammaValueByKey(key);
      const detailNode = gammaDetailByKey(key);
      const tone = structureToneForKey(key, structure);
      const valueChanged = valueNode
        ? dashboardUIFX.setText(valueNode, structureValueForKey(key, structure), {
            pulse: true,
            tone: tone || "info",
          })
        : false;
      if (detailNode && key === "regime") {
        dashboardUIFX.setText(detailNode, String(
          structure.gamma_regime === "unconfirmed" || structure.gamma_regime === "unavailable"
            ? (structure.gamma_regime_reason_label || "")
            : ""
        ));
      }
      applyGammaTone(card, tone, structureGlowForKey(key, structure));
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
      updateRow(row, prices[symbol] || {}, seriesPoints[symbol] || []);
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
    const buildEndpoint = () => {
      if (!endpoint) return "";
      const url = new URL(endpoint, window.location.origin);
      const pageParams = new URLSearchParams(window.location.search);
      pageParams.forEach((value, key) => {
        if (!url.searchParams.has(key)) {
          url.searchParams.set(key, value);
        }
      });
      return url.toString();
    };
    const setTapeRefreshState = (loading) => {
      tapeRefreshBtn.disabled = !!loading;
      tapeRefreshBtn.classList.toggle("is-loading", !!loading);
      tapeRefreshBtn.setAttribute("aria-busy", loading ? "true" : "false");
    };
    const refreshTape = async () => {
      if (!endpoint || tapeRefreshBtn.disabled) return;
      setTapeRefreshState(true);
      if (typeof window.showDashboardLoading === "function") {
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
        applyTapeSnapshot(payload.quotes || {}, payload.updated_label || "");
        if (tapeHasRenderableValues()) {
          setStreamStatus("Live", payload.updated_label || "just now");
        }
      } catch (_error) {
        // Keep existing rows/status if the manual refresh fails.
      } finally {
        if (typeof window.completeDashboardLoading === "function") {
          window.completeDashboardLoading();
        }
        setTapeRefreshState(false);
      }
    };
    tapeRefreshBtn.addEventListener("click", () => {
      void refreshTape();
    });
    if (!tapeHasRenderableValues()) {
      window.setTimeout(() => {
        if (!tapeHasRenderableValues()) {
          void refreshTape();
        }
      }, 650);
    }
  }

  if (decisionCard && decisionRefreshBtn) {
    const endpoint = String(decisionCard.dataset.refreshEndpoint || "").trim();
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
      } catch (_error) {
        // Keep the current planning state visible if the manual refresh fails.
      } finally {
        if (showLoading && typeof window.completeDashboardLoading === "function") {
          window.completeDashboardLoading();
        }
        if (decisionLead && String(decisionLead.textContent || "").trim().length > 0) {
          setPlanningHydrationState(false);
        }
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
  const shell = document.getElementById("dashboardModeShell");
  if (!shell) return;

  const stateButtons = Array.from(document.querySelectorAll("[data-discipline-state]"));
  const modeButtons = Array.from(document.querySelectorAll("[data-discipline-mode]"));
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
      link.setAttribute("aria-disabled", blocked ? "true" : "false");
      link.classList.toggle("isDisabled", blocked);
      link.classList.toggle("is-guarded", blocked);
      link.title = blocked
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

  const applyUiState = () => {
    syncShellState();
    syncButtonGroup(stateButtons, "disciplineState", uiState.disciplineState);
    syncButtonGroup(modeButtons, "disciplineMode", uiState.disciplineMode);
    syncGateButtons();
    syncTradeActions();
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
