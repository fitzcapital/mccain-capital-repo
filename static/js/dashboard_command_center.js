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
  const decisionBiasValue = document.getElementById("dashboardDecisionBiasValue");
  const decisionRiskValue = document.getElementById("dashboardDecisionRiskValue");
  const decisionPlanValue = document.getElementById("dashboardDecisionPlanValue");
  const decisionTradeGateValue = document.getElementById("dashboardDecisionTradeGateValue");
  const briefCardShell = document.getElementById("dashboardBriefCardShell");
  if (!rows.length || !statusNode || !updatedNode) return;

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

  const inferPreviousClose = (quote) => {
    const explicit = asNum((quote || {}).prev_close ?? (quote || {}).prior_close ?? (quote || {}).previous_close);
    if (explicit !== null) return explicit;
    const price = asNum((quote || {}).price);
    const pct = asNum((quote || {}).pct_change);
    if (price === null || pct === null || Math.abs(100.0 + pct) < 1e-9) return null;
    const prior = price / (1 + (pct / 100.0));
    return Number.isFinite(prior) ? prior : null;
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

  const formatFreshness = (iso, state) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) return { full: `${state} · unavailable`, compact: state };
    const ageS = Math.max(0, (Date.now() - ts) / 1000);
    const compact = ageS >= 60 ? `${Math.floor(ageS / 60)}m` : `${Math.floor(ageS)}s`;
    return {
      full: `${state} · ${ageS.toFixed(1)}s old`,
      compact,
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
    const pct = asNum((quote || {}).pct_change);
    const absChange = inferAbsoluteChange(price, pct);
    const state = deriveState(quote);
    const lastNode = row.querySelector('[data-role="last"]');
    const chgpNode = row.querySelector('[data-role="chgp"]');
    const detailChgNode = detailNode?.querySelector('[data-role="detail-chg"]');
    const detailOpenNode = detailNode?.querySelector('[data-role="detail-open"]');
    const detailPrevNode = detailNode?.querySelector('[data-role="detail-prev"]');
    const detailLiveNode = detailNode?.querySelector('[data-role="detail-live"]');
    const openValue = asNum((quote || {}).day_open ?? (quote || {}).open);
    const prevCloseValue = inferPreviousClose(quote);
    const freshness = formatFreshness((quote || {}).as_of, state);
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
        lastNode.textContent = formatValue(price, 2);
      } else if (!hasMeaningfulText(lastNode)) {
        lastNode.textContent = "loading...";
      }
    }
    if (chgpNode) {
      if (pct !== null) {
        chgpNode.textContent = `${formatSigned(pct, 2)}%`;
      } else if (!hasMeaningfulText(chgpNode)) {
        chgpNode.textContent = "loading...";
      }
    }

    row.classList.remove("is-up", "is-down", "is-flat", "is-live", "is-delayed", "is-missing");
    row.classList.add(`is-${stateClass(state)}`);
    row.classList.add(pct > 0 ? "is-up" : pct < 0 ? "is-down" : "is-flat");

    if (detailChgNode) {
      if (absChange !== null) {
        detailChgNode.textContent = formatSigned(absChange, 2);
      } else if (!hasMeaningfulText(detailChgNode)) {
        detailChgNode.textContent = "loading...";
      }
    }
    if (detailOpenNode && openValue !== null) detailOpenNode.textContent = formatValue(openValue, 2);
    if (detailPrevNode && prevCloseValue !== null) detailPrevNode.textContent = formatValue(prevCloseValue, 2);
    if (detailLiveNode && String((quote || {}).as_of || "").trim()) detailLiveNode.textContent = freshness.compact;
  };

  const setStreamStatus = (label, detail) => {
    statusNode.textContent = label;
    statusNode.dataset.tone = label.toLowerCase().includes("retry")
      ? "delayed"
      : label.toLowerCase().includes("connect")
      ? "off"
      : "live";
    updatedNode.textContent = detail;
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

  const updateDecisionPanel = (panel) => {
    if (!panel || typeof panel !== "object") return;
    if (decisionBiasValue) decisionBiasValue.textContent = String(panel.bias || "Unavailable");
    if (decisionRiskValue) decisionRiskValue.textContent = String(panel.risk_size || "Unavailable");
    if (decisionPlanValue) {
      const fullPlan = String(panel.plan || "Wait");
      decisionPlanValue.textContent = truncateText(fullPlan, 88);
      decisionPlanValue.title = fullPlan;
    }
    if (decisionTradeGateValue) {
      const fullGate = String(panel.trade_gate || "Wait");
      decisionTradeGateValue.textContent = truncateText(fullGate, 74);
      decisionTradeGateValue.title = fullGate;
    }
    if (decisionStatusChip) {
      const tone = String(panel.status_tone || "").toLowerCase();
      decisionStatusChip.textContent = String(panel.status || "Unavailable");
      decisionStatusChip.classList.remove("positive", "negative", "warning", "info");
      if (["positive", "negative", "warning", "info"].includes(tone)) {
        decisionStatusChip.classList.add(tone);
      }
    }
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
      gammaMeta.textContent = String(
        payload.status_text
          || structure.gamma_regime_reason_label
          || structure.secondary_structure_degraded_reason
          || "Gamma context unavailable"
      );
      gammaMeta.classList.remove("is-stale", "is-loading", "is-unavailable");
      if (state === "stale" || state === "loading" || state === "unavailable") {
        gammaMeta.classList.add(`is-${state}`);
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
      if (valueNode) valueNode.textContent = structureValueForKey(key, structure);
      if (detailNode && key === "regime") {
        detailNode.textContent = String(
          structure.gamma_regime === "unconfirmed" || structure.gamma_regime === "unavailable"
            ? (structure.gamma_regime_reason_label || "")
            : ""
        );
      }
      applyGammaTone(card, structureToneForKey(key, structure), structureGlowForKey(key, structure));
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
    stream = new EventSource("/stream/market");
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
        const response = await fetch(endpoint, {
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
