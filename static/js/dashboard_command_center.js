(function () {
  const preview = document.getElementById("calendarPreview");
  const title = document.getElementById("calendarPreviewTitle");
  const status = document.getElementById("calendarPreviewStatus");
  const net = document.getElementById("previewNet");
  const trades = document.getElementById("previewTrades");
  const record = document.getElementById("previewRecord");
  const balance = document.getElementById("previewBalance");
  const open = document.getElementById("calendarPreviewOpen");
  const close = document.getElementById("calendarPreviewClose");
  if (!preview || !title || !status || !net || !trades || !record || !balance || !open || !close) return;

  const buttons = Array.from(document.querySelectorAll(".dayPreviewButton"));
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
  const gammaStrip = document.getElementById("dashboardGammaStrip");
  const gammaMeta = document.getElementById("dashboardGammaMeta");
  if (!rows.length || !statusNode || !updatedNode) return;

  let freshTimer = null;
  let openSymbol = "";

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

  const gammaCardByKey = (key) => document.getElementById(`dashboardGammaChip-${key}`);
  const gammaValueByKey = (key) => document.getElementById(`dashboardGammaValue-${key}`);

  const applyGammaTone = (node, tone, glow) => {
    if (!node) return;
    node.classList.remove("is-positive", "is-negative", "is-info", "has-glow");
    if (tone === "positive") node.classList.add("is-positive");
    if (tone === "negative") node.classList.add("is-negative");
    if (tone === "info") node.classList.add("is-info");
    if (glow) node.classList.add("has-glow");
  };

  const updateGammaStrip = (payload) => {
    if (!gammaStrip || !payload || typeof payload !== "object") return;
    const state = String(payload.state || "live");
    gammaStrip.dataset.gammaState = state;
    gammaStrip.classList.remove("is-live", "is-stale", "is-loading", "is-unavailable");
    gammaStrip.classList.add(`is-${state}`);
    if (gammaMeta) {
      gammaMeta.textContent = String(payload.status_text || "Gamma context unavailable");
      gammaMeta.classList.remove("is-stale", "is-loading", "is-unavailable");
      if (state === "stale" || state === "loading" || state === "unavailable") {
        gammaMeta.classList.add(`is-${state}`);
      }
    }
    const entries = Array.isArray(payload.entries) ? payload.entries : [];
    entries.forEach((entry) => {
      const key = String(entry && entry.key || "");
      if (!key) return;
      const card = gammaCardByKey(key);
      const valueNode = gammaValueByKey(key);
      if (valueNode) valueNode.textContent = String(entry.value || "--");
      applyGammaTone(card, String(entry.tone || ""), Boolean(entry.glow));
    });
  };

  const connect = () => {
    const stream = new EventSource("/stream/market");
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
      const prices = payload && typeof payload === "object" ? (payload.prices || {}) : {};
      const seriesPoints = payload && typeof payload === "object" ? (payload.series_points || {}) : {};
      updateGammaStrip(payload && typeof payload === "object" ? payload.dashboard_gamma : null);
      rows.forEach((row) => {
        const symbol = String(row.dataset.watchSymbol || "").toUpperCase();
        updateRow(row, prices[symbol] || {}, seriesPoints[symbol] || []);
      });
      const liveCards = rows.filter((row) => row.classList.contains("is-live")).length;
      const delayedCards = rows.filter((row) => row.classList.contains("is-delayed")).length;
      document.dispatchEvent(new CustomEvent("dashboard:tape-state", {
        detail: {
          hasLive: liveCards > 0,
          hasDelayed: delayedCards > 0,
        },
      }));
      setStreamStatus("Live", formatClock(payload.updated_at || payload.server_ts || new Date().toISOString()));
      updatedNode.classList.remove("is-fresh");
      window.requestAnimationFrame(() => updatedNode.classList.add("is-fresh"));
      if (freshTimer) window.clearTimeout(freshTimer);
      freshTimer = window.setTimeout(() => {
        updatedNode.classList.remove("is-fresh");
      }, 1400);
    };
    stream.onerror = () => {
      try {
        stream.close();
      } catch (_err) {
        // Ignore stream close failures.
      }
      setStreamStatus("Retrying", "reconnecting…");
      window.setTimeout(connect, 3000);
    };
  };

  setStreamStatus("Connecting", "waiting for first tick…");
  connect();
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
