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
  const cards = Array.from(document.querySelectorAll(".dashboardCoreTapeStat[data-symbol]"));
  const statusNode = document.getElementById("dashboardTapeStreamStatus");
  const updatedNode = document.getElementById("dashboardTapeUpdatedAt");
  const gammaStrip = document.getElementById("dashboardGammaStrip");
  const gammaMeta = document.getElementById("dashboardGammaMeta");
  if (!cards.length || !statusNode || !updatedNode) return;
  let freshTimer = null;

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

  const inferAbsoluteChange = (price, pctChange) => {
    const p = asNum(price);
    const pct = asNum(pctChange);
    if (p === null || pct === null) return null;
    const prior = p / (1 + (pct / 100));
    if (!Number.isFinite(prior)) return null;
    return p - prior;
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

  let sparklineId = 0;

  const buildSmoothPath = (coords) => {
    if (!coords.length) return "";
    if (coords.length === 1) return `M ${coords[0][0].toFixed(2)} ${coords[0][1].toFixed(2)}`;
    let path = `M ${coords[0][0].toFixed(2)} ${coords[0][1].toFixed(2)}`;
    for (let index = 0; index < coords.length - 1; index += 1) {
      const current = coords[index];
      const next = coords[index + 1];
      const midpointX = (current[0] + next[0]) / 2;
      path += ` Q ${current[0].toFixed(2)} ${current[1].toFixed(2)} ${midpointX.toFixed(2)} ${((current[1] + next[1]) / 2).toFixed(2)}`;
    }
    const last = coords[coords.length - 1];
    path += ` T ${last[0].toFixed(2)} ${last[1].toFixed(2)}`;
    return path;
  };

  const buildSparkline = (points, tone, state, options = {}) => {
    const values = (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : null))
      .filter((value) => value !== null)
      .slice(-20);
    const sparkId = `spark-${sparklineId += 1}`;
    const toneColor = tone === "up" ? "#62efbf" : tone === "down" ? "#ff8e98" : "#b5c5d9";
    const profile = String(options.profile || "normal");
    const symbolClass = String(options.symbol || "").toLowerCase() === "spx" ? "symbol-spx" : "symbol-vix";
    const isLive = state === "Live";
    const isDelayed = state === "Delayed";
    const verticalBias = profile === "compressed" ? 0.58 : profile === "directional" ? 1.18 : 0.92;
    if (values.length < 2) {
      const coords = [[0, 14], [120, 14]];
      const path = buildSmoothPath(coords);
      return `<svg viewBox="0 0 120 28" class="marketMiniSpark ${symbolClass} is-${stateClass(state)}" aria-hidden="true">
        <defs>
          <linearGradient id="${sparkId}-stroke" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="${toneColor}" stop-opacity=".18" />
            <stop offset="100%" stop-color="${toneColor}" stop-opacity=".92" />
          </linearGradient>
          <linearGradient id="${sparkId}-fill" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stop-color="${toneColor}" stop-opacity=".10" />
            <stop offset="100%" stop-color="${toneColor}" stop-opacity="0" />
          </linearGradient>
        </defs>
        <path class="marketMiniSparkArea flat" d="${path} L 120 28 L 0 28 Z" fill="url(#${sparkId}-fill)" />
        <path class="marketMiniSparkLine flat" d="${path}" stroke="url(#${sparkId}-stroke)" />
        <circle class="marketMiniSparkEnd flat" cx="120" cy="14" r="2.1" />
      </svg>`;
    }
    const width = 120;
    const height = 28;
    let minV = Math.min(...values);
    let maxV = Math.max(...values);
    if (Math.abs(maxV - minV) < 1e-9) maxV = minV + 1;
    const pad = (maxV - minV) * 0.12;
    minV -= pad;
    maxV += pad;
    const step = width / Math.max(values.length - 1, 1);
    const coords = values.map((value, index) => {
      const x = index * step;
      const normalized = ((maxV - value) / (maxV - minV));
      const centered = ((normalized - 0.5) * verticalBias) + 0.5;
      const bounded = Math.max(0.06, Math.min(0.94, centered));
      const y = bounded * (height - 4) + 2;
      return [x, y];
    });
    const path = buildSmoothPath(coords);
    const lineEnd = coords[coords.length - 1];
    const areaPath = `${path} L 120 28 L 0 28 Z`;
    const gradientStartOpacity = isLive ? ".22" : isDelayed ? ".12" : ".10";
    const gradientEndOpacity = isLive ? ".98" : isDelayed ? ".64" : ".52";
    const fillOpacity = isLive ? ".14" : isDelayed ? ".08" : ".06";
    return `<svg viewBox="0 0 120 28" class="marketMiniSpark ${symbolClass} is-${stateClass(state)}" aria-hidden="true">
      <defs>
        <linearGradient id="${sparkId}-stroke" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="${toneColor}" stop-opacity="${gradientStartOpacity}" />
          <stop offset="65%" stop-color="${toneColor}" stop-opacity="${Math.max(0.35, Number(gradientEndOpacity) - 0.22)}" />
          <stop offset="100%" stop-color="${toneColor}" stop-opacity="${gradientEndOpacity}" />
        </linearGradient>
        <linearGradient id="${sparkId}-fill" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stop-color="${toneColor}" stop-opacity="${fillOpacity}" />
          <stop offset="100%" stop-color="${toneColor}" stop-opacity="0" />
        </linearGradient>
      </defs>
      <path class="marketMiniSparkArea ${tone}" d="${areaPath}" fill="url(#${sparkId}-fill)" />
      <path class="marketMiniSparkLine ${tone}" d="${path}" stroke="url(#${sparkId}-stroke)" />
      <circle class="marketMiniSparkEnd ${tone}" cx="${lineEnd[0].toFixed(2)}" cy="${lineEnd[1].toFixed(2)}" r="2.1" />
    </svg>`;
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

  const updateCard = (card, quote, points) => {
    const price = asNum((quote || {}).price);
    const pct = asNum((quote || {}).pct_change);
    const tone = pct === null ? "flat" : pct > 0 ? "up" : pct < 0 ? "down" : "flat";
    const state = deriveState(quote);
    const freshnessNode = card.querySelector('[data-role="freshness"]');
    const priceNode = card.querySelector('[data-role="price"]');
    const changeNode = card.querySelector('[data-role="change-line"]');
    const sparkNode = card.querySelector('[data-role="sparkline"]');
    const contextPrimaryNode = card.querySelector('[data-role="context-primary"]');
    const contextSecondaryNode = card.querySelector('[data-role="context-secondary"]');
    const infoPrimaryNode = card.querySelector('[data-role="info-primary"]');
    const asOf = String((quote || {}).as_of || "").trim();
    const freshness = formatFreshness(asOf, state);
    const values = (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : null))
      .filter((value) => value !== null);
    const low = values.length ? Math.min(...values) : null;
    const high = values.length ? Math.max(...values) : null;
    const range = low !== null && high !== null ? high - low : null;
    const pctOfRange = (price !== null && low !== null && high !== null && range && range > 0)
      ? (price - low) / range
      : null;
    const formatRange = (lo, hi) => {
      if (lo === null || hi === null) return "Range —";
      return `Range ${lo.toFixed(2)}–${hi.toFixed(2)}`;
    };
    const parseLevel = (value) => {
      if (value === null || value === undefined) return null;
      const cleaned = String(value).replace(/[^0-9.-]/g, "");
      return asNum(cleaned);
    };
    const mainFlip = parseLevel(document.getElementById("dashboardGammaValue-main_flip")?.textContent || "");
    const arrow = pct === null ? "→" : pct > 0 ? "↑" : pct < 0 ? "↓" : "→";
    const changeClass = pct === null ? "is-flat" : pct > 0 ? "is-up" : pct < 0 ? "is-down" : "is-flat";
    const symbol = String(card.dataset.symbol || "").toUpperCase();
    const positionLabel = (() => {
      if (pctOfRange === null) return "Inside Range";
      if (pctOfRange >= 0.8) return "Near High";
      if (pctOfRange <= 0.2) return "Near Low";
      return "Mid";
    })();
    const spxContextPrimary = (() => {
      if (price !== null && mainFlip !== null) {
        const diff = price - mainFlip;
        if (Math.abs(diff) <= 2) return "At Flip";
        return `${diff > 0 ? "Above" : "Below"} Flip ${diff > 0 ? "+" : ""}${diff.toFixed(0)} pts`;
      }
      if (pctOfRange >= 0.8) return "Near Session High";
      if (pctOfRange <= 0.2) return "Near Session Low";
      return "Inside Range";
    })();
    const spxContextSecondary = (() => {
      if (state !== "Live") return "Last session";
      if (price !== null && mainFlip !== null && pct !== null) {
        if (Math.abs(pct) < 0.05) return "Balanced tape";
        if (price >= mainFlip && pct > 0) return "Acceptance";
        if (price >= mainFlip && pct < 0) return "Rejection";
        if (price < mainFlip && pct < 0) return "Weak structure";
        if (price < mainFlip && pct > 0) return "Rejection";
      }
      if (pct !== null && Math.abs(pct) >= 0.3) return "Expansion building";
      return "Balanced tape";
    })();
    const vixContextPrimary = (() => {
      if (pct !== null) {
        if (pct <= -1) return "Falling";
        if (pct < 0) return "Compressed";
        if (pct >= 1) return "Firming";
        if (pct > 0) return "Elevated";
      }
      if (pctOfRange !== null && pctOfRange >= 0.75) return "Elevated";
      return "Compressed";
    })();
    const vixContextSecondary = (() => {
      if (state !== "Live") return "Last session";
      if (pct !== null) {
        if (pct <= -1) return "Risk easing";
        if (pct < 0) return "Stable";
        if (pct >= 1) return "Expansion";
        if (pct > 0) return "Risk firming";
      }
      return "Stable";
    })();
    if (freshnessNode) {
      freshnessNode.textContent = freshness.compact;
      freshnessNode.setAttribute("title", freshness.full);
    }
    if (priceNode) {
      priceNode.textContent = price === null ? "—" : price.toFixed(2);
    }
    if (changeNode) {
      changeNode.innerHTML = `
        <span class="dashboardTapeDeltaValue ${changeClass}">${formatSigned(inferAbsoluteChange(price, pct), 2)}</span>
        <span class="dashboardTapeDeltaPct ${changeClass}">${formatSigned(pct, 2)}%</span>
        <span class="dashboardTapeDeltaArrow ${changeClass}">${arrow}</span>
      `;
    }
    if (sparkNode) {
      const profile = symbol === "VIX"
        ? (state !== "Live" ? "compressed" : (vixContextPrimary === "Compressed" || vixContextSecondary === "Stable" ? "compressed" : (vixContextPrimary === "Firming" || vixContextSecondary === "Expansion" ? "directional" : "normal")))
        : (state !== "Live" ? "compressed" : (spxContextPrimary === "At Flip" || spxContextSecondary === "Balanced tape" ? "compressed" : (spxContextSecondary === "Acceptance" || spxContextSecondary === "Expansion building" || spxContextSecondary === "Weak structure" ? "directional" : "normal")));
      sparkNode.innerHTML = buildSparkline(points, tone, state, { profile, symbol });
    }
    if (contextPrimaryNode) {
      contextPrimaryNode.textContent = symbol === "VIX" ? vixContextPrimary : spxContextPrimary;
    }
    if (contextSecondaryNode) {
      contextSecondaryNode.textContent = symbol === "VIX" ? vixContextSecondary : spxContextSecondary;
    }
    if (infoPrimaryNode) {
      if (symbol === "VIX") {
        infoPrimaryNode.textContent = low !== null && high !== null
          ? `${formatRange(low, high)}`
          : `Intraday tone: ${vixContextSecondary.toLowerCase()}`;
      } else if (mainFlip !== null && price !== null) {
        const diff = price - mainFlip;
        infoPrimaryNode.textContent = `Dist to Flip: ${diff > 0 ? "+" : ""}${diff.toFixed(0)} pts`;
      } else if (low !== null && high !== null) {
        infoPrimaryNode.textContent = `${formatRange(low, high)} • ${positionLabel}`;
      } else {
        infoPrimaryNode.textContent = positionLabel;
      }
    }
    card.classList.toggle("glow-green", (pct || 0) > 0);
    card.classList.toggle("glow-red", (pct || 0) < 0);
    card.classList.remove("is-live", "is-delayed", "is-missing");
    card.classList.add(`is-${stateClass(state)}`);
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
      cards.forEach((card) => {
        const symbol = String(card.dataset.symbol || "").toUpperCase();
        updateCard(card, prices[symbol] || {}, seriesPoints[symbol] || []);
      });
      const liveCards = cards.filter((card) => card.classList.contains("is-live")).length;
      const delayedCards = cards.filter((card) => card.classList.contains("is-delayed")).length;
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
