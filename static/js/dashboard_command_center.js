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

  const buildSparkline = (points, tone, state) => {
    const values = (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : null))
      .filter((value) => value !== null);
    if (values.length < 2) {
      const emptyLabel = state === "Live"
        ? "Awaiting ticks"
        : state === "Delayed"
        ? "Snapshot only"
        : "Feed standby";
      return `<div class="marketMiniSparkEmpty is-${stateClass(state)}">${emptyLabel}</div>`;
    }
    const width = 120;
    const height = 28;
    let minV = Math.min(...values);
    let maxV = Math.max(...values);
    if (Math.abs(maxV - minV) < 1e-9) maxV = minV + 1;
    const step = width / Math.max(values.length - 1, 1);
    const coords = values.map((value, index) => {
      const x = index * step;
      const y = ((maxV - value) / (maxV - minV)) * (height - 2) + 1;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });
    return `<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true"><polyline class="marketMiniSparkLine ${tone}" points="${coords.join(" ")}" /></svg>`;
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

  const sourceBadgeLabel = (quote) => {
    const provider = String((quote || {}).provider || "").toLowerCase();
    const reason = String((quote || {}).reason || (quote || {}).data_reason || "").toLowerCase();
    if (provider === "tradier" && reason.startsWith("tradier_stream_")) return "Tradier Stream";
    if (provider === "tradier" && reason.startsWith("tradier_live")) return "Tradier Live Quote";
    if (provider === "tradier" && reason.startsWith("tradier_close")) return "Tradier Close";
    if (provider === "massive") return "Fallback Snapshot";
    if (provider === "yfinance") return "Yahoo Fallback";
    if (provider) return `${provider[0].toUpperCase()}${provider.slice(1)} Fallback`;
    return "Feed unavailable";
  };

  const updateCard = (card, quote, points) => {
    const price = asNum((quote || {}).price);
    const pct = asNum((quote || {}).pct_change);
    const tone = pct === null ? "flat" : pct > 0 ? "up" : pct < 0 ? "down" : "flat";
    const state = deriveState(quote);
    const stateChip = card.querySelector('[data-role="state-chip"]');
    const freshnessNode = card.querySelector('[data-role="freshness"]');
    const priceNode = card.querySelector('[data-role="price"]');
    const changeNode = card.querySelector('[data-role="change-line"]');
    const sparkNode = card.querySelector('[data-role="sparkline"]');
    const sourceNode = card.querySelector('[data-role="source-badge"]');
    const gapLine = card.querySelector('[data-role="gap-line"]');
    const gapChip = card.querySelector('[data-role="gap-chip"]');
    const rangeNode = card.querySelector('[data-role="range-line"]');
    const asOf = String((quote || {}).as_of || "").trim();
    const freshness = formatFreshness(asOf, state);

    if (stateChip) {
      stateChip.textContent = state;
      stateChip.classList.remove("state-live", "state-delayed", "state-missing");
      stateChip.classList.add(`state-${stateClass(state)}`);
    }
    if (freshnessNode) {
      freshnessNode.textContent = freshness.compact;
      freshnessNode.setAttribute("title", freshness.full);
    }
    if (priceNode) {
      priceNode.textContent = price === null ? "—" : price.toFixed(2);
    }
    if (changeNode) {
      changeNode.textContent = `${formatSigned(inferAbsoluteChange(price, pct), 2)} · ${formatSigned(pct, 2)}%`;
    }
    if (sourceNode) {
      sourceNode.textContent = sourceBadgeLabel(quote);
    }
    if (sparkNode) {
      sparkNode.innerHTML = buildSparkline(points, tone, state);
    }
    if (rangeNode) {
      const values = (Array.isArray(points) ? points : [])
        .map((row) => (row && typeof row === "object" ? asNum(row.v) : null))
        .filter((value) => value !== null);
      if (!values.length) {
        rangeNode.textContent = "—";
      } else {
        const low = Math.min(...values);
        const high = Math.max(...values);
        rangeNode.textContent = Math.abs(high - low) < 0.01 ? high.toFixed(2) : `${low.toFixed(2)}-${high.toFixed(2)}`;
      }
    }

    const dayOpen = Array.isArray(points) && points.length ? asNum(points[0].v) : null;
    const prevClose = price !== null && pct !== null ? price / (1 + (pct / 100)) : null;
    const gap = dayOpen !== null && prevClose !== null ? dayOpen - prevClose : null;
    const gapPct = gap !== null && prevClose ? (gap / prevClose) * 100 : null;
    const gapText = gap === null || gapPct === null ? "—" : `${formatSigned(gap, 2)} (${formatSigned(gapPct, 2)}%)`;
    if (gapChip) {
      gapChip.textContent = gapText;
      gapChip.classList.remove("positive", "negative");
      if ((gap || 0) > 0) gapChip.classList.add("positive");
      if ((gap || 0) < 0) gapChip.classList.add("negative");
    }
    if (gapLine) {
      gapLine.classList.remove("is-positive", "is-negative");
      if ((gap || 0) > 0) gapLine.classList.add("is-positive");
      if ((gap || 0) < 0) gapLine.classList.add("is-negative");
      gapLine.setAttribute("title", `Gap O/N: ${gapText}`);
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
