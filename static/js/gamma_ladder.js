(function () {
  const root = document.querySelector("[data-gamma-ladder]");
  if (!root) return;

  const defaultSymbol = String(root.dataset.defaultSymbol || "SPX").toUpperCase();
  const defaultWindowPreset = String(root.dataset.defaultWindow || "standard").toLowerCase();
  const apiUrl = String(root.dataset.apiUrl || "/api/gamma-ladder");
  const pills = Array.from(root.querySelectorAll("[data-gamma-symbol-pill]"));
  const windowPills = Array.from(root.querySelectorAll("[data-gamma-window-pill]"));
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
  const summaryNode = root.querySelector("[data-gamma-summary]");
  const legendItems = Array.from(root.querySelectorAll("[data-gamma-legend]"));

  let currentSymbol = defaultSymbol;
  let currentWindowPreset = defaultWindowPreset;
  let refreshTimer = null;
  let switchTimer = null;
  let controller = null;
  let requestSequence = 0;
  let inFlightSymbol = "";
  let hasLoadedData = false;
  let lastSummaryText = "Loading focused window…";

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

  const applyTheme = (symbol, regime) => {
    root.dataset.symbol = symbol;
    root.dataset.regime = regime;
    root.classList.remove(symbolThemeClass("spx"), symbolThemeClass("spy"), symbolThemeClass("qqq"));
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
  };

  const renderError = (message) => {
    if (errorNode) {
      errorNode.hidden = false;
      errorNode.textContent = message || "Gamma ladder unavailable.";
    }
    if (board) board.hidden = !hasLoadedData;
    if (loading) loading.hidden = true;
    setVisualState("is-error");
    if (summaryNode) {
      summaryNode.textContent = hasLoadedData
        ? "Refresh failed. Showing the last successful gamma ladder."
        : "Gamma ladder unavailable.";
    }
  };

  const setActiveSymbol = (symbol) => {
    currentSymbol = String(symbol || defaultSymbol).toUpperCase();
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
    if (headerExpiration) headerExpiration.textContent = payload.expiration_label || payload.expiration || "—";
    if (headerUpdated) {
      headerUpdated.textContent = payload.updated_label ? `Updated ${payload.updated_label}` : "Updated —";
    }
    if (summaryNode) {
      const visible = Number(payload.rows_visible) || 0;
      const total = Number(payload.rows_total) || visible;
      const windowPreset = titleCase(payload.window_preset || currentWindowPreset);
      lastSummaryText =
        total > visible
          ? `Showing ${visible} of ${total} strikes near spot · ${windowPreset} window.`
          : `Showing ${visible} focused strike${visible === 1 ? "" : "s"} · ${windowPreset} window.`;
      summaryNode.textContent = lastSummaryText;
    }
    setActiveWindowPreset(payload.window_preset || currentWindowPreset);
    applyTheme(symbol, regime);
  };

  const renderGammaLadderRows = (payload) => {
    if (!rowsHost) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!rows.length) {
      rowsHost.innerHTML = '<div class="gamma-ladder-emptyState">No ladder rows available.</div>';
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

    rowsHost.innerHTML = rows
      .map((row, index) => {
        const strike = Number(row.strike);
        const spot = Number(payload.spot) || 0;
        const callGex = Number(row.call_gex) || 0;
        const putGex = Math.abs(Number(row.put_gex) || 0);
        const netGex = Number(row.net_gex) || 0;
        const { strengthRatio, gammaState } = rowStateMeta[index];
        const zoneMeta = zoneMetaByIndex.get(index);
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
          strengthRatio >= 0.82 || row.is_strongest
            ? " is-dominant"
            : strengthRatio >= 0.52
              ? " is-strong"
              : strengthRatio >= 0.24
                ? " is-medium"
                : " is-light";
        const callWidth = Math.max(0, Math.min(46, (Math.abs(callGex) / maxSide) * 46));
        const putWidth = Math.max(0, Math.min(46, (putGex / maxSide) * 46));
        const netRatio = Math.max(-1, Math.min(1, netGex / maxSide));
        const netX = 50 + netRatio * 44;
        const netBarWidth = Math.max(2, Math.min(44, (Math.abs(netGex) / maxSide) * 44));
        const netBarX = netGex >= 0 ? 50 : 50 - netBarWidth;
        const flipClass = row.is_flip ? " is-flip" : "";
        const strongestClass = row.is_strongest ? " is-strongest" : "";
        const spotClass = row.is_spot_nearest ? " is-spot" : "";
        const sideClass = row.is_above_spot ? " is-above-spot" : row.is_below_spot ? " is-below-spot" : " is-at-spot";
        const zoneClass = zoneMeta
          ? ` is-${zoneMeta.type}-zone is-zone-${zoneMeta.position}${zoneMeta.isFocal ? " is-zone-focal" : ""}`
          : "";
        const stateClass = ` gamma-${gammaState}`;
        const strikeDigits = strike >= 1000 ? 0 : 2;
        const symbol = String(payload.symbol || currentSymbol).toLowerCase();
        const distanceDigits = symbol === "spx" ? 2 : 2;
        const distanceFromSpot = strike - spot;
        const distanceLabel = `${distanceFromSpot >= 0 ? "+" : ""}${formatNumber(distanceFromSpot, distanceDigits)}`;
        const [baseRed, baseGreen, baseBlue] = colorSet.base;
        const [accentRed, accentGreen, accentBlue] = colorSet.accent;
        const putStart = rgba(baseRed, baseGreen, baseBlue, 0.08 + strengthRatio * 0.08);
        const putEnd = rgba(baseRed, baseGreen, baseBlue, 0.22 + strengthRatio * 0.16);
        const callStart = rgba(baseRed, baseGreen, baseBlue, 0.12 + strengthRatio * 0.10);
        const callEnd = rgba(baseRed, baseGreen, baseBlue, 0.32 + strengthRatio * 0.26);
        const netStart = rgba(baseRed, baseGreen, baseBlue, 0.42 + strengthRatio * 0.14);
        const netEnd = rgba(accentRed, accentGreen, accentBlue, 0.92 - strengthRatio * 0.04);
        const overlayFill = row.is_strongest
          ? ' fill="rgba(255, 209, 102, 0.22)"'
          : "";
        const behavior =
          row.is_strongest
            ? "Dominant dealer node. Price can gravitate toward this level while positioning remains concentrated."
            : row.is_spot_nearest
              ? "Current market location. Use this row to judge whether price is entering compression or expansion territory."
              : zoneMeta?.type === "compression"
                ? "Positive gamma cluster. Volatility suppression is elevated and price may behave stickier inside this corridor."
                : zoneMeta?.type === "expansion"
                  ? "Negative gamma cluster. Expansion risk is elevated and movement can accelerate through this corridor."
                  : gammaBehavior[gammaState] || gammaBehavior.neutral;
        const tags = [
          row.is_flip ? '<span class="gamma-ladder-tag gamma-ladder-tag--flip">Transition Area</span>' : "",
          row.is_spot_nearest ? '<span class="gamma-ladder-tag gamma-ladder-tag--spot">Current Market</span>' : "",
          row.is_strongest ? '<span class="gamma-ladder-tag gamma-ladder-tag--strongest">Dealer Magnet</span>' : "",
          !row.is_strongest && !row.is_spot_nearest && !row.is_flip && zoneMeta?.isFocal
            ? `<span class="gamma-ladder-tag gamma-ladder-tag--${zoneMeta.type}">${
                zoneMeta.type === "compression" ? "Compression Zone" : "Expansion Risk"
              }</span>`
            : "",
        ]
          .filter(Boolean)
          .join("");
        const tooltipPayload = escapeHtml(
          JSON.stringify({
            strike: formatNumber(strike, strikeDigits),
            distance: distanceLabel,
            call: formatCompact(callGex),
            put: formatCompact(-putGex),
            net: formatCompact(netGex),
            behavior,
          })
        );
        return `
          <button
            class="gamma-ladder-row${flipClass}${strongestClass}${spotClass}${sideClass}${intensityClass}${stateClass}${zoneClass}"
            type="button"
            data-gamma-row
            data-tooltip="${tooltipPayload}"
            aria-label="Strike ${formatNumber(strike, strikeDigits)} net gamma ${formatCompact(netGex)}"
          >
            <span class="gamma-ladder-row__strikeWrap">
              <span class="gamma-ladder-row__strike">${formatNumber(strike, strikeDigits)}</span>
              <span class="gamma-ladder-row__tone">${row.is_above_spot ? "Above spot" : row.is_below_spot ? "Below spot" : "At spot"}</span>
            </span>
            <span class="gamma-ladder-row__viz">
              <svg viewBox="0 0 100 20" preserveAspectRatio="none" aria-hidden="true">
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
                <line x1="50" y1="1" x2="50" y2="19" class="gamma-ladder-row__axis"></line>
                <rect x="${50 - putWidth}" y="7" width="${putWidth}" height="6" rx="3" fill="url(#gamma-put-${symbol}-${index})" class="gamma-ladder-row__putBar"></rect>
                <rect x="50" y="7" width="${callWidth}" height="6" rx="3" fill="url(#gamma-call-${symbol}-${index})" class="gamma-ladder-row__callBar"></rect>
                <rect x="${netBarX}" y="5" width="${netBarWidth}" height="10" rx="4" fill="url(#gamma-net-${symbol}-${index})" class="gamma-ladder-row__netBar"></rect>
                ${row.is_strongest ? `<rect x="${netBarX}" y="4" width="${netBarWidth}" height="12" rx="5"${overlayFill} class="gamma-ladder-row__strongestOverlay"></rect>` : ""}
                <circle cx="${netX}" cy="10" r="${row.is_strongest ? 3 : 2.4}" class="gamma-ladder-row__netDot"></circle>
              </svg>
              ${row.is_flip ? '<span class="gamma-ladder-row__flipLine" aria-hidden="true"></span>' : ""}
              ${row.is_spot_nearest ? '<span class="gamma-ladder-row__spotMarker" aria-hidden="true"></span>' : ""}
            </span>
            <span class="gamma-ladder-row__netWrap">
              <span class="gamma-ladder-row__net">${formatCompact(netGex)}</span>
              <span class="gamma-ladder-row__micro">${tags}</span>
            </span>
          </button>
        `;
      })
      .join("");
    root.style.setProperty(
      "--gamma-ladder-visible-rows",
      String(Math.max(1, Math.min(rows.length, 10)))
    );
    hasLoadedData = true;
    setVisualState("");
    if (loading) loading.hidden = true;
    if (board) board.hidden = false;
  };

  const hideTooltip = () => {
    if (!tooltip) return;
    tooltip.hidden = true;
    tooltip.textContent = "";
  };

  const showTooltip = (event) => {
    if (!tooltip) return;
    const row = event.target.closest("[data-gamma-row]");
    const legendItem = event.target.closest("[data-gamma-legend]");
    if (!row && !legendItem) return;
    let payload = null;
    try {
      payload = JSON.parse((row || legendItem).dataset.tooltip || "{}");
    } catch (_err) {
      payload = null;
    }
    if (!payload) return;
    if (row) {
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
    const rect = root.getBoundingClientRect();
    tooltip.hidden = false;
    tooltip.style.left = `${event.clientX - rect.left + 16}px`;
    tooltip.style.top = `${event.clientY - rect.top + 16}px`;
  };

  const refreshLoop = () => {
    if (refreshTimer) window.clearInterval(refreshTimer);
    refreshTimer = window.setInterval(() => {
      fetchGammaLadder(currentSymbol, { force: true });
    }, 60000);
  };

  const fetchGammaLadder = async (symbol, { force = false } = {}) => {
    const normalized = String(symbol || defaultSymbol).toUpperCase();
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

  pills.forEach((pill) => {
    pill.addEventListener("click", () => {
      const nextSymbol = String(pill.dataset.gammaSymbolPill || "").toUpperCase();
      if (!nextSymbol || nextSymbol === currentSymbol) return;
      window.clearTimeout(switchTimer);
      switchTimer = window.setTimeout(() => {
        setActiveSymbol(nextSymbol);
        fetchGammaLadder(nextSymbol);
      }, 120);
    });
  });

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

  root.addEventListener("mousemove", showTooltip);
  root.addEventListener("mouseleave", hideTooltip);
  root.addEventListener("focusout", (event) => {
    if (!root.contains(event.relatedTarget)) hideTooltip();
  });

  setActiveSymbol(defaultSymbol);
  setActiveWindowPreset(defaultWindowPreset);
  fetchGammaLadder(defaultSymbol);
})();
