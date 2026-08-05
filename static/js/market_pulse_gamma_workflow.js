(function (rootScope, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (rootScope) rootScope.MarketPulseGammaWorkflow = api;
  if (rootScope && rootScope.document) api.init(rootScope.document);
})(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  const LEVEL_KEYS = new Set(["main_flip", "local_flip", "call_wall", "put_wall"]);
  const EVENT_LEVELS_UPDATED = "market-pulse:levels-updated";
  const EVENT_LEVEL_SELECTED = "market-pulse:gamma-level-selected";
  const MAX_EVENT_AGE_MS = 5 * 60 * 1000;
  const PLAYBOOK_PIN_STORAGE_KEY = "mccain.marketPulse.playbookPinned.v1";

  const normalizePinPreference = (value) => value === true || value === "true" || value === "1";

  const initPlaybookPin = (documentRoot) => {
    const toggle = documentRoot.querySelector("[data-playbook-pin-toggle]");
    const body = documentRoot.body;
    if (!toggle || !body) return null;
    const label = toggle.querySelector("[data-playbook-pin-label]");
    const storage = documentRoot.defaultView?.localStorage;
    let pinned = false;
    try {
      pinned = normalizePinPreference(storage?.getItem(PLAYBOOK_PIN_STORAGE_KEY));
    } catch (_error) {
      pinned = false;
    }
    const render = () => {
      body.classList.toggle("is-playbook-pinned", pinned);
      toggle.setAttribute("aria-pressed", pinned ? "true" : "false");
      toggle.title = pinned
        ? "Let the Playbook header scroll with the page"
        : "Keep the Playbook header visible while scrolling";
      if (label) label.textContent = pinned ? "Unpin header" : "Pin header";
    };
    toggle.addEventListener("click", () => {
      pinned = !pinned;
      try {
        storage?.setItem(PLAYBOOK_PIN_STORAGE_KEY, pinned ? "1" : "0");
      } catch (_error) {
        // The visible state still works when storage is unavailable.
      }
      render();
    });
    render();
    return { isPinned: () => pinned };
  };

  const normalizeSymbol = (value) => String(value || "").trim().toUpperCase();

  const numericPrice = (value) => {
    if (value === null || value === undefined || String(value).trim() === "") return null;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  };

  const normalizeLevel = (level, fallbackSymbol = "") => {
    const source = level && typeof level === "object" ? level : {};
    const key = String(source.key || "").trim().toLowerCase();
    const price = numericPrice(source.price);
    return {
      key,
      price,
      classification: String(source.classification || "unclassified"),
      symbol: normalizeSymbol(source.symbol || fallbackSymbol),
      valid: LEVEL_KEYS.has(key) && price !== null && source.valid !== false,
    };
  };

  const nextSelectionState = (current, action) => {
    const state = current && typeof current === "object" ? current : {};
    const source = action && typeof action === "object" ? action : {};
    const pinnedKey = String(state.pinnedKey || "");
    const previewKey = String(state.previewKey || "");
    const key = String(source.key || "");
    if (source.valid === false) return { pinnedKey, previewKey };
    if (source.type === "pin-toggle") {
      return { pinnedKey: pinnedKey === key ? "" : key, previewKey: "" };
    }
    if (source.type === "preview-start") return { pinnedKey, previewKey: key };
    if (source.type === "preview-end") return { pinnedKey, previewKey: "" };
    if (source.type === "external-pin") return { pinnedKey: key, previewKey: "" };
    return { pinnedKey, previewKey };
  };

  const shouldAcceptEvent = (
    detail,
    currentSymbol,
    lastTimestamp = 0,
    now = Date.now(),
  ) => {
    const source = detail && typeof detail === "object" ? detail : {};
    if (normalizeSymbol(source.symbol) !== normalizeSymbol(currentSymbol)) return false;
    if (source.valid === false || numericPrice(source.price) === null) return false;
    const timestamp = Number(source.timestamp) || 0;
    if (timestamp && timestamp < Number(lastTimestamp || 0)) return false;
    if (timestamp && Number(now) - timestamp > MAX_EVENT_AGE_MS) return false;
    return true;
  };

  const formatPrice = (value) => Number(value).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  const init = (documentRoot) => {
    const playbookPin = initPlaybookPin(documentRoot);
    const deck = documentRoot.getElementById("marketPulseGammaLevelDeck");
    const chartHost = documentRoot.getElementById("spxExecutionHeroChart");
    const chartRail = documentRoot.getElementById("spxExecutionHeroLevelRail");
    if (!deck || !chartHost || !chartRail) return { playbookPin };

    const symbol = normalizeSymbol(chartHost.dataset.symbol || "SPY");
    const rows = () => Array.from(deck.querySelectorAll("[data-market-pulse-level-key]"));
    let selectionState = { pinnedKey: "", previewKey: "" };
    let lastTimestamp = 0;

    const chartLevel = (key) => chartRail.querySelector(
      `[data-market-pulse-chart-level-key="${String(key).replaceAll('"', '')}"]`,
    );

    const applyHighlight = () => {
      const { pinnedKey, previewKey } = selectionState;
      const activeKey = previewKey || pinnedKey;
      rows().forEach((row) => {
        const isPinned = Boolean(pinnedKey) && row.dataset.marketPulseLevelKey === pinnedKey;
        const isPreview = Boolean(previewKey) && row.dataset.marketPulseLevelKey === previewKey;
        row.classList.toggle("is-pinned", isPinned);
        row.classList.toggle("is-preview", isPreview);
        row.setAttribute("aria-pressed", isPinned ? "true" : "false");
      });
      chartRail.querySelectorAll("[data-market-pulse-chart-level-key]").forEach((item) => {
        item.classList.toggle(
          "is-gamma-highlighted",
          Boolean(activeKey) && item.dataset.marketPulseChartLevelKey === activeKey,
        );
      });
      const activeChartLevel = activeKey ? chartLevel(activeKey) : null;
      chartHost.classList.toggle("has-gamma-highlight", Boolean(activeChartLevel));
      chartHost.dataset.gammaHighlightKey = activeChartLevel ? activeKey : "";
      if (activeChartLevel) {
        chartHost.style.setProperty("--gamma-highlight-y", activeChartLevel.style.top || "50%");
      } else {
        chartHost.style.removeProperty("--gamma-highlight-y");
      }
    };

    const dispatchSelection = (row, mode) => {
      const level = normalizeLevel({
        key: row.dataset.marketPulseLevelKey,
        price: row.dataset.marketPulseLevelPrice,
        classification: row.dataset.marketPulseLevelClassification,
        symbol: row.dataset.marketPulseLevelSymbol,
      }, symbol);
      if (!level.valid) return;
      documentRoot.dispatchEvent(new CustomEvent(EVENT_LEVEL_SELECTED, {
        detail: { ...level, mode, timestamp: Date.now() },
      }));
    };

    deck.addEventListener("click", (event) => {
      const row = event.target.closest("[data-market-pulse-level-key]");
      if (!row || row.disabled) return;
      const key = row.dataset.marketPulseLevelKey;
      selectionState = nextSelectionState(selectionState, { type: "pin-toggle", key });
      applyHighlight();
      dispatchSelection(row, selectionState.pinnedKey ? "pinned" : "cleared");
    });

    const preview = (event) => {
      const row = event.target.closest("[data-market-pulse-level-key]");
      if (!row || row.disabled) return;
      selectionState = nextSelectionState(selectionState, {
        type: "preview-start",
        key: row.dataset.marketPulseLevelKey,
      });
      applyHighlight();
      dispatchSelection(row, "preview");
    };
    const clearPreview = (event) => {
      const row = event.target.closest("[data-market-pulse-level-key]");
      if (!row) return;
      selectionState = nextSelectionState(selectionState, { type: "preview-end" });
      applyHighlight();
      const pinnedRow = selectionState.pinnedKey
        ? deck.querySelector(`[data-market-pulse-level-key="${selectionState.pinnedKey}"]`)
        : null;
      dispatchSelection(pinnedRow || row, pinnedRow ? "pinned" : "cleared");
    };
    deck.addEventListener("pointerover", preview);
    deck.addEventListener("pointerout", clearPreview);
    deck.addEventListener("focusin", preview);
    deck.addEventListener("focusout", clearPreview);

    documentRoot.addEventListener(EVENT_LEVEL_SELECTED, (event) => {
      const detail = event.detail && typeof event.detail === "object" ? event.detail : {};
      if (detail.mode !== "advanced-ladder") return;
      if (!shouldAcceptEvent(detail, symbol, lastTimestamp)) return;
      lastTimestamp = Math.max(lastTimestamp, Number(detail.timestamp) || 0);
      const row = deck.querySelector(`[data-market-pulse-level-key="${detail.key}"]`);
      if (!row) return;
      selectionState = nextSelectionState(selectionState, {
        type: "external-pin",
        key: detail.key,
      });
      applyHighlight();
    });

    documentRoot.addEventListener(EVENT_LEVELS_UPDATED, (event) => {
      const detail = event.detail && typeof event.detail === "object" ? event.detail : {};
      const timestamp = Number(detail.timestamp) || 0;
      if (normalizeSymbol(detail.symbol) !== symbol) return;
      if (timestamp && timestamp < lastTimestamp) return;
      lastTimestamp = Math.max(lastTimestamp, timestamp);
      const spot = numericPrice(detail.spot);
      (Array.isArray(detail.levels) ? detail.levels : []).forEach((entry) => {
        const normalized = normalizeLevel(entry, detail.symbol);
        const row = deck.querySelector(`[data-market-pulse-level-key="${normalized.key}"]`);
        if (!row) return;
        row.dataset.marketPulseLevelPrice = normalized.valid ? String(normalized.price) : "";
        row.disabled = !normalized.valid;
        row.setAttribute("aria-disabled", normalized.valid ? "false" : "true");
        row.classList.toggle("is-unavailable", !normalized.valid);
        const value = row.querySelector(`[data-market-pulse-level-value="${normalized.key}"]`);
        const distance = row.querySelector(`[data-market-pulse-level-distance="${normalized.key}"]`);
        if (value) value.textContent = normalized.valid ? formatPrice(normalized.price) : "Unavailable";
        if (distance) {
          distance.textContent = normalized.valid && spot !== null
            ? `${normalized.price - spot >= 0 ? "+" : ""}${(normalized.price - spot).toFixed(1)} pts`
            : "No valid distance";
        }
      });
      if (spot !== null) {
        ["marketPulseStatusSpot", "marketPulseGammaDeckSpot", "marketPulseStructurePrimarySpot"]
          .forEach((id) => {
            const node = documentRoot.getElementById(id);
            if (node) node.textContent = formatPrice(spot);
          });
      }
      applyHighlight();
    });

    const railObserver = new MutationObserver(applyHighlight);
    railObserver.observe(chartRail, { childList: true, subtree: true });
    applyHighlight();
    return { applyHighlight, getPinnedKey: () => selectionState.pinnedKey, playbookPin };
  };

  return {
    init,
    initPlaybookPin,
    normalizePinPreference,
    PLAYBOOK_PIN_STORAGE_KEY,
    normalizeLevel,
    nextSelectionState,
    shouldAcceptEvent,
    EVENT_LEVELS_UPDATED,
    EVENT_LEVEL_SELECTED,
  };
});
