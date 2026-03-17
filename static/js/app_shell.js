(() => {
  "use strict";

  const config = window.APP_SHELL_CONFIG || {};
  const doc = document;
  const body = doc.body;
  const focusableSelector = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "[tabindex]:not([tabindex='-1'])",
  ].join(",");

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
      // Safari private browsing and hardened contexts can throw.
    }
  };

  const qs = (selector) => doc.querySelector(selector);
  const qsa = (selector) => Array.from(doc.querySelectorAll(selector));

  let previousDrawerFocus = null;

  function closeMoreMenu() {
    const menu = doc.getElementById("moreMenu");
    if (menu) menu.classList.remove("open");
    const btn = qs(".moreBtn");
    if (btn) btn.setAttribute("aria-expanded", "false");
  }

  function closeQuickPanel() {
    const panel = doc.getElementById("quickPanel");
    const btn = doc.getElementById("quickToggleBtn");
    if (panel) panel.classList.remove("open");
    if (btn) btn.setAttribute("aria-expanded", "false");
  }

  function syncDrawerScrollLock() {
    body.style.overflow = body.classList.contains("drawer-open") ? "hidden" : "";
  }

  function syncDrawerToggles(isOpen) {
    ["menuToggleBtn", "mobileDockMenuBtn"].forEach((id) => {
      const btn = doc.getElementById(id);
      if (btn) btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
    });
  }

  function getDrawerFocusable() {
    const drawer = doc.getElementById("drawer");
    if (!drawer) return [];
    return qsa("#drawer " + focusableSelector).filter((el) => el.offsetParent !== null);
  }

  function handleDrawerKeydown(event) {
    if (event.key === "Escape") {
      closeDrawer();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = getDrawerFocusable();
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    const active = doc.activeElement;
    if (event.shiftKey && active === first) {
      event.preventDefault();
      last.focus();
      return;
    }
    if (!event.shiftKey && active === last) {
      event.preventDefault();
      first.focus();
    }
  }

  function focusDrawerPrimary() {
    const drawer = doc.getElementById("drawer");
    if (!drawer) return;
    const focusable = getDrawerFocusable();
    const target = focusable.find((el) => el.classList.contains("drawerClose"))
      || focusable.find((el) => el.closest(".drawerQuickGrid"))
      || focusable[0];
    if (target) target.focus();
  }

  function openDrawer() {
    const drawer = doc.getElementById("drawer");
    const overlay = doc.getElementById("drawerOverlay");
    if (!drawer || !overlay) return;
    previousDrawerFocus = doc.activeElement instanceof HTMLElement ? doc.activeElement : null;
    closeMoreMenu();
    closeQuickPanel();
    drawer.classList.add("open");
    overlay.classList.add("open");
    body.classList.add("drawer-open");
    syncDrawerToggles(true);
    syncDrawerScrollLock();
    doc.addEventListener("keydown", handleDrawerKeydown, true);
    window.requestAnimationFrame(focusDrawerPrimary);
  }

  function openTradingWindowSettings() {
    openDrawer();
    window.requestAnimationFrame(() => {
      const section = doc.getElementById("drawerTradingWindow");
      const editor = doc.getElementById("tradingWindowDrawerEditor");
      if (!section || !editor) return;
      editor.open = true;
      section.scrollIntoView({ block: "nearest", behavior: "smooth" });
      const target = editor.querySelector("input, select, textarea, button");
      if (target) target.focus();
    });
  }

  function initTradingWindowIntent() {
    const params = new URLSearchParams(window.location.search || "");
    if (params.get("tw") !== "settings") return;
    openTradingWindowSettings();
    params.delete("tw");
    const nextQuery = params.toString();
    const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ""}${window.location.hash || ""}`;
    window.requestAnimationFrame(() => {
      try {
        window.history.replaceState({}, "", nextUrl);
      } catch (_err) {
        // Ignore environments that block history updates.
      }
    });
  }

  function closeDrawer() {
    const drawer = doc.getElementById("drawer");
    const overlay = doc.getElementById("drawerOverlay");
    if (drawer) drawer.classList.remove("open");
    if (overlay) overlay.classList.remove("open");
    body.classList.remove("drawer-open");
    syncDrawerToggles(false);
    syncDrawerScrollLock();
    doc.removeEventListener("keydown", handleDrawerKeydown, true);
    if (previousDrawerFocus && doc.contains(previousDrawerFocus)) {
      previousDrawerFocus.focus();
    }
    previousDrawerFocus = null;
  }

  function toggleMoreMenu() {
    const menu = doc.getElementById("moreMenu");
    const btn = qs(".moreBtn");
    if (!menu) return;
    closeQuickPanel();
    menu.classList.toggle("open");
    if (btn) btn.setAttribute("aria-expanded", menu.classList.contains("open") ? "true" : "false");
  }

  function toggleQuickPanel() {
    const panel = doc.getElementById("quickPanel");
    const btn = doc.getElementById("quickToggleBtn");
    if (!panel || !btn) return;
    closeMoreMenu();
    panel.classList.toggle("open");
    btn.setAttribute("aria-expanded", panel.classList.contains("open") ? "true" : "false");
  }

  function isTypingContext(el) {
    if (!el) return false;
    const tag = (el.tagName || "").toLowerCase();
    return tag === "input" || tag === "textarea" || tag === "select" || !!el.isContentEditable;
  }

  function initTradingWindowForms() {
    qsa(".tradingWindowForm").forEach((form) => {
      const testMode = form.querySelector("[data-tw-test-mode]");
      const testFields = form.querySelectorAll("[data-tw-test-field]");
      if (!testMode || !testFields.length) return;
      const applyState = () => {
        const enabled = !!testMode.checked;
        testFields.forEach((field) => {
          field.disabled = !enabled;
        });
      };
      testMode.addEventListener("change", applyState);
      applyState();
    });
  }

  function initMenus() {
    window.closeMoreMenu = closeMoreMenu;
    window.closeQuickPanel = closeQuickPanel;
    window.toggleMoreMenu = toggleMoreMenu;
    window.toggleQuickPanel = toggleQuickPanel;
    window.openDrawer = openDrawer;
    window.openTradingWindowSettings = openTradingWindowSettings;
    window.closeDrawer = closeDrawer;

    window.addEventListener("click", (event) => {
      const target = event.target;
      const isMoreButton = target.closest && target.closest(".moreBtn");
      const isInsideMore = target.closest && target.closest("#moreMenu");
      if (!isMoreButton && !isInsideMore) closeMoreMenu();

      const panel = doc.getElementById("quickPanel");
      const btn = doc.getElementById("quickToggleBtn");
      if (!panel || !btn) return;
      const insidePanel = target.closest && target.closest("#quickPanel");
      const toggle = target.closest && target.closest("#quickToggleBtn");
      if (!insidePanel && !toggle) closeQuickPanel();
    });

    doc.addEventListener("click", (event) => {
      const link = event.target.closest && event.target.closest("#drawer a");
      if (link) closeDrawer();
    });

    doc.addEventListener("click", (event) => {
      const link = event.target.closest && event.target.closest("a[href]");
      if (!link) return;
      if ((link.getAttribute("target") || "").toLowerCase() === "_blank") return;
      const href = String(link.getAttribute("href") || "").trim();
      if (!href || href.startsWith("#")) return;
      closeMoreMenu();
      closeQuickPanel();
      if (link.closest && link.closest("#drawer")) closeDrawer();
    }, true);

    ["pageshow", "pagehide"].forEach((name) => {
      window.addEventListener(name, () => {
        closeDrawer();
        closeMoreMenu();
        closeQuickPanel();
        syncDrawerScrollLock();
      });
    });

    window.addEventListener("resize", () => {
      if (window.innerWidth > 640) {
        closeDrawer();
        syncDrawerScrollLock();
      }
    });
  }

  function initRowMenus() {
    if (window.toggleRowMenu) return;
    window.toggleRowMenu = (tradeId, event) => {
      event.preventDefault();
      event.stopPropagation();
      const menu = doc.getElementById(`rowMenu-${tradeId}`);
      const btn = event.currentTarget;
      if (!menu || !btn) return;
      qsa(".rowMoreMenu.open").forEach((item) => {
        if (item !== menu) item.classList.remove("open");
      });
      menu.classList.toggle("open");
      if (!menu.classList.contains("open")) return;
      menu.style.position = "fixed";
      menu.style.zIndex = "999999";
      menu.style.visibility = "hidden";
      menu.style.display = "block";
      const btnRect = btn.getBoundingClientRect();
      const menuRect = menu.getBoundingClientRect();
      let top = btnRect.bottom + 6;
      if (top + menuRect.height > window.innerHeight - 8) {
        top = btnRect.top - menuRect.height - 6;
      }
      let left = btnRect.right - menuRect.width;
      left = Math.max(8, Math.min(left, window.innerWidth - menuRect.width - 8));
      menu.style.top = `${top}px`;
      menu.style.left = `${left}px`;
      menu.style.visibility = "visible";
    };

    doc.addEventListener("click", () => {
      qsa(".rowMoreMenu.open").forEach((menu) => menu.classList.remove("open"));
    });
  }

  function initShortcutsAndActions() {
    const navShortcutMap = {
      gd: "/dashboard",
      gt: "/trades",
      gj: "/journal",
      ga: "/analytics",
      gc: "/calculator",
      ng: "/goals",
      nt: "/trades/new",
      nj: config.newEntryUrl || "/journal/new",
    };
    let comboBuffer = "";
    let comboTs = 0;

    const handlePowerShortcut = (event) => {
      const now = Date.now();
      if (now - comboTs > 900) comboBuffer = "";
      comboTs = now;
      const key = (event.key || "").toLowerCase();
      if (!/^[a-z\/\?]$/.test(key)) return false;
      if (key === "/") {
        const search = doc.getElementById("search");
        if (search) {
          event.preventDefault();
          search.focus();
          return true;
        }
      }
      if (key === "?") {
        const panel = doc.getElementById("shortcutHelp");
        if (panel) {
          panel.classList.toggle("open");
          event.preventDefault();
          return true;
        }
      }
      comboBuffer = (comboBuffer + key).slice(-2);
      const href = navShortcutMap[comboBuffer];
      if (href) {
        event.preventDefault();
        window.location.href = href;
        return true;
      }
      return false;
    };

    window.confirmDelete = (formId) => {
      if (window.confirm("Delete this? This can't be undone.")) {
        doc.getElementById(formId)?.submit();
      }
    };

    window.confirmClear = (formId) => {
      if (window.confirm("Clear ALL trade data? This wipes the trades table.")) {
        doc.getElementById(formId)?.submit();
      }
    };

    window.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        const search = doc.getElementById("search");
        if (search) {
          event.preventDefault();
          search.focus();
        }
      }
      if (!isTypingContext(doc.activeElement) && !event.ctrlKey && !event.metaKey && !event.altKey) {
        handlePowerShortcut(event);
      }
      if (event.key === "Escape") closeDrawer();
    });
  }

  function updateETClock() {
    try {
      const now = new Date();
      const timeStr = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
      }).format(now);
      const weekday = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        weekday: "short",
      }).format(now);
      const hm = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).format(now).split(":");
      const nyMinutes = (Number(hm[0] || 0) * 60) + Number(hm[1] || 0);
      const clock = doc.getElementById("etClock");
      const mode = doc.getElementById("modePill");
      const modeLabel = doc.getElementById("sessionStateLabel");
      if (clock) clock.textContent = `${timeStr} ET`;
      if (mode) {
        const isWeekend = weekday === "Sat" || weekday === "Sun";
        mode.classList.remove(
          "modeStateWeekend",
          "modeStatePre",
          "modeStateTrading",
          "modeStatePost",
          "modeStateLoading"
        );
        let modeText = "Weekend";
        let modeTitle = "US market is closed for the weekend";
        if (isWeekend) {
          modeText = "Weekend";
          mode.classList.add("modeStateWeekend");
        } else if (nyMinutes < 570) {
          modeText = "Pre-Market";
          modeTitle = "US market is closed before the regular session opens at 9:30 AM ET";
          mode.classList.add("modeStatePre");
        } else if (nyMinutes < 960) {
          modeText = "Regular Session";
          modeTitle = "US market is open (9:30 AM - 4:00 PM ET)";
          mode.classList.add("modeStateTrading");
        } else {
          modeText = "After Hours";
          modeTitle = "US market is closed after the regular session ends at 4:00 PM ET";
          mode.classList.add("modeStatePost");
        }
        if (modeLabel) {
          modeLabel.textContent = modeText;
        } else {
          mode.textContent = modeText;
        }
        mode.title = modeTitle;
      }
    } catch (err) {
      console.error(err);
    }
  }

  function initThemeAndGuided() {
    const themeMigrationKey = "mc_theme_v2";
    const themeOrder = ["obsidian", "galaxy", "black", "steel", "red", "green", "default", "white"];
    const themeLabels = {
      obsidian: "Theme: True Dark",
      black: "Theme: Midnight",
      default: "Theme: Classic",
      galaxy: "Theme: Midnight Galaxy",
      red: "Theme: Red",
      green: "Theme: Green",
      steel: "Theme: Steel",
    };

    const syncThemeButtons = (theme) => {
      qsa("[data-theme-toggle-label]").forEach((btn) => {
        btn.textContent = themeLabels[theme] || themeLabels.default;
      });
    };

    const applyTheme = (theme) => {
      const normalized = themeOrder.includes(theme) ? theme : "black";
      body.setAttribute("data-theme", normalized);
      syncThemeButtons(normalized);
    };

    const applyGuidedHighlights = (enabled) => {
      const anchors = qsa("a.btn[href]");
      anchors.forEach((anchor) => anchor.classList.remove("guideHot"));
      if (!enabled) return;
      const paths = [
        "/calculator",
        "/trades/upload/statement",
        "/trades",
        "/analytics?tab=performance",
        "/journal/review/weekly",
      ];
      anchors.forEach((anchor) => {
        const href = (anchor.getAttribute("href") || "").trim();
        if (!href) return;
        if (paths.some((path) => href === path || href.startsWith(path + "&") || href.startsWith(path + "?"))) {
          anchor.classList.add("guideHot");
        }
      });
    };

    const setGuidedMode = (on) => {
      const enabled = !!on;
      body.classList.toggle("guidedMode", enabled);
      const btn = doc.getElementById("guidedModeBtn");
      if (btn) btn.textContent = `Guided Mode: ${enabled ? "On" : "Off"}`;
      applyGuidedHighlights(enabled);
      storageSet("mc_guided_mode", enabled ? "1" : "0");
      storageSet("mc_guided_seen", "1");
    };

    window.toggleTheme = () => {
      const current = body.getAttribute("data-theme") || "obsidian";
      const idx = themeOrder.indexOf(current);
      const next = themeOrder[(idx + 1) % themeOrder.length];
      storageSet("mc_theme", next);
      applyTheme(next);
    };

    window.toggleGuidedMode = () => {
      setGuidedMode(!body.classList.contains("guidedMode"));
    };

    if (storageGet(themeMigrationKey) !== "1") {
      const savedTheme = storageGet("mc_theme");
      if (!savedTheme || savedTheme === "galaxy" || savedTheme === "black") {
        storageSet("mc_theme", "obsidian");
      }
      storageSet(themeMigrationKey, "1");
    }

    applyTheme(storageGet("mc_theme") || "obsidian");
    const savedGuide = storageGet("mc_guided_mode");
    const firstRunSeen = storageGet("mc_guided_seen");
    if (savedGuide === "1") setGuidedMode(true);
    else if (savedGuide === "0") setGuidedMode(false);
    else setGuidedMode(!firstRunSeen);
  }

  function enforcePrimaryCta() {
    const scopes = qsa(".actionRow, .rightActions, .leftActions, .mobileActionGrid");
    const isExcluded = (btn) => !!(
      btn.closest(".nav")
      || btn.closest(".drawer")
      || btn.closest(".mobileDock")
      || btn.closest(".quickPanel")
      || btn.closest(".workflowRail")
      || btn.classList.contains("danger")
    );
    const candidates = [];
    scopes.forEach((scope) => {
      scope.querySelectorAll(".btn, button.btn").forEach((btn) => {
        if (btn.disabled || btn.offsetParent === null || isExcluded(btn)) return;
        candidates.push(btn);
      });
    });
    if (!candidates.length) return;
    const scoreButton = (btn) => {
      const txt = (btn.textContent || "").trim().toLowerCase();
      const href = (btn.getAttribute("href") || "").toLowerCase();
      const inHero = !!btn.closest(".pageHero, .dashboardHero");
      let score = 0;
      if (inHero) score += 7;
      if (btn.classList.contains("primary")) score += 4;
      if (/(calculate|upload|save|add|new|import|analyze|review|run|start|open)/.test(txt)) score += 5;
      if (/(calculator|upload|import|analytics|journal\/new|journal\/review|trades\/new)/.test(href)) score += 3;
      if (/(back|cancel|reset|delete|clear|prev|next|more)/.test(txt)) score -= 6;
      return score;
    };
    let winner = candidates[0];
    let best = -999;
    candidates.forEach((btn) => {
      const score = scoreButton(btn);
      if (score > best) {
        best = score;
        winner = btn;
      }
    });
    candidates.forEach((btn) => {
      btn.classList.remove("primary", "ctaSecondary", "ctaLink");
      if (btn === winner) {
        btn.classList.add("primary");
      } else if (btn.closest(".pageHero, .dashboardHero")) {
        btn.classList.add("ctaSecondary");
      } else {
        btn.classList.add("ctaLink");
      }
    });
  }

  function compactDenseGrid(grid, itemSelector, keepCount, label) {
    const cards = Array.from(grid.querySelectorAll(`:scope > ${itemSelector}`));
    if (cards.length <= keepCount || grid.dataset.compacted === "1") return;
    const isAlwaysVisible = (card) => card.classList.contains("mobileKeep");
    cards.forEach((card, index) => {
      if (index >= keepCount && !isAlwaysVisible(card)) card.classList.add("mobileDenseExtra");
    });
    const toggle = doc.createElement("button");
    toggle.type = "button";
    toggle.className = "btn statToggleBtn";
    toggle.textContent = "More";
    toggle.title = `Show more ${label.toLowerCase()}`;
    toggle.setAttribute("aria-label", `Show more ${label.toLowerCase()}`);
    toggle.addEventListener("click", () => {
      const expanded = grid.classList.toggle("showAllStats");
      toggle.textContent = expanded ? "Less" : "More";
      const nextTitle = expanded ? `Show fewer ${label.toLowerCase()}` : `Show more ${label.toLowerCase()}`;
      toggle.title = nextTitle;
      toggle.setAttribute("aria-label", nextTitle);
    });
    grid.insertAdjacentElement("afterend", toggle);
    grid.dataset.compacted = "1";
  }

  function setupMobileDensityCompaction() {
    if (window.innerWidth > 760) return;
    qsa(".statRow:not([data-no-mobile-compact='1'])").forEach((row) => compactDenseGrid(row, ".stat", 3, "Stats"));
    qsa(".metricStrip:not([data-no-mobile-compact='1'])").forEach((row) => compactDenseGrid(row, ".metric", 2, "Metrics"));
    qsa(".insightGrid").forEach((row) => compactDenseGrid(row, ".insightCard", 2, "Insights"));
    qsa(".dashboardSupportGrid:not([data-no-mobile-compact='1'])").forEach((row) => compactDenseGrid(row, ".supportCard", 2, "Panels"));
    qsa(".analyticsTopStats").forEach((row) => compactDenseGrid(row, ".analyticsTopStat", 2, "Stats"));
    qsa(".statusBadgeStrip").forEach((row) => compactDenseGrid(row, ".statusBadge", 4, "Status"));
  }

  function initShortcutHelp() {
    const host = doc.createElement("div");
    host.id = "shortcutHelp";
    host.className = "quickPanel";
    host.style.left = "14px";
    host.style.right = "auto";
    host.style.bottom = "18px";
    host.style.width = "320px";
    host.innerHTML = `
      <div class="menuTitle">Power Shortcuts</div>
      <div class="tiny line16">
        <b>/</b> focus search · <b>g d</b> dashboard · <b>g t</b> trades · <b>g j</b> journal ·
        <b>g a</b> analytics · <b>g c</b> plan risk · <b>n t</b> new trade · <b>n j</b> new journal
      </div>
    `;
    body.appendChild(host);
    window.toggleShortcutHelp = () => {
      host.classList.toggle("open");
    };
    doc.addEventListener("keydown", (event) => {
      if (event.key === "Escape") host.classList.remove("open");
    });
  }

  function initCardStagger() {
    qsa(".card, .metric, .calcCard, .insightCard").forEach((el, index) => {
      el.style.setProperty("--stagger", String(index % 8));
    });
  }

  function init() {
    initTradingWindowForms();
    initMenus();
    initRowMenus();
    initShortcutsAndActions();
    initThemeAndGuided();
    enforcePrimaryCta();
    setupMobileDensityCompaction();
    initShortcutHelp();
    initCardStagger();
    initTradingWindowIntent();
    window.setInterval(updateETClock, 1000);
    updateETClock();
  }

  init();
})();
