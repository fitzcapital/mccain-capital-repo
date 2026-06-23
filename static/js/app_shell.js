(() => {
  "use strict";

  const config = window.APP_SHELL_CONFIG || {};
  const doc = document;
  const docEl = doc.documentElement;
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
  let previousMobileMenuFocus = null;
  let clockTimer = null;
  let drawerResizeTimer = null;

  function isMobileAppViewport() {
    return window.matchMedia && window.matchMedia("(max-width: 760px)").matches;
  }

  function focusWithoutScroll(target) {
    if (!target || typeof target.focus !== "function") return;
    try {
      target.focus({ preventScroll: true });
    } catch (_err) {
      target.focus();
    }
  }

  function closeMoreMenu() {
    const menu = doc.getElementById("moreMenu");
    if (menu) menu.classList.remove("open");
    const btn = qs(".moreBtn");
    if (btn) btn.setAttribute("aria-expanded", "false");
    if (menu) menu.setAttribute("aria-hidden", "true");
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
    ["menuToggleBtn"].forEach((id) => {
      const btn = doc.getElementById(id);
      if (btn) btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
    });
  }

  function syncMobileMenuToggle(isOpen) {
    const btn = doc.getElementById("mobileDockMenuBtn");
    if (btn) btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
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
    focusWithoutScroll(target);
  }

  function openDrawer() {
    if (isMobileAppViewport()) {
      openMobileMenu();
      return;
    }
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
      focusWithoutScroll(previousDrawerFocus);
    }
    previousDrawerFocus = null;
  }

  function getMobileMenuFocusable() {
    const sheet = doc.getElementById("mobileMenuSheet");
    if (!sheet) return [];
    return qsa("#mobileMenuSheet " + focusableSelector).filter((el) => el.offsetParent !== null);
  }

  function handleMobileMenuKeydown(event) {
    if (event.key === "Escape") {
      closeMobileMenu();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = getMobileMenuFocusable();
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

  function focusMobileMenuPrimary() {
    const target = getMobileMenuFocusable()[0];
    focusWithoutScroll(target);
  }

  function openMobileMenu() {
    const sheet = doc.getElementById("mobileMenuSheet");
    const overlay = doc.getElementById("mobileMenuOverlay");
    if (!sheet || !overlay) return;
    previousMobileMenuFocus = doc.activeElement instanceof HTMLElement ? doc.activeElement : null;
    closeMoreMenu();
    closeQuickPanel();
    closeDrawer();
    sheet.classList.add("open");
    overlay.classList.add("open");
    sheet.setAttribute("aria-hidden", "false");
    overlay.setAttribute("aria-hidden", "false");
    body.classList.add("mobile-menu-open");
    syncMobileMenuToggle(true);
    doc.addEventListener("keydown", handleMobileMenuKeydown, true);
    window.requestAnimationFrame(focusMobileMenuPrimary);
  }

  function closeMobileMenu() {
    const sheet = doc.getElementById("mobileMenuSheet");
    const overlay = doc.getElementById("mobileMenuOverlay");
    if (sheet) {
      sheet.classList.remove("open");
      sheet.setAttribute("aria-hidden", "true");
    }
    if (overlay) {
      overlay.classList.remove("open");
      overlay.setAttribute("aria-hidden", "true");
    }
    body.classList.remove("mobile-menu-open");
    syncMobileMenuToggle(false);
    doc.removeEventListener("keydown", handleMobileMenuKeydown, true);
    if (previousMobileMenuFocus && doc.contains(previousMobileMenuFocus)) {
      focusWithoutScroll(previousMobileMenuFocus);
    }
    previousMobileMenuFocus = null;
  }

  function toggleMoreMenu() {
    const menu = doc.getElementById("moreMenu");
    const btn = qs(".moreBtn");
    if (!menu) return;
    closeQuickPanel();
    menu.classList.toggle("open");
    const isOpen = menu.classList.contains("open");
    if (btn) btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
    menu.setAttribute("aria-hidden", isOpen ? "false" : "true");
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
    window.closeDrawer = closeDrawer;
    window.openMobileMenu = openMobileMenu;
    window.closeMobileMenu = closeMobileMenu;

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
      const target = event.target;
      const link = target.closest && target.closest("#mobileMenuSheet a");
      const button = target.closest && target.closest("#mobileMenuSheet button.mobileMenuRow");
      if (link || button) closeMobileMenu();
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
      if (link.closest && link.closest("#mobileMenuSheet")) closeMobileMenu();
    }, true);

    ["pageshow", "pagehide"].forEach((name) => {
      window.addEventListener(name, () => {
        closeDrawer();
        closeMobileMenu();
        closeMoreMenu();
        closeQuickPanel();
        syncDrawerScrollLock();
      });
    });

    window.addEventListener("resize", () => {
      if (window.innerWidth <= 640) return;
      window.clearTimeout(drawerResizeTimer);
      drawerResizeTimer = window.setTimeout(() => {
        closeDrawer();
        closeMobileMenu();
        syncDrawerScrollLock();
      }, 120);
    });

    const syncMobileViewportClass = () => {
      body.classList.toggle("is-mobile-app", isMobileAppViewport());
      if (!isMobileAppViewport()) closeMobileMenu();
    };
    syncMobileViewportClass();
    window.addEventListener("resize", syncMobileViewportClass);
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
        if (typeof window.navigateWithShellLoading === "function") {
          window.navigateWithShellLoading(href);
        } else {
          window.location.href = href;
        }
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

  function getEasternClockParts(now) {
    const etParts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).formatToParts(now);
    const part = (type) => etParts.find((item) => item.type === type)?.value || "";
    const rawHour = String(part("hour") || "0").padStart(2, "0");
    const hour = rawHour === "24" ? "00" : rawHour;
    const hourNumber = Number(hour);
    const minute = String(part("minute") || "0").padStart(2, "0");
    const second = String(part("second") || "0").padStart(2, "0");
    return {
      weekday: part("weekday"),
      year: Number(part("year") || 0),
      month: Number(part("month") || 1),
      day: Number(part("day") || 1),
      hour: hourNumber,
      minute: Number(minute),
      second: Number(second),
      timeLabel: `${hour}:${minute}:${second}`,
      preciseTimeLabel: `${hour}:${minute}:${second}`,
      displayTimeLabel: `${hour}:${minute}:${second}`,
    };
  }

  function getLocalClockParts(now) {
    const localParts = new Intl.DateTimeFormat(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).formatToParts(now);
    const part = (type) => localParts.find((item) => item.type === type)?.value || "";
    const rawHour = String(part("hour") || "0").padStart(2, "0");
    const hour = rawHour === "24" ? "00" : rawHour;
    const minute = String(part("minute") || "0").padStart(2, "0");
    const second = String(part("second") || "0").padStart(2, "0");
    return {
      timeLabel: `${hour}:${minute}:${second}`,
      preciseTimeLabel: `${hour}:${minute}:${second}`,
    };
  }

  function getMarketClockState(now = new Date()) {
    const et = getEasternClockParts(now);
    const etNow = new Date(Date.UTC(
      et.year,
      Math.max(0, et.month - 1),
      et.day,
      et.hour,
      et.minute,
      et.second
    ));
    const nySeconds = (et.hour * 3600) + (et.minute * 60) + et.second;
    const isWeekend = et.weekday === "Sat" || et.weekday === "Sun";
    const marketOpenSeconds = 9 * 3600 + 30 * 60;
    const marketCloseSeconds = 16 * 3600;

    const formatDuration = (secondsUntil) => {
      const totalMinutes = Math.max(1, Math.ceil(Number(secondsUntil || 0) / 60));
      const hours = Math.floor(totalMinutes / 60);
      const minutes = totalMinutes % 60;
      if (hours > 0 && minutes > 0) return `${hours}h ${minutes}m`;
      if (hours > 0) return `${hours}h`;
      return `${minutes}m`;
    };

    const nextMarketOpen = (() => {
      const next = new Date(etNow.getTime());
      if (!isWeekend && nySeconds < marketOpenSeconds) {
        next.setUTCHours(9, 30, 0, 0);
        return next;
      }
      if (!isWeekend && nySeconds < marketCloseSeconds) return null;
      next.setUTCDate(next.getUTCDate() + 1);
      next.setUTCHours(9, 30, 0, 0);
      while (next.getUTCDay() === 0 || next.getUTCDay() === 6) {
        next.setUTCDate(next.getUTCDate() + 1);
      }
      return next;
    })();

    if (!isWeekend && nySeconds < marketOpenSeconds) {
      return {
        state: "premarket",
        timeLabel: et.displayTimeLabel,
        stateLabel: "Pre-Market",
        preciseTimeLabel: et.preciseTimeLabel,
        statusText: `Opens in ${formatDuration(marketOpenSeconds - nySeconds)}`,
        modeTitle: "US regular session opens at 9:30 AM ET",
      };
    }
    if (!isWeekend && nySeconds < marketCloseSeconds) {
      return {
        state: "open",
        timeLabel: et.displayTimeLabel,
        stateLabel: "Market Open",
        preciseTimeLabel: et.preciseTimeLabel,
        statusText: `Closes in ${formatDuration(marketCloseSeconds - nySeconds)}`,
        modeTitle: "US regular market is open until 4:00 PM ET",
      };
    }

    const nextOpenSeconds = nextMarketOpen
      ? Math.max(60, Math.floor((nextMarketOpen.getTime() - etNow.getTime()) / 1000))
      : 0;
    return {
      state: "closed",
      timeLabel: et.displayTimeLabel,
      stateLabel: "Market Closed",
      preciseTimeLabel: et.preciseTimeLabel,
      statusText: nextMarketOpen ? `Opens in ${formatDuration(nextOpenSeconds)}` : "Closed",
      modeTitle: "US regular market session is closed",
    };
  }

  function updateETClock() {
    try {
      const marketClock = getMarketClockState();
      const clock = doc.getElementById("etClock");
      const statusClock = doc.getElementById("marketStatusClock");
      const stateLabelNode = doc.getElementById("marketStatusStateLabel");
      const countdown = doc.getElementById("marketStatusCountdown");
      const mobileSessionLabel = doc.getElementById("mobileMarketSessionLabel");
      const isCompactHeader = window.matchMedia
        ? window.matchMedia("(max-width: 768px)").matches
        : false;
      const compactTime = marketClock.timeLabel.split(":").slice(0, 2).join(":");
      const compactStatus = marketClock.statusText
        .replace(/^Closes in\s+/i, "")
        .replace(/^Opens in\s+/i, "")
        .replace(/^Closed$/i, "closed");
      const compactStatusShort = compactStatus.includes("h")
        ? compactStatus.replace(/\s+\d+m$/i, "")
        : compactStatus;
      if (clock) clock.textContent = isCompactHeader ? compactTime : marketClock.timeLabel;
      if (stateLabelNode) stateLabelNode.textContent = marketClock.stateLabel || "Market";
      if (statusClock) {
        statusClock.classList.remove("is-loading", "is-open", "is-premarket", "is-closed");
        statusClock.classList.add(`is-${marketClock.state}`);
        statusClock.dataset.marketState = marketClock.state;
        statusClock.title = `${marketClock.preciseTimeLabel || marketClock.timeLabel} · ${marketClock.statusText}. ${marketClock.modeTitle}.`;
        if (countdown) {
          countdown.textContent = isCompactHeader
            ? (compactStatusShort !== "closed" && !compactStatusShort.includes("h")
              ? `${compactStatusShort} left`
              : compactStatusShort)
            : marketClock.statusText;
        }
      }
      if (mobileSessionLabel) {
        const mobileSessionState = marketClock.state === "open"
          ? "open"
          : marketClock.state === "premarket"
            ? "premarket"
            : "closed";
        mobileSessionLabel.textContent = mobileSessionState === "open"
          ? "Open"
          : mobileSessionState === "premarket"
            ? "Premarket"
            : "Closed";
        const mobileMeta = mobileSessionLabel.closest(".mobileTopbarMeta");
        if (mobileMeta) mobileMeta.dataset.sessionState = mobileSessionState;
      }
    } catch (err) {
      console.error(err);
    }
  }

  function scheduleETClockTick() {
    window.clearTimeout(clockTimer);
    clockTimer = window.setTimeout(() => {
      updateETClock();
      scheduleETClockTick();
    }, document.visibilityState === "hidden" ? 15000 : 1000);
  }

  function initThemeAndGuided() {
    const fontModeKey = "mc_font_mode";
    const starAnimationKey = "mc_star_animation";
    const fontLabels = {
      clean: "Font: Tech",
      hand: "Font: Tech",
    };
    const starLabels = {
      on: "Star Animation: On",
      off: "Star Animation: Off",
    };
    const reducedMotionQuery = typeof window.matchMedia === "function"
      ? window.matchMedia("(prefers-reduced-motion: reduce)")
      : null;
    const starfieldHost = doc.getElementById("shellStarfield");
    const starfieldCanvas = doc.getElementById("shellStarfieldCanvas");
    const starfieldState = {
      enabled: true,
      active: false,
      stars: [],
      streaks: [],
      frame: 0,
      width: 0,
      height: 0,
      dpr: 1,
      lastTs: 0,
      drawTs: 0,
      resizeTimer: 0,
      ctx: starfieldCanvas ? starfieldCanvas.getContext("2d") : null,
    };

    const syncStarButtons = (enabled) => {
      qsa("[data-star-toggle-label]").forEach((btn) => {
        btn.textContent = enabled ? starLabels.on : starLabels.off;
        const trigger = btn.closest("button");
        if (trigger) trigger.setAttribute("aria-pressed", enabled ? "true" : "false");
      });
    };

    const applyTheme = () => {
      body.setAttribute("data-theme", "cinematic-nebula");
    };

    const syncFontButtons = (mode) => {
      const label = fontLabels[mode] || fontLabels.clean;
      qsa("[data-font-toggle-label]").forEach((btn) => {
        btn.textContent = label;
        btn.setAttribute("aria-pressed", mode === "hand" ? "true" : "false");
      });
    };

    const applyFontMode = (mode) => {
      const normalized = "clean";
      doc.documentElement.setAttribute("data-font-mode", normalized);
      body.setAttribute("data-font-mode", normalized);
      syncFontButtons(normalized);
      storageSet(fontModeKey, normalized);
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

    const reducedMotion = () => !!reducedMotionQuery?.matches;

    const perfMode = () => String(
      docEl.getAttribute("data-perf-mode")
      || storageGet("mc_perf_mode")
      || ""
    ).trim().toLowerCase();

    const lowPerf = () =>
      perfMode() === "low"
      || perfMode() === "low-gpu"
      || docEl.classList.contains("perf-mode-low-gpu");

    const starfieldTheme = () => ({
      field: "rgba(3, 8, 18, 0.18)",
      white: "rgba(244, 250, 255, 0.98)",
      ice: "rgba(164, 232, 255, 0.95)",
      blue: "rgba(84, 170, 255, 0.9)",
      line: "rgba(148, 224, 255, 0.16)",
      streak: "rgba(196, 241, 255, 0.28)",
      tail: "rgba(72, 162, 255, 0.02)",
    });

    const clearStarfield = () => {
      if (!starfieldState.ctx) return;
      starfieldState.ctx.setTransform(1, 0, 0, 1, 0, 0);
      starfieldState.ctx.clearRect(0, 0, starfieldCanvas.width, starfieldCanvas.height);
    };

    const starfieldSize = () => {
      const viewportWidth = Math.max(window.innerWidth || 0, docEl.clientWidth || 0, 320);
      const viewportHeight = Math.max(window.innerHeight || 0, docEl.clientHeight || 0, 320);
      const fieldWidth = Math.ceil(Math.max(
        viewportWidth,
        docEl.scrollWidth || 0,
        body.scrollWidth || 0,
        docEl.offsetWidth || 0,
        body.offsetWidth || 0
      ));
      const fieldHeight = Math.ceil(Math.max(
        viewportHeight,
        docEl.scrollHeight || 0,
        body.scrollHeight || 0,
        docEl.offsetHeight || 0,
        body.offsetHeight || 0
      ));
      return { viewportWidth, viewportHeight, fieldWidth, fieldHeight };
    };

    const syncStarfieldShellSize = () => {
      const { fieldWidth, fieldHeight } = starfieldSize();
      docEl.style.setProperty("--shell-starfield-width", `${fieldWidth}px`);
      docEl.style.setProperty("--shell-starfield-height", `${fieldHeight}px`);
      if (starfieldHost) {
        starfieldHost.style.width = `${fieldWidth}px`;
        starfieldHost.style.height = `${fieldHeight}px`;
      }
      return { fieldWidth, fieldHeight };
    };

    const buildStarfield = () => {
      if (!starfieldCanvas || !starfieldState.ctx) return;
      const { viewportWidth, fieldWidth, fieldHeight } = starfieldSize();
      const dpr = lowPerf() ? 1 : Math.min(window.devicePixelRatio || 1, 1.8);
      starfieldState.width = fieldWidth;
      starfieldState.height = fieldHeight;
      starfieldState.dpr = dpr;
      starfieldCanvas.width = Math.round(fieldWidth * dpr);
      starfieldCanvas.height = Math.round(fieldHeight * dpr);
      syncStarfieldShellSize();
      starfieldCanvas.style.width = `${fieldWidth}px`;
      starfieldCanvas.style.height = `${fieldHeight}px`;
      starfieldState.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      const baseStarCount = lowPerf()
        ? (viewportWidth < 720 ? 70 : 110)
        : (viewportWidth < 720 ? 180 : 300);
      const starCount = lowPerf() ? baseStarCount : Math.round(baseStarCount * 1.35);
      const maxDepth = 3.4;
      starfieldState.stars = Array.from({ length: starCount }, () => {
        const depth = 0.8 + (Math.random() * maxDepth);
        const bright = depth > 2.55 || Math.random() > 0.9;
        const velocity = reducedMotion() ? 0.12 : 0.35 + (depth * 0.48);
        return {
          x: Math.random() * fieldWidth,
          y: Math.random() * fieldHeight,
          depth,
          vx: velocity,
          vy: (Math.random() - 0.5) * (reducedMotion() ? 0.012 : 0.075),
          radius: bright ? 1.4 + (depth * 0.42) : 0.45 + (depth * 0.24),
          alpha: bright ? 0.82 + Math.random() * 0.16 : 0.2 + (depth * 0.14),
          pulse: Math.random() * Math.PI * 2,
          pulseSpeed: reducedMotion() ? 0.006 : 0.018 + Math.random() * 0.028,
          bright,
          tint: bright && Math.random() > 0.56 ? "ice" : (Math.random() > 0.82 ? "blue" : "white"),
        };
      });
      starfieldState.streaks = [];
      clearStarfield();
    };

    const spawnStreak = () => {
      if (reducedMotion()) return;
      const width = starfieldState.width;
      const height = starfieldState.height;
      starfieldState.streaks.push({
        x: -120 - (Math.random() * 120),
        y: (Math.random() * height * 0.52) + (height * 0.08),
        vx: 10 + Math.random() * 7,
        vy: 0.6 + Math.random() * 0.8,
        length: 34 + Math.random() * 42,
        life: 12 + Math.random() * 8,
        ttl: 12 + Math.random() * 8,
      });
      if (starfieldState.streaks.length > 4) starfieldState.streaks.shift();
    };

    const drawStarfield = (timestamp = 0) => {
      if (!starfieldState.active || !starfieldState.ctx) return;
      const ctx = starfieldState.ctx;
      const width = starfieldState.width;
      const height = starfieldState.height;
      const palette = starfieldTheme();
      const gpuQuiet = lowPerf();
      const minFrameMs = gpuQuiet ? 55 : 0;
      if (minFrameMs && starfieldState.drawTs && timestamp - starfieldState.drawTs < minFrameMs) {
        starfieldState.frame = window.requestAnimationFrame(drawStarfield);
        return;
      }
      starfieldState.drawTs = timestamp;
      const dt = starfieldState.lastTs ? Math.min((timestamp - starfieldState.lastTs) / 16.67, 2) : 1;
      starfieldState.lastTs = timestamp;
      ctx.clearRect(0, 0, width, height);
      ctx.fillStyle = palette.field;
      ctx.fillRect(0, 0, width, height);
      ctx.save();
      ctx.globalCompositeOperation = "screen";

      if (!lowPerf() && Math.random() > 0.985) spawnStreak();

      for (let i = 0; i < starfieldState.stars.length; i += 1) {
        const star = starfieldState.stars[i];
        const trail = (gpuQuiet || reducedMotion()) ? 0 : Math.min(2.8, star.depth * 0.8);
        const pulse = 0.78 + Math.sin(star.pulse) * 0.26;
        const radius = star.radius * pulse;
        star.x += star.vx * dt;
        star.y += star.vy * dt;
        star.pulse += star.pulseSpeed * dt;
        if (star.x - trail > width + 24) {
          star.x = -20 - (Math.random() * 80);
          star.y = Math.random() * height;
        }
        if (star.y < -14) star.y = height + 14;
        else if (star.y > height + 14) star.y = -14;

        if (!gpuQuiet && !reducedMotion() && star.depth > 2.1) {
          const gradient = ctx.createLinearGradient(star.x - trail, star.y, star.x, star.y);
          gradient.addColorStop(0, palette.tail);
          gradient.addColorStop(1, palette.line);
          ctx.strokeStyle = gradient;
          ctx.lineWidth = Math.max(0.25, star.depth * 0.2);
          ctx.globalAlpha = Math.min(0.18, star.alpha * 0.12);
          ctx.beginPath();
          ctx.moveTo(star.x - trail, star.y);
          ctx.lineTo(star.x, star.y);
          ctx.stroke();
        }

        ctx.beginPath();
        ctx.fillStyle = palette[star.tint];
        ctx.shadowBlur = gpuQuiet ? 0 : (star.bright ? 18 : 8);
        ctx.shadowColor = gpuQuiet ? "transparent" : (star.tint === "blue" ? palette.blue : palette.ice);
        ctx.globalAlpha = Math.max(0.14, Math.min(1, star.alpha * pulse));
        ctx.arc(star.x, star.y, radius, 0, Math.PI * 2);
        ctx.fill();
      }

      for (let i = starfieldState.streaks.length - 1; i >= 0; i -= 1) {
        const streak = starfieldState.streaks[i];
        streak.x += streak.vx * dt;
        streak.y += streak.vy * dt;
        streak.life -= dt;
        if (streak.life <= 0 || streak.x - streak.length > width + 80 || streak.y > height + 80) {
          starfieldState.streaks.splice(i, 1);
          continue;
        }
        const opacity = Math.max(0, streak.life / streak.ttl) * 0.85;
        const gradient = ctx.createLinearGradient(streak.x - streak.length, streak.y - (streak.length * 0.06), streak.x, streak.y);
        gradient.addColorStop(0, palette.tail);
        gradient.addColorStop(0.45, palette.streak);
        gradient.addColorStop(1, palette.white);
        ctx.strokeStyle = gradient;
        ctx.lineWidth = 0.8;
        ctx.globalAlpha = opacity * 0.7;
        ctx.shadowBlur = 6;
        ctx.shadowColor = palette.ice;
        ctx.beginPath();
        ctx.moveTo(streak.x - streak.length, streak.y - (streak.length * 0.06));
        ctx.lineTo(streak.x, streak.y);
        ctx.stroke();
      }
      ctx.restore();
      ctx.shadowBlur = 0;
      ctx.globalAlpha = 1;
      starfieldState.frame = window.requestAnimationFrame(drawStarfield);
    };

    const stopStarfield = () => {
      starfieldState.active = false;
      starfieldState.lastTs = 0;
      starfieldState.drawTs = 0;
      if (starfieldState.frame) {
        window.cancelAnimationFrame(starfieldState.frame);
        starfieldState.frame = 0;
      }
      clearStarfield();
      body.removeAttribute("data-starfield");
      syncStarfieldShellSize();
      if (starfieldHost) starfieldHost.setAttribute("aria-hidden", "true");
    };

    const startStarfield = () => {
      if (!starfieldState.enabled || !starfieldCanvas || !starfieldState.ctx) return;
      buildStarfield();
      starfieldState.active = true;
      body.setAttribute("data-starfield", "on");
      if (starfieldHost) starfieldHost.setAttribute("aria-hidden", "false");
      if (starfieldState.frame) window.cancelAnimationFrame(starfieldState.frame);
      starfieldState.frame = window.requestAnimationFrame(drawStarfield);
    };

    const scheduleStarfieldRebuild = (delay = 160) => {
      window.clearTimeout(starfieldState.resizeTimer);
      starfieldState.resizeTimer = window.setTimeout(() => {
        if (!starfieldState.enabled || reducedMotion() || doc.visibilityState === "hidden") return;
        const { fieldWidth, fieldHeight } = starfieldSize();
        const heightDelta = Math.abs(fieldHeight - starfieldState.height);
        const widthDelta = Math.abs(fieldWidth - starfieldState.width);
        if (heightDelta > 24 || widthDelta > 8) startStarfield();
      }, delay);
    };

    const syncStarfieldMotionState = () => {
      if (doc.visibilityState === "hidden") {
        stopStarfield();
        return;
      }
      if (reducedMotion()) {
        stopStarfield();
        return;
      }
      startStarfield();
    };

    const setStarAnimationEnabled = (enabled) => {
      starfieldState.enabled = !!enabled;
      storageSet(starAnimationKey, enabled ? "1" : "0");
      syncStarButtons(enabled);
      if (enabled && !reducedMotion() && doc.visibilityState !== "hidden") {
        startStarfield();
      } else {
        stopStarfield();
      }
    };

    window.toggleGuidedMode = () => {
      setGuidedMode(!body.classList.contains("guidedMode"));
    };

    window.toggleAppFont = () => {
      applyFontMode("clean");
    };
    window.toggleStarAnimation = () => {
      setStarAnimationEnabled(!starfieldState.enabled);
    };

    applyTheme();
    applyFontMode(storageGet(fontModeKey) || doc.documentElement.getAttribute("data-font-mode") || "clean");
    const savedGuide = storageGet("mc_guided_mode");
    const firstRunSeen = storageGet("mc_guided_seen");
    if (savedGuide === "1") setGuidedMode(true);
    else if (savedGuide === "0") setGuidedMode(false);
    else setGuidedMode(!firstRunSeen);
    const savedStarAnimation = storageGet(starAnimationKey);
    const lowGpuStarDefaultKey = "mc_star_animation_low_gpu_default_v1";
    const lowGpuDefaultApplied = storageGet(lowGpuStarDefaultKey) === "1";
    if (lowPerf() && !lowGpuDefaultApplied) {
      storageSet(lowGpuStarDefaultKey, "1");
      setStarAnimationEnabled(false);
    } else {
      setStarAnimationEnabled(savedStarAnimation == null ? !lowPerf() : savedStarAnimation !== "0");
    }

    if (reducedMotionQuery && typeof reducedMotionQuery.addEventListener === "function") {
      reducedMotionQuery.addEventListener("change", () => {
        syncStarfieldMotionState();
      });
    }
    window.addEventListener("resize", () => {
      scheduleStarfieldRebuild(120);
    });
    if (typeof ResizeObserver === "function") {
      const resizeObserver = new ResizeObserver(() => {
        scheduleStarfieldRebuild(160);
      });
      resizeObserver.observe(body);
      resizeObserver.observe(docEl);
    }
    window.setTimeout(() => {
      scheduleStarfieldRebuild(0);
    }, 900);
    doc.addEventListener("visibilitychange", syncStarfieldMotionState);
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
      if (scope.dataset.preservePrimary === "true") return;
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
    const revealNodes = qsa([
      ".card",
      ".panel",
      ".metric",
      ".calcCard",
      ".insightCard",
      ".marketPulseSection",
      ".marketPulseExecutionHeroPanel",
    ].join(", "));
    revealNodes.forEach((el, index) => {
      el.style.setProperty("--stagger", String(index % 8));
      if (el.dataset.motionRevealBound === "1") return;
      el.dataset.motionRevealBound = "1";
      if (!window.mcUIFX || !window.mcUIFX.motionAllowed()) return;
      el.classList.add("ui-reveal-ready");
    });
    if (window.mcUIFX && window.mcUIFX.motionAllowed()) {
      window.mcUIFX.revealNodes(revealNodes);
    } else {
      revealNodes.forEach((el) => el.classList.add("ui-reveal-enter"));
    }
  }

  function initMotionFx() {
    const LOW_PERF_VALUES = new Set(["low", "low-gpu"]);
    const motionTimers = new WeakMap();
    const reducedMotionQuery = typeof window.matchMedia === "function"
      ? window.matchMedia("(prefers-reduced-motion: reduce)")
      : null;

    const clearTimer = (node) => {
      const timer = motionTimers.get(node);
      if (timer) {
        window.clearTimeout(timer);
        motionTimers.delete(node);
      }
    };

    const reflow = (node) => {
      void node.offsetWidth;
    };

    const perfMode = () => String(
      docEl.getAttribute("data-perf-mode")
      || storageGet("mc_perf_mode")
      || ""
    ).trim().toLowerCase();

    const isLowPerfMode = () =>
      LOW_PERF_VALUES.has(perfMode())
      || docEl.classList.contains("perf-mode-low-gpu");

    const isReducedMotion = () => !!reducedMotionQuery?.matches;

    const motionAllowed = ({ essential = false } = {}) => {
      if (isReducedMotion()) return false;
      if (!essential && isLowPerfMode()) return false;
      return true;
    };

    const restartAnimation = (node, classNames, duration) => {
      if (!node || !classNames.length) return false;
      clearTimer(node);
      classNames.forEach((className) => node.classList.remove(className));
      reflow(node);
      classNames.forEach((className) => node.classList.add(className));
      const timer = window.setTimeout(() => {
        classNames.forEach((className) => node.classList.remove(className));
        motionTimers.delete(node);
      }, duration);
      motionTimers.set(node, timer);
      return true;
    };

    const flashValue = (node, direction = "neutral", { essential = true, duration = 220 } = {}) => {
      if (!motionAllowed({ essential })) return false;
      const tone = ["up", "down"].includes(direction) ? direction : "neutral";
      return restartAnimation(node, ["price-tick", `is-${tone}`], duration);
    };

    const pulseNode = (node, tone = "neutral", { essential = false, duration = 200 } = {}) => {
      if (!motionAllowed({ essential })) return false;
      const normalizedTone = ["positive", "negative", "warning", "info", "neutral"].includes(tone)
        ? tone
        : "neutral";
      return restartAnimation(node, ["status-pulse", `tone-${normalizedTone}`], duration);
    };

    const revealNodes = (nodes, { essential = false } = {}) => {
      if (!motionAllowed({ essential })) {
        (Array.isArray(nodes) ? nodes : []).forEach((node) => {
          if (!node) return;
          node.classList.remove("ui-reveal-ready");
          node.classList.add("ui-reveal-enter");
        });
        return;
      }
      window.requestAnimationFrame(() => {
        (Array.isArray(nodes) ? nodes : []).forEach((node, index) => {
          if (!node) return;
          node.style.setProperty("--stagger", String(index % 10));
          node.classList.add("ui-reveal-ready");
          window.requestAnimationFrame(() => node.classList.add("ui-reveal-enter"));
        });
      });
    };

    window.mcUIFX = {
      flashValue,
      isLowPerfMode,
      isReducedMotion,
      motionAllowed,
      pulseNode,
      revealNodes,
    };
  }

  function init() {
    initMotionFx();
    initTradingWindowForms();
    initMenus();
    initRowMenus();
    initShortcutsAndActions();
    initThemeAndGuided();
    enforcePrimaryCta();
    setupMobileDensityCompaction();
    initShortcutHelp();
    initCardStagger();
    updateETClock();
    scheduleETClockTick();
    doc.addEventListener("visibilitychange", scheduleETClockTick);
  }

  init();
})();
