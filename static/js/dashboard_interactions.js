(() => {
  "use strict";

  const focusableSelector = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "[tabindex]:not([tabindex='-1'])",
  ].join(",");

  let activeSurface = null;
  let activeInvoker = null;

  const isNativeDialog = (surface) => (
    surface instanceof HTMLDialogElement && typeof surface.showModal === "function"
  );

  const surfaceIsOpen = (surface) => Boolean(
    surface && (surface.open || surface.dataset.surfaceOpen === "1" || !surface.hidden)
  );

  const focusableWithin = (surface) => Array.from(surface.querySelectorAll(focusableSelector))
    .filter((node) => !node.hidden && node.getAttribute("aria-hidden") !== "true");

  const closeSurface = (surface = activeSurface, { restoreFocus = true } = {}) => {
    if (!surface) return;
    if (isNativeDialog(surface) && surface.open) {
      surface.close();
    } else {
      surface.hidden = true;
    }
    surface.dataset.surfaceOpen = "0";
    document.body.classList.remove("dashboardSurfaceOpen", "modalOpen");
    const invoker = activeInvoker;
    if (surface === activeSurface) {
      activeSurface = null;
      activeInvoker = null;
    }
    if (restoreFocus && invoker && document.contains(invoker)) {
      window.setTimeout(() => invoker.focus(), 0);
    }
    surface.dispatchEvent(new CustomEvent("dashboard:surface-closed", { bubbles: true }));
  };

  const openSurface = (target, invoker = document.activeElement) => {
    const surface = typeof target === "string" ? document.getElementById(target) : target;
    if (!surface) return false;
    if (activeSurface && activeSurface !== surface) closeSurface(activeSurface, { restoreFocus: false });
    activeSurface = surface;
    activeInvoker = invoker instanceof HTMLElement ? invoker : null;
    surface.hidden = false;
    surface.dataset.surfaceOpen = "1";
    document.body.classList.add("dashboardSurfaceOpen", "modalOpen");
    if (isNativeDialog(surface) && !surface.open) surface.showModal();
    const initial = surface.querySelector("[data-surface-initial-focus]") || focusableWithin(surface)[0];
    window.setTimeout(() => initial?.focus(), 0);
    surface.dispatchEvent(new CustomEvent("dashboard:surface-opened", { bubbles: true }));
    return true;
  };

  const bindSurface = (surface) => {
    if (!surface || surface.dataset.surfaceBound === "1") return;
    surface.dataset.surfaceBound = "1";
    surface.addEventListener("cancel", (event) => {
      event.preventDefault();
      closeSurface(surface);
    });
    surface.addEventListener("click", (event) => {
      if (event.target === surface || event.target.closest("[data-dashboard-surface-close]")) {
        closeSurface(surface);
      }
    });
  };

  const bindTriggers = (root = document) => {
    root.querySelectorAll("[data-dashboard-surface]").forEach(bindSurface);
    root.querySelectorAll("[data-dashboard-surface-open]").forEach((trigger) => {
      if (trigger.dataset.surfaceTriggerBound === "1") return;
      trigger.dataset.surfaceTriggerBound = "1";
      trigger.addEventListener("click", (event) => {
        const id = String(trigger.dataset.dashboardSurfaceOpen || "");
        if (!id) return;
        event.preventDefault();
        openSurface(id, trigger);
      });
    });
  };

  document.addEventListener("keydown", (event) => {
    if (!activeSurface || !surfaceIsOpen(activeSurface)) return;
    if (event.key === "Escape") {
      event.preventDefault();
      closeSurface(activeSurface);
      return;
    }
    if (event.key !== "Tab") return;
    const nodes = focusableWithin(activeSurface);
    if (!nodes.length) {
      event.preventDefault();
      return;
    }
    const first = nodes[0];
    const last = nodes[nodes.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  });

  const operationStatus = (() => {
    const root = document.getElementById("dashboardOperationStatus");
    const text = root?.querySelector("[data-operation-text]");
    const retry = root?.querySelector("[data-operation-retry]");
    let retryHandler = null;
    const render = (state, message, options = {}) => {
      if (!root || !text) return;
      root.hidden = false;
      root.dataset.state = state;
      text.textContent = message;
      retryHandler = typeof options.retry === "function" ? options.retry : null;
      if (retry) retry.hidden = !retryHandler;
      if (state === "success") {
        window.setTimeout(() => {
          if (root.dataset.state === "success") root.hidden = true;
        }, 5000);
      }
    };
    retry?.addEventListener("click", () => retryHandler?.());
    return {
      loading: (message) => render("loading", message),
      success: (message) => render("success", `${message} · ${new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`),
      stale: (message, retryAction) => render("stale", message, { retry: retryAction }),
      error: (message, retryAction) => render("error", message, { retry: retryAction }),
      idle: () => { if (root) root.hidden = true; },
    };
  })();

  const markOperation = (control, message) => {
    if (!control || control.dataset.operationActive === "1") return false;
    control.dataset.operationActive = "1";
    control.classList.add("is-loading");
    control.setAttribute("aria-busy", "true");
    operationStatus.loading(message);
    return true;
  };

  const finishOperation = (control, state, message, retry) => {
    if (control) {
      delete control.dataset.operationActive;
      control.classList.remove("is-loading");
      control.setAttribute("aria-busy", "false");
    }
    operationStatus[state]?.(message, retry);
  };

  const bindOperations = () => {
    const controls = [
      [document.getElementById("dashboardPlanningRefreshBtn"), "Refreshing planning context"],
      [document.getElementById("dashboardTapeRefreshBtn"), "Refreshing market tape"],
      [document.querySelector("[data-dashboard-sync-run]"), "Running live upload sync"],
      [document.getElementById("dashboardDriftRefreshBtn"), "Recomputing balances"],
    ];
    controls.forEach(([control, message]) => {
      if (!control || control.dataset.operationBound === "1") return;
      control.dataset.operationBound = "1";
      control.dataset.dashboardOperation = "1";
      control.addEventListener("click", () => markOperation(control, message), { capture: true });
    });
    document.addEventListener("dashboard:tape-state", (event) => {
      const control = document.getElementById("dashboardTapeRefreshBtn");
      if (control?.dataset.operationActive === "1") {
        finishOperation(control, "success", "Market tape updated");
      }
      if (event.detail?.state === "stale") operationStatus.stale("Market tape is stale. Confirm the source before acting.");
    });
    document.addEventListener("dashboard:operation-result", (event) => {
      const detail = event.detail || {};
      const control = detail.controlId ? document.getElementById(detail.controlId) : null;
      // Planning and tape refreshes also hydrate in the background. Only announce
      // results for a user-started operation so passive polling does not create
      // misleading success or failure banners.
      if (detail.controlId && control?.dataset.operationActive !== "1" && detail.force !== true) return;
      const state = ["success", "stale", "error"].includes(detail.state) ? detail.state : "success";
      finishOperation(control, state, detail.message || "Dashboard operation complete", detail.retry);
    });
  };

  const bindBrokerForms = () => {
    document.querySelectorAll("[data-dashboard-broker-form]").forEach((form) => {
      if (form.dataset.brokerFormBound === "1") return;
      form.dataset.brokerFormBound = "1";
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const button = form.querySelector("button[type='submit']");
        const kind = String(form.dataset.dashboardBrokerForm || "manual");
        const labels = {
          manual: "Saving manual broker values",
          refresh: "Refreshing broker diagnostics",
          seed: "Seeding headed dashboard session",
        };
        if (!markOperation(button, labels[kind] || "Updating broker metrics")) return;
        if (button) button.disabled = true;
        try {
          const response = await fetch(form.action, {
            method: "POST",
            credentials: "same-origin",
            headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest" },
            body: new FormData(form),
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok || payload.ok === false) {
            throw new Error(String(payload.message || "Broker operation failed."));
          }
          finishOperation(button, "success", String(payload.message || "Broker metrics updated"));
          window.setTimeout(() => window.location.reload(), 650);
        } catch (error) {
          const message = error instanceof Error ? error.message : "Broker operation failed.";
          finishOperation(button, "error", `${message} Manual values were preserved.`, () => form.requestSubmit());
          if (button) button.disabled = false;
        }
      });
    });
  };

  const pressureCheck = (() => {
    const surface = document.getElementById("dashboardResetModal");
    const category = document.getElementById("dashboardResetTriggerCategory");
    const note = document.getElementById("dashboardResetNote");
    const duration = document.getElementById("dashboardResetDuration");
    const timerNode = document.getElementById("dashboardResetTimerValue");
    const timerButton = document.getElementById("dashboardResetTimerStart");
    const checks = Array.from(document.querySelectorAll("[data-reset-check]"));
    const feedback = document.getElementById("dashboardResetFeedback");
    let timerId = 0;
    let remaining = Number(duration?.value || 60);
    let timerComplete = false;

    const renderTimer = () => {
      if (!timerNode) return;
      const minutes = Math.floor(remaining / 60);
      const seconds = remaining % 60;
      timerNode.textContent = `${minutes}:${String(seconds).padStart(2, "0")}`;
      timerNode.dataset.complete = timerComplete ? "1" : "0";
    };
    const stopTimer = () => {
      if (timerId) window.clearInterval(timerId);
      timerId = 0;
    };
    const resetTimer = () => {
      stopTimer();
      remaining = Number(duration?.value || 60);
      timerComplete = false;
      if (timerButton) timerButton.textContent = "Start reset";
      renderTimer();
    };
    const startTimer = () => {
      if (timerId || timerComplete) return;
      if (timerButton) timerButton.textContent = "Reset running";
      timerId = window.setInterval(() => {
        remaining = Math.max(0, remaining - 1);
        if (!remaining) {
          stopTimer();
          timerComplete = true;
          if (timerButton) timerButton.textContent = "Reset complete";
        }
        renderTimer();
      }, 1000);
    };
    const setFeedback = (message) => {
      if (!feedback) return;
      feedback.textContent = message;
      feedback.hidden = !message;
    };
    const validateProceed = () => {
      if (!category?.value) {
        setFeedback("Name the pressure trigger before proceeding.");
        category?.focus();
        return false;
      }
      if (!timerComplete) {
        setFeedback("Complete the reset timer before proceeding aligned.");
        timerButton?.focus();
        return false;
      }
      const missing = checks.find((check) => !check.checked);
      if (missing) {
        setFeedback("Confirm setup, confirmation, rules, stop, and risk before proceeding.");
        missing.focus();
        return false;
      }
      setFeedback("");
      return true;
    };
    const syncReflection = () => {
      const reflection = document.getElementById("dashboardReflectionUrgency");
      if (!reflection || !category?.value) return;
      const categoryLabel = category.options[category.selectedIndex]?.text || category.value;
      reflection.value = [categoryLabel, note?.value.trim()].filter(Boolean).join(": ").slice(0, 220);
      reflection.dispatchEvent(new Event("input", { bubbles: true }));
      reflection.dispatchEvent(new Event("change", { bubbles: true }));
    };
    timerButton?.addEventListener("click", startTimer);
    duration?.addEventListener("change", resetTimer);
    surface?.addEventListener("dashboard:surface-opened", resetTimer);
    surface?.addEventListener("dashboard:surface-closed", stopTimer);
    document.addEventListener("click", (event) => {
      const action = event.target.closest("[data-reset-action]");
      if (!action) return;
      syncReflection();
    }, { capture: true });
    renderTimer();
    return { validateProceed, syncReflection };
  })();

  const commandPalette = (() => {
    const surface = document.getElementById("dashboardCommandPalette");
    const input = document.getElementById("dashboardCommandSearch");
    const list = document.getElementById("dashboardCommandResults");
    const empty = document.getElementById("dashboardCommandEmpty");
    let activeIndex = 0;
    let visibleCommands = [];
    const commands = [
      { label: "Run Pressure Check", detail: "Reset urgency and confirm alignment", keywords: "reset pressure urgent discipline", action: () => document.getElementById("dashboardResetTrigger")?.click() },
      { label: "Open Market Pulse", detail: "Inspect gamma levels and the active playbook", keywords: "market pulse gamma ladder", href: "/market-pulse" },
      { label: "Record a Trade", detail: "Create a new trade entry", keywords: "trade add new", href: "/trades/new" },
      { label: "Import Statement", detail: "Upload or synchronize broker activity", keywords: "import upload statement broker", href: "/trades/upload/statement?ws=live" },
      { label: "Start Journal Entry", detail: "Capture the current session", keywords: "journal note reflection", href: "/journal/new" },
      { label: "Refresh Planning", detail: "Refresh gamma and planning context", keywords: "refresh plan gamma", available: () => Boolean(document.getElementById("dashboardPlanningRefreshBtn")), unavailable: "Planning refresh is not available in this view.", action: () => document.getElementById("dashboardPlanningRefreshBtn")?.click() },
      { label: "Refresh Market Tape", detail: "Request the latest tape state", keywords: "refresh tape market", available: () => Boolean(document.getElementById("dashboardTapeRefreshBtn")), unavailable: "Market tape refresh is not available in this view.", action: () => document.getElementById("dashboardTapeRefreshBtn")?.click() },
      { label: "Manage Broker Metrics", detail: "Review manual values, sources, and diagnostics", keywords: "broker equity drawdown metrics", available: () => Boolean(document.getElementById("dashboardBrokerDrawer")), unavailable: "Select an account before managing broker metrics.", action: () => document.querySelector("[data-dashboard-surface-open='dashboardBrokerDrawer']")?.click() },
      { label: "Review Behavior", detail: "Open the behavior analytics heatmap", keywords: "behavior analytics review", href: "/analytics?tab=behavior" },
    ];
    const isEditable = (node) => node instanceof HTMLElement && (
      node.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(node.tagName)
    );
    const execute = (command) => {
      if (!command) return;
      if (command.available && !command.available()) {
        operationStatus.stale(command.unavailable || "That action is unavailable right now.");
        return;
      }
      closeSurface(surface, { restoreFocus: false });
      if (command.href) window.location.assign(command.href);
      else command.action?.();
    };
    const render = () => {
      if (!list) return;
      const query = String(input?.value || "").trim().toLowerCase();
      visibleCommands = commands.filter((command) => `${command.label} ${command.detail} ${command.keywords}`.toLowerCase().includes(query));
      activeIndex = Math.min(activeIndex, Math.max(0, visibleCommands.length - 1));
      list.innerHTML = "";
      visibleCommands.forEach((command, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "dashboardCommandResult";
        button.setAttribute("role", "option");
        button.setAttribute("aria-selected", index === activeIndex ? "true" : "false");
        const available = !command.available || command.available();
        button.setAttribute("aria-disabled", available ? "false" : "true");
        button.innerHTML = `<span><strong>${command.label}</strong><small>${command.detail}</small></span><span aria-hidden="true">→</span>`;
        button.addEventListener("mouseenter", () => { activeIndex = index; render(); });
        button.addEventListener("click", () => execute(command));
        list.appendChild(button);
      });
      if (empty) empty.hidden = visibleCommands.length > 0;
      input?.setAttribute("aria-activedescendant", visibleCommands.length ? `dashboard-command-${activeIndex}` : "");
      Array.from(list.children).forEach((node, index) => { node.id = `dashboard-command-${index}`; });
    };
    input?.addEventListener("input", () => { activeIndex = 0; render(); });
    input?.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown") activeIndex = Math.min(activeIndex + 1, visibleCommands.length - 1);
      else if (event.key === "ArrowUp") activeIndex = Math.max(activeIndex - 1, 0);
      else if (event.key === "Enter") return execute(visibleCommands[activeIndex]);
      else return;
      event.preventDefault();
      render();
    });
    document.addEventListener("keydown", (event) => {
      if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "k" || isEditable(event.target)) return;
      event.preventDefault();
      openSurface(surface, document.querySelector("[data-dashboard-surface-open='dashboardCommandPalette']"));
    });
    surface?.addEventListener("dashboard:surface-opened", () => {
      if (input) input.value = "";
      activeIndex = 0;
      render();
    });
    render();
    return { render };
  })();

  const setForwardPaceStatus = (card, state, message) => {
    const status = card?.querySelector("[data-forward-pace-status]");
    if (!status) return;
    status.dataset.state = state;
    status.textContent = message;
  };

  document.addEventListener("submit", async (event) => {
    const form = event.target.closest?.("[data-forward-pace-form]");
    if (!form || form.dataset.submitting === "1") return;
    event.preventDefault();

    const card = form.closest("[data-forward-pace-card]");
    if (!card) return;
    const submitter = event.submitter;
    const formData = new FormData(form);
    if (submitter?.name) formData.set(submitter.name, submitter.value);
    const buttons = Array.from(form.querySelectorAll("[data-forward-pace-submit]"));
    const scrollTop = window.scrollY;
    const settingsWereOpen = Boolean(form.closest("details")?.open);
    const csrfToken = document.querySelector('meta[name="csrf-token"]')?.content || "";

    form.dataset.submitting = "1";
    buttons.forEach((button) => { button.disabled = true; });
    setForwardPaceStatus(card, "pending", "Saving projection…");

    try {
      const response = await fetch(form.action, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Accept": "application/json",
          "X-CSRF-Token": csrfToken,
          "X-Dashboard-Partial": "forward-pace",
        },
        body: formData,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok !== true || !String(payload.fragment || "").trim()) {
        throw new Error(payload.error || "The projection could not be saved.");
      }

      const template = document.createElement("template");
      template.innerHTML = String(payload.fragment).trim();
      const replacement = template.content.querySelector("[data-forward-pace-card]");
      if (!replacement) throw new Error("The updated projection was incomplete.");
      const replacementDetails = replacement.querySelector("details");
      if (replacementDetails) replacementDetails.open = settingsWereOpen;
      card.replaceWith(replacement);
      window.scrollTo(0, scrollTop);
      setForwardPaceStatus(replacement, "success", payload.message || "Projection updated.");
    } catch (error) {
      setForwardPaceStatus(
        card,
        "error",
        error?.message || "The projection could not be saved. Try again."
      );
      form.dataset.submitting = "0";
      buttons.forEach((button) => { button.disabled = false; });
    }
  });

  const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => mutation.addedNodes.forEach((node) => {
      if (node instanceof HTMLElement) bindTriggers(node);
    }));
  });

  bindTriggers(document);
  bindOperations();
  bindBrokerForms();
  observer.observe(document.body, { childList: true, subtree: true });

  window.dashboardSurfaces = { open: openSurface, close: closeSurface, init: bindTriggers };
  window.dashboardOperations = operationStatus;
  window.dashboardPressureCheck = pressureCheck;
  window.dashboardCommandPalette = commandPalette;
})();
