(function initSelfControlConsole() {
  const root = document.querySelector("[data-self-control-console]");
  if (!root) return;

  const startModal = document.getElementById("selfControlStartModal");
  const cancelModal = document.getElementById("selfControlCancelModal");
  const startCopy = startModal?.querySelector("[data-self-control-start-copy]");
  const cancelText = cancelModal?.querySelector("[data-self-control-cancel-text]");
  const countdownValue = root.querySelector("[data-self-control-countdown-value]");
  const countdownNode = root.querySelector("[data-self-control-countdown]");

  let pendingStartForm = null;
  let pendingCancelForm = null;

  const openModal = (node) => {
    if (!node) return;
    node.classList.add("is-open");
    node.setAttribute("aria-hidden", "false");
    document.body.classList.add("modalOpen");
  };

  const closeModal = (node) => {
    if (!node) return;
    node.classList.remove("is-open");
    node.setAttribute("aria-hidden", "true");
    if (!document.querySelector(".selfControlModal.is-open")) {
      document.body.classList.remove("modalOpen");
    }
  };

  const closeAllModals = () => {
    closeModal(startModal);
    closeModal(cancelModal);
    pendingStartForm = null;
    pendingCancelForm = null;
    if (cancelText) cancelText.value = "";
  };

  const formatDuration = (secondsRemaining) => {
    const safe = Math.max(0, secondsRemaining);
    const hours = Math.floor(safe / 3600);
    const minutes = Math.floor((safe % 3600) / 60);
    const seconds = safe % 60;
    if (hours > 0) {
      return [hours, minutes, seconds].map((value) => String(value).padStart(2, "0")).join(":");
    }
    return [minutes, seconds].map((value) => String(value).padStart(2, "0")).join(":");
  };

  const tickCountdown = () => {
    if (!countdownNode || !countdownValue) return;
    if (countdownNode.dataset.status !== "active") return;
    const targetRaw = String(countdownNode.dataset.endAt || "");
    const target = Date.parse(targetRaw);
    if (Number.isNaN(target)) {
      countdownValue.textContent = "--:--";
      return;
    }
    const remaining = Math.max(0, Math.floor((target - Date.now()) / 1000));
    countdownValue.textContent = formatDuration(remaining);
  };

  const refreshState = async () => {
    try {
      const response = await fetch("/api/self-control/state", { cache: "no-store" });
      const payload = await response.json();
      if (!payload || payload.ok === false) return;
      const session = payload.session || null;
      if (!countdownNode || !countdownValue) return;
      if (!session) {
        if (countdownNode.dataset.sessionId) window.location.reload();
        return;
      }
      const incomingId = String(session.id || "");
      const currentId = String(countdownNode.dataset.sessionId || "");
      if (incomingId !== currentId || String(session.status || "") !== String(countdownNode.dataset.status || "")) {
        window.location.reload();
        return;
      }
      countdownNode.dataset.endAt = String(session.planned_end_at || "");
      countdownNode.dataset.status = String(session.status || "");
      tickCountdown();
    } catch (_err) {
    }
  };

  root.querySelectorAll("[data-self-control-open-start]").forEach((button) => {
    button.addEventListener("click", () => {
      const form = button.closest("form");
      if (!form) return;
      pendingStartForm = form;
      const label = String(button.dataset.sessionLabel || "this focus session").trim();
      if (startCopy) {
        startCopy.textContent = `Start ${label} and activate the current discipline lock?`;
      }
      openModal(startModal);
    });
  });

  root.querySelectorAll("[data-self-control-open-cancel]").forEach((button) => {
    button.addEventListener("click", () => {
      const form = button.closest("form");
      if (!form) return;
      pendingCancelForm = form;
      openModal(cancelModal);
      window.setTimeout(() => cancelText?.focus(), 30);
    });
  });

  document.querySelectorAll("[data-self-control-modal-close]").forEach((button) => {
    button.addEventListener("click", closeAllModals);
  });

  startModal?.querySelector("[data-self-control-confirm-start]")?.addEventListener("click", () => {
    if (!pendingStartForm) return;
    const hidden = pendingStartForm.querySelector("[data-self-control-start-confirmed]");
    if (hidden) hidden.value = "1";
    pendingStartForm.submit();
  });

  cancelModal?.querySelector("[data-self-control-confirm-cancel]")?.addEventListener("click", () => {
    if (!pendingCancelForm || !cancelText) return;
    const reason = String(cancelText.value || "").trim();
    if (!reason) {
      cancelText.focus();
      cancelText.setAttribute("aria-invalid", "true");
      return;
    }
    const hidden = pendingCancelForm.querySelector("[data-self-control-cancel-reason]");
    if (hidden) hidden.value = reason;
    pendingCancelForm.submit();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeAllModals();
  });

  tickCountdown();
  if (countdownNode) {
    window.setInterval(tickCountdown, 1000);
    window.setInterval(refreshState, 30000);
  }
})();
