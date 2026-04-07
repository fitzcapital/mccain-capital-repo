(function () {
  const root = document.getElementById("tradeReviewForm");
  if (!root || !window.mcTradeReviewAuto) return;

  const scoreInput = document.getElementById("manualGradeScore");
  const letterInput = document.getElementById("manualGradeLetter");
  const classificationInput = document.getElementById("classificationOverride");
  const reasonInput = document.getElementById("gradeOverrideReason");
  const useAutoBtn = document.getElementById("useAutoGradeBtn");
  const setupInput = document.getElementById("reviewSetupInput");
  const setupMirror = document.getElementById("reviewSetupMirror");
  const setupCustom = document.getElementById("reviewSetupCustom");
  const reviewedRiskPercentInput = document.getElementById("reviewedRiskPercentInput");
  const planGradeInput = document.getElementById("planGradeInput");

  const parseTagValue = (value) =>
    String(value || "")
      .split(/[\n,;]+/)
      .map((item) => item.trim())
      .filter(Boolean);

  const serializeTagValue = (items) => Array.from(new Set(items.filter(Boolean))).join(", ");

  const syncSingleChipGroup = (rack) => {
    const targetId = rack.getAttribute("data-chip-single-target");
    const mirrorId = rack.getAttribute("data-chip-mirror-target");
    const target = targetId ? document.getElementById(targetId) : null;
    const mirror = mirrorId ? document.getElementById(mirrorId) : null;
    const buttons = Array.from(rack.querySelectorAll("[data-chip-value]"));
    if (!target || !buttons.length) return;

    const applyState = (value) => {
      buttons.forEach((button) => {
        const selected = button.getAttribute("data-chip-value") === value;
        button.classList.toggle("is-selected", selected);
        if (selected) button.setAttribute("data-selected", "1");
        else button.removeAttribute("data-selected");
      });
    };

    applyState(String(target.value || "").trim());

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        const nextValue = button.getAttribute("data-chip-value") || "";
        const alreadySelected = String(target.value || "").trim() === nextValue;
        target.value = alreadySelected ? "" : nextValue;
        if (mirror) mirror.value = target.value;
        if (setupCustom && !alreadySelected) setupCustom.value = "";
        applyState(target.value.trim());
      });
    });
  };

  const syncMultiChipGroup = (rack) => {
    const targetId = rack.getAttribute("data-chip-multi-target");
    const target = targetId ? document.getElementById(targetId) : null;
    const buttons = Array.from(rack.querySelectorAll("[data-chip-value]"));
    if (!target || !buttons.length) return;

    const applyState = () => {
      const values = new Set(parseTagValue(target.value).map((item) => item.toLowerCase()));
      buttons.forEach((button) => {
        const selected = values.has(String(button.getAttribute("data-chip-value") || "").toLowerCase());
        button.classList.toggle("is-selected", selected);
        if (selected) button.setAttribute("data-selected", "1");
        else button.removeAttribute("data-selected");
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        const rawValue = String(button.getAttribute("data-chip-value") || "").trim();
        if (!rawValue) return;
        const current = parseTagValue(target.value);
        const lowered = current.map((item) => item.toLowerCase());
        const idx = lowered.indexOf(rawValue.toLowerCase());
        if (idx >= 0) current.splice(idx, 1);
        else current.push(rawValue);
        target.value = serializeTagValue(current);
        applyState();
      });
    });

    applyState();
  };

  const syncSegmentedControl = (group) => {
    const selectId = group.getAttribute("data-select-target");
    const select = selectId ? document.getElementById(selectId) : null;
    const buttons = Array.from(group.querySelectorAll("[data-value]"));
    if (!select || !buttons.length) return;

    const applyState = () => {
      const current = String(select.value || "");
      buttons.forEach((button) => {
        button.classList.toggle("is-selected", String(button.getAttribute("data-value") || "") === current);
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        select.value = String(button.getAttribute("data-value") || "");
        applyState();
      });
    });

    applyState();
  };

  const syncFillButtons = (group) => {
    const buttons = Array.from(group.querySelectorAll("[data-fill-target]"));
    if (!buttons.length) return;
    const targetId = buttons[0].getAttribute("data-fill-target");
    const target = targetId ? document.getElementById(targetId) : null;
    if (!target) return;

    const applyState = () => {
      const current = String(target.value || "").trim();
      buttons.forEach((button) => {
        const selected = current === String(button.getAttribute("data-fill-value") || "").trim();
        button.classList.toggle("is-selected", selected);
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        const value = String(button.getAttribute("data-fill-value") || "").trim();
        target.value = value;
        const suggestedPlanGrade = String(button.getAttribute("data-plan-grade") || "").trim();
        if (planGradeInput && suggestedPlanGrade && !String(planGradeInput.value || "").trim()) {
          planGradeInput.value = suggestedPlanGrade;
        }
        applyState();
      });
    });

    target.addEventListener("input", () => {
      const raw = Number(target.value || "");
      if (planGradeInput && !String(planGradeInput.value || "").trim() && Number.isFinite(raw)) {
        if (raw >= 25 || raw === 20) planGradeInput.value = "95";
      }
      applyState();
    });

    if (!String(target.value || "").trim()) {
      const defaultButton = buttons.find((button) => String(button.getAttribute("data-fill-value") || "") === "20");
      if (defaultButton) defaultButton.click();
    } else {
      applyState();
    }
  };

  root.querySelectorAll("[data-chip-single-target]").forEach(syncSingleChipGroup);
  root.querySelectorAll("[data-chip-multi-target]").forEach(syncMultiChipGroup);
  root.querySelectorAll("[data-select-target]").forEach(syncSegmentedControl);
  root.querySelectorAll("[data-fill-group]").forEach(syncFillButtons);

  if (setupCustom && setupInput) {
    setupCustom.addEventListener("input", () => {
      const value = String(setupCustom.value || "").trim();
      setupInput.value = value;
      if (setupMirror) setupMirror.value = value;
      root.querySelectorAll("[data-chip-single-target]").forEach((rack) => {
        rack.querySelectorAll("[data-chip-value]").forEach((button) => {
          const selected = String(button.getAttribute("data-chip-value") || "") === value;
          button.classList.toggle("is-selected", selected);
          if (selected) button.setAttribute("data-selected", "1");
          else button.removeAttribute("data-selected");
        });
      });
    });
  }

  if (scoreInput && letterInput) {
    scoreInput.addEventListener("input", () => {
      const raw = Number(scoreInput.value || 0);
      if (!Number.isFinite(raw)) return;
      if (raw >= 90) letterInput.value = "A";
      else if (raw >= 75) letterInput.value = "B";
      else if (raw >= 60) letterInput.value = "C";
      else if (raw >= 40) letterInput.value = "D";
      else letterInput.value = "F";
    });
  }

  if (useAutoBtn) {
    useAutoBtn.addEventListener("click", () => {
      if (scoreInput) scoreInput.value = "";
      if (letterInput) letterInput.value = "";
      if (classificationInput) classificationInput.value = "";
      if (reasonInput) reasonInput.value = "";
    });
  }
})();
