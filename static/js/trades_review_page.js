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
  const completionCard = document.getElementById("tradesReviewCompletionCard");
  const completionStateNode = document.getElementById("tradesReviewCompletionState");
  const completionPctNode = document.getElementById("tradesReviewCompletionPct");
  const completionLeadNode = document.getElementById("tradesReviewCompletionLead");
  const completionChipNode = document.getElementById("tradesReviewCompletionChip");
  const completionMeterFill = document.getElementById("tradesReviewCompletionMeterFill");
  const completionChecklist = document.getElementById("tradesReviewCompletionChecklist");

  const parseTagValue = (value) =>
    String(value || "")
      .split(/[\n,;]+/)
      .map((item) => item.trim())
      .filter(Boolean);

  const serializeTagValue = (items) => Array.from(new Set(items.filter(Boolean))).join(", ");
  const textPresent = (...values) => values.some((value) => String(value || "").trim().length > 0);
  const selectValue = (name) => {
    const input = root.querySelector(`[name="${name}"]`);
    return input ? String(input.value || "").trim() : "";
  };
  const inputValue = (name) => {
    const input = root.querySelector(`[name="${name}"]`);
    return input ? String(input.value || "").trim() : "";
  };

  const computeCompletion = () => {
    const setupIdentified = textPresent(setupInput?.value) && String(setupInput.value).trim() !== "Unknown";
    const thesisPresent = textPresent(inputValue("thesis_note"));
    const reviewPresent = textPresent(inputValue("review_note"), inputValue("improvement_note"));
    const plannedRisk = inputValue("planned_risk_dollars");
    const reviewedRisk = inputValue("reviewed_risk_dollars");
    const reviewedRiskPct = inputValue("reviewed_risk_percent");
    const riskCaptured = textPresent(plannedRisk, reviewedRisk, reviewedRiskPct);
    const stopCaptured = textPresent(inputValue("reviewed_stop_price"));
    const targetCaptured = textPresent(inputValue("reviewed_target_price"));
    const executionReviewed = textPresent(
      inputValue("entry_quality_note"),
      inputValue("exit_quality_note"),
      inputValue("execution_grade"),
      selectValue("reviewed_execution_quality"),
    );
    const finalGradePresent = true;
    const classificationPresent = true;
    const planVerdictPresent = textPresent(
      inputValue("size_rule_note"),
      selectValue("reviewed_sizing_quality"),
      selectValue("reviewed_stop_discipline"),
      selectValue("reviewed_within_plan"),
    );
    const items = [
      { key: "setup", label: "Setup identified", done: setupIdentified },
      { key: "thesis", label: "Thesis present", done: thesisPresent },
      { key: "reflection", label: "Review reflection present", done: reviewPresent },
      { key: "risk", label: "Risk captured", done: riskCaptured },
      { key: "stop", label: "Stop captured", done: stopCaptured },
      { key: "target", label: "Target captured", done: targetCaptured },
      { key: "execution", label: "Execution reviewed", done: executionReviewed },
      { key: "final_grade", label: "Final grade present", done: finalGradePresent },
      { key: "classification", label: "Classification present", done: classificationPresent },
      { key: "plan_verdict", label: "Sizing / plan verdict present", done: planVerdictPresent },
    ];
    const pct = Math.round((items.filter((item) => item.done).length / items.length) * 100);
    let label = "Not Reviewed";
    let tone = "warn";
    if (pct >= 84) {
      label = "Fully Reviewed";
      tone = "positive";
    } else if (pct >= 42) {
      label = "Partially Reviewed";
      tone = "info";
    }
    const shortMap = {
      "Setup identified": "setup",
      "Thesis present": "thesis",
      "Review reflection present": "review note",
      "Risk captured": "risk",
      "Stop captured": "stop",
      "Target captured": "target",
      "Execution reviewed": "execution review",
      "Final grade present": "final grade",
      "Classification present": "classification",
      "Sizing / plan verdict present": "plan verdict",
    };
    const missing = items.filter((item) => !item.done).map((item) => shortMap[item.label] || item.label.toLowerCase());
    const missingSummary = missing.length
      ? `Missing ${missing.slice(0, 2).join(" + ")}${missing.length > 2 ? ` + ${missing.length - 2} more` : ""}`
      : "All core review checks logged.";
    return { items, pct, label, tone, missingSummary };
  };

  const renderCompletion = () => {
    if (!completionCard || !completionChecklist) return;
    const model = computeCompletion();
    completionCard.classList.remove("tradesReviewCompletionCard-positive", "tradesReviewCompletionCard-info", "tradesReviewCompletionCard-warn");
    completionCard.classList.add(`tradesReviewCompletionCard-${model.tone}`);
    completionCard.dataset.reviewCompletionPct = String(model.pct);
    completionCard.dataset.reviewStateLabel = model.label;
    if (completionStateNode) completionStateNode.textContent = model.label;
    if (completionPctNode) completionPctNode.textContent = `${model.pct}%`;
    if (completionLeadNode) completionLeadNode.textContent = model.missingSummary;
    if (completionChipNode) {
      completionChipNode.textContent = model.label;
      completionChipNode.classList.remove("tradeReviewState-positive", "tradeReviewState-info", "tradeReviewState-warn", "tradeReviewState-negative");
      completionChipNode.classList.add(`tradeReviewState-${model.tone}`);
    }
    if (completionMeterFill) completionMeterFill.style.width = `${model.pct}%`;
    model.items.forEach((item) => {
      const node = completionChecklist.querySelector(`[data-review-check="${item.key}"]`);
      if (!node) return;
      node.classList.toggle("is-done", item.done);
      node.classList.toggle("is-missing", !item.done);
    });
  };

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
      renderCompletion();
    });
  }

  root.addEventListener("input", renderCompletion);
  root.addEventListener("change", renderCompletion);
  root.addEventListener("click", () => {
    window.requestAnimationFrame(renderCompletion);
  });
  renderCompletion();
})();
