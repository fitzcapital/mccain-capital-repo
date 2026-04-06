(function () {
  const root = document.getElementById("tradeReviewForm");
  if (!root || !window.mcTradeReviewAuto) return;

  const scoreInput = document.getElementById("manualGradeScore");
  const letterInput = document.getElementById("manualGradeLetter");
  const classificationInput = document.getElementById("classificationOverride");
  const reasonInput = document.getElementById("gradeOverrideReason");
  const useAutoBtn = document.getElementById("useAutoGradeBtn");

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
