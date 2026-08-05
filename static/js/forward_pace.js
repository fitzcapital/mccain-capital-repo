(function () {
  const app = document.getElementById("forwardPaceApp");
  if (!app) return;

  const form = document.getElementById("forwardPaceForm");
  const pdfButton = document.getElementById("forwardPacePdfButton");
  const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") || "";
  let latestPayload = null;
  let latestProjection = null;
  let refreshTimer = 0;

  const money = (value) => Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  const setText = (id, value) => {
    const node = document.getElementById(id);
    if (node) node.textContent = value;
  };

  function readPayload() {
    const data = new FormData(form);
    return Object.fromEntries(data.entries());
  }

  async function postJson(path, payload) {
    const response = await fetch(path, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
      body: JSON.stringify(payload),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body.ok === false) throw new Error(body.error || "Forward pace request failed.");
    return body;
  }

  function renderProjection(projection) {
    latestProjection = projection;
    const weeklyTax = Number(projection.weekly.federal_tax || 0) + Number(projection.weekly.state_tax || 0);
    const totalNet = Number(projection.totals.net || 0);
    const balanceTone = totalNet >= 0 ? "positive" : "negative";
    const projectionEnd = projection.schedule?.length ? projection.schedule[projection.schedule.length - 1].end : projection.inputs.start_date;
    setText("forwardPaceHeadline", `${money(projection.weekly.net)} / week`);
    setText("forwardPaceWeeklyNetHero", money(projection.weekly.net));
    setText("forwardPaceWeeklyNetSignal", money(projection.weekly.net));
    setText("forwardPaceProjectedBalance", money(projection.totals.projected_balance));
    setText("forwardPaceProjectedProfit", money(totalNet));
    setText("forwardPaceProjectedBalanceMeta", `${money(projection.inputs.base_balance)} base + net pace projection.`);
    setText("forwardPaceProjectedProfitMeta", `${projection.inputs.weeks} weeks of projected net after tax and buffer.`);
    setText(
      "forwardPaceProjectedMeta",
      `${projection.inputs.weeks} weeks · ${projection.inputs.state} · ${projection.inputs.payouts_per_week} payouts/week`
    );
    setText("forwardPaceProjectionHorizon", `${projection.inputs.weeks} weeks`);
    setText("forwardPaceRunwayLabel", `${projection.inputs.weeks} trading weeks`);
    setText("forwardPaceProjectionEnd", projectionEnd || "--");
    setText("forwardPaceProjectionEndSignal", projectionEnd || "--");
    setText("forwardPaceTrajectoryEnd", money(projection.totals.projected_balance));
    setText("forwardPaceWeeklyGross", money(projection.weekly.gross));
    setText("forwardPaceWeeklyTax", money(weeklyTax));
    setText("forwardPaceWeeklyBuffer", money(projection.weekly.buffer));
    setText("forwardPaceWeeklyNet", money(projection.weekly.net));
    setText("forwardPaceFederalTax", money(projection.tax.federal_annual));
    setText("forwardPaceStateRate", `${Number(projection.tax.state_rate || 0).toFixed(2)}%`);
    setText("forwardPaceEffectiveTax", `${Number(projection.tax.effective_tax_rate || 0).toFixed(2)}%`);
    const profitMetric = document.getElementById("forwardPaceProfitMetric");
    if (profitMetric) profitMetric.dataset.tone = balanceTone;
    const balanceStage = document.getElementById("forwardPaceBalanceStage");
    if (balanceStage) balanceStage.dataset.tone = balanceTone;
    const commandBoard = document.getElementById("forwardPaceCommandBoard");
    if (commandBoard) commandBoard.dataset.tone = balanceTone;
    const trajectoryCard = document.getElementById("forwardPaceTrajectoryCard");
    if (trajectoryCard) trajectoryCard.dataset.tone = balanceTone;

    const schedule = document.getElementById("forwardPaceSchedule");
    if (schedule) {
      schedule.innerHTML = projection.schedule.map((row) => `
      <div class="forwardPaceWeek">
        <div>
          <span>Week ${row.week}</span>
          <strong>${row.start} → ${row.end}</strong>
        </div>
        <div><span>Gross</span><strong>${money(row.gross)}</strong></div>
        <div><span>Tax</span><strong>${money(Number(row.federal_tax || 0) + Number(row.state_tax || 0))}</strong></div>
        <div><span>Buffer</span><strong>${money(row.buffer)}</strong></div>
        <div><span>Net</span><strong>${money(row.net)}</strong></div>
        <div><span>Balance</span><strong>${money(row.projected_balance)}</strong></div>
      </div>
      `).join("");
    }
    renderTrajectory(projection.schedule || []);
  }

  function renderTrajectory(schedule) {
    const node = document.getElementById("forwardPaceTrajectory");
    if (!node) return;
    if (!Array.isArray(schedule) || !schedule.length) {
      node.innerHTML = "";
      return;
    }
    const balances = schedule.map((row) => Number(row.projected_balance || 0));
    const min = Math.min(...balances);
    const max = Math.max(...balances);
    const span = Math.max(1, max - min);
    const points = schedule.map((row, idx) => {
      const x = schedule.length === 1 ? 50 : (idx / (schedule.length - 1)) * 100;
      const y = 100 - ((Number(row.projected_balance || 0) - min) / span) * 100;
      return { x, y, label: `W${row.week}`, balance: money(row.projected_balance) };
    });
    const polyline = points.map((point) => `${point.x},${point.y}`).join(" ");
    node.innerHTML = `
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
        <defs>
          <linearGradient id="forwardPaceTrajectoryFill" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="rgba(94,220,255,0.24)"></stop>
            <stop offset="100%" stop-color="rgba(97,255,184,0.32)"></stop>
          </linearGradient>
          <linearGradient id="forwardPaceTrajectoryStroke" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#63d7ff"></stop>
            <stop offset="100%" stop-color="#7bffbf"></stop>
          </linearGradient>
        </defs>
        <polyline class="forwardPaceTrajectoryLine" points="${polyline}"></polyline>
      </svg>
      <div class="forwardPaceTrajectoryDots">
        ${points.map((point, idx) => `
          <div class="forwardPaceTrajectoryDot${idx === points.length - 1 ? " is-final" : ""}" style="left:${point.x}%;" title="${point.label} · ${point.balance}">
            <span>${point.label}</span>
          </div>
        `).join("")}
      </div>
    `;
  }

  async function refreshProjection() {
    latestPayload = readPayload();
    const payload = await postJson("/api/forward-pace/projection", latestPayload);
    renderProjection(payload.projection);
  }

  function scheduleRefresh() {
    window.clearTimeout(refreshTimer);
    refreshTimer = window.setTimeout(() => {
      refreshProjection().catch((error) => {
        setText("forwardPaceProjectedMeta", error.message || "Projection unavailable.");
      });
    }, 120);
  }

  form?.addEventListener("input", scheduleRefresh);
  form?.addEventListener("change", scheduleRefresh);

  pdfButton?.addEventListener("click", async () => {
    const payload = latestPayload || readPayload();
    pdfButton.disabled = true;
    try {
      if (!latestProjection) await refreshProjection();
      const response = await fetch("/forward-pace/pdf", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
        body: JSON.stringify(payload),
      });
      if (!response.ok) throw new Error("PDF export failed.");
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "forward-pace-projection.pdf";
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      setText("forwardPaceProjectedMeta", error.message || "Could not download PDF.");
    } finally {
      pdfButton.disabled = false;
    }
  });

  refreshProjection().catch(() => {});
})();
