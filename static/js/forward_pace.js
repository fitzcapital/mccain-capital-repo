(function () {
  const app = document.getElementById("forwardPaceApp");
  if (!app) return;
  const form = document.getElementById("forwardPaceForm");
  const submit = document.getElementById("forwardPaceSubmit");
  const state = document.getElementById("forwardPaceUpdateState");
  const pdf = document.getElementById("forwardPacePdfButton");
  const csrf = document.querySelector('meta[name="csrf-token"]')?.content || "";
  let latestPayload = null;

  const money = (value) => Number(value || 0).toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
  const setText = (id, value) => { const node = document.getElementById(id); if (node) node.textContent = value; };
  const readPayload = () => Object.fromEntries(new FormData(form).entries());
  const formatDate = (value) => value ? new Date(`${value}T12:00:00`).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }) : "Not reached";

  function setState(kind, title, detail) {
    state.dataset.state = kind;
    state.querySelector("strong").textContent = title;
    setText("forwardPaceUpdatedAt", detail);
  }
  function syncPhase() {
    const performance = form.querySelector('input[name="account_phase"]:checked')?.value === "performance";
    document.getElementById("forwardLifecyclePerformance").classList.toggle("is-active", performance);
  }
  async function requestProjection(payload) {
    const response = await fetch("/api/forward-pace/projection", { method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json", "X-CSRF-Token": csrf }, body: JSON.stringify(payload) });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body.ok === false) throw new Error(body.error || "Projection failed.");
    return body.projection;
  }
  function renderTrajectory(projection) {
    const node = document.getElementById("forwardPaceTrajectory");
    const schedule = projection.schedule || [];
    if (!schedule.length) { node.innerHTML = ""; return; }
    const lifecycle = projection.lifecycle;
    const values = [Number(projection.inputs.current_balance || 0), ...schedule.map(row => Number(row.projected_balance || 0))];
    const levels = lifecycle.milestones.filter(row => Number(row.value) > 0);
    const scale = [...values, ...levels.map(row => Number(row.value))];
    const rawMin = Math.min(...scale), rawMax = Math.max(...scale), rawSpan = Math.max(1, rawMax - rawMin);
    const min = rawMin - rawSpan * .08, max = rawMax + rawSpan * .08, span = max - min;
    const x = index => values.length === 1 ? 50 : 5 + index / (values.length - 1) * 90;
    const y = value => 8 + (max - value) / span * 78;
    const points = values.map((value, index) => `${x(index)},${y(value)}`).join(" ");
    const endDate = schedule.at(-1)?.end || projection.window.target_date;
    node.innerHTML = `<div class="forwardLifecyclePlot"><div class="forwardLifecyclePlotY"><span>${money(rawMax)}</span><span>${money(rawMin)}</span></div><svg viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">${[8,34,60,86].map(gridY => `<line class="forwardLifecycleGrid" x1="5" y1="${gridY}" x2="95" y2="${gridY}"></line>`).join("")}${levels.map(row => `<line class="forwardLifecycleLevel is-${row.key}" x1="5" y1="${y(Number(row.value))}" x2="95" y2="${y(Number(row.value))}"></line>`).join("")}<polyline class="forwardLifecyclePlotLine" points="${points}"></polyline><circle class="forwardLifecyclePlotPoint is-start" cx="${x(0)}" cy="${y(values[0])}" r="1.4"></circle><circle class="forwardLifecyclePlotPoint is-end" cx="${x(values.length-1)}" cy="${y(values.at(-1))}" r="1.7"></circle></svg><div class="forwardLifecyclePlotX"><span>${formatDate(projection.window.start_date)}</span><span>${formatDate(endDate)}</span></div></div><div class="forwardLifecycleChartLegend">${levels.map(row => `<span class="is-${row.key}">${row.label} ${money(row.value)}</span>`).join("")}</div>`;
  }
  function render(projection) {
    latestPayload = readPayload();
    const l = projection.lifecycle;
    const recommendation = l.recommendation;
    setText("forwardLifecyclePhase", l.phase_label);
    setText("forwardLifecycleNextLabel", l.next_label);
    setText("forwardLifecycleNextAmount", money(l.next_milestone));
    setText("forwardLifecycleNextText", `${money(l.next_remaining)} remaining at the current pace.`);
    setText("forwardLifecycleNextDate", formatDate(l.next_date));
    setText("forwardPaceDecisionEyebrow", recommendation.eyebrow);
    setText("forwardPaceDecisionTitle", recommendation.title);
    setText("forwardPaceDecisionDetail", recommendation.detail);
    setText("forwardPaceDecisionBalance", money(recommendation.required_balance));
    setText("forwardPaceDecisionProfit", money(recommendation.additional_profit));
    setText("forwardPaceDecisionDate", formatDate(recommendation.ready_date));
    setText("forwardPaceDecisionSessions", recommendation.sessions_to_ready ? `${recommendation.sessions_to_ready} trading days` : "Ready now");
    setText("forwardPaceDecisionAfter", l.phase === "performance" && recommendation.decision_amount > 0 ? money(recommendation.post_action_balance) : "--");
    setText("forwardPaceProjectedStatus", recommendation.projected_status);
    document.getElementById("forwardLifecycleNext").dataset.status = recommendation.action;
    setText("forwardLifecycleCurrent", money(projection.inputs.current_balance));
    setText("forwardLifecycleProjected", money(projection.totals.projected_balance));
    setText("forwardLifecycleDaily", money(projection.inputs.daily_profit));
    setText("forwardLifecycleWeekly", money(projection.weekly.net));
    setText("forwardPaceTrajectoryEnd", money(projection.totals.projected_balance));
    setText("forwardLifecycleTheoretical", l.phase === "performance" ? money(l.theoretical_capacity) : "--");
    setText("forwardLifecycleProtected", l.phase === "performance" ? money(l.protected_capacity) : "--");
    setText("forwardLifecyclePostBalance", l.phase === "performance" ? money(l.post_payout_balance) : "--");
    setText("forwardLifecyclePostCushion", l.phase === "performance" ? money(l.post_payout_cushion) : "--");
    setText("forwardLifecycleLossState", l.phase === "performance" ? l.loss_limit_state : "Evaluation rules");
    setText("forwardLifecycleLossLimit", l.phase === "performance" ? money(l.applicable_loss_limit) : "--");
    setText("forwardLifecycleProtectedFloor", l.phase === "performance" ? `Protected floor ${money(l.protected_floor)}` : "Loss rules still apply during evaluation");
    setText("forwardLifecycleRuleSource", l.rule_source);
    const safety = document.getElementById("forwardLifecycleSafety");
    safety.dataset.status = l.phase === "performance" ? (l.payout_safe ? "safe" : l.proposed_payout > 0 ? "unsafe" : "neutral") : "neutral";
    setText("forwardLifecyclePayoutStatus", l.phase !== "performance" ? "Available after performance buffer" : l.proposed_payout <= 0 ? "Enter a proposed payout" : l.payout_safe ? "Within protected plan" : "Outside protected plan");
    setText("forwardLifecycleSafetyText", l.phase !== "performance" ? "Pass the evaluation, then use Performance mode to protect the loss limit before withdrawing." : l.buffer_reached ? `Loss limit fixed. ${money(l.protected_capacity)} is available above your selected cushion.` : "Buffer not reached. A payout reduces balance while the loss limit is still current/trailing.");
    document.getElementById("forwardLifecyclePerformanceOutput").hidden = l.phase !== "performance";
    const decisionSteps = [
      {label:"Current balance", value:money(projection.inputs.current_balance), state:"complete"},
      {label:`At ${money(projection.inputs.daily_profit)}/day`, value:money(projection.totals.projected_balance), state:"active"},
      {label:recommendation.action === "withdraw" ? "Strategic payout" : "Next unlock", value:recommendation.action === "withdraw" ? money(recommendation.decision_amount) : money(recommendation.required_balance), state:"next"}
    ];
    document.getElementById("forwardLifecycleLadder").innerHTML = decisionSteps.map((row, index) => `<article class="forwardLifecycleStep is-${row.state}"><i>${row.state === "complete" ? "✓" : index + 1}</i><div><span>${row.label}</span><strong>${row.value}</strong></div><small>${index === 0 ? "Now" : index === 1 ? formatDate(projection.window.target_date) : recommendation.action === "withdraw" ? "Available now" : formatDate(recommendation.ready_date)}</small></article>`).join("");
    document.getElementById("forwardPaceScenarioGrid").innerHTML = l.scenarios.map(row => `<article class="forwardPaceScenario is-${row.key}"><div><span>${row.label}</span><strong>${Math.round(row.multiplier*100)}% pace</strong></div><strong>${money(row.daily_profit)}/day</strong><small>${l.phase === "evaluation" ? `Pass ${formatDate(row.evaluation_date)}` : `Buffer ${formatDate(row.buffer_date)} · Protected payout ${formatDate(row.protected_payout_date)}`}</small></article>`).join("");
    const schedule = document.getElementById("forwardPaceSchedule");
    schedule.innerHTML = projection.schedule.map(row => `<div class="forwardPaceWeek"><div><span>Period ${row.week}${row.partial ? " · Partial" : ""}</span><strong>${row.start} → ${row.end} · ${row.sessions} days</strong></div><div><span>Trading Profit</span><strong>${money(row.net)}</strong></div><div><span>Projected Balance</span><strong>${money(row.projected_balance)}</strong></div></div>`).join("");
    setText("forwardPaceScheduleSummary", `View all ${projection.schedule.length} periods`);
    renderTrajectory(projection);
    setState("success", "Lifecycle updated", `Updated ${new Date().toLocaleTimeString("en-US")}`);
    submit.disabled = true;
  }
  function showError(error) { setState("error", "Update failed", "Review the inputs and try again"); submit.disabled = false; const node = document.getElementById("forwardPaceError"); node.textContent = error.message; node.hidden = false; }
  async function update() { setState("updating", "Updating lifecycle", "Recalculating milestones and payout protection"); submit.disabled = true; try { render(await requestProjection(readPayload())); } catch (error) { showError(error); } }
  form.addEventListener("input", () => { setState("dirty", "Changes not applied", "Update the lifecycle to use these inputs"); submit.disabled = false; });
  form.addEventListener("change", event => { if (event.target.name === "account_phase") syncPhase(); setState("dirty", "Changes not applied", "Update the lifecycle to use these inputs"); submit.disabled = false; });
  form.addEventListener("submit", event => { event.preventDefault(); update(); });
  pdf.addEventListener("click", async () => { const response = await fetch("/forward-pace/pdf", { method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json", "X-CSRF-Token": csrf }, body: JSON.stringify(latestPayload || readPayload()) }); if (!response.ok) return; const url = URL.createObjectURL(await response.blob()); const link = document.createElement("a"); link.href = url; link.download = "account-lifecycle-projection.pdf"; link.click(); URL.revokeObjectURL(url); });
  const end = new Date(`${app.dataset.today}T12:00:00`); end.setDate(end.getDate() + 83); form.elements.target_date.value = end.toISOString().slice(0,10);
  syncPhase(); update();
})();
