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
    setText("forwardPaceHeadline", `${money(projection.weekly.net)} / week`);
    setText("forwardPaceProjectedBalance", money(projection.totals.projected_balance));
    setText(
      "forwardPaceProjectedMeta",
      `${projection.inputs.weeks} weeks · ${projection.inputs.state} · ${projection.inputs.payouts_per_week} payouts/week`
    );
    setText("forwardPaceWeeklyGross", money(projection.weekly.gross));
    setText("forwardPaceWeeklyTax", money(weeklyTax));
    setText("forwardPaceWeeklyBuffer", money(projection.weekly.buffer));
    setText("forwardPaceWeeklyNet", money(projection.weekly.net));
    setText("forwardPaceFederalTax", money(projection.tax.federal_annual));
    setText("forwardPaceStateRate", `${Number(projection.tax.state_rate || 0).toFixed(2)}%`);
    setText("forwardPaceEffectiveTax", `${Number(projection.tax.effective_tax_rate || 0).toFixed(2)}%`);

    const schedule = document.getElementById("forwardPaceSchedule");
    if (!schedule) return;
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
