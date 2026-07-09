(function initExecutiveCommandCenter() {
  const root = document.querySelector(".executiveCommandPage");
  const dataNode = document.getElementById("executive-operating-months");
  if (!root || !dataNode) return;

  const STORAGE_KEY = "mccain.executive.operatingLedger.v1";
  const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
  const numberValue = (value) => {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
  };
  const formatMoney = (value) => money.format(numberValue(value));
  const escapeHtml = (value) =>
    String(value ?? "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[char]);

  let months = [];
  try {
    months = JSON.parse(dataNode.textContent || "[]");
  } catch (_err) {
    months = [];
  }
  if (!Array.isArray(months) || !months.length) return;

  const defaultState = {
    selectedMonth: months[0].id,
    advancedOpen: false,
    inputs: {},
    ledger: {},
    adjustments: {},
  };
  const loadState = () => {
    try {
      return { ...defaultState, ...(JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}") || {}) };
    } catch (_err) {
      return { ...defaultState };
    }
  };
  let state = loadState();
  const saveState = () => localStorage.setItem(STORAGE_KEY, JSON.stringify(state));

  const monthById = new Map(months.map((month) => [month.id, month]));
  const selectedMonth = () => monthById.get(state.selectedMonth) || months[0];
  const monthInputs = (month) => ({
    openingBOA: month.openingBOA,
    openingCurrent: 0,
    tradingCash: 0,
    currentMonth: month.id,
    pendingDeposits: 0,
    pendingBills: 0,
    surplusSweepToBOA: 0,
    tradingPayout: 0,
    evalPurchases: 0,
    unexpectedExpenses: 0,
    ...(state.inputs[month.id] || {}),
  });

  const entryId = (month, type, name, index) =>
    `${month.id}:${type}:${String(name).replace(/[^a-z0-9]+/gi, "-").toLowerCase()}:${index}`;

  const ledgerState = (entry) => ({
    status: "Planned",
    actual: "",
    notes: "",
    ...(state.ledger[entry.id] || {}),
  });

  const effectiveAmount = (entry) => {
    const saved = ledgerState(entry);
    if (saved.status === "Skipped") return 0;
    const actual = numberValue(saved.actual);
    return actual > 0 && ["Paid", "Adjusted"].includes(saved.status)
      ? actual
      : numberValue(entry.amount);
  };

  const isIncome = (entry) => ["Deposit", "Payout"].includes(entry.type);
  const isOutflow = (entry) => ["Bill", "Eval"].includes(entry.type);

  const signedImpact = (entry) => {
    const amount = effectiveAmount(entry);
    if (isIncome(entry) || entry.type === "Sweep") return amount;
    return -amount;
  };

  const baseLedgerEntries = (month) => {
    const entries = [];
    const deposits = month.deposits || {};
    [
      ["Paycheck 1", "Current", "Deposit", "Current paycheck 1", deposits.currentPaycheck1],
      ["Paycheck 1", "BOA", "Deposit", "BOA paycheck 1", deposits.boaPaycheck1],
      ["Paycheck 2", "Current", "Deposit", "Current paycheck 2", deposits.currentPaycheck2],
      ["Paycheck 2", "BOA", "Deposit", "BOA paycheck 2", deposits.boaPaycheck2],
      ["Monthly", "BOA", "Deposit", "Wife contribution", deposits.wifeContribution],
    ].forEach(([timing, account, type, description, amount], index) => {
      entries.push({
        id: entryId(month, type, description, index),
        timing,
        account,
        type,
        description,
        amount: numberValue(amount),
      });
    });
    [...(month.bills || []), ...(month.subscriptions || [])].forEach((bill, index) => {
      entries.push({
        id: entryId(month, "Bill", bill.name, index),
        timing: bill.timing || "Monthly",
        account: bill.account || "Current",
        type: "Bill",
        description: bill.name,
        amount: numberValue(bill.amount),
      });
    });
    return entries.filter((entry) => entry.amount);
  };

  const adjustmentEntries = (month) => (state.adjustments[month.id] || []).map((item, index) => ({
    id: item.id || `${month.id}:adjustment:${index}`,
    timing: item.timing || "Adjustment",
    account: item.account || "BOA",
    type: item.type || "Bill",
    description: item.description || "Adjustment",
    amount: numberValue(item.amount),
    isAdjustment: true,
  })).filter((entry) => entry.amount);

  const calculateRequiredFloat = (inputs, entries) => {
    const openingCurrent = numberValue(inputs.openingCurrent);
    const currentIncome = entries
      .filter((entry) => entry.account === "Current" && isIncome(entry))
      .reduce((total, entry) => total + effectiveAmount(entry), 0);
    const currentBills = entries
      .filter((entry) => entry.account === "Current" && isOutflow(entry))
      .reduce((total, entry) => total + effectiveAmount(entry), 0);
    return Math.max(0, currentBills - currentIncome - openingCurrent);
  };

  const buildProjectionEntries = (month, inputs) => {
    const baseEntries = baseLedgerEntries(month);
    const advancedEntries = [
      {
        id: `${month.id}:deposit:pending`,
        timing: "Pending",
        account: "Current",
        type: "Deposit",
        description: "Pending deposits",
        amount: numberValue(inputs.pendingDeposits),
      },
      {
        id: `${month.id}:bill:pending`,
        timing: "Pending",
        account: "Current",
        type: "Bill",
        description: "Pending bills",
        amount: numberValue(inputs.pendingBills),
      },
      {
        id: `${month.id}:sweep:boa`,
        timing: "After bills clear",
        account: "Current → BOA",
        type: "Sweep",
        description: "Extra surplus sweep into BOA",
        amount: numberValue(inputs.surplusSweepToBOA),
      },
      {
        id: `${month.id}:payout:trading`,
        timing: "Trading",
        account: "BOA",
        type: "Payout",
        description: "Trading payout received",
        amount: numberValue(inputs.tradingPayout),
      },
      {
        id: `${month.id}:eval:purchases`,
        timing: "Trading",
        account: "BOA",
        type: "Eval",
        description: "Trading evaluation purchases",
        amount: numberValue(inputs.evalPurchases),
      },
      {
        id: `${month.id}:expense:unexpected`,
        timing: "As needed",
        account: "BOA",
        type: "Bill",
        description: "Unexpected expenses",
        amount: numberValue(inputs.unexpectedExpenses),
      },
    ].filter((entry) => entry.amount > 0);
    const entriesBeforeFloat = [...baseEntries, ...advancedEntries, ...adjustmentEntries(month)];
    const requiredFloat = calculateRequiredFloat(inputs, entriesBeforeFloat);
    const floatEntry = {
      id: `${month.id}:transfer:auto-current-float`,
      timing: "Auto-calculated",
      account: "BOA → Current",
      type: "Transfer",
      description: "Required Current float",
      amount: requiredFloat,
    };
    return requiredFloat > 0 ? [...entriesBeforeFloat, floatEntry] : entriesBeforeFloat;
  };

  function projectMonth(month, inputs, entries) {
    let boa = numberValue(inputs.openingBOA);
    let current = numberValue(inputs.openingCurrent);
    let totalIncome = 0;
    let totalBills = 0;
    let remainingBills = 0;
    const openingBOA = boa;
    const requiredCurrentFloat = entries
      .filter((entry) => entry.description === "Required Current float")
      .reduce((total, entry) => total + effectiveAmount(entry), 0);

    entries.forEach((entry) => {
      const saved = ledgerState(entry);
      const amount = effectiveAmount(entry);
      if (isIncome(entry)) totalIncome += amount;
      if (isOutflow(entry)) {
        totalBills += amount;
        if (!["Paid", "Skipped"].includes(saved.status)) remainingBills += amount;
      }

      if (entry.account === "BOA → Current") {
        boa -= amount;
        current += amount;
      } else if (entry.account === "Current → BOA") {
        current -= amount;
        boa += amount;
      } else if (entry.account === "BOA") {
        boa += signedImpact(entry);
      } else if (entry.account === "Current") {
        current += signedImpact(entry);
      } else if (entry.account === "Trading") {
        boa += signedImpact(entry);
      }
    });

    const projectedBOAClose = boa;
    const projectedCurrentClose = current;
    const protectedFloor = numberValue(month.protectedFloor);
    const redLine = numberValue(month.redLine);
    const targetClose = numberValue(month.targetCloseLow);
    const surplusAboveFloor = projectedBOAClose - protectedFloor;
    const gapToTarget = targetClose - projectedBOAClose;
    const treasuryGrowth = projectedBOAClose - openingBOA;
    const netCashFlow = totalIncome - totalBills;

    let floorStatus = "TARGET HIT";
    let recommendedAction = "Target hit. Hold the floor and promote only after all bills clear.";
    if (projectedBOAClose < redLine) {
      floorStatus = "RED LINE";
      recommendedAction = "Freeze extras. Repair treasury before any optional spending or trading purchases.";
    } else if (projectedBOAClose < protectedFloor) {
      floorStatus = "BELOW FLOOR";
      recommendedAction = "Below floor. Protect treasury first. No extra evaluations.";
    } else if (projectedBOAClose < targetClose) {
      floorStatus = "SAFE BUT BELOW TARGET";
      recommendedAction = "Safe but below target. Keep discipline and avoid leaks.";
    }

    return {
      projectedBOAClose,
      projectedCurrentClose,
      requiredCurrentFloat,
      totalIncome,
      totalBills,
      netCashFlow,
      treasuryGrowth,
      surplusAboveFloor,
      gapToTarget,
      remainingBills,
      floorStatus,
      redLineStatus: projectedBOAClose < redLine ? "Below red line" : "Clear",
      recommendedAction,
    };
  }

  const nodes = {
    controls: root.querySelector("[data-executive-current-state]"),
    advancedPanel: root.querySelector("[data-exec-advanced-panel]"),
    advancedState: root.querySelector("[data-exec-advanced-state]"),
    advancedToggle: root.querySelector("[data-exec-toggle-advanced]"),
    monthSelector: root.querySelector("[data-exec-month-selector]"),
    selectedLabel: root.querySelector("[data-exec-selected-label]"),
    summary: root.querySelector("[data-exec-projection-summary]"),
    action: root.querySelector("[data-exec-action-card]"),
    miniTrends: root.querySelector("[data-exec-mini-trends]"),
    chartGrid: root.querySelector("[data-exec-chart-grid]"),
    calendar: root.querySelector("[data-exec-calendar-board]"),
    ledger: root.querySelector("[data-exec-ledger-body]"),
    timeline: root.querySelector("[data-exec-projection-timeline]"),
    budgetCurrent: root.querySelector("[data-exec-budget-current]"),
    budgetBoa: root.querySelector("[data-exec-budget-boa]"),
    adjustmentList: root.querySelector("[data-exec-adjustment-list]"),
  };

  const inputNodes = Array.from(root.querySelectorAll("[data-exec-input]"));
  const adjustmentNodes = Array.from(root.querySelectorAll("[data-exec-adjustment]"));
  const monthSelect = root.querySelector('[data-exec-input="currentMonth"]');
  if (monthSelect) {
    monthSelect.innerHTML = months
      .map((month) => `<option value="${month.id}">${month.label}</option>`)
      .join("");
  }

  const groupedTiming = (entry) => {
    const timing = String(entry.timing || "Monthly");
    if (timing.includes("Paycheck 1") || timing.includes("First Half")) return "Paycheck 1";
    if (timing.includes("Paycheck 2") || timing.includes("Second Half")) return "Paycheck 2";
    if (timing.includes("End")) return "End-of-Month";
    if (timing.includes("Monthly")) return "Variable / Lifestyle";
    return "Mid-Month";
  };

  function renderInputs(month) {
    const inputs = monthInputs(month);
    inputNodes.forEach((input) => {
      const key = input.dataset.execInput;
      input.value = inputs[key] ?? "";
    });
    if (nodes.advancedPanel) nodes.advancedPanel.hidden = !state.advancedOpen;
    if (nodes.controls) nodes.controls.classList.toggle("is-collapsed", !state.advancedOpen);
    if (nodes.advancedToggle) {
      nodes.advancedToggle.textContent = state.advancedOpen
        ? "Hide Advanced Assumptions"
        : "Show Advanced Assumptions";
    }
    if (nodes.advancedState) {
      nodes.advancedState.textContent = state.advancedOpen
        ? "Advanced assumptions visible"
        : "Advanced assumptions hidden";
    }
  }

  function renderMonthSelector(month) {
    nodes.monthSelector.innerHTML = months
      .map((item) => `
        <button class="executiveMonthButton ${item.id === month.id ? "is-active" : ""}"
                type="button" data-exec-month="${item.id}">
          <span>${item.label}</span>
          <strong>${item.phase}</strong>
        </button>
      `)
      .join("");
  }

  function renderSummary(month, projection) {
    nodes.selectedLabel.textContent = `${month.label} · ${month.phase}`;
    const cards = [
      ["Projected BOA Close", formatMoney(projection.projectedBOAClose)],
      ["Projected Current Close", formatMoney(projection.projectedCurrentClose)],
      ["Required Current Float", formatMoney(projection.requiredCurrentFloat)],
      ["BOA Growth This Month", formatMoney(projection.treasuryGrowth)],
      ["Surplus Above Floor", formatMoney(projection.surplusAboveFloor)],
      ["Gap to Target", projection.gapToTarget > 0 ? formatMoney(projection.gapToTarget) : "$0.00"],
      ["Status", projection.floorStatus],
      ["Target Close", formatMoney(month.targetCloseLow)],
    ];
    nodes.summary.innerHTML = cards
      .map(([label, value]) => `<article><span>${label}</span><strong>${value}</strong></article>`)
      .join("");
    const tone = projection.projectedBOAClose < month.redLine
      ? "danger"
      : projection.projectedBOAClose < month.protectedFloor
        ? "watch"
        : "healthy";
    nodes.action.className = `executiveActionCard is-${tone}`;
    nodes.action.innerHTML = `
      <span>${projection.floorStatus}</span>
      <strong>${projection.recommendedAction}</strong>
    `;
  }

  const accountTotal = (entries, account, matcher) => entries
    .filter((entry) => entry.account === account)
    .filter((entry) => (matcher ? matcher(entry) : true))
    .reduce((total, entry) => total + effectiveAmount(entry), 0);

  const categoryFor = (entry) => {
    const name = String(entry.description || "").toLowerCase();
    if (name.includes("rent")) return "Housing";
    if (name.includes("car") || name.includes("gas") || name.includes("progressive")) {
      return "Transportation";
    }
    if (
      name.includes("power")
      || name.includes("verizon")
      || name.includes("at&t")
      || name.includes("internet")
      || name.includes("phone")
    ) {
      return "Utilities / Phone / Internet";
    }
    if (
      name.includes("chase")
      || name.includes("amex")
      || name.includes("discover")
      || name.includes("credit")
      || name.includes("indigo")
      || name.includes("irs")
    ) {
      return "Debt Paydown";
    }
    if (name.includes("food") || name.includes("dates")) return "Food / Dates";
    if (entry.timing === "First Half" || entry.timing === "Second Half") return "Subscriptions";
    if (entry.type === "Eval" || name.includes("trading") || name.includes("business")) {
      return "Trading / Business";
    }
    return "Miscellaneous";
  };

  const chartCard = (title, body, legend = "") => `
    <article class="executiveChartCard">
      <div class="executiveChartHead">
        <strong>${title}</strong>
        ${legend}
      </div>
      ${body}
    </article>
  `;

  const lineChart = (rows) => {
    const width = 520;
    const height = 210;
    const pad = 28;
    const values = rows.flatMap((row) => [row.floor, row.target, row.projected]);
    const min = Math.min(...values) * 0.92;
    const max = Math.max(...values) * 1.06;
    const x = (index) => pad + (index * (width - pad * 2)) / Math.max(1, rows.length - 1);
    const y = (value) => height - pad - ((value - min) / Math.max(1, max - min)) * (height - pad * 2);
    const points = (key) => rows.map((row, index) => `${x(index)},${y(row[key])}`).join(" ");
    const labels = rows.map((row, index) => `
      <text x="${x(index)}" y="${height - 6}" text-anchor="middle">${row.label}</text>
    `).join("");
    return `
      <svg class="executiveLineChart" viewBox="0 0 ${width} ${height}" role="img" aria-label="BOA treasury projection line chart">
        <path d="M${pad} ${pad}H${width - pad}M${pad} ${height / 2}H${width - pad}M${pad} ${height - pad}H${width - pad}" class="chartGridLine"></path>
        <polyline points="${points("floor")}" class="chartLine chartLineFloor"></polyline>
        <polyline points="${points("target")}" class="chartLine chartLineTarget"></polyline>
        <polyline points="${points("projected")}" class="chartLine chartLineProjected"></polyline>
        ${labels}
      </svg>
    `;
  };

  const barChart = (rows) => {
    const max = Math.max(...rows.map((row) => Math.abs(row.value)), 1);
    return `
      <div class="executiveBarChart">
        ${rows.map((row) => `
          <div class="executiveBarRow">
            <span>${escapeHtml(row.label)}</span>
            <div><i style="width:${Math.min(100, Math.abs(row.value) / max * 100)}%"></i></div>
            <strong>${formatMoney(row.value)}</strong>
          </div>
        `).join("")}
      </div>
    `;
  };

  const donutChart = (rows) => {
    const total = rows.reduce((sum, row) => sum + row.value, 0) || 1;
    const colors = ["#54f6eb", "#7fa3ff", "#d9b56d", "#5ad184", "#9cb6ff", "#c7d2fe", "#8bd3ff", "#6f87b9"];
    let cursor = 0;
    const segments = rows.map((row, index) => {
      const start = cursor;
      const end = cursor + (row.value / total) * 100;
      cursor = end;
      return `${colors[index % colors.length]} ${start}% ${end}%`;
    }).join(", ");
    return `
      <div class="executiveDonutWrap">
        <div class="executiveDonut" style="background:conic-gradient(${segments})"><span>${formatMoney(total)}</span></div>
        <div class="executiveDonutLegend">
          ${rows.map((row, index) => `
            <div><i style="background:${colors[index % colors.length]}"></i><span>${escapeHtml(row.label)}</span><strong>${formatMoney(row.value)}</strong></div>
          `).join("")}
        </div>
      </div>
    `;
  };

  function renderVisualGraphs(month, projection, entries) {
    const currentIncome = accountTotal(entries, "Current", isIncome);
    const currentBills = accountTotal(entries, "Current", isOutflow);
    const boaIncome = accountTotal(entries, "BOA", isIncome);
    const boaBills = accountTotal(entries, "BOA", isOutflow);
    const subscriptionsTotal = entries
      .filter((entry) => entry.timing === "First Half" || entry.timing === "Second Half")
      .reduce((total, entry) => total + effectiveAmount(entry), 0);
    const lifestyleTotal = entries
      .filter((entry) => ["Food / Dates", "Gas", "Haircuts"].includes(entry.description))
      .reduce((total, entry) => total + effectiveAmount(entry), 0);
    const paidBills = entries
      .filter(isOutflow)
      .filter((entry) => ledgerState(entry).status === "Paid")
      .reduce((total, entry) => total + effectiveAmount(entry), 0);
    const totalBills = entries.filter(isOutflow).reduce((total, entry) => total + effectiveAmount(entry), 0);
    const healthPercent = Math.min(100, (projection.projectedBOAClose / month.targetCloseLow) * 100);
    const healthLabel = healthPercent < 70
      ? "Needs repair"
      : healthPercent < 100
        ? "Safe but below target"
        : "Target hit";

    let rollingBOA = projection.projectedBOAClose;
    let rollingCurrent = projection.projectedCurrentClose;
    const lineRows = months.slice(0, 6).map((item) => {
      const inputs = { ...monthInputs(item), openingBOA: rollingBOA, openingCurrent: rollingCurrent };
      const itemEntries = buildProjectionEntries(item, inputs);
      const itemProjection = projectMonth(item, inputs, itemEntries);
      rollingBOA = itemProjection.projectedBOAClose;
      rollingCurrent = itemProjection.projectedCurrentClose;
      return {
        label: item.label.split(" ")[0],
        floor: item.protectedFloor,
        target: item.targetCloseLow,
        projected: itemProjection.projectedBOAClose || item.targetCloseLow,
      };
    });

    const categoryMap = new Map();
    entries.filter(isOutflow).forEach((entry) => {
      const category = categoryFor(entry);
      categoryMap.set(category, (categoryMap.get(category) || 0) + effectiveAmount(entry));
    });
    const categories = Array.from(categoryMap, ([label, value]) => ({ label, value }))
      .filter((row) => row.value > 0);

    nodes.miniTrends.innerHTML = [
      ["BOA Growth This Month", projection.treasuryGrowth, projection.treasuryGrowth >= 0 ? "Healthy" : "Watch"],
      ["Current Float Required", projection.requiredCurrentFloat, projection.requiredCurrentFloat > 0 ? "Watch" : "Clear"],
      ["Surplus Above Floor", projection.surplusAboveFloor, projection.surplusAboveFloor >= 0 ? "Healthy" : "Repair"],
      ["Remaining Bills", projection.remainingBills, projection.remainingBills > 0 ? "Planned" : "Clear"],
      ["Rule Score", 60, "60/80"],
    ].map(([label, value, status]) => {
      const numeric = typeof value === "number" ? value : numberValue(value);
      const progress = label === "Rule Score"
        ? 75
        : Math.min(100, Math.abs(numeric) / Math.max(1, month.targetCloseLow) * 100);
      return `
        <article class="executiveMiniTrend">
          <span>${label}</span>
          <strong>${label === "Rule Score" ? "60/80" : formatMoney(numeric)}</strong>
          <em>${status}</em>
          <i><b style="width:${progress}%"></b></i>
        </article>
      `;
    }).join("");

    const healthGauge = `
      <div class="executiveGaugeCard">
        <div class="executiveGaugeTrack"><i style="width:${healthPercent}%"></i></div>
        <strong>${Math.round(healthPercent)}%</strong>
        <span>${healthLabel}</span>
      </div>
    `;
    const gapProgress = Math.min(100, (projection.projectedBOAClose / month.targetCloseLow) * 100);
    const billProgress = Math.min(100, (paidBills / Math.max(1, totalBills)) * 100);
    const utilityCards = `
      <div class="executiveUtilityChartGrid">
        ${chartCard("Treasury Health Gauge", healthGauge)}
        ${chartCard("Gap to Target", `
          <div class="executiveProgressVisual">
            <span>${formatMoney(projection.projectedBOAClose)} of ${formatMoney(month.targetCloseLow)}</span>
            <i><b style="width:${gapProgress}%"></b></i>
            <strong>${projection.gapToTarget > 0 ? `${formatMoney(projection.gapToTarget)} gap` : "Target covered"}</strong>
          </div>
        `)}
        ${chartCard("Monthly Bill Progress", `
          <div class="executiveProgressVisual">
            <span>${formatMoney(paidBills)} paid of ${formatMoney(totalBills)}</span>
            <i><b style="width:${billProgress}%"></b></i>
            <strong>${formatMoney(Math.max(0, totalBills - paidBills))} remaining</strong>
          </div>
        `)}
      </div>
    `;

    nodes.chartGrid.innerHTML = `
      ${chartCard(
        "BOA Treasury Projection",
        lineChart(lineRows),
        '<div class="executiveChartLegend"><span class="is-floor">Floor</span><span class="is-target">Target</span><span class="is-projected">Projected</span></div>'
      )}
      ${chartCard("Monthly Cash Flow", barChart([
        { label: "Current Income", value: currentIncome },
        { label: "Current Bills", value: currentBills },
        { label: "BOA Income", value: boaIncome },
        { label: "BOA Bills", value: boaBills },
        { label: "Planned Float", value: projection.requiredCurrentFloat },
        { label: "Treasury Growth", value: projection.treasuryGrowth },
      ]))}
      ${chartCard("Bill Category Mix", donutChart(categories))}
      ${chartCard("Monthly Obligation Breakdown", barChart([
        { label: "Current obligations", value: currentBills },
        { label: "BOA obligations", value: boaBills },
        { label: "Subscriptions", value: subscriptionsTotal },
        { label: "Lifestyle variable", value: lifestyleTotal },
      ]))}
      ${utilityCards}
    `;
  }

  function renderCalendar(month, entries) {
    const columns = ["Paycheck 1", "Mid-Month", "Paycheck 2", "End-of-Month", "Variable / Lifestyle"];
    nodes.calendar.innerHTML = columns
      .map((column) => {
        const cards = entries
          .filter((entry) => ["Bill", "Transfer", "Payout", "Eval", "Sweep"].includes(entry.type))
          .filter((entry) => groupedTiming(entry) === column)
          .map((entry) => {
            const saved = ledgerState(entry);
            const checked = saved.status === "Paid" ? "checked" : "";
            return `
              <article class="executiveCalendarItem">
                <label>
                  <input type="checkbox" data-exec-paid="${entry.id}" ${checked}>
                  <strong>${escapeHtml(entry.description)}</strong>
                </label>
                <span>${formatMoney(entry.amount)} · ${escapeHtml(entry.account)}</span>
                <small>${escapeHtml(entry.timing)}</small>
              </article>
            `;
          })
          .join("");
        return `<div class="executiveCalendarColumn"><h4>${column}</h4>${cards || "<p>No items.</p>"}</div>`;
      })
      .join("");
  }

  function renderLedger(entries) {
    nodes.ledger.innerHTML = entries
      .map((entry) => {
        const saved = ledgerState(entry);
        return `
          <tr>
            <td>${escapeHtml(entry.timing)}</td>
            <td>${escapeHtml(entry.account)}</td>
            <td>${escapeHtml(entry.type)}</td>
            <td>${escapeHtml(entry.description)}</td>
            <td>${formatMoney(entry.amount)}</td>
            <td><input type="number" step="0.01" data-exec-actual="${entry.id}" value="${escapeHtml(saved.actual || "")}" placeholder="Actual"></td>
            <td>
              <select data-exec-status="${entry.id}">
                ${["Planned", "Paid", "Skipped", "Adjusted"].map((status) =>
                  `<option value="${status}" ${saved.status === status ? "selected" : ""}>${status}</option>`
                ).join("")}
              </select>
            </td>
            <td>${formatMoney(signedImpact(entry))}</td>
            <td><input type="text" data-exec-ledger-note="${entry.id}" value="${escapeHtml(saved.notes || "")}" placeholder="Notes"></td>
          </tr>
        `;
      })
      .join("");
  }

  function renderBudgetDetails(month, projection) {
    const budgetRow = (item) => {
      const timing = item.timing ? ` — ${escapeHtml(item.timing)}` : "";
      return `<div class="executiveBudgetRow"><span>${escapeHtml(item.name)}${timing}</span><strong>${formatMoney(item.amount)}</strong></div>`;
    };
    nodes.budgetCurrent.innerHTML = [...(month.bills || []), ...(month.subscriptions || [])]
      .filter((item) => item.account === "Current")
      .map(budgetRow)
      .join("");
    nodes.budgetBoa.innerHTML = [
      ...(month.bills || []).filter((item) => item.account === "BOA"),
      { name: "Wife contribution", amount: 300, timing: "Monthly" },
      { name: "Calculated Current float", amount: projection.requiredCurrentFloat, timing: "Auto" },
    ].map(budgetRow).join("");
  }

  function renderAdjustmentList(month) {
    const items = state.adjustments[month.id] || [];
    nodes.adjustmentList.innerHTML = items.length
      ? items.map((item) => `
          <div class="executiveAdjustmentRow">
            <span>${escapeHtml(item.description)} · ${escapeHtml(item.account)} · ${escapeHtml(item.type)}</span>
            <strong>${formatMoney(item.amount)}</strong>
            <button class="btn" type="button" data-exec-remove-adjustment="${item.id}">Remove</button>
          </div>
        `).join("")
      : "<p>No adjustments added.</p>";
  }

  function renderTimeline(startProjection) {
    let rollingBOA = startProjection.projectedBOAClose;
    let rollingCurrent = startProjection.projectedCurrentClose;
    const timeline = months.slice(0, 6).map((month) => {
      const inputs = { ...monthInputs(month), openingBOA: rollingBOA, openingCurrent: rollingCurrent };
      const entries = buildProjectionEntries(month, inputs);
      const projection = projectMonth(month, inputs, entries);
      rollingBOA = projection.projectedBOAClose;
      rollingCurrent = projection.projectedCurrentClose;
      const hitTarget = projection.projectedBOAClose >= month.targetCloseLow;
      const protectedFloor = projection.projectedBOAClose >= month.protectedFloor;
      const status = projection.projectedBOAClose < month.redLine
        ? "danger"
        : protectedFloor
          ? "healthy"
          : "watch";
      return `
        <article class="executiveTimelineProjection is-${status}">
          <span>${month.label}</span>
          <strong>${formatMoney(projection.projectedBOAClose)}</strong>
          <small>Opening ${formatMoney(inputs.openingBOA)}</small>
          <small>Floor ${formatMoney(month.protectedFloor)}</small>
          <small>Target ${formatMoney(month.targetCloseLow)}</small>
          <em>${protectedFloor ? "Floor protected" : "Repair mode"} · ${hitTarget ? "Target hit" : `${formatMoney(Math.max(0, month.targetCloseLow - projection.projectedBOAClose))} gap`}</em>
        </article>
      `;
    });
    nodes.timeline.innerHTML = timeline.join("");
  }

  function render() {
    const month = selectedMonth();
    const inputs = monthInputs(month);
    const entries = buildProjectionEntries(month, inputs);
    const projection = projectMonth(month, inputs, entries);
    renderInputs(month);
    renderMonthSelector(month);
    renderSummary(month, projection);
    renderVisualGraphs(month, projection, entries);
    renderCalendar(month, entries);
    renderLedger(entries);
    renderBudgetDetails(month, projection);
    renderAdjustmentList(month);
    renderTimeline(projection);
  }

  root.addEventListener("input", (event) => {
    const input = event.target.closest("[data-exec-input]");
    if (!input) return;
    const month = selectedMonth();
    const key = input.dataset.execInput;
    state.inputs[month.id] = { ...monthInputs(month), [key]: input.value };
    saveState();
    render();
  });

  root.addEventListener("change", (event) => {
    const monthSelectNode = event.target.closest("[data-exec-input='currentMonth']");
    if (monthSelectNode) {
      state.selectedMonth = monthSelectNode.value;
      saveState();
      render();
      return;
    }
    const paid = event.target.closest("[data-exec-paid]");
    const status = event.target.closest("[data-exec-status]");
    const actual = event.target.closest("[data-exec-actual]");
    if (paid) {
      const id = paid.dataset.execPaid;
      state.ledger[id] = { ...ledgerState({ id }), status: paid.checked ? "Paid" : "Planned" };
      saveState();
      render();
    } else if (status) {
      const id = status.dataset.execStatus;
      state.ledger[id] = { ...ledgerState({ id }), status: status.value };
      saveState();
      render();
    } else if (actual) {
      const id = actual.dataset.execActual;
      state.ledger[id] = { ...ledgerState({ id }), actual: actual.value };
      saveState();
      render();
    }
  });

  root.addEventListener("click", (event) => {
    const monthButton = event.target.closest("[data-exec-month]");
    if (monthButton) {
      state.selectedMonth = monthButton.dataset.execMonth;
      saveState();
      render();
      return;
    }
    if (event.target.closest("[data-exec-toggle-advanced]")) {
      state.advancedOpen = !state.advancedOpen;
      saveState();
      render();
      return;
    }
    if (event.target.closest("[data-exec-add-adjustment]")) {
      const month = selectedMonth();
      const item = { id: `${month.id}:adjustment:${Date.now()}` };
      adjustmentNodes.forEach((node) => {
        item[node.dataset.execAdjustment] = node.value;
      });
      item.amount = numberValue(item.amount);
      if (!item.description || !item.amount) return;
      state.adjustments[month.id] = [...(state.adjustments[month.id] || []), item];
      adjustmentNodes.forEach((node) => {
        if (node.tagName !== "SELECT") node.value = "";
      });
      saveState();
      render();
      return;
    }
    const remove = event.target.closest("[data-exec-remove-adjustment]");
    if (remove) {
      const month = selectedMonth();
      state.adjustments[month.id] = (state.adjustments[month.id] || [])
        .filter((item) => item.id !== remove.dataset.execRemoveAdjustment);
      saveState();
      render();
      return;
    }
    if (event.target.closest("[data-exec-recalculate]")) {
      render();
    }
  });

  root.addEventListener("focusout", (event) => {
    const note = event.target.closest("[data-exec-ledger-note]");
    if (!note) return;
    const id = note.dataset.execLedgerNote;
    state.ledger[id] = { ...ledgerState({ id }), notes: note.value };
    saveState();
  });

  render();
})();
