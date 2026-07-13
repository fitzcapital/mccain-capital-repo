(function initExecutiveCommandCenter() {
  const root = document.querySelector(".executiveCommandPage");
  const dataNode = document.getElementById("executive-operating-months");
  if (!root || !dataNode) return;

  const STORAGE_KEY = "mccain.executive.operatingLedger.v2";
  const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
  const numberValue = (value) => {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
  };
  const formatMoney = (value) => money.format(numberValue(value));
  const formatCompactMoney = (value) => {
    const amount = numberValue(value);
    const sign = amount > 0 ? "+" : amount < 0 ? "-" : "";
    const absolute = Math.abs(amount);
    const compact = absolute >= 1000 ? `$${(absolute / 1000).toFixed(1)}K` : money.format(absolute);
    return `${sign}${compact}`;
  };
  const escapeHtml = (value) =>
    String(value ?? "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[char]);

  let months = [];
  let staticModel = {};
  try {
    months = JSON.parse(dataNode.textContent || "[]");
    staticModel = JSON.parse(document.getElementById("executive-static-model")?.textContent || "{}");
  } catch (_err) {
    months = [];
  }
  if (!Array.isArray(months) || !months.length) return;

  const defaultState = {
    selectedMonth: months[0].id,
    activeTab: "overview",
    yearViewOpen: false,
    advancedOpen: false,
    inputs: {},
    ledger: {},
    adjustments: {},
    notes: {},
    quickExpense: "",
    sideAction: "",
    sidePanelExpanded: false,
    cushionSelectedMonth: "",
    cushionMode: "cushion",
    chartOpen: {},
    ledgerSearch: "",
    ledgerFilter: "",
    ledgerSort: "timing",
    ledgerPage: 1,
    centerMonthOnRender: false,
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
  const selectedMonthIndex = () => Math.max(0, months.findIndex((month) => month.id === selectedMonth().id));
  const monthInputs = (month) => ({
    openingBOA: month.openingBOA,
    openingCurrent: month.openingCurrent || 0,
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

  const nodes = {
    controls: root.querySelector("[data-executive-current-state]"),
    projectionControlsSummary: root.querySelector("[data-exec-projection-controls-summary]"),
    advancedPanel: root.querySelector("[data-exec-advanced-panel]"),
    advancedState: root.querySelector("[data-exec-advanced-state]"),
    advancedToggle: root.querySelector("[data-exec-toggle-advanced]"),
    monthSelector: root.querySelector("[data-exec-month-selector]"),
    monthHeader: root.querySelector("[data-exec-month-header]"),
    selectedLabel: root.querySelector("[data-exec-selected-label]"),
    kpiBand: root.querySelector("[data-exec-kpi-band]"),
    summary: root.querySelector("[data-exec-projection-summary]"),
    action: root.querySelector("[data-exec-action-card]"),
    overviewSummary: root.querySelector("[data-exec-overview-summary]"),
    miniTrends: root.querySelector("[data-exec-mini-trends]"),
    insights: root.querySelector("[data-exec-insights]"),
    chartGrid: root.querySelector("[data-exec-chart-grid]"),
    calendarSummary: root.querySelector("[data-exec-calendar-summary]"),
    calendar: root.querySelector("[data-exec-calendar-board]"),
    billDetails: root.querySelector("[data-exec-bill-details]"),
    ledger: root.querySelector("[data-exec-ledger-body]"),
    ledgerPager: root.querySelector("[data-exec-ledger-pager]"),
    ledgerSearch: root.querySelector("[data-exec-ledger-search]"),
    ledgerFilter: root.querySelector("[data-exec-ledger-filter]"),
    ledgerSort: root.querySelector("[data-exec-ledger-sort]"),
    budgetGroups: root.querySelector("[data-exec-budget-groups]"),
    treasuryCharts: root.querySelector("[data-exec-treasury-charts]"),
    reviewAccordion: root.querySelector("[data-exec-review-accordion]"),
    timeline: root.querySelector("[data-exec-projection-timeline]"),
    yearView: root.querySelector("[data-exec-year-view]"),
    yearGrid: root.querySelector("[data-exec-year-grid]"),
    sidePanel: root.querySelector("[data-exec-side-panel]"),
    priorities: root.querySelector("[data-exec-priorities]"),
    adjustmentList: root.querySelector("[data-exec-adjustment-list]"),
  };
  const inputNodes = Array.from(root.querySelectorAll("[data-exec-input]"));
  const adjustmentNodes = Array.from(root.querySelectorAll("[data-exec-adjustment]"));
  const monthSelect = root.querySelector('[data-exec-input="currentMonth"]');
  if (monthSelect) {
    monthSelect.innerHTML = months.map((month) => `<option value="${month.id}">${month.label}</option>`).join("");
  }

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
    return actual > 0 && ["Paid", "Adjusted"].includes(saved.status) ? actual : numberValue(entry.amount);
  };
  const isIncome = (entry) => ["Deposit", "Payout"].includes(entry.type);
  const isOutflow = (entry) => ["Bill", "Eval"].includes(entry.type);
  const signedImpact = (entry) => {
    const amount = effectiveAmount(entry);
    return isIncome(entry) || entry.type === "Sweep" ? amount : -amount;
  };
  const treasuryStatus = (month, projection) => {
    const projected = numberValue(projection.projectedBOAClose);
    const target = numberValue(month.targetCloseLow);
    const floor = numberValue(month.protectedFloor);
    const absoluteFloor = numberValue(month.redLine);
    if (projected < absoluteFloor) return { label: "Danger / Emergency Defense", tone: "danger", risk: "Critical" };
    if (projected < floor) return { label: "Caution / Protect Cash", tone: "watch", risk: "High" };
    if (projected < target) return { label: "Healthy / Watch Spending", tone: "watch", risk: "Moderate" };
    return { label: "Target Secured", tone: "target", risk: "Low" };
  };
  const compactStatus = (label) => {
    if (String(label).includes("Target Secured")) return "Secured";
    if (String(label).includes("Healthy")) return "Watch";
    if (String(label).includes("Caution")) return "Protect Cash";
    if (String(label).includes("Danger")) return "Emergency";
    return label;
  };
  const compactChartLabel = (label) => String(label)
    .replace("Utilities / Phone / Internet", "Utilities")
    .replace("Food / Dates", "Food")
    .replace("Trading / Business", "Trading")
    .replace("Debt Paydown", "Debt")
    .replace("Miscellaneous", "Misc.");
  const compactCalendarLabel = (label) => String(label)
    .replace("Current paycheck 1", "Current Pay")
    .replace("Current paycheck 2", "Current Pay")
    .replace("BOA paycheck 1", "BOA Pay")
    .replace("BOA paycheck 2", "BOA Pay")
    .replace("Wife contribution", "Wife")
    .replace("Required Current float", "Current Float")
    .replace("Chase fixed payment", "Chase")
    .replace("Verizon catch-up / phone", "Verizon catch-up")
    .replace("Food / Dates", "Food")
    .replace("Haircuts", "Haircut");
  const biweeklyDaysForMonth = (year, monthNumber, start) => {
    const daysInMonth = new Date(year, monthNumber, 0).getDate();
    return Array.from({ length: daysInMonth }, (_, index) => index + 1)
      .filter((day) => {
        const date = new Date(year, monthNumber - 1, day);
        const diffDays = Math.round((date - start) / 86400000);
        return diffDays >= 0 && diffDays % 14 === 0;
      });
  };
  const paydayDaysForMonth = (year, monthNumber) => biweeklyDaysForMonth(
    year,
    monthNumber,
    new Date(2026, 6, 3),
  );
  const wifeContributionDaysForMonth = (year, monthNumber) => biweeklyDaysForMonth(
    year,
    monthNumber,
    new Date(2026, 6, 10),
  );

  const timingDay = (entry, index) => {
    const explicitDay = numberValue(entry.dueDay);
    if (explicitDay > 0) return Math.round(explicitDay);
    const timing = String(entry.timing || "Monthly");
    if (timing.includes("Paycheck 1") || timing.includes("First Half")) return Math.min(14, 2 + (index % 9));
    if (timing.includes("Paycheck 2") || timing.includes("Second Half")) return Math.min(28, 17 + (index % 10));
    if (timing.includes("Split")) return index % 2 ? 17 : 3;
    if (timing.includes("End")) return 28;
    if (timing.includes("Pending")) return 10;
    if (timing.includes("Trading")) return 20;
    return 12 + (index % 12);
  };

  const eventToneClass = (entry) => {
    const description = String(entry.description || "").toLowerCase();
    const account = String(entry.account || "").toLowerCase();
    if (description.includes("wife contribution")) return "is-wife";
    if (description.includes("paycheck")) return "is-paycheck";
    if (entry.type === "Transfer" || entry.type === "Sweep") return "is-transfer";
    if (entry.type === "Deposit" || entry.type === "Payout") return "is-deposit";
    if (entry.type === "Bill" && account.includes("boa")) return "is-boa-bill";
    if (entry.type === "Bill" && (entry.timing === "First Half" || entry.timing === "Second Half")) return "is-subscription";
    if (entry.type === "Bill") return "is-current-bill";
    return "is-current";
  };

  const nextEvent = (month, entries, predicate) => {
    const [year, monthNumber] = month.id.split("-").map(Number);
    const now = new Date();
    const startDay = now.getFullYear() === year && now.getMonth() + 1 === monthNumber ? now.getDate() : 1;
    return entries
      .filter((entry) => predicate(entry) && numberValue(entry.dueDay) >= startDay)
      .sort((a, b) => numberValue(a.dueDay) - numberValue(b.dueDay) || Math.abs(signedImpact(b)) - Math.abs(signedImpact(a)))[0]
      || entries.filter(predicate).sort((a, b) => numberValue(a.dueDay) - numberValue(b.dueDay))[0];
  };

  const eventSummary = (entry) => entry
    ? `${compactCalendarLabel(entry.description)} · Day ${entry.dueDay} · ${formatMoney(entry.amount)}`
    : "No scheduled event";

  const compactEventSummary = (entry) => entry
    ? `${compactCalendarLabel(entry.description)} · D${entry.dueDay} · ${formatMoney(entry.amount)}`
    : "None";

  const summaryEventLabel = (entry, fallback) => entry
    ? `${compactCalendarLabel(entry.description)} D${entry.dueDay}`
    : fallback;

  const shortMonthLabel = (month) => {
    const [name, year] = String(month.label || "").split(" ");
    return `${String(name || "").slice(0, 3)} ${year || ""}`.trim();
  };

  const conciseAction = (text) => String(text || "")
    .replace("Target hit. Hold the floor and promote only after all bills clear.", "Hold floor. Promote after bills clear.")
    .replace("Safe but below target. Keep discipline and avoid leaks.", "Protect cash. Avoid leaks.");

  const baseLedgerEntries = (month) => {
    const deposits = month.deposits || {};
    const [year, monthNumber] = month.id.split("-").map(Number);
    const fundDays = paydayDaysForMonth(year, monthNumber);
    const depositDays = fundDays.slice(0, 2);
    const firstDepositDay = depositDays[0] || 3;
    const secondDepositDay = depositDays[1] || 17;
    const entries = [
      [String(firstDepositDay), "Current", "Deposit", "Current paycheck 1", deposits.currentPaycheck1],
      [String(firstDepositDay), "BOA", "Deposit", "BOA paycheck 1", deposits.boaPaycheck1],
      [String(secondDepositDay), "Current", "Deposit", "Current paycheck 2", deposits.currentPaycheck2],
      [String(secondDepositDay), "BOA", "Deposit", "BOA paycheck 2", deposits.boaPaycheck2],
    ].map(([timing, account, type, description, amount], index) => ({
      id: entryId(month, type, description, index),
      timing,
      account,
      type,
      description,
      amount: numberValue(amount),
      dueDay: Number.parseInt(timing, 10) || timingDay({ timing }, index),
    }));
    const wifeContribution = numberValue(deposits.wifeContribution);
    if (wifeContribution) {
      const wifeFundDays = wifeContributionDaysForMonth(year, monthNumber);
      const wifeDepositAmount = wifeContribution / 2;
      wifeFundDays.forEach((day, index) => {
        entries.push({
          id: entryId(month, "Deposit", `Wife contribution ${day}`, index),
          timing: `Day ${day}`,
          account: "BOA",
          type: "Deposit",
          description: "Wife contribution",
          amount: wifeDepositAmount,
          dueDay: day,
        });
      });
    }
    [...(month.bills || []), ...(month.subscriptions || [])].forEach((bill, index) => {
      const repeatDays = Array.isArray(bill.repeatDays) ? bill.repeatDays : [];
      const scheduledDays = repeatDays.length ? repeatDays : [timingDay(bill, index)];
      scheduledDays.forEach((day, repeatIndex) => {
        entries.push({
          id: entryId(month, "Bill", `${bill.name} ${day}`, index + repeatIndex),
          timing: repeatDays.length ? `Day ${day}` : bill.timing || "Monthly",
          account: bill.account || "Current",
          type: "Bill",
          description: bill.name,
          amount: numberValue(bill.amount),
          dueDay: day,
        });
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
    dueDay: timingDay(item, index),
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

  const calculateCurrentClose = (inputs, entries) => entries.reduce((balance, entry) => {
    const amount = effectiveAmount(entry);
    if (entry.account === "BOA → Current") return balance + amount;
    if (entry.account === "Current → BOA") return balance - amount;
    if (entry.account === "Current") return balance + signedImpact(entry);
    return balance;
  }, numberValue(inputs.openingCurrent));

  const buildProjectionEntries = (month, inputs) => {
    const baseEntries = baseLedgerEntries(month);
    const advancedEntries = [
      ["Pending", "Current", "Deposit", "Pending deposits", inputs.pendingDeposits],
      ["Pending", "Current", "Bill", "Pending bills", inputs.pendingBills],
      ["After bills clear", "Current → BOA", "Sweep", "Extra surplus sweep into BOA", inputs.surplusSweepToBOA],
      ["Trading", "BOA", "Payout", "Trading payout received", inputs.tradingPayout],
      ["Trading", "BOA", "Eval", "Trading evaluation purchases", inputs.evalPurchases],
      ["As needed", "BOA", "Bill", "Unexpected expenses", inputs.unexpectedExpenses],
    ].map(([timing, account, type, description, amount], index) => ({
      id: `${month.id}:${type.toLowerCase()}:${String(description).replace(/[^a-z0-9]+/gi, "-").toLowerCase()}`,
      timing,
      account,
      type,
      description,
      amount: numberValue(amount),
      dueDay: timingDay({ timing }, index),
    })).filter((entry) => entry.amount > 0);
    const entriesBeforeFloat = [...baseEntries, ...advancedEntries, ...adjustmentEntries(month)];
    const requiredFloat = calculateRequiredFloat(inputs, entriesBeforeFloat);
    const floatEntry = {
      id: `${month.id}:transfer:auto-current-float`,
      timing: "Auto-calculated",
      account: "BOA → Current",
      type: "Transfer",
      description: "Required Current float",
      amount: requiredFloat,
      dueDay: 1,
    };
    const entriesWithFloat = requiredFloat > 0
      ? [...entriesBeforeFloat, floatEntry]
      : entriesBeforeFloat;
    const septemberSurplus = month.id === "2026-09"
      ? Math.max(0, calculateCurrentClose(inputs, entriesWithFloat))
      : 0;
    const septemberSweep = {
      id: `${month.id}:sweep:september-post-bill-surplus`,
      timing: "After bills clear",
      account: "Current → BOA",
      type: "Sweep",
      description: "September post-bill surplus sweep",
      amount: septemberSurplus,
      dueDay: 30,
    };
    return septemberSurplus > 0
      ? [...entriesWithFloat, septemberSweep]
      : entriesWithFloat;
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
    const protectedFloor = numberValue(month.protectedFloor);
    const redLine = numberValue(month.redLine);
    const targetClose = numberValue(month.targetCloseLow);
    let floorStatus = "Target Secured";
    let recommendedAction = "Target hit. Hold the floor and promote only after all bills clear.";
    if (projectedBOAClose < redLine) {
      floorStatus = "Danger / Emergency Defense";
      recommendedAction = "Freeze extras. Repair treasury before optional spending or trading purchases.";
    } else if (projectedBOAClose < protectedFloor) {
      floorStatus = "Caution / Protect Cash";
      recommendedAction = "Protect treasury first. No extra evaluations.";
    } else if (projectedBOAClose < targetClose) {
      floorStatus = "Healthy / Watch Spending";
      recommendedAction = "Safe but below target. Keep discipline and avoid leaks.";
    }
    return {
      projectedBOAClose,
      projectedCurrentClose: current,
      requiredCurrentFloat,
      totalIncome,
      totalBills,
      netCashFlow: totalIncome - totalBills,
      treasuryGrowth: projectedBOAClose - openingBOA,
      surplusAboveFloor: projectedBOAClose - protectedFloor,
      gapToTarget: targetClose - projectedBOAClose,
      remainingBills,
      floorStatus,
      recommendedAction,
    };
  }

  const accountTotal = (entries, account, matcher) => entries
    .filter((entry) => entry.account === account)
    .filter((entry) => (matcher ? matcher(entry) : true))
    .reduce((total, entry) => total + effectiveAmount(entry), 0);

  const categoryFor = (entry) => {
    const name = String(entry.description || "").toLowerCase();
    if (name.includes("rent")) return "Housing";
    if (name.includes("car") || name.includes("gas") || name.includes("progressive")) return "Transportation";
    if (name.includes("power") || name.includes("verizon") || name.includes("at&t") || name.includes("internet") || name.includes("phone")) return "Utilities / Phone / Internet";
    if (name.includes("chase") || name.includes("amex") || name.includes("discover") || name.includes("credit") || name.includes("indigo") || name.includes("irs")) return "Debt Paydown";
    if (name.includes("food") || name.includes("dates")) return "Food / Dates";
    if (entry.timing === "First Half" || entry.timing === "Second Half") return "Subscriptions";
    if (entry.type === "Eval" || name.includes("trading") || name.includes("business")) return "Trading / Business";
    return "Miscellaneous";
  };

  const chartCard = (title, body, legend = "", defaultOpen = true) => {
    const isOpen = Object.prototype.hasOwnProperty.call(state.chartOpen, title)
      ? state.chartOpen[title] !== false
      : defaultOpen;
    return `
    <details class="executiveChartCard" ${isOpen ? "open" : ""} data-exec-chart-module="${escapeHtml(title)}">
      <summary class="executiveChartHead"><strong>${title}</strong>${legend}</summary>
      ${body}
    </details>
  `;
  };
  const lineChart = (rows, keys = ["floor", "target", "projected"]) => {
    const width = 520;
    const height = 230;
    const padX = 42;
    const padTop = 42;
    const padBottom = 30;
    const values = rows.flatMap((row) => keys.map((key) => numberValue(row[key])));
    const min = Math.min(...values) * 0.92;
    const max = Math.max(...values) * 1.06;
    const x = (index) => padX + (index * (width - padX * 2)) / Math.max(1, rows.length - 1);
    const y = (value) => height - padBottom - ((value - min) / Math.max(1, max - min)) * (height - padTop - padBottom);
    const points = (key) => rows.map((row, index) => `${x(index)},${y(row[key])}`).join(" ");
    return `
      <svg class="executiveLineChart" viewBox="0 0 ${width} ${height}" role="img">
        <path d="M${padX} ${padTop}H${width - padX}M${padX} ${height / 2}H${width - padX}M${padX} ${height - padBottom}H${width - padX}" class="chartGridLine"></path>
        ${keys.map((key) => `<polyline points="${points(key)}" class="chartLine chartLine-${key}"></polyline>`).join("")}
        ${["target", "floor", "absoluteFloor"].filter((key) => keys.includes(key) && rows[0]?.[key]).map((key) => {
          const yPos = y(rows[0][key]);
          return `<g class="threshold threshold-${key}"><line x1="${padX}" y1="${yPos}" x2="${width - padX}" y2="${yPos}"></line></g>`;
        }).join("")}
        ${rows.map((row, index) => {
          const label = rows.length > 6 && index % 2 === 1 && index !== rows.length - 1 ? "" : String(row.label).slice(0, 3);
          return label ? `<text x="${x(index)}" y="${height - 8}" text-anchor="middle">${escapeHtml(label)}</text>` : "";
        }).join("")}
      </svg>
    `;
  };
  const cushionStatus = (row) => {
    if (row.projected >= row.target) {
      return { label: "Protected", tone: "is-protected", action: "Target covered. Hold the floor and protect surplus." };
    }
    if (row.projected >= row.floor) {
      return { label: "Watch", tone: "is-watch", action: "Above Floor. Limit extras until Target is covered." };
    }
    if (row.projected >= row.absoluteFloor) {
      return { label: "Defend", tone: "is-defend", action: "Protect cash. Delay non-essential spending." };
    }
    return { label: "Danger", tone: "is-danger", action: "Emergency defense. No discretionary outflow." };
  };
  const treasuryCushionChart = (rows, selectedMonthKey, mode = "cushion") => {
    const safeMode = mode === "projected" ? "projected" : "cushion";
    const selectedRow = rows.find((row) => row.month.id === selectedMonthKey) || rows[0];
    const selectedStatus = cushionStatus(selectedRow);
    const width = 760;
    const height = 322;
    const padX = 50;
    const padTop = 34;
    const padBottom = 52;
    const values = safeMode === "projected"
      ? rows.flatMap((row) => [row.projected, row.target, row.floor, row.absoluteFloor])
      : rows.flatMap((row) => [
        row.projected - row.target,
        row.projected - row.floor,
        row.projected - row.absoluteFloor,
        0,
      ]);
    const min = Math.min(...values) - 350;
    const max = Math.max(...values) + 350;
    const innerWidth = width - padX * 2;
    const innerHeight = height - padTop - padBottom;
    const groupWidth = innerWidth / Math.max(1, rows.length);
    const x = (index) => padX + groupWidth * index + groupWidth / 2;
    const y = (value) => height - padBottom - ((value - min) / Math.max(1, max - min)) * innerHeight;
    const projectedPoints = rows.map((row, index) => `${x(index)},${y(row.projected)}`).join(" ");
    const cushionPoints = (key) => rows.map((row, index) => `${x(index)},${y(row.projected - row[key])}`).join(" ");
    const zeroY = y(0);
    const zone = (row, index) => {
      const left = padX + groupWidth * index + 8;
      const zoneWidth = Math.max(22, groupWidth - 16);
      const targetY = y(row.target);
      const floorY = y(row.floor);
      const hardY = y(row.absoluteFloor);
      const bottomY = height - padBottom;
      return `
        <rect x="${left}" y="${padTop}" width="${zoneWidth}" height="${Math.max(1, targetY - padTop)}" rx="7" class="cushionZone is-protected"></rect>
        <rect x="${left}" y="${targetY}" width="${zoneWidth}" height="${Math.max(1, floorY - targetY)}" rx="7" class="cushionZone is-watch"></rect>
        <rect x="${left}" y="${floorY}" width="${zoneWidth}" height="${Math.max(1, hardY - floorY)}" rx="7" class="cushionZone is-defend"></rect>
        <rect x="${left}" y="${hardY}" width="${zoneWidth}" height="${Math.max(1, bottomY - hardY)}" rx="7" class="cushionZone is-danger"></rect>
      `;
    };
    const metric = (row) => safeMode === "projected" ? row.projected : row.projected - row.absoluteFloor;
    const cushionText = (value, label) => value >= 0
      ? `${formatMoney(value)} above ${label}`
      : `Short ${formatMoney(Math.abs(value))} to ${label}`;
    return `
      <div class="executiveCushionWrap">
        <div class="executiveCushionToolbar">
          <p>${safeMode === "projected" ? "Actual projected close plotted against Target, Floor, and Hard Floor." : "Dollar cushion above each protection line. Below $0 means defense is required."}</p>
          <div class="executiveCushionModes" role="group" aria-label="Treasury cushion chart mode">
            ${["cushion", "projected"].map((key) => `<button type="button" class="${safeMode === key ? "is-active" : ""}" data-cushion-mode="${key}">${key === "cushion" ? "Cushion" : "Projected Close"}</button>`).join("")}
          </div>
        </div>
        <div class="executiveCushionBody">
          <div class="executiveCushionStage">
            <svg class="executiveLineChart executiveCushionChart ${safeMode === "cushion" ? "is-cushion-mode" : "is-projected-mode"}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Treasury cushion by month">
              <path d="M${padX} ${padTop}H${width - padX}M${padX} ${height / 2}H${width - padX}M${padX} ${height - padBottom}H${width - padX}" class="chartGridLine"></path>
              ${safeMode === "projected" ? rows.map(zone).join("") : `<line x1="${padX}" y1="${zeroY}" x2="${width - padX}" y2="${zeroY}" class="cushionZeroLine"></line><text x="${padX - 8}" y="${zeroY + 3}" text-anchor="end">$0</text>`}
              ${safeMode === "projected" ? `<polyline points="${projectedPoints}" class="chartLine chartLineProjected cushionProjectedLine"></polyline>` : `
                <polyline points="${cushionPoints("target")}" class="chartLine cushionBufferLine is-target"></polyline>
                <polyline points="${cushionPoints("floor")}" class="chartLine cushionBufferLine is-floor"></polyline>
                <polyline points="${cushionPoints("absoluteFloor")}" class="chartLine cushionBufferLine is-absolute"></polyline>
              `}
              ${rows.map((row, index) => {
                const status = cushionStatus(row);
                const isSelected = selectedRow.month.id === row.month.id;
                const detail = `${row.label}: projected ${formatMoney(row.projected)}. Target ${formatMoney(row.target)}. Floor ${formatMoney(row.floor)}. Hard Floor ${formatMoney(row.absoluteFloor)}. ${cushionText(row.projected - row.absoluteFloor, "Hard Floor")}.`;
                const pointY = safeMode === "projected" ? y(row.projected) : y(row.projected - row.absoluteFloor);
                return `
                  <g class="cushionPointGroup ${isSelected ? "is-selected" : ""}" data-cushion-month="${escapeHtml(row.month.id)}">
                    <circle cx="${x(index)}" cy="${pointY}" r="${isSelected ? 7 : 5}" class="cushionPoint ${status.tone}"></circle>
                    <title>${escapeHtml(detail)}</title>
                  </g>
                  <text x="${x(index)}" y="${height - 18}" text-anchor="middle">${escapeHtml(String(row.label).slice(0, 3))}</text>
                `;
              }).join("")}
            </svg>
            <div class="executiveCushionMonthStrip">
              ${rows.map((row) => {
                const status = cushionStatus(row);
                const isSelected = selectedRow.month.id === row.month.id;
                const value = metric(row);
                const fullDisplay = safeMode === "projected" ? formatMoney(value) : cushionText(value, "Hard Floor");
                const chipDisplay = formatCompactMoney(value);
                return `<button type="button" class="${status.tone} ${isSelected ? "is-selected" : ""}" data-cushion-month="${escapeHtml(row.month.id)}" title="${escapeHtml(`${row.label} ${status.label}: ${fullDisplay}`)}"><span>${escapeHtml(String(row.label).slice(0, 3))}</span><strong>${escapeHtml(chipDisplay)}</strong><em>${escapeHtml(safeMode === "projected" ? "Projected" : "Hard Floor")}</em></button>`;
              }).join("")}
            </div>
          </div>
          <aside class="executiveCushionReadout ${selectedStatus.tone}">
            <span>${escapeHtml(selectedRow.label)} Status</span>
            <strong>${selectedStatus.label}</strong>
            <dl>
              <div><dt>Projected Close</dt><dd>${formatMoney(selectedRow.projected)}</dd></div>
              <div><dt>Target Position</dt><dd>${cushionText(selectedRow.projected - selectedRow.target, "Target")}</dd></div>
              <div><dt>Floor Position</dt><dd>${cushionText(selectedRow.projected - selectedRow.floor, "Floor")}</dd></div>
              <div><dt>Hard Floor Position</dt><dd>${cushionText(selectedRow.projected - selectedRow.absoluteFloor, "Hard Floor")}</dd></div>
            </dl>
            <p>${escapeHtml(selectedStatus.action)}</p>
          </aside>
        </div>
      </div>
    `;
  };
  const barChart = (rows) => {
    const max = Math.max(...rows.map((row) => Math.abs(row.value)), 1);
    return `<div class="executiveBarChart">${rows.map((row) => `
      <div class="executiveBarRow">
        <span>${escapeHtml(row.label)}</span>
        <div><i style="width:${Math.min(100, Math.abs(row.value) / max * 100)}%"></i></div>
        <strong>${formatMoney(row.value)}</strong>
      </div>
    `).join("")}</div>`;
  };
  const donutChart = (rows) => {
    const total = rows.reduce((sum, row) => sum + row.value, 0) || 1;
    const colors = ["#5bd6d0", "#6f9ce8", "#d5aa55", "#58c884", "#927ee6", "#d978a7", "#78c6e6", "#d48b5b"];
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
          ${rows.map((row, index) => {
            const percent = Math.round((row.value / total) * 100);
            return `<div title="${escapeHtml(`${row.label} · ${formatMoney(row.value)} · ${percent}%`)}"><i style="background:${colors[index % colors.length]}"></i><span>${escapeHtml(compactChartLabel(row.label))}<em>${percent}%</em></span><strong>${formatMoney(row.value)}</strong></div>`;
          }).join("")}
        </div>
      </div>
    `;
  };
  const progressVisual = (label, value, max, detail) => `
    <div class="executiveProgressVisual">
      <span>${escapeHtml(label)}</span>
      <i><b style="width:${Math.min(100, Math.max(0, value / Math.max(1, max) * 100))}%"></b></i>
      <strong>${escapeHtml(String(detail).split("·")[0].trim())}</strong>
      ${String(detail).includes("·") ? `<em>${escapeHtml(compactStatus(String(detail).split("·").slice(1).join("·").trim()))}</em>` : ""}
    </div>
  `;

  function rollingRows(startMonth) {
    let rollingBOA = 0;
    let rollingCurrent = 0;
    return months.slice(0, 12).map((item, index) => {
      const resetToConfiguredOpening = index === 0 || item.id === startMonth.id;
      const inputs = {
        ...monthInputs(item),
        openingBOA: resetToConfiguredOpening ? monthInputs(item).openingBOA : rollingBOA,
        openingCurrent: resetToConfiguredOpening ? monthInputs(item).openingCurrent : rollingCurrent,
      };
      const entries = buildProjectionEntries(item, inputs);
      const itemProjection = projectMonth(item, inputs, entries);
      rollingBOA = itemProjection.projectedBOAClose;
      rollingCurrent = itemProjection.projectedCurrentClose;
      return {
        month: item,
        projection: itemProjection,
        openingBOA: inputs.openingBOA,
        label: item.label.split(" ")[0],
        floor: item.protectedFloor,
        target: item.targetCloseLow,
        absoluteFloor: item.redLine,
        projected: itemProjection.projectedBOAClose || item.targetCloseLow,
      };
    });
  }

  function currentContext() {
    const month = selectedMonth();
    const inputs = monthInputs(month);
    const entries = buildProjectionEntries(month, inputs);
    const projection = projectMonth(month, inputs, entries);
    return { month, inputs, entries, projection };
  }

  function renderInputs(month) {
    const inputs = monthInputs(month);
    inputNodes.forEach((input) => {
      const key = input.dataset.execInput;
      input.value = inputs[key] ?? "";
    });
    if (nodes.advancedPanel) nodes.advancedPanel.hidden = !state.advancedOpen;
    if (nodes.controls) {
      nodes.controls.classList.add("is-open");
      nodes.controls.classList.remove("is-collapsed");
      nodes.controls.setAttribute("aria-hidden", "false");
    }
    if (nodes.advancedToggle) nodes.advancedToggle.textContent = state.advancedOpen ? "Hide Advanced Assumptions" : "Show Advanced Assumptions";
    if (nodes.advancedState) nodes.advancedState.textContent = state.advancedOpen ? "Advanced assumptions visible" : "Advanced assumptions hidden";
  }

  function renderProjectionControlsSummary(month, projection) {
    if (!nodes.projectionControlsSummary) return;
    nodes.projectionControlsSummary.innerHTML = [
      month.label,
      `BOA ${formatMoney(monthInputs(month).openingBOA)}`,
      `Projected ${formatMoney(projection.projectedBOAClose)}`,
    ].map((item) => `<span>${escapeHtml(item)}</span>`).join("");
  }

  function renderMonthSelector(month, shouldCenter = false) {
    const currentIndex = 0;
    nodes.monthSelector.innerHTML = months.map((item, index) => {
      const completed = index < currentIndex;
      const future = index > currentIndex && item.id !== month.id;
      const phaseShort = String(item.phase || item.label)
        .replace(" Month", "")
        .replace("Five-Figure", "Five Figure");
      return `
        <button class="executiveMonthButton ${item.id === month.id ? "is-active" : ""} ${completed ? "is-complete" : ""} ${future ? "is-future" : ""}"
                type="button" data-exec-month="${item.id}">
          <span>${completed ? "✓ " : ""}${escapeHtml(phaseShort)}</span>
          <strong>${escapeHtml(item.label)}</strong>
        </button>
      `;
    }).join("");
    if (shouldCenter) {
      nodes.monthSelector.querySelector(".is-active")?.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
    }
  }

  function renderMonthHeader(month, projection, entries) {
    const outflows = entries.filter(isOutflow);
    const remainingCount = outflows.filter((entry) => !["Paid", "Skipped"].includes(ledgerState(entry).status)).length;
    const monthEnd = new Date(`${month.id}-28T12:00:00`);
    const now = new Date();
    const daysRemaining = Math.max(0, Math.ceil((monthEnd - now) / 86400000));
    nodes.monthHeader.innerHTML = `
      <div class="executiveMonthTitle">
        <h3>${escapeHtml(month.label)}</h3>
        <span>${escapeHtml(month.phase)}</span>
      </div>
      ${[
        ["Current Treasury", formatMoney(monthInputs(month).openingBOA)],
        ["Protected Floor", formatMoney(month.protectedFloor)],
        ["Absolute Floor", formatMoney(month.redLine)],
        ["Projected Close", formatMoney(projection.projectedBOAClose)],
        ["Status", compactStatus(projection.floorStatus)],
        ["Days Remaining", daysRemaining],
      ].map(([label, value]) => `<article><span>${label}</span><strong>${value}</strong></article>`).join("")}
    `;
  }

  function renderTabs() {
    if (!root.querySelector(`[data-exec-tab="${state.activeTab}"]`) || !root.querySelector(`[data-exec-panel="${state.activeTab}"]`)) {
      state.activeTab = "overview";
    }
    root.querySelectorAll("[data-exec-tab]").forEach((button) => {
      button.classList.toggle("primary", button.dataset.execTab === state.activeTab);
    });
    root.querySelectorAll("[data-exec-panel]").forEach((panel) => {
      panel.classList.toggle("is-active", panel.dataset.execPanel === state.activeTab);
    });
  }

  function renderOverview(month, projection, entries) {
    if (!nodes.overviewSummary) return;
    const status = treasuryStatus(month, projection);
    const nextCash = nextEvent(month, entries, (entry) =>
      ["Deposit", "Payout"].includes(entry.type));
    const nextBill = nextEvent(month, entries, isOutflow);
    nodes.overviewSummary.innerHTML = `
      <div class="executiveOverviewHero is-${status.tone}">
        <span>${escapeHtml(status.label)}</span>
        <strong>${escapeHtml(projection.recommendedAction)}</strong>
      </div>
      <div class="executiveOverviewGrid">
        ${[
          ["Current Treasury", formatMoney(monthInputs(month).openingBOA), "Starting BOA position"],
          ["Projected Close", formatMoney(projection.projectedBOAClose), compactStatus(status.label)],
          ["Floor / Target", `${formatMoney(month.protectedFloor)} / ${formatMoney(month.targetCloseLow)}`, `${formatMoney(Math.max(0, projection.gapToTarget))} gap`],
          ["Next Cash Event", eventSummary(nextCash), "Deposit / transfer watch"],
          ["Next Bill Event", eventSummary(nextBill), "Obligation watch"],
          ["CEO Action", projection.gapToTarget > 0 ? "Protect floor first" : "Hold floor and avoid leaks", status.risk],
        ].map(([label, value, meta]) => `
          <article>
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
            <em>${escapeHtml(meta)}</em>
          </article>
        `).join("")}
      </div>
    `;
  }

  function renderSummary(month, projection) {
    nodes.selectedLabel.textContent = `${month.label} · ${month.phase}`;
    const status = treasuryStatus(month, projection);
    nodes.kpiBand.innerHTML = [
      ["Current Treasury", formatMoney(monthInputs(month).openingBOA), "Blue"],
      ["Projected Close", formatMoney(projection.projectedBOAClose), compactStatus(status.label)],
      ["Floor", formatMoney(month.protectedFloor), "Protected"],
      ["Absolute Floor", formatMoney(month.redLine), "Never cross"],
      ["Target", formatMoney(month.targetCloseLow), projection.gapToTarget > 0 ? "Open" : "Hit"],
      ["Cash Runway", `${Math.max(1, Math.round(projection.projectedBOAClose / Math.max(1, projection.totalBills) * 30))} days`, status.risk],
      ["CEO Score", `${staticModel.score_total || 60}/80`, "B+"],
    ].map(([label, value, itemStatus]) => `<article class="executiveKpiCard is-${label.toLowerCase().replace(/[^a-z]+/g, "-")}"><span>${label}</span><strong>${value}</strong><em>${itemStatus}</em></article>`).join("");
    nodes.summary.innerHTML = [
      ["Current Treasury", formatMoney(monthInputs(month).openingBOA)],
      ["Projected BOA Close", formatMoney(projection.projectedBOAClose)],
      ["Floor", formatMoney(month.protectedFloor)],
      ["Absolute Floor", formatMoney(month.redLine)],
      ["Target", formatMoney(month.targetCloseLow)],
      ["Status", compactStatus(projection.floorStatus)],
    ].map(([label, value]) => `<article><span>${label}</span><strong>${value}</strong></article>`).join("");
    nodes.action.className = `executiveActionCard is-${status.tone}`;
    nodes.action.innerHTML = `<span>${status.label}</span><strong>${projection.recommendedAction}</strong>`;
    const priorityActions = [
      "Protect the monthly floor",
      "Keep evaluation discipline clean",
      "Execute payment plan",
      "Maintain no-spend policy",
      "Wait for clean pass conditions",
      "Keep weekly health rhythm",
    ];
    nodes.priorities.innerHTML = `
      <div class="executivePriorityTable" role="table" aria-label="Current priorities">
        <div class="executivePriorityRow is-head" role="row">
          <span>Focus</span><span>Status</span><span>Next Action</span><span>Due</span>
        </div>
        ${(staticModel.operating_priorities || []).slice(0, 6).map((item, index) => `
          <div class="executivePriorityRow" role="row">
            <span>${escapeHtml(item)}</span>
            <strong class="is-${index === 0 ? "active" : index === 1 ? "cleared" : index === 2 ? "blocked" : "queued"}">${index === 0 ? "Active" : index === 1 ? "Cleared" : index === 2 ? "Blocked" : "Queued"}</strong>
            <em>${escapeHtml(priorityActions[index] || "Review and execute")}</em>
            <small>${index < 3 ? "This week" : "Month-end"}</small>
          </div>
        `).join("")}
      </div>
    `;
  }

  function renderVisualGraphs(month, projection, entries) {
    const currentIncome = accountTotal(entries, "Current", isIncome);
    const currentBills = accountTotal(entries, "Current", isOutflow);
    const boaIncome = accountTotal(entries, "BOA", isIncome);
    const boaBills = accountTotal(entries, "BOA", isOutflow);
    const subscriptionsTotal = entries.filter((entry) => entry.timing === "First Half" || entry.timing === "Second Half").reduce((total, entry) => total + effectiveAmount(entry), 0);
    const lifestyleTotal = entries.filter((entry) => ["Food / Dates", "Gas", "Haircut"].includes(entry.description)).reduce((total, entry) => total + effectiveAmount(entry), 0);
    const totalBills = entries.filter(isOutflow).reduce((total, entry) => total + effectiveAmount(entry), 0);
    const healthPercent = Math.min(100, (projection.projectedBOAClose / month.targetCloseLow) * 100);
    const lineRows = rollingRows(month, projection).slice(0, 6);
    if (!lineRows.some((row) => row.month.id === state.cushionSelectedMonth)) {
      state.cushionSelectedMonth = month.id;
    }
    const status = treasuryStatus(month, projection);
    const categoryMap = new Map();
    entries.filter(isOutflow).forEach((entry) => {
      const category = categoryFor(entry);
      categoryMap.set(category, (categoryMap.get(category) || 0) + effectiveAmount(entry));
    });
    const categories = Array.from(categoryMap, ([label, value]) => ({ label, value })).filter((row) => row.value > 0);

    nodes.miniTrends.innerHTML = [
      ["Current Treasury", monthInputs(month).openingBOA, "Blue"],
      ["Projected Close", projection.projectedBOAClose, compactStatus(status.label)],
      ["Floor", month.protectedFloor, "Protected"],
      ["Hard Floor", month.redLine, "Emergency line"],
      ["Target", month.targetCloseLow, projection.gapToTarget > 0 ? "Open" : "Hit"],
      ["Scheduled Bills", totalBills, "On Plan"],
    ].map(([label, value, status]) => {
      const progress = label === "Rule Score" ? numberValue(value) / 80 * 100 : Math.min(100, Math.abs(numberValue(value)) / Math.max(1, month.targetCloseLow) * 100);
      return `<article class="executiveMiniTrend"><span>${label}</span><strong>${label === "Rule Score" ? `${value}/80` : formatMoney(value)}</strong><em>${status}</em><i><b style="width:${progress}%"></b></i></article>`;
    }).join("");
    if (nodes.insights) {
      nodes.insights.innerHTML = [
        projection.projectedBOAClose >= month.protectedFloor ? "Treasury above Floor." : "Floor is under pressure.",
        projection.projectedBOAClose >= month.redLine ? "Hard Floor protected." : "Hard Floor defense.",
        projection.treasuryGrowth >= 0 ? `Surplus ${formatMoney(projection.treasuryGrowth)}.` : "Treasury contraction.",
        projection.gapToTarget <= 0 ? "Target pace intact." : `${formatMoney(projection.gapToTarget)} to Target.`,
        `${formatMoney(totalBills)} bills planned.`,
      ].map((text) => `<article><span>Insight</span><strong>${escapeHtml(text)}</strong></article>`).join("");
    }

    nodes.chartGrid.innerHTML = `
      ${chartCard("Protection · Treasury Cushion", treasuryCushionChart(lineRows, state.cushionSelectedMonth, state.cushionMode), '<div class="executiveChartLegend"><span class="is-target">Protected</span><span class="is-floor">Watch</span><span class="is-absolute">Defend</span><span class="is-projected">Projected</span></div>')}
      ${chartCard("Cash Flow", barChart([
        { label: "Current Income", value: currentIncome },
        { label: "Current Bills", value: currentBills },
        { label: "BOA Income", value: boaIncome },
        { label: "BOA Bills", value: boaBills },
        { label: "Planned Float", value: projection.requiredCurrentFloat },
        { label: "Treasury Growth", value: projection.treasuryGrowth },
      ]))}
      ${chartCard("Allocation", donutChart(categories))}
      ${chartCard("Obligations", barChart([
        { label: "Current obligations", value: currentBills },
        { label: "BOA obligations", value: boaBills },
        { label: "Subscriptions", value: subscriptionsTotal },
        { label: "Lifestyle variable", value: lifestyleTotal },
      ]), "", false)}
      ${chartCard("Risk", progressVisual("Projected close vs target", healthPercent, 100, `${Math.round(healthPercent)}% of target · ${compactStatus(status.label)}`), "", false)}
    `;
  }

  function renderTreasury(month, projection, entries) {
    const rows = rollingRows(month, projection).slice(0, 12);
    const totalBills = entries.filter(isOutflow).reduce((total, entry) => total + effectiveAmount(entry), 0);
    const allocation = (staticModel.allocation || []).map(([label, value]) => ({ label, value: numberValue(value) }));
    nodes.treasuryCharts.innerHTML = `
      ${chartCard("12-Month Treasury Path", lineChart(rows, ["projected"]))}
      ${chartCard("Protection Ladder", lineChart(rows, ["target", "floor", "absoluteFloor", "projected"]))}
      ${chartCard("Monthly Cash Flow", barChart([
        { label: "Income", value: projection.totalIncome },
        { label: "Bills", value: projection.totalBills },
        { label: "Net flow", value: projection.netCashFlow },
        { label: "Treasury growth", value: projection.treasuryGrowth },
      ]))}
      ${chartCard("Floor Progress", progressVisual("Projected close vs floor", projection.projectedBOAClose, month.protectedFloor, `${formatMoney(projection.surplusAboveFloor)} above floor`))}
      ${chartCard("Reserve Ratio", progressVisual("BOA close vs monthly bills", projection.projectedBOAClose, Math.max(1, projection.totalBills * 2), `${(projection.projectedBOAClose / Math.max(1, projection.totalBills)).toFixed(1)}x bills`))}
      ${chartCard("Gap to Target", progressVisual("Projected close vs target", projection.projectedBOAClose, month.targetCloseLow, projection.gapToTarget > 0 ? `${formatMoney(projection.gapToTarget)} gap` : "Target covered"))}
      ${chartCard("Net Worth Trend", lineChart(rows.map((row) => ({ ...row, projected: row.projected - 6000 })), ["projected"]))}
      ${chartCard("Capital Allocation", donutChart(allocation))}
      ${chartCard("Year Progress", progressVisual("Months completed", selectedMonthIndex() + 1, 12, `${selectedMonthIndex() + 1}/12 operating checkpoints`))}
      ${chartCard("Monthly Savings Rate", progressVisual("Net flow vs income", Math.max(0, projection.netCashFlow), projection.totalIncome, `${Math.round(Math.max(0, projection.netCashFlow) / Math.max(1, projection.totalIncome) * 100)}% savings rate`))}
    `;
  }

  function renderCalendar(month, entries) {
    const [year, monthNumber] = month.id.split("-").map(Number);
    const daysInMonth = new Date(year, monthNumber, 0).getDate();
    const firstDay = new Date(year, monthNumber - 1, 1).getDay();
    const paydayDays = paydayDaysForMonth(year, monthNumber);
    const paydaySet = new Set(paydayDays);
    const byDay = new Map();
    entries.filter((entry) => ["Bill", "Transfer", "Payout", "Eval", "Sweep", "Deposit"].includes(entry.type)).forEach((entry) => {
      const day = Math.max(1, Math.min(daysInMonth, entry.dueDay || 1));
      byDay.set(day, [...(byDay.get(day) || []), entry]);
    });
    const cells = [];
    for (let i = 0; i < firstDay; i += 1) cells.push('<div class="executiveCalendarDay is-empty"></div>');
    for (let day = 1; day <= daysInMonth; day += 1) {
      const items = byDay.get(day) || [];
      const isPayday = paydaySet.has(day);
      const visibleItems = items.slice(0, 3);
      const hiddenCount = Math.max(0, items.length - visibleItems.length);
      cells.push(`
        <div class="executiveCalendarDay ${isPayday ? "is-payday" : ""}">
          <div class="executiveCalendarDayHead">
            <strong>${day}</strong>
            ${isPayday ? '<em>Payday</em>' : ""}
          </div>
          ${visibleItems.map((entry) => {
            const accountClass = String(entry.account).toLowerCase().includes("boa") ? "is-boa" : String(entry.account).toLowerCase().includes("trading") ? "is-trading" : "is-current";
            return `
              <button type="button" class="executiveCalendarPill ${accountClass} ${eventToneClass(entry)} is-${String(entry.type).toLowerCase()}" data-exec-bill="${entry.id}" title="${escapeHtml(entry.description)} · ${formatMoney(entry.amount)}">
                <span>${escapeHtml(compactCalendarLabel(entry.description))}</span>
              </button>
            `;
          }).join("")}
          ${hiddenCount ? `<button type="button" class="executiveCalendarPill is-overflow" data-exec-day="${day}"><span>+${hiddenCount} more</span></button>` : ""}
        </div>
      `);
    }
    nodes.calendar.innerHTML = `
      <div class="executivePaydayLegend">${paydayDays.map((day) => `<span>Payday ${escapeHtml(month.label.split(" ")[0])} ${day}</span>`).join("")}</div>
      <div class="executiveCalendarLegend" aria-label="Calendar legend">
        <span class="is-paycheck">Paycheck</span>
        <span class="is-wife">Wife contribution</span>
        <span class="is-boa-bill">BOA bill</span>
        <span class="is-current-bill">Current bill</span>
        <span class="is-subscription">Subscription</span>
        <span class="is-transfer">Transfer</span>
      </div>
      <div class="executiveCalendarWeekdays"><span>Sun</span><span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span></div>
      <div class="executiveCalendarGrid">${cells.join("")}</div>
    `;
  }

  function renderCalendarSummary(month, entries) {
    if (!nodes.calendarSummary) return;
    const [year, monthNumber] = month.id.split("-").map(Number);
    const paydayDays = paydayDaysForMonth(year, monthNumber);
    const scheduledEntries = entries.filter((entry) => ["Bill", "Transfer", "Payout", "Eval", "Sweep", "Deposit"].includes(entry.type));
    const outflowCount = scheduledEntries.filter(isOutflow).length;
    const cashCount = scheduledEntries.filter((entry) => ["Deposit", "Payout", "Transfer", "Sweep"].includes(entry.type)).length;
    const nextCash = nextEvent(month, scheduledEntries, (entry) => ["Deposit", "Payout", "Transfer", "Sweep"].includes(entry.type));
    const nextBill = nextEvent(month, scheduledEntries, isOutflow);
    nodes.calendarSummary.innerHTML = `
      <span class="is-month" title="${escapeHtml(month.label)}">${escapeHtml(shortMonthLabel(month))}</span>
      <span class="is-paydays" title="${paydayDays.length ? `Paydays ${paydayDays.join(" / ")}` : "No paydays"}">${paydayDays.length ? `Pay ${paydayDays.join("/")}` : "No pay"}</span>
      <span title="${outflowCount} scheduled bills">${outflowCount} bills</span>
      <span title="${cashCount} cash events">${cashCount} cash</span>
      <span class="is-event" title="${escapeHtml(eventSummary(nextCash))}">Cash ${escapeHtml(summaryEventLabel(nextCash, "None"))}</span>
      <span class="is-event" title="${escapeHtml(eventSummary(nextBill))}">Bill ${escapeHtml(summaryEventLabel(nextBill, "None"))}</span>
    `;
  }

  function renderBillDetails(entries, id) {
    const entry = entries.find((item) => item.id === id);
    if (!entry) return;
    nodes.billDetails.hidden = false;
    nodes.billDetails.innerHTML = `
      <button class="btn" type="button" data-exec-close-bill>Close</button>
      <span>${escapeHtml(entry.account)} · ${escapeHtml(entry.type)}</span>
      <strong>${escapeHtml(entry.description)}</strong>
      <p>${formatMoney(entry.amount)} scheduled for day ${entry.dueDay || "TBD"}.</p>
    `;
  }

  function renderDayDetails(entries, day) {
    const dayEntries = entries
      .filter((entry) => numberValue(entry.dueDay) === numberValue(day))
      .sort((a, b) => Math.abs(signedImpact(b)) - Math.abs(signedImpact(a)));
    if (!dayEntries.length) return;
    nodes.billDetails.hidden = false;
    nodes.billDetails.innerHTML = `
      <button class="btn" type="button" data-exec-close-bill>Close</button>
      <span>Day ${escapeHtml(day)} · ${dayEntries.length} scheduled events</span>
      <strong>Calendar details</strong>
      <div class="executiveDayEventList">
        ${dayEntries.map((entry) => `
          <button type="button" class="executiveDayEvent ${eventToneClass(entry)}" data-exec-bill="${entry.id}" title="${escapeHtml(entry.description)} · ${formatMoney(entry.amount)}">
            <span>${escapeHtml(entry.account)} · ${escapeHtml(entry.type)}</span>
            <strong>${escapeHtml(compactCalendarLabel(entry.description))}</strong>
            <em>${formatMoney(entry.amount)}</em>
          </button>
        `).join("")}
      </div>
    `;
  }

  function filteredLedger(entries) {
    const search = String(state.ledgerSearch || "").toLowerCase();
    const filtered = entries.filter((entry) => {
      const matchesSearch = !search || `${entry.timing} ${entry.account} ${entry.type} ${entry.description}`.toLowerCase().includes(search);
      const matchesFilter = !state.ledgerFilter || entry.account.includes(state.ledgerFilter);
      return matchesSearch && matchesFilter;
    });
    return filtered.sort((a, b) => {
      if (state.ledgerSort === "amount") return effectiveAmount(b) - effectiveAmount(a);
      if (state.ledgerSort === "status") return ledgerState(a).status.localeCompare(ledgerState(b).status);
      if (state.ledgerSort === "account") return a.account.localeCompare(b.account);
      return numberValue(a.dueDay) - numberValue(b.dueDay);
    });
  }

  function renderLedger(entries) {
    const rows = filteredLedger(entries);
    const pageSize = 10;
    const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
    state.ledgerPage = Math.min(Math.max(1, state.ledgerPage), pageCount);
    const pageRows = rows.slice((state.ledgerPage - 1) * pageSize, state.ledgerPage * pageSize);
    if (nodes.ledgerSearch) nodes.ledgerSearch.value = state.ledgerSearch;
    if (nodes.ledgerFilter) nodes.ledgerFilter.value = state.ledgerFilter;
    if (nodes.ledgerSort) nodes.ledgerSort.value = state.ledgerSort;
    nodes.ledger.innerHTML = pageRows.map((entry) => {
      const saved = ledgerState(entry);
      return `
        <tr>
          <td>${escapeHtml(entry.timing)}</td>
          <td>${escapeHtml(entry.account)}</td>
          <td>${escapeHtml(entry.type)}</td>
          <td>${escapeHtml(entry.description)}</td>
          <td>${formatMoney(entry.amount)}</td>
          <td><input type="number" step="0.01" data-exec-actual="${entry.id}" value="${escapeHtml(saved.actual || "")}" placeholder="Actual"></td>
          <td><select data-exec-status="${entry.id}">${["Planned", "Paid", "Skipped", "Adjusted"].map((status) => `<option value="${status}" ${saved.status === status ? "selected" : ""}>${status}</option>`).join("")}</select></td>
          <td>${formatMoney(signedImpact(entry))}</td>
          <td><input type="text" data-exec-ledger-note="${entry.id}" value="${escapeHtml(saved.notes || "")}" placeholder="Notes"></td>
        </tr>
      `;
    }).join("");
    nodes.ledgerPager.innerHTML = `
      <button class="btn" type="button" data-exec-ledger-page="${state.ledgerPage - 1}" ${state.ledgerPage <= 1 ? "disabled" : ""}>Previous</button>
      <span>Page ${state.ledgerPage} of ${pageCount} · ${rows.length} entries</span>
      <button class="btn" type="button" data-exec-ledger-page="${state.ledgerPage + 1}" ${state.ledgerPage >= pageCount ? "disabled" : ""}>Next</button>
    `;
  }

  function renderBudgetDetails(month, projection) {
    const entries = [...(month.bills || []), ...(month.subscriptions || [])];
    const groups = [
      ["Current", entries.filter((item) => item.account === "Current")],
      ["BOA", [...entries.filter((item) => item.account === "BOA"), { name: "Wife contribution", amount: 300, timing: "Monthly" }, { name: "Calculated Current float", amount: projection.requiredCurrentFloat, timing: "Auto" }]],
      ["Subscriptions", month.subscriptions || []],
      ["Trading", [{ name: "TradingView / CBOE / eval discipline", amount: 30.9, timing: "Monthly" }]],
      ["Lifestyle", entries.filter((item) => ["Food / Dates", "Gas", "Haircut"].includes(item.name))],
    ];
    nodes.budgetGroups.innerHTML = groups.map(([label, items], index) => `
      <details class="executiveBudgetGroup" ${index < 2 ? "open" : ""}>
        <summary><span>${escapeHtml(label)}</span><strong>${formatMoney(items.reduce((total, item) => total + numberValue(item.amount), 0))}</strong></summary>
        <div>${items.map((item) => `<div class="executiveBudgetRow"><span>${escapeHtml(item.name)}${item.timing ? ` · ${escapeHtml(item.timing)}` : ""}</span><strong>${formatMoney(item.amount)}</strong></div>`).join("") || "<p>No items.</p>"}</div>
      </details>
    `).join("");
  }

  function renderAdjustmentList(month) {
    const items = state.adjustments[month.id] || [];
    nodes.adjustmentList.innerHTML = items.length
      ? items.map((item) => `<div class="executiveAdjustmentRow"><span>${escapeHtml(item.description)} · ${escapeHtml(item.account)} · ${escapeHtml(item.type)}</span><strong>${formatMoney(item.amount)}</strong><button class="btn" type="button" data-exec-remove-adjustment="${item.id}">Remove</button></div>`).join("")
      : "<p>No adjustments added.</p>";
  }

  function renderReview(month) {
    nodes.reviewAccordion.innerHTML = [1, 2, 3, 4].map((week) => {
      const key = `${month.id}:week-${week}`;
      const saved = state.notes[key] || {};
      return `
        <details class="executiveReviewWeek" ${week === 1 ? "open" : ""}>
          <summary><span>Week ${week}</span><strong>${saved.complete ? "Complete" : "Open"}</strong></summary>
          <div class="executiveNoteGrid">
            ${(staticModel.notes || []).map((label) => `
              <label><span>${escapeHtml(label)}</span><textarea data-exec-review-note="${key}:${escapeHtml(label)}" rows="3" placeholder="Write the answer during weekly review.">${escapeHtml(saved[label] || "")}</textarea></label>
            `).join("")}
            <label class="executiveCheck"><input type="checkbox" data-exec-review-complete="${key}" ${saved.complete ? "checked" : ""}> <span>Weekly review complete</span></label>
          </div>
        </details>
      `;
    }).join("");
  }

  function renderTimeline(startProjection) {
    const rows = rollingRows(selectedMonth(), startProjection).slice(0, 12);
    nodes.timeline.innerHTML = rows.map(({ month, projection, openingBOA }) => {
      const status = treasuryStatus(month, projection);
      return `
        <article class="executiveTimelineProjection is-${status.tone}">
          <span>${escapeHtml(month.label)}</span>
          <strong>${formatMoney(projection.projectedBOAClose)}</strong>
          <small>Opening ${formatMoney(openingBOA)}</small>
          <small>Floor ${formatMoney(month.protectedFloor)}</small>
          <small>Target ${formatMoney(month.targetCloseLow)}</small>
          <em>${escapeHtml(status.label)}</em>
        </article>
      `;
    }).join("");
  }

  function renderYearView(projection) {
    nodes.yearView.hidden = !state.yearViewOpen;
    const rows = rollingRows(selectedMonth(), projection).slice(0, 12);
    nodes.yearGrid.innerHTML = rows.map(({ month, projection, openingBOA }) => {
      const entries = buildProjectionEntries(month, monthInputs(month));
      const bills = entries.filter(isOutflow);
      const status = treasuryStatus(month, projection);
      return `
        <button class="executiveYearCard" type="button" data-exec-month="${month.id}">
          <span>${escapeHtml(month.label)}</span>
          <strong>${formatMoney(projection.projectedBOAClose)}</strong>
          <small>Opening ${formatMoney(openingBOA)}</small>
          <small>Target ${formatMoney(month.targetCloseLow)}</small>
          <small>Status ${escapeHtml(status.label)}</small>
          <small>CEO ${staticModel.score_total || 60}/80 · ${bills.length} bills scheduled</small>
        </button>
      `;
    }).join("");
  }

  function renderSidePanel(month, projection, entries) {
    const status = treasuryStatus(month, projection);
    const nextCash = nextEvent(month, entries, (entry) =>
      ["Deposit", "Payout"].includes(entry.type));
    const nextBill = nextEvent(month, entries, isOutflow);
    const healthPercent = Math.min(100, Math.max(0, projection.projectedBOAClose / Math.max(1, month.targetCloseLow) * 100));
    const activeAction = ["adjustment", "note", "expense"].includes(state.sideAction) ? state.sideAction : "";
    const isExpanded = Boolean(state.sidePanelExpanded);
    nodes.sidePanel.classList.toggle("is-expanded", isExpanded);
    nodes.sidePanel.classList.toggle("has-action", Boolean(activeAction));
    const quickPanel = {
      adjustment: `
        <div class="executiveQuickActionPanel is-open is-adjustment">
          <div><span>Adjustment</span><button class="btn executiveMiniCollapseBtn" type="button" data-exec-side-action-close aria-label="Collapse quick action">⌄</button></div>
          <label><span>Description</span><input type="text" data-exec-quick-adjustment placeholder="Example: Bonus deposit"></label>
          <label><span>Amount</span><input type="number" step="0.01" data-exec-quick-amount></label>
          <button class="btn primary" type="button" data-exec-quick-add-adjustment>Add Adjustment</button>
        </div>
      `,
      note: `
        <div class="executiveQuickActionPanel is-open is-note">
          <div><span>Note</span><button class="btn executiveMiniCollapseBtn" type="button" data-exec-side-action-close aria-label="Collapse quick action">⌄</button></div>
          <label><span>Quick note</span><textarea data-exec-quick-note rows="3" placeholder="CEO note">${escapeHtml(state.notes[`${month.id}:quick`]?.note || "")}</textarea></label>
        </div>
      `,
      expense: `
        <div class="executiveQuickActionPanel is-open is-expense">
          <div><span>Expense</span><button class="btn executiveMiniCollapseBtn" type="button" data-exec-side-action-close aria-label="Collapse quick action">⌄</button></div>
          <label><span>Amount</span><input type="number" step="0.01" data-exec-quick-expense placeholder="Amount"></label>
          <button class="btn primary" type="button" data-exec-quick-add-expense>Add Expense</button>
        </div>
      `,
    }[activeAction] || "";
    nodes.sidePanel.innerHTML = `
      <div class="executiveSideHeader ${isExpanded ? "is-expanded" : "is-collapsed"}">
        <span class="appPill">CEO Panel</span>
        <div>
          <h3>${escapeHtml(compactStatus(status.label))}</h3>
          <small>${isExpanded ? "Settings open" : "Quick glance"}</small>
        </div>
        <button class="executiveSideExpandBtn" type="button" data-exec-side-panel-toggle aria-expanded="${isExpanded ? "true" : "false"}" aria-label="${isExpanded ? "Collapse CEO panel" : "Expand CEO panel"}">⌄</button>
      </div>
      <div class="executiveSideStatusCard is-${status.tone}">
        <div>
          <span>Treasury State</span>
          <strong>${escapeHtml(status.risk)} risk</strong>
        </div>
        <em>${Math.round(healthPercent)}%</em>
        <i><b style="width:${healthPercent}%"></b></i>
      </div>
      <div class="executiveSideEventGrid">
        <article><span>Cash</span><strong>${escapeHtml(compactEventSummary(nextCash))}</strong></article>
        <article><span>Bill</span><strong>${escapeHtml(compactEventSummary(nextBill))}</strong></article>
      </div>
      <div class="executiveSideActionCard">
        <span>Action</span>
        <strong>${escapeHtml(conciseAction(projection.recommendedAction))}</strong>
      </div>
      ${isExpanded ? `
        <div class="executiveQuickAdd executiveQuickActionDock">
          <div class="executiveQuickActionDockHead">
            <span>Quick Actions</span>
            <em>${activeAction ? `${activeAction} open` : "Select one"}</em>
          </div>
          <div class="executiveQuickActionTabs">
            ${[
              ["adjustment", "Adjust"],
              ["note", "Note"],
              ["expense", "Expense"],
            ].map(([key, label]) => `<button class="btn ${activeAction === key ? "primary" : ""}" type="button" data-exec-side-action="${key}">${label}</button>`).join("")}
          </div>
          ${quickPanel}
        </div>
      ` : ""}
      <div class="executiveSideMore ${isExpanded ? "is-open" : ""}">
        <article><span>Month</span><strong>${escapeHtml(month.label)}</strong></article>
        <article><span>Floor Cushion</span><strong>${formatMoney(projection.surplusAboveFloor)}</strong></article>
        <article><span>Target Gap</span><strong>${projection.gapToTarget > 0 ? formatMoney(projection.gapToTarget) : "Target hit"}</strong></article>
      </div>
    `;
  }

  function render() {
    const { month, entries, projection } = currentContext();
    root.classList.add("is-rendering");
    window.setTimeout(() => root.classList.remove("is-rendering"), 180);
    renderInputs(month);
    renderProjectionControlsSummary(month, projection);
    renderTabs();
    renderMonthSelector(month, state.centerMonthOnRender);
    state.centerMonthOnRender = false;
    renderMonthHeader(month, projection, entries);
    renderSummary(month, projection);
    renderOverview(month, projection, entries);
    renderVisualGraphs(month, projection, entries);
    renderTreasury(month, projection, entries);
    renderCalendarSummary(month, entries);
    renderCalendar(month, entries);
    renderLedger(entries);
    renderBudgetDetails(month, projection);
    renderAdjustmentList(month);
    renderReview(month);
    renderTimeline(projection);
    renderYearView(projection);
    renderSidePanel(month, projection, entries);
    saveState();
  }

  function selectMonth(id) {
    if (!monthById.has(id)) return;
    state.selectedMonth = id;
    state.ledgerPage = 1;
    state.centerMonthOnRender = true;
    render();
  }

  inputNodes.forEach((input) => {
    input.addEventListener("input", () => {
      const month = selectedMonth();
      state.inputs[month.id] = { ...monthInputs(month), [input.dataset.execInput]: input.value };
      if (input.dataset.execInput === "currentMonth") state.selectedMonth = input.value;
      render();
    });
  });

  root.addEventListener("input", (event) => {
    const search = event.target.closest("[data-exec-ledger-search]");
    const note = event.target.closest("[data-exec-review-note]");
    if (search) {
      state.ledgerSearch = search.value;
      state.ledgerPage = 1;
      render();
    } else if (note) {
      const [monthWeek, label] = note.dataset.execReviewNote.split(":").reduce((parts, part, index) => {
        if (index < 2) parts[0] = parts[0] ? `${parts[0]}:${part}` : part;
        else parts[1] = parts[1] ? `${parts[1]}:${part}` : part;
        return parts;
      }, ["", ""]);
      state.notes[monthWeek] = { ...(state.notes[monthWeek] || {}), [label]: note.value };
      saveState();
    }
  });

  root.addEventListener("change", (event) => {
    const monthSelectNode = event.target.closest("[data-exec-input='currentMonth']");
    const status = event.target.closest("[data-exec-status]");
    const actual = event.target.closest("[data-exec-actual]");
    const filter = event.target.closest("[data-exec-ledger-filter]");
    const sort = event.target.closest("[data-exec-ledger-sort]");
    const reviewComplete = event.target.closest("[data-exec-review-complete]");
    if (monthSelectNode) {
      selectMonth(monthSelectNode.value);
    } else if (status) {
      const id = status.dataset.execStatus;
      state.ledger[id] = { ...ledgerState({ id }), status: status.value };
      render();
    } else if (actual) {
      const id = actual.dataset.execActual;
      state.ledger[id] = { ...ledgerState({ id }), actual: actual.value };
      render();
    } else if (filter) {
      state.ledgerFilter = filter.value;
      state.ledgerPage = 1;
      render();
    } else if (sort) {
      state.ledgerSort = sort.value;
      render();
    } else if (reviewComplete) {
      const key = reviewComplete.dataset.execReviewComplete;
      state.notes[key] = { ...(state.notes[key] || {}), complete: reviewComplete.checked };
      render();
    }
  });

  root.addEventListener("toggle", (event) => {
    const chartModule = event.target.closest("[data-exec-chart-module]");
    if (!chartModule) return;
    state.chartOpen[chartModule.dataset.execChartModule] = chartModule.open;
    saveState();
  }, true);

  root.addEventListener("click", (event) => {
    const monthButton = event.target.closest("[data-exec-month]");
    const tab = event.target.closest("[data-exec-tab]");
    const bill = event.target.closest("[data-exec-bill]");
    const dayDetails = event.target.closest("[data-exec-day]");
    const page = event.target.closest("[data-exec-ledger-page]");
    const sideAction = event.target.closest("[data-exec-side-action]");
    const cushionMonth = event.target.closest("[data-cushion-month]");
    const cushionMode = event.target.closest("[data-cushion-mode]");
    if (monthButton) {
      selectMonth(monthButton.dataset.execMonth);
    } else if (tab) {
      state.activeTab = tab.dataset.execTab;
      render();
    } else if (event.target.closest("[data-exec-month-prev]")) {
      selectMonth(months[Math.max(0, selectedMonthIndex() - 1)].id);
    } else if (event.target.closest("[data-exec-month-next]")) {
      selectMonth(months[Math.min(months.length - 1, selectedMonthIndex() + 1)].id);
    } else if (event.target.closest("[data-exec-year-toggle]")) {
      state.yearViewOpen = !state.yearViewOpen;
      render();
    } else if (bill) {
      renderBillDetails(currentContext().entries, bill.dataset.execBill);
    } else if (dayDetails) {
      renderDayDetails(currentContext().entries, dayDetails.dataset.execDay);
    } else if (event.target.closest("[data-exec-close-bill]")) {
      nodes.billDetails.hidden = true;
    } else if (page) {
      state.ledgerPage = numberValue(page.dataset.execLedgerPage);
      render();
    } else if (cushionMonth) {
      state.cushionSelectedMonth = cushionMonth.dataset.cushionMonth;
      render();
    } else if (cushionMode) {
      state.cushionMode = cushionMode.dataset.cushionMode === "projected" ? "projected" : "cushion";
      render();
    } else if (sideAction) {
      state.sideAction = state.sideAction === sideAction.dataset.execSideAction ? "" : sideAction.dataset.execSideAction;
      state.sidePanelExpanded = Boolean(state.sideAction);
      render();
    } else if (event.target.closest("[data-exec-side-panel-toggle]")) {
      state.sidePanelExpanded = !state.sidePanelExpanded;
      render();
    } else if (event.target.closest("[data-exec-side-action-close]")) {
      state.sideAction = "";
      state.sidePanelExpanded = false;
      render();
    } else if (event.target.closest("[data-exec-toggle-advanced]")) {
      state.advancedOpen = !state.advancedOpen;
      render();
    } else if (event.target.closest("[data-exec-add-adjustment]")) {
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
      render();
    } else if (event.target.closest("[data-exec-quick-add-adjustment]")) {
      const month = selectedMonth();
      const description = root.querySelector("[data-exec-quick-adjustment]")?.value || "Quick adjustment";
      const amount = numberValue(root.querySelector("[data-exec-quick-amount]")?.value);
      if (!amount) return;
      state.adjustments[month.id] = [...(state.adjustments[month.id] || []), { id: `${month.id}:quick:${Date.now()}`, description, amount, account: "BOA", type: "Deposit", timing: "Quick" }];
      state.sideAction = "";
      render();
    } else if (event.target.closest("[data-exec-quick-add-expense]")) {
      const month = selectedMonth();
      const amount = numberValue(root.querySelector("[data-exec-quick-expense]")?.value);
      if (!amount) return;
      state.adjustments[month.id] = [...(state.adjustments[month.id] || []), { id: `${month.id}:expense:${Date.now()}`, description: "Quick expense", amount, account: "Current", type: "Bill", timing: "Quick" }];
      state.sideAction = "";
      render();
    } else if (event.target.closest("[data-exec-remove-adjustment]")) {
      const month = selectedMonth();
      const remove = event.target.closest("[data-exec-remove-adjustment]");
      state.adjustments[month.id] = (state.adjustments[month.id] || []).filter((item) => item.id !== remove.dataset.execRemoveAdjustment);
      render();
    } else if (event.target.closest("[data-exec-recalculate]")) {
      render();
    }
  });

  root.addEventListener("focusout", (event) => {
    const note = event.target.closest("[data-exec-ledger-note]");
    const quickNote = event.target.closest("[data-exec-quick-note]");
    if (note) {
      const id = note.dataset.execLedgerNote;
      state.ledger[id] = { ...ledgerState({ id }), notes: note.value };
      saveState();
    } else if (quickNote) {
      state.notes[`${selectedMonth().id}:quick`] = { note: quickNote.value };
      saveState();
    }
  });

  nodes.monthSelector.addEventListener("wheel", (event) => {
    if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
    event.preventDefault();
    nodes.monthSelector.scrollLeft += event.deltaY;
  }, { passive: false });
  let dragging = false;
  let dragStartX = 0;
  let dragStartScroll = 0;
  nodes.monthSelector.addEventListener("pointerdown", (event) => {
    dragging = true;
    dragStartX = event.clientX;
    dragStartScroll = nodes.monthSelector.scrollLeft;
    nodes.monthSelector.setPointerCapture(event.pointerId);
  });
  nodes.monthSelector.addEventListener("pointermove", (event) => {
    if (!dragging) return;
    nodes.monthSelector.scrollLeft = dragStartScroll - (event.clientX - dragStartX);
  });
  nodes.monthSelector.addEventListener("pointerup", () => {
    dragging = false;
  });
  root.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
    if (!document.activeElement?.closest(".executiveCommandPage")) return;
    const direction = event.key === "ArrowRight" ? 1 : -1;
    selectMonth(months[Math.min(months.length - 1, Math.max(0, selectedMonthIndex() + direction))].id);
  });

  render();
})();
