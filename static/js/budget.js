(function () {
  const app = document.getElementById("budgetCommandApp");
  if (!app) return;

  const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") || "";
  const categories = ["food", "gas", "household", "subscriptions", "entertainment", "trading", "debt", "savings", "giving", "other"];
  const budgetCategories = ["housing", "debt", "utilities", "insurance", "auto", "subscriptions", "food", "gas", "savings", "other"];
  const seedLines = [
    ["Rent 1st payment", 1320.00, 1, "housing", "bill", "any", true, false, ""],
    ["Rent 2nd payment", 1024.99, 15, "housing", "bill", "any", true, false, ""],
    ["Chase", 374.00, 29, "debt", "debt", "any", true, false, ""],
    ["AMEX", 172.00, 17, "debt", "debt", "any", true, false, ""],
    ["Power", 136.00, 2, "utilities", "bill", "any", true, false, ""],
    ["Verizon", 268.00, 26, "utilities", "bill", "any", true, false, ""],
    ["Concord", 60.00, 1, "debt", "debt", "any", true, false, ""],
    ["Capital One", 65.00, 12, "debt", "debt", "any", true, false, ""],
    ["Progressive", 208.00, 17, "insurance", "bill", "any", true, false, ""],
    ["ATT Internet", 66.00, 16, "utilities", "bill", "any", true, false, ""],
    ["Car note", 740.00, 23, "auto", "bill", "any", true, false, ""],
    ["Subscriptions", 120.00, null, "subscriptions", "subscription", "any", false, false, "monthly"],
    ["Food", 1200.00, null, "food", "food", "split", true, false, "$500/$500 set aside per pay period, plus buffer"],
    ["IRS", 380.00, 29, "debt", "debt", "any", true, false, ""],
    ["Renter Insurance", 30.99, 12, "insurance", "bill", "any", true, false, ""],
    ["Life Insurance", 14.00, 1, "insurance", "bill", "any", true, false, ""],
    ["Gas", 55.00, null, "gas", "gas", "any", true, false, "monthly"],
  ];
  const seedText = seedLines.map(([name, amount, due, , , , , , notes]) => {
    const suffix = due ? ` - ${ordinal(due)}` : notes ? ` - ${notes}` : "";
    return `${name} - ${amount.toFixed(2)}${suffix}`;
  }).join("\n");

  let state = { data: {}, summary: {}, analytics: {}, billFilter: "all", categoryFilter: "", showAllBills: false };
  let draftItems = [];
  let importItems = [];

  const money = (value, digits = 0) => Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  const exactMoney = (value) => money(value, 2);
  const setText = (id, value) => {
    const node = document.getElementById(id);
    if (node) node.textContent = value;
  };
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[char]));
  const showToast = (message) => {
    const toast = document.getElementById("budgetToast");
    if (!toast) return;
    toast.textContent = message;
    toast.hidden = false;
    window.clearTimeout(showToast._timer);
    showToast._timer = window.setTimeout(() => { toast.hidden = true; }, 2400);
  };

  async function api(path, options = {}) {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken, ...(options.headers || {}) },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false) throw new Error(payload.error || `Request failed: ${response.status}`);
    return payload;
  }

  function formPayload(form) {
    const payload = {};
    Array.from(new FormData(form).entries()).forEach(([key, value]) => { payload[key] = value; });
    form.querySelectorAll('input[type="checkbox"]').forEach((input) => { payload[input.name] = input.checked; });
    form.querySelectorAll('input[type="number"]').forEach((input) => {
      payload[input.name] = input.value === "" ? null : Number(input.value);
    });
    return payload;
  }

  async function refresh() {
    const [dataPayload, summaryPayload, analyticsPayload] = await Promise.all([
      api("/api/budget/data"),
      api("/api/budget/summary"),
      api("/api/budget/analytics"),
    ]);
    state.data = dataPayload.data;
    state.summary = summaryPayload.summary;
    state.analytics = analyticsPayload.analytics;
    render();
  }

  function render() {
    renderProfile();
    renderSummary();
    renderActionStrip();
    renderSetupItems();
    renderCurrentBudget();
    renderMonthlyMap();
    renderPaychecks();
    renderLeakAnalysis();
    renderIncomePlan();
    renderBills();
    renderCharges();
    renderGoals();
    renderDebts();
    renderAnalytics();
    renderMoneyIntelligence();
  }

  function renderProfile() {
    const profile = state.data.profile || {};
    const quick = document.getElementById("budgetProfileForm");
    const setup = document.getElementById("budgetMonthlySetupForm");
    [quick, setup].forEach((form) => {
      if (!form) return;
      Object.entries(profile).forEach(([key, value]) => {
        if (!form.elements[key]) return;
        form.elements[key].value = Array.isArray(value) ? value.join(", ") : value ?? "";
      });
    });
    document.querySelectorAll('select[name="category"]').forEach((select) => {
      if (select.options.length > 1) return;
      const list = select.closest("#budgetLineItemForm") ? budgetCategories : categories;
      select.innerHTML = list.map((category) => `<option value="${category}">${category}</option>`).join("");
    });
  }

  function renderSummary() {
    const s = state.summary || {};
    const score = Number(s.cashflow_health_score || 0);
    const card = document.getElementById("budgetScoreCard");
    const ring = document.getElementById("budgetScoreRing");
    if (card) card.dataset.scoreState = s.score_state || "danger";
    if (ring) ring.style.setProperty("--score", `${score}%`);
    setText("budgetHealthScore", score);
    setText("heroTakeHome", money(s.monthly_income));
    setText("heroCashLeft", money(s.projected_cash_left));
    setText("heroIncomeGap", money(Math.max(Number(s.income_gap_to_goal || 0), 0)));
    setText("budgetScoreLabel", scoreLabel(score));
    setText("statMonthlyIncome", exactMoney(s.monthly_income));
    setText("statPlannedOutflow", exactMoney(s.total_planned_outflow));
    setText("statBills", exactMoney(s.fixed_bills_total));
    setText("statPlannedVariables", exactMoney(s.planned_variable_total));
    setText("statActualCharges", exactMoney(s.actual_charges_total));
    setText("statCashLeft", exactMoney(s.projected_cash_left));
    setText("statBillsDueSoon", exactMoney(s.upcoming_bills_total));
    setText("statLeakSpending", exactMoney(state.analytics.leaks_total || 0));
    setText("statGoalGap", exactMoney(Math.max(Number(s.income_gap_to_goal || 0), 0)));
    setText("budgetUpcomingTotal", exactMoney(s.upcoming_bills_total));
    setText("miniIncome", exactMoney(s.monthly_income));
    setText("miniOutflow", exactMoney(s.total_planned_outflow));
    setText("miniCashLeft", exactMoney(s.projected_cash_left));
    setText("miniHealth", scoreLabel(score).split(":")[0]);
  }

  function renderMonthlyMap() {
    const s = state.summary || {};
    const health = scoreLabel(Number(s.cashflow_health_score || 0)).split(":")[0];
    setText("mapIncome", exactMoney(s.monthly_income));
    setText("mapOutflow", exactMoney(s.total_planned_outflow));
    setText("mapCashLeft", exactMoney(s.projected_cash_left));
    setText("mapCashLeftText", exactMoney(s.projected_cash_left));
    setText("mapHealth", health);
    setText("mapPlainLanguage", `You have ${exactMoney(s.projected_cash_left)} left after planned bills and categories.`);
  }

  function pct(value, total) {
    const denominator = Number(total || 0);
    if (!denominator) return 0;
    return Math.max(0, Math.min(999, (Number(value || 0) / denominator) * 100));
  }

  function sumBy(items, predicate) {
    return (items || []).reduce((sum, item) => sum + (predicate(item) ? Number(item.amount || 0) : 0), 0);
  }

  function categoryTotals() {
    const bills = state.data.bills || [];
    return bills.reduce((out, bill) => {
      const key = String(bill.category || bill.type || "other").toLowerCase();
      out[key] = Number(out[key] || 0) + Number(bill.amount || 0);
      return out;
    }, {});
  }

  function budgetDerived() {
    const s = state.summary || {};
    const bills = state.data.bills || [];
    const goals = state.analytics.goal_progress || [];
    const categoriesMap = categoryTotals();
    const income = Number(s.monthly_income || 0);
    const outflow = Number(s.total_planned_outflow || 0);
    const cashLeft = Number(s.projected_cash_left || 0);
    const paidBills = bills.filter((bill) => bill.paid).length;
    const billsPaidPct = bills.length ? (paidBills / bills.length) * 100 : 0;
    const savingsPct = goals.length
      ? goals.reduce((sum, goal) => sum + Number(goal.progress_pct || 0), 0) / goals.length
      : 0;
    const fixedTotal = sumBy(bills, (bill) => !["food", "gas", "other", "savings"].includes(String(bill.category || bill.type || "").toLowerCase()));
    const variableTotal = sumBy(bills, (bill) => ["food", "gas", "other"].includes(String(bill.category || bill.type || "").toLowerCase()));
    const debtTotal = sumBy(bills, (bill) => String(bill.category || bill.type || "").toLowerCase() === "debt");
    const flexTotal = sumBy(bills, (bill) => !bill.due_day);
    const leakRiskValue = Math.min(100, pct(Number(categoriesMap.subscriptions || 0) + Number(state.analytics.leaks_total || 0), income) * 5);
    const leakRiskLabel = leakRiskValue >= 55 ? "High" : leakRiskValue >= 25 ? "Med" : "Low";
    const biggestBill = bills.slice().sort((a, b) => Number(b.amount || 0) - Number(a.amount || 0))[0] || null;
    const upcoming = (s.upcoming_bills_next_14_days || []).slice().sort((a, b) => Number(a.due_day || 99) - Number(b.due_day || 99));
    const allocation = s.paycheck_allocation || {};
    return {
      income,
      outflow,
      cashLeft,
      paidBills,
      billsCount: bills.length,
      billsPaidPct,
      budgetUsedPct: pct(outflow, income),
      cashLeftPct: pct(cashLeft, income),
      savingsPct,
      leakRiskValue,
      leakRiskLabel,
      categoriesMap,
      fixedTotal,
      variableTotal,
      debtTotal,
      flexTotal,
      housingTotal: Number(categoriesMap.housing || 0),
      autoTotal: Number(categoriesMap.auto || 0) + Number(categoriesMap.insurance || 0) + Number(categoriesMap.gas || 0),
      foodTotal: Number(categoriesMap.food || 0),
      biggestBill,
      nextBill: upcoming[0] || null,
      safeWeekly: Math.max(0, cashLeft / 4),
      p1Total: Number((allocation.first_check || {}).bills_total || 0) + Number((allocation.first_check || {}).set_asides_total || 0),
      p2Total: Number((allocation.second_check || {}).bills_total || 0) + Number((allocation.second_check || {}).set_asides_total || 0),
      flexBucketTotal: Number((allocation.flexible || {}).total || 0),
    };
  }

  function ringState(value, dangerAt = 40, stableAt = 70) {
    if (value < dangerAt) return { state: "danger", color: "#ff7895" };
    if (value < stableAt) return { state: "tight", color: "#ffc857" };
    return { state: "strong", color: "#35d18a" };
  }

  function setRing(id, value, label, options = {}) {
    const node = document.getElementById(id);
    if (!node) return;
    const normalized = Math.max(0, Math.min(100, Number(value || 0)));
    const color = options.color || ringState(normalized).color;
    node.style.setProperty("--value", normalized.toFixed(1));
    node.style.setProperty("--ring-color", color);
    node.innerHTML = `<strong>${esc(label)}</strong>`;
    const card = node.closest(".budgetCircleCard");
    if (card) card.dataset.ringState = options.state || ringState(normalized).state;
  }

  function renderMoneyIntelligence() {
    const d = budgetDerived();
    const score = Number((state.summary || {}).cashflow_health_score || 0);
    setRing("ringCashflow", score, `${Math.round(score)}`, ringState(score));
    setRing("ringBillsPaid", d.billsPaidPct, `${d.paidBills}/${d.billsCount}`, ringState(d.billsPaidPct));
    const usedState = d.budgetUsedPct > 95 ? { state: "danger", color: "#ff7895" } : d.budgetUsedPct > 80 ? { state: "tight", color: "#ffc857" } : { state: "strong", color: "#35d18a" };
    setRing("ringBudgetUsed", Math.min(100, d.budgetUsedPct), `${Math.round(d.budgetUsedPct)}%`, usedState);
    setRing("ringSavings", d.savingsPct, `${Math.round(d.savingsPct)}%`, ringState(d.savingsPct));
    const leakState = d.leakRiskValue >= 55 ? { state: "danger", color: "#ff7895" } : d.leakRiskValue >= 25 ? { state: "tight", color: "#ffc857" } : { state: "strong", color: "#35d18a" };
    setRing("ringLeakRisk", d.leakRiskValue, d.leakRiskLabel, leakState);
    renderFlowMap(d);
    renderRatios(d);
    renderSignalCards(d);
    renderAnalyticsHighlights(d);
    renderImprovementAreas(d);
  }

  function renderFlowMap(d) {
    const node = document.getElementById("budgetFlowMap");
    if (!node) return;
    const cashState = d.cashLeft < 0 ? "danger" : d.cashLeftPct < 10 ? "tight" : d.cashLeftPct < 20 ? "stable" : "strong";
    node.dataset.cashState = cashState;
    node.innerHTML = `
      <div class="budgetFlowBlock"><span>Income</span><strong>${exactMoney(d.income)}</strong></div>
      <div class="budgetFlowArrow">→</div>
      <div class="budgetFlowBlock"><span>Planned Outflow</span><strong>${exactMoney(d.outflow)}</strong><small>${Math.round(d.budgetUsedPct)}% of income</small></div>
      <div class="budgetFlowArrow">→</div>
      <div class="budgetFlowBlock is-${cashState}"><span>Cash Left</span><strong>${exactMoney(d.cashLeft)}</strong><small>${Math.round(d.cashLeftPct)}% of income</small></div>
    `;
  }

  function renderRatios(d) {
    const node = document.getElementById("budgetRatioList");
    if (!node) return;
    const rows = [
      ["Housing", d.housingTotal],
      ["Auto", d.autoTotal],
      ["Food", d.foodTotal],
      ["Debt", d.debtTotal],
      ["Cash Left", d.cashLeft],
    ];
    node.innerHTML = rows.map(([label, value]) => {
      const percent = pct(value, d.income);
      return `<div class="budgetRatioRow"><span>${esc(label)}</span><strong>${Math.round(percent)}%</strong><div><i style="width:${Math.min(100, percent)}%"></i></div><small>${exactMoney(value)}</small></div>`;
    }).join("");
  }

  function renderSignalCards(d) {
    const node = document.getElementById("budgetSignalGrid");
    if (!node) return;
    const suggested = d.p2Total > d.p1Total + 200
      ? "Move support from Check 1 into Check 2 buffer."
      : d.foodTotal > 1000
        ? "Track food spend weekly before it drifts."
        : "Assign part of cash left before it disappears.";
    node.innerHTML = [
      ["Biggest Expense", d.biggestBill ? `${d.biggestBill.name} · ${exactMoney(d.biggestBill.amount)}` : "No planned bills"],
      ["Biggest Leak Risk", d.leakRiskLabel === "Low" ? "Controlled" : `${d.leakRiskLabel} risk`],
      ["Next Bill Due", d.nextBill ? `${d.nextBill.name} · ${exactMoney(d.nextBill.amount)} · ${formatDueDay(d.nextBill.due_day)}` : "No bill due soon"],
      ["Safe To Spend", `${exactMoney(d.safeWeekly)} this week`],
      ["Suggested Action", suggested],
    ].map(([label, value]) => `<div class="budgetSignalCard"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join("");
  }

  function renderActionStrip() {
    const s = state.summary || {};
    const leaks = Number(state.analytics.leaks_total || 0);
    const upcoming = Number(s.upcoming_bills_total || 0);
    const incomeGap = Math.max(Number(s.income_gap_to_goal || 0), 0);
    setText("actionBillsDue", money(upcoming));
    setText("actionLeaks", money(leaks));
    setText("actionIncomeGap", money(incomeGap));
    let action = "Start by mapping the month, then log each real charge.";
    if (Number(s.projected_cash_left || 0) < 0) action = "Cashflow is negative. Cut wants or add income before spending again.";
    else if (upcoming > 0) action = `Confirm the next 14 days of bills: ${money(upcoming)} still due.`;
    else if ((state.data.bills || []).length) action = `Planned outflow is ${money(s.total_planned_outflow)}. Protect the cash left.`;
    setText("budgetPrimaryAction", action);
  }

  function scoreLabel(score) {
    if (score <= 39) return "DANGER: cashflow needs immediate tightening.";
    if (score <= 69) return "TIGHT: protect cash and cut leaks.";
    if (score <= 84) return "STABLE: stay disciplined.";
    return "STRONG: budget is under control.";
  }

  function renderSetupItems() {
    const node = document.getElementById("budgetSetupLineItems");
    if (!node) return;
    node.innerHTML = draftItems.length ? `
      <div class="budgetSetupHeader"><span>Draft line items</span><strong>${draftItems.length}</strong></div>
      ${draftItems.map((item, index) => `
        <div class="budgetSetupItem">
          <strong>${esc(item.name)}</strong><span>${exactMoney(item.amount)} · ${formatDueDay(item.due_day)} · ${esc(item.category)}</span>
          <button class="btn danger" type="button" data-remove-draft="${index}">Remove</button>
        </div>
      `).join("")}
    ` : empty("No draft line items. Add one above or paste a full budget.");
  }

  function renderCurrentBudget() {
    const node = document.getElementById("budgetCurrentItems");
    if (!node) return;
    const bills = state.data.bills || [];
    node.innerHTML = bills.length ? `
      <div class="budgetCurrentHeader"><span>${bills.length} planned lines</span><strong>${exactMoney(state.summary.total_planned_outflow)}</strong></div>
      <div class="budgetCurrentScroll">
        ${bills.map((bill) => `
          <div class="budgetCurrentRow">
            <div><strong>${esc(bill.name)}</strong><span>${esc(bill.category)} · ${formatDueDay(bill.due_day)}</span></div>
            <strong class="amount">${exactMoney(bill.amount)}</strong>
          </div>
        `).join("")}
      </div>
    ` : empty("No current budget lines saved yet.");
  }

  function renderPaychecks() {
    const allocation = state.summary.paycheck_allocation || {};
    renderPaycheckCard("budgetPaycheckOne", "Paycheck 1", allocation.first_check);
    renderPaycheckCard("budgetPaycheckTwo", "Paycheck 2", allocation.second_check);
    const flexible = allocation.flexible || {};
    const node = document.getElementById("budgetFlexibleItems");
    if (node) {
      node.innerHTML = `
        <div class="budgetAllocationTop"><span>Flexible / Set Aside</span><strong>${exactMoney(flexible.total)}</strong></div>
        ${renderAllocationItems(flexible.items || []) || empty("No flexible items.")}
      `;
    }
  }

  function renderPaycheckCard(id, title, bucket = {}) {
    const node = document.getElementById(id);
    if (!node) return;
    const rows = [...(bucket.items || []), ...(bucket.set_asides || [])];
    const used = Number(bucket.bills_total || 0) + Number(bucket.set_asides_total || 0);
    const usedPercent = pct(used, bucket.income);
    const cashLeft = Number(bucket.cash_left || 0);
    const statusState = cashLeft < 0 ? "danger" : cashLeft < 200 ? "tight" : "strong";
    const statusLabel = cashLeft < 0 ? "Needs Buffer" : cashLeft < 200 ? "Tight" : "Stable";
    node.innerHTML = `
      <div class="budgetAllocationTop"><span>${title}</span><strong>${exactMoney(bucket.income)}</strong></div>
      <div class="budgetAllocationTotals">
        <div><span>Income</span><strong>${exactMoney(bucket.income)}</strong></div>
        <div><span>Bills</span><strong>${exactMoney(bucket.bills_total)}</strong></div>
        <div><span>Set-asides</span><strong>${exactMoney(bucket.set_asides_total)}</strong></div>
        <div><span>Cash left</span><strong>${exactMoney(bucket.cash_left)}</strong></div>
      </div>
      <div class="budgetPaycheckLoad" aria-hidden="true"><span style="width:${Math.min(100, usedPercent)}%"></span></div>
      ${renderAllocationItems(rows) || empty("No mapped items.")}
      <div class="budgetPaycheckFooter is-${statusState}">Status: ${statusLabel}</div>
    `;
  }

  function renderAllocationItems(items) {
    return items.map((item) => `
      <div class="budgetAllocationItem"><span>${esc(item.name)}</span><strong class="amount">${exactMoney(item.amount)}</strong><small>${formatDueDay(item.due_day)} · ${esc(item.category || item.type || "planned")}</small></div>
    `).join("");
  }

  function renderLeakAnalysis() {
    renderList("budgetLeakList", state.summary.leak_analysis || []);
    const node = document.getElementById("budgetGoalAllocation");
    const allocation = state.summary.goal_allocation || {};
    if (node) node.innerHTML = `
      <div class="pill">Suggested Cash Left Allocation</div>
      <div class="budgetMapGrid">
        <div><span>Buffer / Emergency</span><strong>${exactMoney(allocation.buffer)}</strong></div>
        <div><span>Debt Payoff</span><strong>${exactMoney(allocation.debt_payoff)}</strong></div>
        <div><span>Personal / Flex</span><strong>${exactMoney(allocation.flex)}</strong></div>
      </div>
    `;
  }

  function renderIncomePlan() {
    const profile = state.data.profile || {};
    const income = state.data.income_sources || [];
    const extra = income.filter((item) => item.active && ["trading", "business", "side_hustle"].includes(item.type));
    const trading = monthlyIncome(extra.filter((item) => item.type === "trading"));
    const business = monthlyIncome(extra.filter((item) => item.type === "business" || item.type === "side_hustle"));
    const projected = Number(state.summary.projected_monthly_income || profile.monthly_take_home || 0);
    const gap = Math.max(Number(state.summary.income_gap_to_goal || 0), 0);
    setText("incomeBase", money(profile.monthly_take_home));
    setText("incomeTrading", money(trading));
    setText("incomeBusiness", money(business));
    setText("incomeProjected", money(projected));
    setText("incomeWeeklyGap", money(gap / 4));
    setText("incomeWorkdayGap", money(gap / 20));
    const fill = document.getElementById("incomeProgressFill");
    const target = Number(profile.target_extra_monthly_income || 0);
    if (fill) fill.style.width = `${Math.min(100, target ? ((trading + business) / target) * 100 : 0)}%`;
  }

  function monthlyIncome(items) {
    return items.reduce((sum, item) => {
      const amount = Number(item.amount || 0);
      if (item.frequency === "weekly") return sum + amount * 52 / 12;
      if (item.frequency === "biweekly") return sum + amount * 26 / 12;
      return sum + amount;
    }, 0);
  }

  function renderBills() {
    const table = document.getElementById("budgetBillsTable");
    if (!table) return;
    const toggleButton = document.getElementById("budgetToggleBillsButton");
    const upcomingIds = new Set((state.summary.upcoming_bills_next_14_days || []).map((bill) => bill.id));
    const filteredBills = (state.data.bills || []).filter((bill) => {
      if (state.billFilter === "paid") return bill.paid;
      if (state.billFilter === "unpaid") return !bill.paid;
      if (state.billFilter === "upcoming") return upcomingIds.has(bill.id);
      if (state.billFilter === "essential") return bill.essential;
      return true;
    });
    const sortedBills = filteredBills.slice().sort((a, b) => {
      const aDue = Number(a.due_day || 99);
      const bDue = Number(b.due_day || 99);
      return aDue - bDue || Number(b.amount || 0) - Number(a.amount || 0);
    });
    const bills = state.showAllBills ? sortedBills : sortedBills.slice(0, 5);
    if (toggleButton) {
      toggleButton.textContent = state.showAllBills ? "Show Top 5" : `View All Bills (${filteredBills.length})`;
      toggleButton.hidden = filteredBills.length <= 5;
    }
    table.innerHTML = bills.length ? `
      <div class="budgetRow budgetBillRow budgetBillRowHead"><span>Bill</span><span>Amount</span><span>Due</span><span>Status</span><span>Actions</span></div>
      ${bills.map((bill) => `
        <div class="budgetRow budgetBillRow">
          <div>
            <strong>${esc(bill.name)}</strong>
            <span>${esc(bill.category)}${bill.notes || bill.type ? ` · ${esc(bill.notes || bill.type || "")}` : ""}</span>
          </div>
          <div class="amount">${exactMoney(bill.amount)}</div>
          <div><span class="budgetDuePill">${formatDueDay(bill.due_day)}</span></div>
          <div class="budgetBillStatus">
            <span class="budgetTag ${bill.paid ? "isGood" : "isWarn"}">${bill.paid ? "Paid" : "Unpaid"}</span>
            <span class="budgetTag">${bill.essential ? "Essential" : "Flexible"}</span>
          </div>
          <div class="budgetRowActions">
            ${bill.paid ? "" : `<button class="btn budgetPaidButton" data-mark-paid="${bill.id}" type="button">Paid</button>`}
            <button class="btn" data-edit="bill" data-id="${bill.id}" type="button">Edit</button>
            <button class="btn danger" data-delete="bill" data-id="${bill.id}" type="button">Delete</button>
          </div>
        </div>`).join("")}
      ${!state.showAllBills && filteredBills.length > 5 ? `<div class="budgetTableNote">Showing top 5 upcoming bills. Use View All Bills for the full table.</div>` : ""}
    ` : empty("No bills logged yet. Add rent, utilities, insurance, and subscriptions first.");
  }

  function renderCharges() {
    const table = document.getElementById("budgetChargesTable");
    const chipBox = document.getElementById("budgetCategoryChips");
    if (chipBox) {
      chipBox.innerHTML = ["", ...categories].map((cat) => `
        <button class="btn ${state.categoryFilter === cat ? "primary" : ""}" data-category-filter="${cat}" type="button">${cat || "All"}</button>
      `).join("");
    }
    if (!table) return;
    const charges = (state.data.charges || [])
      .filter((charge) => !state.categoryFilter || charge.category === state.categoryFilter)
      .slice()
      .sort((a, b) => String(b.date).localeCompare(String(a.date)));
    table.innerHTML = charges.length ? charges.map((charge) => `
      <div class="budgetRow budgetChargeRow">
        <div><strong>${esc(charge.name)}</strong><span>${esc(charge.date)} · ${esc(charge.category)}</span></div>
        <div class="amount">${exactMoney(charge.amount)}</div>
        <div><span class="budgetTag ${charge.need_or_want === "leak" ? "isBad" : ""}">${esc(charge.need_or_want)}</span></div>
        <div>${esc(charge.notes || charge.payment_method)}</div>
        <div class="budgetRowActions">
          <button class="btn" data-edit="charge" data-id="${charge.id}" type="button">Edit</button>
          <button class="btn danger" data-delete="charge" data-id="${charge.id}" type="button">Delete</button>
        </div>
      </div>`).join("") : empty("No charges logged yet. Add the latest card swipe or cash spend.");
  }

  function renderGoals() {
    const node = document.getElementById("budgetGoalsGrid");
    if (!node) return;
    const goals = state.analytics.goal_progress || [];
    node.innerHTML = goals.length ? goals.map((goal) => `
      <div class="budgetMiniCard">
        <div class="budgetMiniTop"><strong>${esc(goal.name)}</strong><span>${goal.progress_pct}%</span></div>
        <div class="budgetProgress"><span style="width:${Math.min(100, goal.progress_pct)}%"></span></div>
        <p>${exactMoney(goal.current_amount)} / ${exactMoney(goal.target_amount)} · needs ${exactMoney(goal.monthly_needed)}/mo</p>
        <div class="budgetRowActions"><button class="btn" data-edit="goal" data-id="${goal.id}" type="button">Edit</button><button class="btn danger" data-delete="goal" data-id="${goal.id}" type="button">Delete</button></div>
      </div>`).join("") : `${empty("No savings goals yet.")}<button class="btn secondary" type="button" data-budget-panel="goal">Add Goal</button>`;
  }

  function renderDebts() {
    const node = document.getElementById("budgetDebtsGrid");
    if (!node) return;
    const debts = state.data.debts || [];
    node.innerHTML = debts.length ? debts.map((debt) => `
      <div class="budgetMiniCard">
        <div class="budgetMiniTop"><strong>${esc(debt.name)}</strong><span>${esc(debt.priority)}</span></div>
        <p>Balance ${exactMoney(debt.balance)} · minimum ${exactMoney(debt.minimum_payment)} · APR ${debt.interest_rate}%</p>
        <p>Focus: ${debt.priority === "high" || debt.interest_rate >= 18 ? "Attack this first." : "Keep current."}</p>
        <div class="budgetRowActions"><button class="btn" data-edit="debt" data-id="${debt.id}" type="button">Edit</button><button class="btn danger" data-delete="debt" data-id="${debt.id}" type="button">Delete</button></div>
      </div>`).join("") : `${empty("No debts logged.")}<button class="btn secondary" type="button" data-budget-panel="debt">Add Debt</button>`;
  }

  function renderAnalytics() {
    renderList("budgetWarnings", state.analytics.budget_warnings || []);
    renderList("budgetRecommendations", state.analytics.budget_recommendations || []);
    renderBars("budgetCategoryChart", state.analytics.spending_by_category || {});
    renderBars("budgetNeedChart", state.analytics.need_vs_want_breakdown || {});
    renderLine("budgetCashflowChart", state.analytics.cashflow_by_week || []);
    renderDue("budgetDueChart", state.analytics.bills_by_due_date || []);
    const goalData = {};
    (state.analytics.goal_progress || []).forEach((goal) => { goalData[goal.name] = goal.progress_pct; });
    renderBars("budgetGoalChart", goalData, "%");
    const debt = state.analytics.debt_summary || {};
    const debtNode = document.getElementById("budgetDebtSummary");
    if (debtNode) debtNode.innerHTML = `<strong>${money(debt.total_balance)}</strong><span>Total debt</span><strong>${money(debt.total_minimums)}</strong><span>Monthly minimums</span><strong>${esc(debt.focus_debt || "None")}</strong><span>Suggested focus</span>`;
  }

  function renderAnalyticsHighlights(d) {
    renderBars("budgetCategoryHighlight", d.categoriesMap || {});
    renderBars("budgetMixHighlight", {
      Fixed: d.fixedTotal,
      Variable: d.variableTotal,
      Debt: d.debtTotal,
      Flex: d.flexTotal,
    });
    renderBars("budgetPaycheckHighlight", {
      "Check 1": d.p1Total,
      "Check 2": d.p2Total,
      Flexible: d.flexBucketTotal,
    });
    renderTimelineHighlight();
    renderAllocationHighlight();
  }

  function renderTimelineHighlight() {
    const node = document.getElementById("budgetTimelineHighlight");
    if (!node) return;
    const buckets = { "Week 1": 0, "Week 2": 0, "Week 3": 0, "Week 4": 0, Flexible: 0 };
    (state.data.bills || []).forEach((bill) => {
      const day = Number(bill.due_day || 0);
      const amount = Number(bill.amount || 0);
      if (!day) buckets.Flexible += amount;
      else if (day <= 7) buckets["Week 1"] += amount;
      else if (day <= 14) buckets["Week 2"] += amount;
      else if (day <= 21) buckets["Week 3"] += amount;
      else buckets["Week 4"] += amount;
    });
    renderBars("budgetTimelineHighlight", buckets);
  }

  function renderAllocationHighlight() {
    const node = document.getElementById("budgetAllocationHighlight");
    if (!node) return;
    const allocation = state.summary.goal_allocation || {};
    const total = Number(allocation.buffer || 0) + Number(allocation.debt_payoff || 0) + Number(allocation.flex || 0);
    if (!total) {
      node.innerHTML = empty("No positive cash left to allocate yet.");
      return;
    }
    node.innerHTML = `
      <div class="budgetStackedBar">
        <i style="width:${pct(allocation.buffer, total)}%"></i>
        <b style="width:${pct(allocation.debt_payoff, total)}%"></b>
        <em style="width:${pct(allocation.flex, total)}%"></em>
      </div>
      <div class="budgetStackedLegend">
        <span>Buffer ${exactMoney(allocation.buffer)}</span>
        <span>Debt ${exactMoney(allocation.debt_payoff)}</span>
        <span>Flex ${exactMoney(allocation.flex)}</span>
      </div>
    `;
  }

  function renderImprovementAreas(d) {
    const node = document.getElementById("budgetImproveList");
    if (!node) return;
    const opportunities = [
      { label: "Food budget", current: d.foodTotal, target: Math.min(d.foodTotal, 1000), note: "Track actual spend weekly." },
      { label: "Subscriptions", current: Number(d.categoriesMap.subscriptions || 0), target: Math.min(Number(d.categoriesMap.subscriptions || 0), 80), note: "Review what can be cut." },
      { label: "Second-check load", current: d.p2Total, target: Math.max(0, d.p2Total - 200), note: "Move support from Check 1." },
    ].filter((item) => item.current > item.target);
    node.innerHTML = (opportunities.length ? opportunities : [{ label: "No major leaks", current: 0, target: 0, note: "Keep tracking every charge." }]).slice(0, 3).map((item) => `
      <div class="budgetImproveItem">
        <strong>${esc(item.label)}</strong>
        <span>Current ${exactMoney(item.current)} · Target ${exactMoney(item.target)}</span>
        <small>Potential savings ${exactMoney(Math.max(0, item.current - item.target))}. ${esc(item.note)}</small>
      </div>
    `).join("");
  }

  function renderList(id, items) {
    const node = document.getElementById(id);
    if (node) node.innerHTML = (items.length ? items : ["No data yet."]).map((item) => `<li>${esc(item)}</li>`).join("");
  }

  function renderBars(id, data, suffix = "") {
    const node = document.getElementById(id);
    if (!node) return;
    const entries = Object.entries(data)
      .filter(([, value]) => Number(value) > 0)
      .sort((a, b) => Number(b[1]) - Number(a[1]));
    const max = Math.max(...entries.map(([, value]) => Number(value)), 1);
    node.innerHTML = entries.length ? entries.map(([label, value]) => `
      <div class="budgetBarRow"><span>${esc(label)}</span><strong>${suffix ? `${Number(value).toFixed(1)}${suffix}` : money(value)}</strong><div><i style="width:${Math.max(4, (Number(value) / max) * 100)}%"></i></div></div>
    `).join("") : empty("Add budget lines or charges to generate chart.");
  }

  function renderLine(id, rows) {
    const node = document.getElementById(id);
    if (!node) return;
    if (!rows.length) {
      node.innerHTML = empty("Add budget lines or charges to generate chart.");
      return;
    }
    const values = rows.map((row) => Number(row.cash_left || 0));
    const min = Math.min(...values, 0);
    const max = Math.max(...values, 1);
    const points = values.map((value, index) => {
      const x = rows.length <= 1 ? 0 : (index / (rows.length - 1)) * 320;
      const y = 120 - ((value - min) / (max - min || 1)) * 100 - 10;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    node.innerHTML = `<svg viewBox="0 0 320 130" aria-hidden="true"><polyline points="${points.join(" ")}" fill="none" stroke="#68e7ff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"></polyline></svg>`;
  }

  function renderDue(id, rows) {
    const node = document.getElementById(id);
    if (!node) return;
    node.innerHTML = rows.length ? rows.map((bill) => `
      <span class="budgetDueTile" title="${esc(bill.name)}">
        <span class="budgetDueLabel">${formatDueDay(bill.due_day)}</span>
        <b>${money(bill.amount)}</b>
      </span>
    `).join("") : empty("Add budget lines or charges to generate chart.");
  }

  function renderImportPreview() {
    const node = document.getElementById("budgetImportPreview");
    if (!node) return;
    node.innerHTML = importItems.length ? `
      <div class="budgetPreviewTable">
        <div class="budgetPreviewHead"><span>Name</span><span>Amount</span><span>Due</span><span>Category</span><span>Type</span><span>Allocation</span><span>Notes</span></div>
        ${importItems.map((item, index) => `
          <div class="budgetPreviewRow" data-import-row="${index}">
            <input data-import-field="name" value="${esc(item.name)}">
            <input data-import-field="amount" type="number" min="0" step="0.01" value="${item.amount}">
            <input data-import-field="due_day" type="number" min="1" max="31" step="1" value="${item.due_day ?? ""}">
            <select data-import-field="category">${budgetCategories.map((cat) => `<option value="${cat}" ${cat === item.category ? "selected" : ""}>${cat}</option>`).join("")}</select>
            <select data-import-field="type">${["bill", "debt", "subscription", "food", "gas", "savings", "other"].map((type) => `<option value="${type}" ${type === item.type ? "selected" : ""}>${type}</option>`).join("")}</select>
            <select data-import-field="paycheck_allocation">${["any", "first_check", "second_check", "split"].map((value) => `<option value="${value}" ${value === item.paycheck_allocation ? "selected" : ""}>${value}</option>`).join("")}</select>
            <input data-import-field="notes" value="${esc(item.notes || "")}">
          </div>
        `).join("")}
      </div>
    ` : empty("Preview parsed budget lines before saving.");
  }

  function showSetupPane(name) {
    document.querySelectorAll("[data-budget-setup-pane]").forEach((pane) => {
      const active = pane.dataset.budgetSetupPane === name;
      pane.hidden = !active;
      pane.classList.toggle("is-active", active);
    });
    document.querySelectorAll("[data-budget-setup-tab]").forEach((button) => {
      button.classList.toggle("primary", button.dataset.budgetSetupTab === name);
    });
  }

  function empty(message) {
    return `<div class="budgetEmpty">${esc(message)}</div>`;
  }

  function fillForm(type, item) {
    const form = document.querySelector(`[data-budget-form="${type}"]`);
    if (!form) return;
    showPanel(type);
    Object.entries(item).forEach(([key, value]) => {
      const field = form.elements[key];
      if (!field) return;
      if (field.type === "checkbox") field.checked = Boolean(value);
      else field.value = value ?? "";
    });
    form.scrollIntoView({ behavior: "smooth", block: "center" });
  }

  function showPanel(type) {
    document.querySelectorAll("[data-budget-form]").forEach((form) => {
      const active = form.dataset.budgetForm === type;
      form.hidden = !active;
      form.classList.toggle("is-active", active);
    });
    document.querySelectorAll("[data-budget-panel]").forEach((button) => {
      button.classList.toggle("primary", button.dataset.budgetPanel === type);
    });
  }

  function ordinal(value) {
    const number = Number(value);
    if (!number) return "Flexible";
    const suffix = number % 10 === 1 && number % 100 !== 11 ? "st" : number % 10 === 2 && number % 100 !== 12 ? "nd" : number % 10 === 3 && number % 100 !== 13 ? "rd" : "th";
    return `${number}${suffix}`;
  }

  function formatDueDay(value) {
    return value ? ordinal(value) : "Flexible";
  }

  function inferCategory(name) {
    const text = String(name || "").toLowerCase();
    if (text.includes("rent")) return "housing";
    if (text.includes("car note")) return "auto";
    if (["chase", "amex", "capital one", "concord", "irs"].some((key) => text.includes(key))) return "debt";
    if (["power", "verizon", "att", "internet"].some((key) => text.includes(key))) return "utilities";
    if (["progressive", "renter", "life insurance", "insurance"].some((key) => text.includes(key))) return "insurance";
    if (text.includes("subscription")) return "subscriptions";
    if (text.includes("food")) return "food";
    if (text.includes("gas")) return "gas";
    return "other";
  }

  function inferType(name, category) {
    if (["food", "gas"].includes(category)) return category;
    if (category === "subscriptions") return "subscription";
    if (category === "debt") return "debt";
    return "bill";
  }

  function parseBudgetLines(text) {
    return String(text || "").split(/\n+/).map((line) => parseBudgetLine(line)).filter(Boolean);
  }

  function parseBudgetLine(line) {
    const raw = String(line || "").trim();
    if (!raw) return null;
    const amountMatch = raw.match(/\$?\d[\d,]*(?:\.\d{1,2})?/);
    if (!amountMatch) return null;
    const amount = Number(amountMatch[0].replace(/[$,]/g, ""));
    const before = raw.slice(0, amountMatch.index).replace(/[—|\t]/g, "-").replace(/-+$/, "").trim();
    const after = raw.slice((amountMatch.index || 0) + amountMatch[0].length).replace(/^[\s—|\t-]+/, "").trim();
    const dueMatch = after.match(/(?:due\s*(?:day)?\s*)?(\d{1,2})(?:st|nd|rd|th)?/i);
    const dueDay = dueMatch ? Number(dueMatch[1]) : null;
    const notes = after.replace(/(?:due\s*(?:day)?\s*)?\d{1,2}(?:st|nd|rd|th)?/i, "").replace(/^[\s—|\t-]+/, "").trim();
    const name = before || raw.split(/[-—|\t]/)[0].trim();
    const category = inferCategory(name);
    return {
      name,
      amount,
      due_day: dueDay && dueDay >= 1 && dueDay <= 31 ? dueDay : null,
      category,
      type: inferType(name, category),
      paycheck_allocation: notes.includes("500/500") ? "split" : "any",
      essential: true,
      autopay: false,
      paid: false,
      active: true,
      notes,
    };
  }

  function collectImportItems() {
    const rows = document.querySelectorAll("[data-import-row]");
    return Array.from(rows).map((row) => {
      const item = {};
      row.querySelectorAll("[data-import-field]").forEach((field) => {
        item[field.dataset.importField] = field.type === "number" ? (field.value === "" ? null : Number(field.value)) : field.value;
      });
      item.essential = true;
      item.autopay = false;
      item.paid = false;
      item.active = true;
      return item;
    });
  }

  async function saveBills(items) {
    for (const item of items) {
      if (!item.name || !Number(item.amount || 0)) continue;
      await api("/api/budget/bill", { method: "POST", body: JSON.stringify(item) });
    }
  }

  document.getElementById("budgetProfileToggle")?.addEventListener("click", () => {
    const form = document.getElementById("budgetProfileForm");
    if (form) form.hidden = !form.hidden;
  });

  document.querySelectorAll("[data-budget-panel]").forEach((button) => {
    button.addEventListener("click", () => showPanel(button.dataset.budgetPanel));
  });

  document.querySelectorAll("[data-budget-setup-tab]").forEach((button) => {
    button.addEventListener("click", () => showSetupPane(button.dataset.budgetSetupTab));
  });

  document.querySelectorAll("[data-budget-form]").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const type = form.dataset.budgetForm;
      try {
        await api(`/api/budget/${type}`, { method: "POST", body: JSON.stringify(formPayload(form)) });
        form.reset();
        if (type === "charge") form.elements.date.value = app.dataset.today || "";
        await refresh();
        showToast(`${type[0].toUpperCase()}${type.slice(1)} saved.`);
      } catch (error) {
        showToast(error.message || "Could not save budget item.");
      }
    });
  });

  document.getElementById("budgetProfileForm")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await api("/api/budget/profile", { method: "POST", body: JSON.stringify(formPayload(event.currentTarget)) });
      await refresh();
      event.currentTarget.hidden = true;
      showToast("Profile saved.");
    } catch (error) {
      showToast(error.message || "Could not save profile.");
    }
  });

  document.getElementById("budgetMonthlySetupForm")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = formPayload(event.currentTarget);
    payload.paycheck_dates = String(payload.paycheck_dates || "").split(",").map((item) => item.trim()).filter(Boolean);
    try {
      await api("/api/budget/profile", { method: "POST", body: JSON.stringify(payload) });
      await saveBills(draftItems);
      draftItems = [];
      await refresh();
      showToast("Monthly budget saved.");
    } catch (error) {
      showToast(error.message || "Could not save monthly budget.");
    }
  });

  document.getElementById("budgetLineItemForm")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const payload = formPayload(event.currentTarget);
    if (!payload.name || !Number(payload.amount || 0)) {
      showToast("Line item needs a name and amount.");
      return;
    }
    draftItems.push({ ...payload, paid: false, active: true });
    event.currentTarget.reset();
    event.currentTarget.elements.essential.checked = true;
    renderSetupItems();
  });

  document.getElementById("budgetPreviewImportButton")?.addEventListener("click", () => {
    importItems = parseBudgetLines(document.getElementById("budgetImportText")?.value || "");
    renderImportPreview();
    showToast(importItems.length ? `${importItems.length} lines ready to review.` : "No valid budget lines found.");
  });

  document.getElementById("budgetSaveImportButton")?.addEventListener("click", async () => {
    try {
      const items = collectImportItems().length ? collectImportItems() : importItems;
      await saveBills(items);
      importItems = [];
      renderImportPreview();
      await refresh();
      showToast("Imported budget saved.");
    } catch (error) {
      showToast(error.message || "Could not save imported budget.");
    }
  });

  document.getElementById("budgetSeedButton")?.addEventListener("click", async () => {
    try {
      document.getElementById("budgetImportText").value = seedText;
      await api("/api/budget/profile", {
        method: "POST",
        body: JSON.stringify({ monthly_take_home: 7269.66, pay_frequency: "biweekly", paycheck_amount: null, target_extra_monthly_income: 4000 }),
      });
      await saveBills(seedLines.map(([name, amount, due_day, category, type, paycheck_allocation, essential, autopay, notes]) => ({
        name, amount, due_day, category, type, paycheck_allocation, essential, autopay, notes, paid: false, active: true,
      })));
      await refresh();
      showToast("Fitz current budget loaded.");
    } catch (error) {
      showToast(error.message || "Could not load Fitz budget.");
    }
  });

  document.addEventListener("click", async (event) => {
    const edit = event.target.closest("[data-edit]");
    const del = event.target.closest("[data-delete]");
    const markPaid = event.target.closest("[data-mark-paid]");
    const filter = event.target.closest("[data-filter]");
    const categoryFilter = event.target.closest("[data-category-filter]");
    const toggleBills = event.target.closest("#budgetToggleBillsButton");
    const panelButton = event.target.closest("[data-budget-panel]");
    const removeDraft = event.target.closest("[data-remove-draft]");
    if (panelButton) {
      showPanel(panelButton.dataset.budgetPanel);
    }
    if (removeDraft) {
      draftItems.splice(Number(removeDraft.dataset.removeDraft), 1);
      renderSetupItems();
    }
    if (filter) {
      state.billFilter = filter.dataset.filter || "all";
      renderBills();
      document.querySelectorAll("[data-filter]").forEach((button) => button.classList.toggle("primary", button === filter));
    }
    if (categoryFilter) {
      state.categoryFilter = categoryFilter.dataset.categoryFilter || "";
      renderCharges();
    }
    if (toggleBills) {
      state.showAllBills = !state.showAllBills;
      renderBills();
    }
    if (markPaid) {
      const bill = (state.data.bills || []).find((row) => row.id === markPaid.dataset.markPaid);
      if (!bill) return;
      markPaid.disabled = true;
      try {
        await api("/api/budget/bill", {
          method: "POST",
          body: JSON.stringify({ ...bill, paid: true }),
        });
        await refresh();
        showToast(`${bill.name || "Bill"} marked paid.`);
      } catch (error) {
        markPaid.disabled = false;
        showToast(error.message || "Could not mark bill paid.");
      }
      return;
    }
    if (edit) {
      const type = edit.dataset.edit;
      const key = type === "income" ? "income_sources" : `${type}s`;
      const item = (state.data[key] || []).find((row) => row.id === edit.dataset.id);
      if (item) fillForm(type, item);
    }
    if (del) {
      const type = del.dataset.delete;
      if (!window.confirm(`Delete this ${type}?`)) return;
      try {
        await api(`/api/budget/${type}/${del.dataset.id}`, { method: "DELETE" });
        await refresh();
        showToast(`${type[0].toUpperCase()}${type.slice(1)} deleted.`);
      } catch (error) {
        showToast(error.message || "Could not delete item.");
      }
    }
  });

  document.getElementById("budgetCloseMonthButton")?.addEventListener("click", async () => {
    try {
      const payload = await api("/api/budget/monthly-review", { method: "POST", body: JSON.stringify({}) });
      await refresh();
      showToast(`Month reviewed. Cash left: ${money(payload.review.cash_left)}.`);
    } catch (error) {
      showToast(error.message || "Could not close month.");
    }
  });

  const chargeDate = document.querySelector('#budgetChargeForm input[name="date"]');
  if (chargeDate) chargeDate.value = app.dataset.today || "";
  const importBox = document.getElementById("budgetImportText");
  if (importBox && !importBox.value) importBox.value = seedText;
  const fullAnalytics = document.querySelector(".budgetAnalyticsFold");
  if (fullAnalytics) fullAnalytics.open = false;
  renderImportPreview();
  refresh().catch((error) => showToast(error.message || "Could not load budget."));
})();
