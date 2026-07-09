(function () {
  const app = document.getElementById("budgetCommandApp");
  if (!app) return;

  const STORAGE_KEY = "mccain.budget.monthLedger.v1";
  const accounts = ["Current", "BOA", "Trading"];
  const buckets = ["Paycheck 1", "Paycheck 2"];
  const categories = ["Housing", "Utilities", "Phone", "Insurance", "Credit", "Taxes", "Food", "Lifestyle", "Subscriptions", "Trading", "One-Time"];
  const renewalTypes = ["monthly", "quarterly", "annual", "prepaid"];
  let activePaycheckTab = "paycheck1";
  let activeSubTab = "Personal";

  const uid = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
  const num = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
  const money = (value, digits = 2) => num(value).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[char]));

  function bill(name, amount, dueDate, account, paycheckBucket, category, extras = {}) {
    return {
      id: uid(),
      dueDate,
      name,
      amount,
      account,
      paycheckBucket,
      category,
      recurring: extras.recurring ?? true,
      paid: extras.paid ?? false,
      optional: extras.optional ?? false,
      active: extras.active ?? true,
    };
  }

  function subscription(name, amount, dueDate, group, account, paycheckBucket, renewalType = "monthly", nextRenewalDate = "", notes = "", active = true) {
    return { id: uid(), name, amount, dueDate, group, account, paycheckBucket, renewalType, nextRenewalDate, notes, active, paid: false };
  }

  function julyMonth() {
    return {
      id: "2026-07",
      monthName: "July 2026",
      closed: false,
      accounts: {
        current: { starting: 1000, target: 1000, min: 750, max: 1500, actualEnding: null },
        boa: { starting: 50, actualEnding: null },
        robinhood: { pending: 2700, included: false },
      },
      paychecks: [
        { id: uid(), name: "Paycheck 1", currentDeposit: 1866, boaDeposit: 1866, date: "2026-07-03" },
        { id: uid(), name: "Paycheck 2", currentDeposit: 1866, boaDeposit: 1866, date: "2026-07-17" },
      ],
      bills: [
        bill("Rent", 1180, "2026-07-02", "Current", "Paycheck 1", "Housing"),
        bill("Power", 136, "2026-07-02", "Current", "Paycheck 1", "Utilities"),
        bill("Verizon Catch-up", 267, "2026-07-02", "Current", "Paycheck 1", "Phone"),
        bill("Life Insurance", 13, "2026-07-02", "Current", "Paycheck 1", "Insurance"),
        bill("Credit One", 30, "2026-07-09", "Current", "Paycheck 1", "Credit"),
        bill("Capital One", 62, "2026-07-12", "Current", "Paycheck 1", "Credit"),
        bill("Indigo", 54, "2026-07-14", "Current", "Paycheck 1", "Credit"),
        bill("Groceries/Eating Out", 500, "2026-07-05", "Current", "Paycheck 1", "Food"),
        bill("Groceries/Eating Out", 500, "2026-07-19", "Current", "Paycheck 2", "Food"),
        bill("Gas", 50, "2026-07-05", "Current", "Paycheck 1", "Lifestyle"),
        bill("Haircuts", 100, "2026-07-05", "Current", "Paycheck 1", "Lifestyle"),
        bill("Progressive", 193, "2026-07-17", "Current", "Paycheck 2", "Insurance"),
        bill("AT&T Internet", 65, "2026-07-17", "Current", "Paycheck 2", "Utilities"),
        bill("Verizon", 267, "2026-07-26", "Current", "Paycheck 2", "Phone"),
        bill("IRS", 402, "2026-07-28", "Current", "Paycheck 2", "Taxes"),
        bill("Discover", 137, "2026-07-10", "BOA", "Paycheck 1", "Credit"),
        bill("One-time BOA Balance", 200.81, "2026-07-10", "BOA", "Paycheck 1", "One-Time", { recurring: false }),
        bill("AMEX", 172, "2026-07-17", "BOA", "Paycheck 2", "Credit"),
        bill("Car Note", 737, "2026-07-23", "BOA", "Paycheck 2", "Credit"),
      ],
      subscriptions: [
        subscription("Apple Music", 16.99, "2026-07-05", "Personal", "Current", "Paycheck 1"),
        subscription("AppleCare", 5.99, "2026-07-05", "Personal", "Current", "Paycheck 1"),
        subscription("Gmail", 1.99, "2026-07-05", "Personal", "Current", "Paycheck 1"),
        subscription("iCloud", 2.99, "2026-07-05", "Personal", "Current", "Paycheck 1"),
        subscription("Peacock", 16.99, "2026-07-05", "Streaming", "Current", "Paycheck 1"),
        subscription("UHF", 1.99, "2026-07-05", "Streaming", "Current", "Paycheck 1"),
        subscription("Amazon Prime", 14.99, "2026-07-15", "Personal", "Current", "Paycheck 1"),
        subscription("Audible", 8.99, "2026-07-14", "Personal", "Current", "Paycheck 1"),
        subscription("TV Streaming", 25, "2026-07-26", "Streaming", "Current", "Paycheck 2"),
        subscription("Apollo", 24.99, "2026-07-25", "Streaming", "Current", "Paycheck 2", "monthly", "2026-07-25", "Apollo renews around July 25."),
        subscription("ChatGPT", 20, "2026-07-28", "Personal", "Current", "Paycheck 2"),
        subscription("Falcon prepaid", 0, "2026-09-05", "Streaming", "Current", "Paycheck 2", "prepaid", "2026-09-05", "Prepaid through around September 5."),
        subscription("Apollo annual option", 159.99, "", "Streaming", "Current", "Paycheck 2", "annual", "", "Do not include unless selected.", false),
        subscription("TradingView", 26.90, "2026-07-05", "Trading", "Trading", "Paycheck 1"),
        subscription("CBOE", 4.00, "2026-07-05", "Trading", "Trading", "Paycheck 1"),
        subscription("Funded/evaluation subscription", 250, "", "Trading", "Trading", "Paycheck 2", "monthly", "", "Optional 250-375.", false),
      ],
      trading: {
        tradingView: 26.90,
        cboe: 4,
        evaluationFunded: 250,
        includeEvaluation: false,
        tradingIncome: 0,
        actualTrading: null,
      },
      closeout: {
        actualCurrent: null,
        actualBoa: null,
        actualTotalCash: null,
        foodActual: null,
        subscriptionsActual: null,
        tradingActual: null,
      },
      q3: { augustNormal: true, septemberBonus: true, septemberSecondPaycheckTotal: 9100 },
      sweep: { desiredBuffer: 1000, upcomingBills: 0 },
      notes: "",
    };
  }

  function defaultLedger() {
    return { activeMonthId: "2026-07", archiveVisible: false, months: [julyMonth()] };
  }

  function normalizeMonth(raw) {
    const fresh = julyMonth();
    if (!raw || typeof raw !== "object") return fresh;
    return {
      ...fresh,
      ...raw,
      accounts: {
        current: { ...fresh.accounts.current, ...(raw.accounts || {}).current },
        boa: { ...fresh.accounts.boa, ...(raw.accounts || {}).boa },
        robinhood: { ...fresh.accounts.robinhood, ...(raw.accounts || {}).robinhood },
      },
      paychecks: Array.isArray(raw.paychecks) && raw.paychecks.length ? raw.paychecks : fresh.paychecks,
      bills: Array.isArray(raw.bills) ? raw.bills : fresh.bills,
      subscriptions: Array.isArray(raw.subscriptions) ? raw.subscriptions : fresh.subscriptions,
      trading: { ...fresh.trading, ...(raw.trading || {}) },
      closeout: { ...fresh.closeout, ...(raw.closeout || {}) },
      q3: { ...fresh.q3, ...(raw.q3 || {}) },
      sweep: { ...fresh.sweep, ...(raw.sweep || {}) },
      notes: String(raw.notes || ""),
    };
  }

  function normalizeLedger(raw) {
    if (!raw || typeof raw !== "object") return defaultLedger();
    const months = Array.isArray(raw.months) && raw.months.length ? raw.months.map(normalizeMonth) : [julyMonth()];
    const activeMonthId = months.some((month) => month.id === raw.activeMonthId) ? raw.activeMonthId : months[0].id;
    return { activeMonthId, archiveVisible: Boolean(raw.archiveVisible), months };
  }

  function loadLedger() {
    try {
      return normalizeLedger(JSON.parse(localStorage.getItem(STORAGE_KEY) || "null"));
    } catch (_error) {
      return defaultLedger();
    }
  }

  let ledger = loadLedger();

  function saveLedger() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(ledger));
  }

  function activeMonth() {
    return ledger.months.find((month) => month.id === ledger.activeMonthId) || ledger.months[0];
  }

  function activeBills(month) {
    return (month.bills || []).filter((row) => row.active !== false);
  }

  function activeSubscriptions(month) {
    return (month.subscriptions || []).filter((row) => row.active !== false);
  }

  function sum(rows, predicate = () => true) {
    return rows.reduce((total, row) => total + (predicate(row) ? num(row.amount) : 0), 0);
  }

  function calc(month = activeMonth()) {
    const p1 = month.paychecks[0] || {};
    const p2 = month.paychecks[1] || {};
    const bills = activeBills(month);
    const subs = activeSubscriptions(month);
    const accountBucket = (account, bucket) => sum(bills, (row) => row.account === account && row.paycheckBucket === bucket)
      + sum(subs, (row) => row.account === account && row.paycheckBucket === bucket);
    const currentP1Income = num(month.accounts.current.starting) + num(p1.currentDeposit);
    const currentP1Out = accountBucket("Current", "Paycheck 1");
    const currentP1End = currentP1Income - currentP1Out;
    const currentP2Income = currentP1End + num(p2.currentDeposit);
    const currentP2Out = accountBucket("Current", "Paycheck 2");
    const currentEnd = currentP2Income - currentP2Out;
    const boaP1Income = num(month.accounts.boa.starting) + num(p1.boaDeposit);
    const boaP1Out = accountBucket("BOA", "Paycheck 1");
    const boaP1End = boaP1Income - boaP1Out;
    const boaP2Income = boaP1End + num(p2.boaDeposit);
    const boaP2Out = accountBucket("BOA", "Paycheck 2");
    const boaEnd = boaP2Income - boaP2Out;
    const robinhood = num(month.accounts.robinhood.pending);
    const currentDeposits = num(p1.currentDeposit) + num(p2.currentDeposit);
    const boaDeposits = num(p1.boaDeposit) + num(p2.boaDeposit);
    const currentOut = accountBucket("Current", "Paycheck 1") + accountBucket("Current", "Paycheck 2");
    const boaOut = accountBucket("BOA", "Paycheck 1") + accountBucket("BOA", "Paycheck 2");
    const tradingSubs = sum(subs, (row) => row.group === "Trading" || row.account === "Trading");
    const tradingExpenses = num(month.trading.tradingView) + num(month.trading.cboe)
      + (month.trading.includeEvaluation ? num(month.trading.evaluationFunded) : 0)
      + tradingSubs;
    const tradingNet = num(month.trading.tradingIncome) - tradingExpenses;
    const paidCount = bills.filter((row) => row.paid).length + subs.filter((row) => row.paid).length;
    const totalCount = bills.length + subs.length;
    const totalWithoutRh = currentEnd + boaEnd;
    const totalWithRh = totalWithoutRh + robinhood;
    const actualTotal = month.closeout.actualTotalCash ?? (
      month.closeout.actualCurrent !== null && month.closeout.actualBoa !== null
        ? num(month.closeout.actualCurrent) + num(month.closeout.actualBoa)
        : null
    );
    const normalBoaGrowth = boaDeposits - boaOut;
    const augustBoa = boaEnd + (month.q3.augustNormal ? normalBoaGrowth : 0);
    const septemberBoa = augustBoa + normalBoaGrowth + (month.q3.septemberBonus ? num(month.q3.septemberSecondPaycheckTotal) : 0);
    return {
      currentP1Income,
      currentP1Out,
      currentP1End,
      currentP2Income,
      currentP2Out,
      currentP2End: currentEnd,
      currentEnd,
      boaP1Income,
      boaP1Out,
      boaP1End,
      boaP2Income,
      boaP2Out,
      boaP2End: boaEnd,
      boaEnd,
      boaEndWithRh: boaEnd + robinhood,
      currentDeposits,
      boaDeposits,
      currentOut,
      boaOut,
      totalWithoutRh,
      totalWithRh,
      billsPaidPct: totalCount ? (paidCount / totalCount) * 100 : 0,
      paidCount,
      totalCount,
      tradingExpenses,
      tradingNet,
      foodPlanned: sum(bills, (row) => row.category === "Food"),
      lifestylePlanned: sum(bills, (row) => row.category === "Lifestyle"),
      currentSubs: sum(subs, (row) => row.account === "Current"),
      boaGrowth: boaEnd - num(month.accounts.boa.starting),
      sweepAmount: currentEnd - num(month.sweep.upcomingBills) - num(month.sweep.desiredBuffer),
      actualTotal,
      augustBoa,
      septemberBoa,
      septemberBoaWithRh: septemberBoa + robinhood,
    };
  }

  function stateBadge(kind, label) {
    return `<span class="budgetStateBadge is-${kind}">${esc(label)}</span>`;
  }

  function currentStatus(month, d) {
    if (d.currentEnd >= num(month.accounts.current.target)) return ["stable", "Stable"];
    if (d.currentEnd >= num(month.accounts.current.min)) return ["watch", "Watch"];
    return ["danger", "Needs Action"];
  }

  function render() {
    const month = activeMonth();
    const d = calc(month);
    renderMonthSelect(month);
    renderSummary(month, d);
    renderOverview(month, d);
    renderAlerts(month, d);
    renderSetup(month);
    renderPaycheckView(month, d);
    renderBills(month);
    renderSubscriptions(month);
    renderTrading(month, d);
    renderSweep(month, d);
    renderQ3(month, d);
    renderCloseout(month, d);
    renderArchive();
    renderJson();
  }

  function renderMonthSelect(month) {
    const select = document.getElementById("budgetMonthSelect");
    select.innerHTML = ledger.months
      .map((entry) => `<option value="${esc(entry.id)}" ${entry.id === month.id ? "selected" : ""}>${esc(entry.monthName)}${entry.closed ? " · closed" : ""}</option>`)
      .join("");
  }

  function renderSummary(month, d) {
    const [kind, label] = currentStatus(month, d);
    const cards = [
      ["Current Projected End", money(d.currentEnd), kind],
      ["BOA Projected End", money(d.boaEnd), "stable"],
      ["Total Cash Without RH", money(d.totalWithoutRh), "stable"],
      ["Total Cash With RH", money(d.totalWithRh), "stable"],
      ["Bills Paid %", `${Math.round(d.billsPaidPct)}%`, d.billsPaidPct >= 80 ? "stable" : "watch"],
      ["Current Status", label, kind],
    ];
    document.getElementById("budgetSummaryCards").innerHTML = cards.map(([labelText, value, tone]) => `
      <article class="budgetSummaryCard is-${tone}">
        <span>${esc(labelText)}</span>
        <strong>${esc(value)}</strong>
      </article>
    `).join("");
  }

  function metric(label, value) {
    return `<div><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
  }

  function renderOverview(month, d) {
    const [kind, label] = currentStatus(month, d);
    document.getElementById("budgetCurrentOverview").innerHTML = `
      <div class="budgetColumnHead"><div><span>Operating Account</span><h3>Current</h3></div>${stateBadge(kind, label)}</div>
      <div class="budgetMetricGrid">
        ${metric("Starting", money(month.accounts.current.starting))}
        ${metric("Paycheck Deposits", money(d.currentDeposits))}
        ${metric("Bills Assigned", money(d.currentOut - d.currentSubs))}
        ${metric("Subscriptions", money(d.currentSubs))}
        ${metric("Lifestyle Budget", money(d.lifestylePlanned + d.foodPlanned))}
        ${metric("Projected Ending", money(d.currentEnd))}
      </div>
    `;
    document.getElementById("budgetBoaOverview").innerHTML = `
      <div class="budgetColumnHead"><div><span>Capital Account</span><h3>Bank of America</h3></div>${stateBadge("stable", "Capital")}</div>
      <div class="budgetMetricGrid">
        ${metric("Starting", money(month.accounts.boa.starting))}
        ${metric("Paycheck Deposits", money(d.boaDeposits))}
        ${metric("BOA Bills", money(d.boaOut))}
        ${metric("Robinhood Included", month.accounts.robinhood.included ? "Yes" : "No")}
        ${metric("Ending Without RH", money(d.boaEnd))}
        ${metric("Ending With RH", money(d.boaEndWithRh))}
      </div>
    `;
    document.getElementById("budgetTradingOverview").innerHTML = `
      <div class="budgetColumnHead"><div><span>Business Account</span><h3>Trading</h3></div>${stateBadge(d.tradingNet >= 0 ? "stable" : "watch", d.tradingNet >= 0 ? "Covered" : "Separate")}</div>
      <div class="budgetMetricGrid">
        ${metric("TradingView", money(month.trading.tradingView))}
        ${metric("CBOE", money(month.trading.cboe))}
        ${metric("Evaluation / Funded", month.trading.includeEvaluation ? money(month.trading.evaluationFunded) : "Off")}
        ${metric("Trading Income", money(month.trading.tradingIncome))}
        ${metric("Net Cash Flow", money(d.tradingNet))}
        ${metric("Rule", "Trading pays trading")}
      </div>
    `;
  }

  function renderAlerts(month, d) {
    const alerts = [];
    if (d.currentEnd < num(month.accounts.current.min)) alerts.push(["danger", `Current is below minimum by ${money(num(month.accounts.current.min) - d.currentEnd)}.`]);
    if (d.sweepAmount > 0) alerts.push(["stable", `Sweep candidate: move ${money(d.sweepAmount)} to BOA.`]);
    if (d.tradingNet < 0) alerts.push(["watch", `Trading needs ${money(Math.abs(d.tradingNet))} to cover business expenses.`]);
    if (!alerts.length) alerts.push(["stable", "Plan is controlled. Review paid flags and actuals weekly."]);
    document.getElementById("budgetAlertsPanel").innerHTML = `
      <div class="budgetSectionHead"><div><div class="pill">Alerts</div><h3>What needs attention</h3></div>${stateBadge(month.closed ? "stable" : "watch", month.closed ? "Closed" : "Open")}</div>
      <div class="budgetAlertList">${alerts.map(([tone, text]) => `<div class="budgetAlert is-${tone}">${esc(text)}</div>`).join("")}</div>
    `;
  }

  function renderSetup(month) {
    document.getElementById("budgetMonthSetup").innerHTML = `
      ${field("monthName", "Month Name", "text")}
      ${field("accounts.current.starting", "Starting Current")}
      ${field("accounts.current.target", "Current Target")}
      ${field("accounts.boa.starting", "Starting BOA")}
      ${field("accounts.robinhood.pending", "Robinhood Pending")}
      ${checkbox("accounts.robinhood.included", "Include Robinhood")}
      ${field("paychecks.0.currentDeposit", "P1 Current Deposit")}
      ${field("paychecks.0.boaDeposit", "P1 BOA Deposit")}
      ${field("paychecks.0.date", "P1 Date", "date")}
      ${field("paychecks.1.currentDeposit", "P2 Current Deposit")}
      ${field("paychecks.1.boaDeposit", "P2 BOA Deposit")}
      ${field("paychecks.1.date", "P2 Date", "date")}
      <label class="budgetFull"><span>Notes</span><textarea data-bind="notes" rows="4">${esc(month.notes)}</textarea></label>
    `;
  }

  function renderPaycheckView(month, d) {
    const tabs = document.querySelectorAll("[data-paycheck-tab]");
    tabs.forEach((button) => button.classList.toggle("primary", button.dataset.paycheckTab === activePaycheckTab));
    const rows = activePaycheckTab === "paycheck1"
      ? paycheckRows(month, "Paycheck 1")
      : activePaycheckTab === "paycheck2"
        ? paycheckRows(month, "Paycheck 2")
        : [...paycheckRows(month, "Paycheck 1"), ...paycheckRows(month, "Paycheck 2")];
    const totals = activePaycheckTab === "paycheck1"
      ? [["Income", d.currentP1Income + d.boaP1Income], ["Outflow", d.currentP1Out + d.boaP1Out], ["Remaining", d.currentP1End + d.boaP1End]]
      : activePaycheckTab === "paycheck2"
        ? [["Income", d.currentP2Income + d.boaP2Income], ["Outflow", d.currentP2Out + d.boaP2Out], ["Remaining", d.currentEnd + d.boaEnd]]
        : [["Income", d.currentDeposits + d.boaDeposits + num(month.accounts.current.starting) + num(month.accounts.boa.starting)], ["Outflow", d.currentOut + d.boaOut], ["Remaining", d.totalWithoutRh]];
    document.getElementById("budgetPaycheckView").innerHTML = `
      <div class="budgetPaycheckTotals">${totals.map(([label, value]) => metric(label, money(value))).join("")}</div>
      <div class="budgetLedgerList">${rows.map((row) => `<div><span>${esc(row.name)}</span><small>${esc(row.category || row.group)} · ${esc(row.account)}</small><strong>${money(row.amount)}</strong></div>`).join("")}</div>
    `;
  }

  function paycheckRows(month, bucket) {
    return [
      ...activeBills(month).filter((row) => row.paycheckBucket === bucket),
      ...activeSubscriptions(month).filter((row) => row.paycheckBucket === bucket),
    ].sort((a, b) => String(a.dueDate || "").localeCompare(String(b.dueDate || "")));
  }

  function renderBills(month) {
    document.getElementById("budgetBillsTable").innerHTML = `
      <thead><tr><th>Due Date</th><th>Name</th><th>Amount</th><th>Account</th><th>Paycheck Bucket</th><th>Category</th><th>Recurring</th><th>Paid</th><th>Actions</th></tr></thead>
      <tbody>${month.bills.map((row) => billRow(row)).join("")}</tbody>
    `;
  }

  function billRow(row) {
    return `<tr data-kind="bill" data-id="${row.id}">
      <td><input data-field="dueDate" type="date" value="${esc(row.dueDate || "")}"></td>
      <td><input data-field="name" value="${esc(row.name)}"></td>
      <td><input data-field="amount" type="number" step="0.01" value="${esc(row.amount)}"></td>
      <td>${select("account", accounts, row.account)}</td>
      <td>${select("paycheckBucket", buckets, row.paycheckBucket)}</td>
      <td>${select("category", categories, row.category)}</td>
      <td><input data-field="recurring" type="checkbox" ${row.recurring ? "checked" : ""}></td>
      <td><input data-field="paid" type="checkbox" ${row.paid ? "checked" : ""}></td>
      <td><button class="btn" type="button" data-row-action="duplicate">Duplicate</button><button class="btn danger" type="button" data-row-action="delete">Delete</button></td>
    </tr>`;
  }

  function renderSubscriptions(month) {
    document.querySelectorAll("[data-sub-tab]").forEach((button) => button.classList.toggle("primary", button.dataset.subTab === activeSubTab));
    const rows = month.subscriptions.filter((row) => row.group === activeSubTab);
    document.getElementById("budgetSubscriptionsTable").innerHTML = `
      <thead><tr><th>Name</th><th>Amount</th><th>Due</th><th>Account</th><th>Renewal</th><th>Next Renewal</th><th>Active</th><th>Actions</th></tr></thead>
      <tbody>${rows.map((row) => subRow(row)).join("")}</tbody>
    `;
  }

  function subRow(row) {
    return `<tr data-kind="subscription" data-id="${row.id}">
      <td><input data-field="name" value="${esc(row.name)}"></td>
      <td><input data-field="amount" type="number" step="0.01" value="${esc(row.amount)}"></td>
      <td><input data-field="dueDate" type="date" value="${esc(row.dueDate || "")}"></td>
      <td>${select("account", accounts, row.account)}</td>
      <td>${select("renewalType", renewalTypes, row.renewalType)}</td>
      <td><input data-field="nextRenewalDate" type="date" value="${esc(row.nextRenewalDate || "")}"></td>
      <td><input data-field="active" type="checkbox" ${row.active ? "checked" : ""}></td>
      <td><button class="btn" type="button" data-row-action="duplicate">Duplicate</button><button class="btn danger" type="button" data-row-action="delete">Delete</button></td>
    </tr>`;
  }

  function renderTrading(month, d) {
    document.getElementById("budgetTradingBusiness").innerHTML = `
      <div class="budgetFormGrid">
        ${field("trading.tradingView", "TradingView")}
        ${field("trading.cboe", "CBOE")}
        ${field("trading.evaluationFunded", "Evaluation / Funded")}
        ${checkbox("trading.includeEvaluation", "Include evaluation")}
        ${field("trading.tradingIncome", "Trading Income")}
      </div>
      <div class="budgetPaycheckTotals">${metric("Current Monthly Cost", money(d.tradingExpenses))}${metric("Trading Payout Income", money(month.trading.tradingIncome))}${metric("Net Trading Cash Flow", money(d.tradingNet))}</div>
      <ul class="budgetRules"><li>Trading pays trading.</li><li>Personal money should not routinely fund trading.</li><li>First goal: trading covers subscriptions.</li><li>Second goal: trading covers evaluations.</li><li>Third goal: trading contributes to BOA.</li></ul>
    `;
  }

  function renderSweep(month, d) {
    document.getElementById("budgetSweepCalculator").innerHTML = `
      <div class="budgetFormGrid">
        ${field("sweep.upcomingBills", "Upcoming Bills")}
        ${field("sweep.desiredBuffer", "Desired Buffer")}
      </div>
      <div class="budgetSweepCard ${d.sweepAmount > 0 ? "is-stable" : "is-watch"}">${d.sweepAmount > 0 ? `Move ${money(d.sweepAmount)} to BOA` : "Do not transfer"}</div>
    `;
  }

  function renderQ3(month, d) {
    document.getElementById("budgetQ3Projection").innerHTML = `
      <div class="budgetFormGrid">
        ${checkbox("q3.augustNormal", "August normal month")}
        ${checkbox("q3.septemberBonus", "September bonus paycheck option")}
        ${field("q3.septemberSecondPaycheckTotal", "September second paycheck total")}
      </div>
      <div class="budgetScenarioGrid">
        <div>${metric("July Projected", money(d.boaEnd))}${metric("August Projected", money(d.augustBoa))}${metric("September Without RH", money(d.septemberBoa))}</div>
        <div>${metric("September With RH", money(d.septemberBoaWithRh))}${metric("Without RH Target", "$13,000-$14,000")}${metric("With RH Target", "$15,700-$16,700")}</div>
      </div>
    `;
  }

  function renderCloseout(month, d) {
    const actualTotal = d.actualTotal;
    document.getElementById("budgetCloseStatus").innerHTML = month.closed ? stateBadge("stable", "Closed") : stateBadge("watch", "Open");
    document.getElementById("budgetCloseout").innerHTML = `
      <div class="budgetFormGrid">
        ${field("closeout.actualCurrent", "Actual Ending Current")}
        ${field("closeout.actualBoa", "Actual Ending BOA")}
        ${field("closeout.actualTotalCash", "Actual Total Cash")}
        ${field("closeout.foodActual", "Food Actual")}
        ${field("closeout.subscriptionsActual", "Subscriptions Actual")}
        ${field("closeout.tradingActual", "Trading Actual")}
        <label class="budgetFull"><span>Closeout Notes</span><textarea data-bind="notes" rows="4">${esc(month.notes)}</textarea></label>
      </div>
      <div class="budgetVarianceGrid">
        ${metric("Projected Current vs Actual", variance(d.currentEnd, month.closeout.actualCurrent))}
        ${metric("Projected BOA vs Actual", variance(d.boaEnd, month.closeout.actualBoa))}
        ${metric("Projected Total vs Actual", actualTotal === null ? "—" : variance(d.totalWithoutRh, actualTotal))}
      </div>
    `;
  }

  function variance(projected, actual) {
    if (actual === null || actual === undefined || actual === "") return "—";
    const diff = num(actual) - num(projected);
    return `${money(diff)} (${diff >= 0 ? "over" : "under"})`;
  }

  function renderArchive() {
    const panel = document.getElementById("budgetArchivePanel");
    panel.hidden = !ledger.archiveVisible;
    const closed = ledger.months.filter((month) => month.closed);
    document.getElementById("budgetArchiveTable").innerHTML = `
      <thead><tr><th>Month</th><th>Projected Total</th><th>Actual Total</th><th>BOA Growth</th><th>Bills Paid %</th><th>Notes</th><th></th></tr></thead>
      <tbody>${closed.map((month) => {
        const d = calc(month);
        return `<tr><td>${esc(month.monthName)}</td><td>${money(d.totalWithoutRh)}</td><td>${d.actualTotal === null ? "—" : money(d.actualTotal)}</td><td>${money(d.boaGrowth)}</td><td>${Math.round(d.billsPaidPct)}%</td><td>${esc(month.notes || "")}</td><td><button class="btn" type="button" data-open-month="${month.id}">Open</button></td></tr>`;
      }).join("") || '<tr><td colspan="7">No closed months yet.</td></tr>'}</tbody>
    `;
  }

  function renderJson() {
    document.getElementById("budgetJsonBox").value = JSON.stringify(ledger, null, 2);
  }

  function field(path, label, type = "number") {
    const value = getPath(path);
    return `<label><span>${esc(label)}</span><input data-bind="${esc(path)}" type="${type}" value="${value === null ? "" : esc(value)}"></label>`;
  }

  function checkbox(path, label) {
    return `<label class="budgetCheck"><input data-bind="${esc(path)}" type="checkbox" ${getPath(path) ? "checked" : ""}><span>${esc(label)}</span></label>`;
  }

  function select(fieldName, list, value) {
    return `<select data-field="${esc(fieldName)}">${list.map((item) => `<option value="${esc(item)}" ${item === value ? "selected" : ""}>${esc(item)}</option>`).join("")}</select>`;
  }

  function getPath(path) {
    return path.split(".").reduce((target, key) => Array.isArray(target) ? target[Number(key)] : target?.[key], activeMonth());
  }

  function setPath(path, value) {
    const month = activeMonth();
    const parts = path.split(".");
    let target = month;
    parts.slice(0, -1).forEach((key) => {
      target = Array.isArray(target) ? target[Number(key)] : target[key];
    });
    const last = parts[parts.length - 1];
    if (Array.isArray(target)) target[Number(last)] = value;
    else target[last] = value;
    if (path === "monthName") {
      const nextId = monthIdFromName(value, month.id);
      const duplicate = ledger.months.some((entry) => entry !== month && entry.id === nextId);
      if (!duplicate) {
        month.id = nextId;
        ledger.activeMonthId = nextId;
      }
    }
  }

  function monthIdFromName(label, fallback) {
    const date = new Date(`${label} 1`);
    if (Number.isNaN(date.getTime())) return fallback;
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
  }

  function persist(message) {
    saveLedger();
    render();
    if (message) showToast(message);
  }

  function showToast(message) {
    const toast = document.getElementById("budgetToast");
    toast.textContent = message;
    toast.hidden = false;
    clearTimeout(showToast.timer);
    showToast.timer = setTimeout(() => { toast.hidden = true; }, 2200);
  }

  function newMonth() {
    const base = activeMonth();
    const next = duplicateMonth(base);
    const date = new Date(`${base.monthName} 1`);
    if (!Number.isNaN(date.getTime())) {
      date.setMonth(date.getMonth() + 1);
      next.monthName = date.toLocaleString("en-US", { month: "long", year: "numeric" });
      next.id = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
    } else {
      next.id = uid();
      next.monthName = `${base.monthName} Next`;
    }
    const d = calc(base);
    next.closed = false;
    next.accounts.current.starting = Math.max(0, d.currentEnd);
    next.accounts.boa.starting = Math.max(0, d.boaEnd);
    next.accounts.current.actualEnding = null;
    next.accounts.boa.actualEnding = null;
    next.closeout = julyMonth().closeout;
    next.bills = next.bills.map((row) => ({ ...row, id: uid(), paid: false }));
    next.subscriptions = next.subscriptions.map((row) => ({ ...row, id: uid(), paid: false }));
    ledger.months.push(next);
    ledger.activeMonthId = next.id;
    persist("New month created.");
  }

  function duplicateMonth(month = activeMonth()) {
    const copy = normalizeMonth(JSON.parse(JSON.stringify(month)));
    copy.id = `${month.id}-copy-${uid()}`;
    copy.monthName = `${month.monthName} Copy`;
    copy.closed = false;
    copy.bills = copy.bills.map((row) => ({ ...row, id: uid(), paid: false }));
    copy.subscriptions = copy.subscriptions.map((row) => ({ ...row, id: uid(), paid: false }));
    return copy;
  }

  function exportJson() {
    const blob = new Blob([JSON.stringify(ledger, null, 2)], { type: "application/json" });
    const anchor = document.createElement("a");
    anchor.href = URL.createObjectURL(blob);
    anchor.download = "mccain-budget-ledger.json";
    anchor.click();
    URL.revokeObjectURL(anchor.href);
  }

  function updateRow(rowElement, eventTarget) {
    const month = activeMonth();
    const collection = rowElement.dataset.kind === "bill" ? month.bills : month.subscriptions;
    const row = collection.find((item) => item.id === rowElement.dataset.id);
    if (!row || !eventTarget.dataset.field) return;
    const fieldName = eventTarget.dataset.field;
    row[fieldName] = eventTarget.type === "checkbox" ? eventTarget.checked : eventTarget.type === "number" ? num(eventTarget.value) : eventTarget.value;
  }

  app.addEventListener("change", (event) => {
    const monthSelect = event.target.closest("#budgetMonthSelect");
    const bind = event.target.closest("[data-bind]");
    const row = event.target.closest("[data-kind][data-id]");
    const importFile = event.target.closest("#budgetImportFile");
    if (monthSelect) {
      ledger.activeMonthId = monthSelect.value;
      persist();
      return;
    }
    if (bind) {
      const value = bind.type === "checkbox" ? bind.checked : bind.type === "number" ? (bind.value === "" ? null : num(bind.value)) : bind.value;
      setPath(bind.dataset.bind, value);
      persist();
      return;
    }
    if (row) {
      updateRow(row, event.target);
      persist();
      return;
    }
    if (importFile?.files?.[0]) {
      const reader = new FileReader();
      reader.onload = () => {
        try {
          ledger = normalizeLedger(JSON.parse(String(reader.result || "{}")));
          persist("Budget ledger imported.");
        } catch (_error) {
          showToast("Import failed. JSON was invalid.");
        }
      };
      reader.readAsText(importFile.files[0]);
      importFile.value = "";
    }
  });

  app.addEventListener("click", (event) => {
    const action = event.target.closest("[data-budget-action]")?.dataset.budgetAction;
    const paycheckTab = event.target.closest("[data-paycheck-tab]");
    const subTab = event.target.closest("[data-sub-tab]");
    const rowAction = event.target.closest("[data-row-action]");
    const openMonth = event.target.closest("[data-open-month]");
    const month = activeMonth();
    if (paycheckTab) {
      activePaycheckTab = paycheckTab.dataset.paycheckTab;
      render();
      return;
    }
    if (subTab && !action) {
      activeSubTab = subTab.dataset.subTab;
      render();
      return;
    }
    if (openMonth) {
      ledger.activeMonthId = openMonth.dataset.openMonth;
      ledger.archiveVisible = false;
      persist();
      return;
    }
    if (action === "new-month") newMonth();
    if (action === "duplicate-month") {
      const index = ledger.months.findIndex((entry) => entry.id === month.id);
      const source = index > 0 ? ledger.months[index - 1] : month;
      const copy = duplicateMonth(source);
      ledger.months.push(copy);
      ledger.activeMonthId = copy.id;
      persist("Previous month duplicated.");
    }
    if (action === "close-month") {
      month.closed = true;
      persist("Month closed.");
    }
    if (action === "toggle-archive") {
      ledger.archiveVisible = !ledger.archiveVisible;
      persist();
    }
    if (action === "add-bill") {
      month.bills.push(bill("New Bill", 0, "", "Current", "Paycheck 1", "One-Time", { recurring: false }));
      persist("Bill added.");
    }
    if (action === "add-subscription") {
      month.subscriptions.push(subscription("New Subscription", 0, "", activeSubTab, activeSubTab === "Trading" ? "Trading" : "Current", "Paycheck 1"));
      persist("Subscription added.");
    }
    if (action === "export-json") exportJson();
    if (rowAction) {
      const row = rowAction.closest("[data-kind][data-id]");
      const collection = row.dataset.kind === "bill" ? month.bills : month.subscriptions;
      const index = collection.findIndex((item) => item.id === row.dataset.id);
      if (index < 0) return;
      if (rowAction.dataset.rowAction === "delete") collection.splice(index, 1);
      if (rowAction.dataset.rowAction === "duplicate") collection.splice(index + 1, 0, { ...collection[index], id: uid(), name: `${collection[index].name} Copy` });
      persist(rowAction.dataset.rowAction === "delete" ? "Row deleted." : "Row duplicated.");
    }
  });

  saveLedger();
  render();
})();
