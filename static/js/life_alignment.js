(function () {
  const app = document.getElementById("lifeAlignmentApp");
  if (!app) return;

  const form = document.getElementById("lifeAlignmentForm");
  const toast = document.getElementById("lifeAlignmentToast");
  const errorBox = document.getElementById("lifeAlignmentError");
  const detailsPanel = document.getElementById("lifeDetailsPanel");
  const detailsToggle = document.getElementById("lifeDetailsToggle");
  const fastSaveButton = document.getElementById("lifeFastSaveButton");
  const lockButton = document.getElementById("lifeLockButton");
  const unlockButton = document.getElementById("lifeUnlockButton");
  const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") || "";
  const autoFilled = { water: false, walk: false };

  const numberFields = [
    "water_oz",
    "water_goal_oz",
    "pushups",
    "squats",
    "walk_minutes",
    "steps",
    "sleep_hours",
  ];
  const checkboxFields = ["workout_completed", "devotion_completed", "journal_completed"];
  const accountabilityMessages = [
    "Less is more. Stack clean days.",
    "Protect the day.",
    "One clean win is enough.",
    "You don't fix losses. You outlast them.",
    "Discipline today. Freedom tomorrow.",
    "Do the small things like they matter — because they do.",
  ];

  const clamp = (value, min, max) => Math.min(max, Math.max(min, Number(value) || 0));
  const fmt = (value, digits = 0) => Number(value || 0).toFixed(digits).replace(/\.0$/, "");

  async function api(path, options = {}) {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrfToken,
        ...(options.headers || {}),
      },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || `Request failed: ${response.status}`);
    }
    return payload;
  }

  function showError(message) {
    if (!errorBox) return;
    errorBox.textContent = message || "";
    errorBox.hidden = !message;
  }

  function showToast(message = "Alignment logged. Keep stacking.") {
    if (!toast) return;
    toast.textContent = message;
    toast.hidden = false;
    window.clearTimeout(showToast._timer);
    showToast._timer = window.setTimeout(() => {
      toast.hidden = true;
    }, 2600);
  }

  function setText(id, value) {
    const node = document.getElementById(id);
    if (node) node.textContent = value;
  }

  function setWidth(id, value) {
    const node = document.getElementById(id);
    if (node) node.style.width = `${clamp(value, 0, 100)}%`;
  }

  function scoreState(score) {
    if (score <= 39) return { state: "off-track", label: "OFF TRACK" };
    if (score <= 69) return { state: "building", label: "BUILDING" };
    return { state: "locked-in", label: "LOCKED IN" };
  }

  function scoreMessage(score) {
    if (score === 0) return "Start with one small win.";
    if (score <= 39) return "Get back in alignment.";
    if (score <= 69) return "Keep stacking.";
    if (score <= 99) return "Strong day forming.";
    return "Ready to lock the day.";
  }

  function ruleStatus(value) {
    if (value === "yes") return { label: "FOLLOWED", state: "yes" };
    if (value === "no") return { label: "RULE BREAK", state: "no" };
    return { label: "UNCONFIRMED", state: "not_yet" };
  }

  function missionForMissing(missing) {
    if (!missing.length) return "🎯 Today's Mission: Maintain alignment and lock the day.";
    const map = {
      Water: "Hit water goal",
      Workout: "Complete workout",
      Walk: "20 min walk",
      Devotion: "Complete devotion",
      Journal: "Log today's reflection",
    };
    return `🎯 Today's Mission: ${missing.map((item) => map[item] || item.toLowerCase()).join(" + ")}.`;
  }

  function calculateLocalScore(entry) {
    let score = 0;
    if (Number(entry.water_goal_oz || 0) > 0 && Number(entry.water_oz || 0) >= Number(entry.water_goal_oz || 0)) score += 20;
    if (entry.workout_completed) score += 20;
    if (Number(entry.walk_minutes || 0) >= 20) score += 15;
    if (Number(entry.sleep_hours || 0) >= 7) score += 15;
    if (entry.devotion_completed) score += 15;
    if (entry.journal_completed) score += 15;
    if (entry.followed_rules === "no") score = Math.min(score, 69);
    return Math.min(score, 100);
  }

  function missingToday(entry) {
    const missing = [];
    if (!(Number(entry.water_goal_oz || 0) > 0 && Number(entry.water_oz || 0) >= Number(entry.water_goal_oz || 0))) missing.push("Water");
    if (!entry.workout_completed) missing.push("Workout");
    if (Number(entry.walk_minutes || 0) < 20) missing.push("Walk");
    if (!entry.devotion_completed) missing.push("Devotion");
    if (!entry.journal_completed) missing.push("Journal");
    return missing;
  }

  function modeForScore(score) {
    if (score >= 80) return "LOCKED IN";
    if (score >= 50) return "IN PROGRESS";
    if (score > 0) return "OFF TRACK";
    return "NOT STARTED";
  }

  function applyEntry(entry) {
    if (!form || !entry) return;
    numberFields.forEach((field) => {
      const input = form.elements[field];
      if (input) input.value = entry[field] ?? 0;
    });
    checkboxFields.forEach((field) => {
      const input = form.elements[field];
      if (input) input.checked = Boolean(entry[field]);
    });
    if (form.elements.workout_type) form.elements.workout_type.value = entry.workout_type || "Push-ups / Squats";
    if (form.elements.mood) form.elements.mood.value = entry.mood || "neutral";
    if (form.elements.followed_rules) form.elements.followed_rules.value = entry.followed_rules || "not_yet";
    if (form.elements.notes) form.elements.notes.value = entry.notes || "";
    updateFastToggles(entry);
    updateTodayUi(entry);
    setLocked(Boolean(entry.locked), entry);
  }

  function readForm() {
    const payload = {};
    numberFields.forEach((field) => {
      payload[field] = Number(form.elements[field]?.value || 0);
    });
    checkboxFields.forEach((field) => {
      payload[field] = Boolean(form.elements[field]?.checked);
    });
    payload.workout_type = form.elements.workout_type?.value || "Push-ups / Squats";
    payload.mood = form.elements.mood?.value || "neutral";
    payload.followed_rules = form.elements.followed_rules?.value || "not_yet";
    payload.notes = form.elements.notes?.value || "";
    return payload;
  }

  function currentEntryPreview() {
    const entry = readForm();
    entry.discipline_score = calculateLocalScore(entry);
    entry.missing_today = missingToday(entry);
    return entry;
  }

  function updateTodayUi(entry) {
    const score = Number(entry.discipline_score ?? calculateLocalScore(entry));
    const state = scoreState(score);
    const missing = Array.isArray(entry.missing_today) ? entry.missing_today : missingToday(entry);
    const card = document.getElementById("lifeAlignmentScoreCard");
    const ring = document.getElementById("lifeScoreRing");
    const rules = ruleStatus(entry.followed_rules || "not_yet");
    if (card) card.dataset.scoreState = state.state;
    if (ring) ring.style.setProperty("--score", `${score}%`);
    setText("lifeAlignmentScore", String(score));
    setText("lifeScoreMessage", entry.score_message || scoreMessage(score));
    setText("lifeTodayMode", entry.today_mode || modeForScore(score));
    setText("lifeDailyStatus", entry.daily_status || (missing.length ? "STARTED" : "COMPLETE"));
    setText("lifeRulesStatus", rules.label);
    setText("lifeMissingToday", missing.length ? `Missing: ${missing.join(", ")}` : "Fully aligned today.");
    setText("lifeTodayMission", missionForMissing(missing));
    setText("progressScoreStatus", state.label);
    const rulesStatus = document.getElementById("lifeRulesStatus");
    if (rulesStatus) rulesStatus.dataset.ruleState = rules.state;

    const ruleBox = document.getElementById("lifeRuleMessage");
    const ruleMessage =
      entry.rule_message ||
      (entry.followed_rules === "no"
        ? "Rule break logged. Review it. Don't hide it."
        : entry.followed_rules === "yes" && score >= 70
          ? "Clean execution day."
          : "");
    if (ruleBox) {
      ruleBox.textContent = ruleMessage;
      ruleBox.hidden = !ruleMessage;
      ruleBox.dataset.ruleState = entry.followed_rules || "not_yet";
    }

    const waterGoal = Number(entry.water_goal_oz || 100) || 100;
    setText("progressWaterText", `${fmt(entry.water_oz)} / ${fmt(waterGoal)} oz`);
    setWidth("progressWaterFill", (Number(entry.water_oz || 0) / waterGoal) * 100);
    const waterHit = Number(entry.water_oz || 0) >= waterGoal;
    setText("progressWaterStatus", waterHit ? "HIT" : "MISSING");
    setText("progressWaterFeedback", waterHit ? "Good. Keep stacking." : "Hydrate before you negotiate with yourself.");

    setText("progressWalkText", `${fmt(entry.walk_minutes)} / 20 min`);
    setWidth("progressWalkFill", (Number(entry.walk_minutes || 0) / 20) * 100);
    const walkHit = Number(entry.walk_minutes || 0) >= 20;
    setText("progressWalkStatus", walkHit ? "HIT" : "MISSING");
    setText("progressWalkFeedback", walkHit ? "Movement logged." : "Get the 20-minute walk done.");

    setText("progressSleepText", `${fmt(entry.sleep_hours, 1)} / 7 hr`);
    setWidth("progressSleepFill", (Number(entry.sleep_hours || 0) / 7) * 100);
    const sleepHit = Number(entry.sleep_hours || 0) >= 7;
    setText("progressSleepStatus", sleepHit ? "HIT" : "MISSING");
    setText("progressSleepFeedback", sleepHit ? "Recovery protected." : "Prioritize rest tonight.");

    setText("progressScoreText", `${score} / 100`);
    setWidth("progressScoreFill", score);
    setText(
      "progressScoreFeedback",
      state.label === "OFF TRACK"
        ? "Start with one controllable."
        : state.label === "BUILDING"
          ? "Keep stacking."
          : "Strong day forming."
    );
  }

  function updateFastToggles(entry) {
    const water = document.querySelector('[data-fast-toggle="water"]');
    const workout = document.querySelector('[data-fast-toggle="workout"]');
    const walk = document.querySelector('[data-fast-toggle="walk"]');
    const devotion = document.querySelector('[data-fast-toggle="devotion"]');
    const journal = document.querySelector('[data-fast-toggle="journal"]');
    if (water) water.checked = Number(entry.water_goal_oz || 0) > 0 && Number(entry.water_oz || 0) >= Number(entry.water_goal_oz || 0);
    if (workout) workout.checked = Boolean(entry.workout_completed);
    if (walk) walk.checked = Number(entry.walk_minutes || 0) >= 20;
    if (devotion) devotion.checked = Boolean(entry.devotion_completed);
    if (journal) journal.checked = Boolean(entry.journal_completed);
  }

  function updateAnalytics(analytics) {
    setStreak("Workout", analytics.current_workout_streak || 0, analytics.best_workout_streak || 0);
    setStreak("Water", analytics.current_water_goal_streak || 0, analytics.best_water_streak || 0);
    setStreak("Journal", analytics.current_journal_streak || 0, analytics.best_journal_streak || 0);
    setStreak("Devotion", analytics.current_devotion_streak || 0, analytics.best_devotion_streak || 0);
    setText("metricWeeklyCompletion", `${analytics.weekly_completion_percentage || 0}%`);
    setText("metricMonthlyCompletion", `${analytics.monthly_completion_percentage || 0}%`);
    setText("metricAverageWater", `${fmt(analytics.average_water_intake, 1)} oz`);
    setText("metricAverageSleep", `${fmt(analytics.average_sleep, 1)} hr`);
    setText("metricMonthlyWorkouts", analytics.total_workouts_this_month || 0);
    setText("metricMonthlyWalk", analytics.total_walking_minutes_this_month || 0);
    setText("metricAverageScore", analytics.average_discipline_score || 0);
    setText("metricLockedDays", analytics.locked_days_count || 0);
    setText("metricRuleBreaks", analytics.rule_break_count || 0);
    renderInsights(analytics.accountability_insights || []);
  }

  function setStreak(name, streak, best) {
    const key = name === "Water" ? "Water" : name;
    const card = document.getElementById(`metric${key}Card`);
    const value = document.getElementById(`metric${key}Streak`);
    const meta = document.getElementById(`metric${key}Meta`);
    if (value) value.textContent = streak;
    const label = streak >= 7 ? "🔥 Locked In" : streak >= 3 ? "Momentum" : streak >= 1 ? "Building" : "Start today";
    if (meta) meta.textContent = `${label} · Best: ${best} days`;
    if (card) {
      card.dataset.streakState = streak >= 7 ? "locked" : streak >= 3 ? "glow" : streak === 0 ? "muted" : "active";
    }
  }

  function renderInsights(insights) {
    const list = document.getElementById("lifeInsightList");
    if (!list) return;
    list.innerHTML = (insights.length ? insights : ["No insights yet. Log today to start the read."])
      .map((item) => `<li>${item}</li>`)
      .join("");
  }

  function renderLineChart(id, entries, key, maxValue, color) {
    const node = document.getElementById(id);
    if (!node) return;
    const width = 320;
    const height = 140;
    const values = entries.map((entry) => clamp(entry[key], 0, maxValue));
    const points = values.map((value, index) => {
      const x = entries.length <= 1 ? 0 : (index / (entries.length - 1)) * width;
      const y = height - (value / maxValue) * (height - 18) - 9;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });
    node.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-hidden="true">
        <path d="M0 ${height - 18} H${width}" class="lifeAlignmentChartBase"></path>
        <polyline points="${points.join(" ")}" fill="none" stroke="${color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"></polyline>
        ${points
          .filter((_, index) => index % 5 === 0 || index === points.length - 1)
          .map((point) => {
            const [x, y] = point.split(",");
            return `<circle cx="${x}" cy="${y}" r="3.5" fill="${color}"></circle>`;
          })
          .join("")}
      </svg>
    `;
  }

  function renderBarChart(id, entries, key, maxValue, color) {
    const node = document.getElementById(id);
    if (!node) return;
    node.innerHTML = entries
      .map((entry) => {
        const value = clamp((Number(entry[key] || 0) / maxValue) * 100, 2, 100);
        const label = `${entry.date}: ${fmt(entry[key])}`;
        return `<span class="lifeAlignmentBar" title="${label}" style="height:${value}%;background:${color}"></span>`;
      })
      .join("");
  }

  function renderWorkoutGrid(entries) {
    const node = document.getElementById("lifeWorkoutGrid");
    if (!node) return;
    node.innerHTML = entries
      .map((entry) => {
        const done = Boolean(entry.workout_completed);
        const label = `${entry.date}: ${done ? "Workout complete" : "No workout logged"}`;
        return `<span class="${done ? "is-complete" : ""}" title="${label}"></span>`;
      })
      .join("");
  }

  function setLocked(locked, entry = {}) {
    app.dataset.locked = locked ? "true" : "false";
    Array.from(form.elements).forEach((element) => {
      if (element.name !== "csrf_token" && element.id !== "lifeUnlockButton") element.disabled = locked;
    });
    document.querySelectorAll("[data-fast-toggle]").forEach((input) => {
      input.disabled = locked;
    });
    if (fastSaveButton) fastSaveButton.disabled = locked;
    const detailSave = form.querySelector('button[type="submit"]');
    if (detailSave) detailSave.disabled = locked;
    if (lockButton) lockButton.hidden = locked;
    if (unlockButton) unlockButton.hidden = !locked;
    const badge = document.getElementById("lifeLockedBadge");
    if (badge) badge.hidden = !locked;
    const unlockedState = document.getElementById("lifeUnlockedState");
    if (unlockedState) unlockedState.hidden = locked;
    const confirm = document.getElementById("lifeLockConfirm");
    if (confirm) confirm.hidden = !locked;
    setText("lifeFinalScore", `${entry.discipline_score || 0}/100`);
  }

  async function save(action = "") {
    showError("");
    const body = readForm();
    if (action) body.action = action;
    const payload = await api("/api/life-alignment/today", {
      method: "POST",
      body: JSON.stringify(body),
    });
    applyEntry(payload.entry);
    updateAnalytics(payload.analytics || {});
    await refresh();
    return payload.entry;
  }

  async function refresh() {
    const [todayPayload, historyPayload, analyticsPayload] = await Promise.all([
      api("/api/life-alignment/today"),
      api("/api/life-alignment/history"),
      api("/api/life-alignment/analytics"),
    ]);
    applyEntry(todayPayload.entry);
    updateAnalytics(analyticsPayload.analytics);
    renderLineChart("lifeScoreChart", historyPayload.entries, "discipline_score", 100, "#69d6ff");
    renderBarChart("lifeWaterChart", historyPayload.entries, "water_oz", 140, "linear-gradient(180deg,#68e7ff,#2b74ff)");
    renderWorkoutGrid(historyPayload.entries);
    renderLineChart("lifeWalkChart", historyPayload.entries, "walk_minutes", 60, "#39d982");
  }

  detailsToggle?.addEventListener("click", () => {
    const opening = Boolean(detailsPanel?.hidden);
    if (detailsPanel) detailsPanel.hidden = !opening;
    detailsToggle.textContent = opening ? "Hide Details" : "Show Details";
    detailsToggle.setAttribute("aria-expanded", String(opening));
  });

  document.querySelectorAll("[data-fast-toggle]").forEach((input) => {
    input.addEventListener("change", () => {
      const target = input.getAttribute("data-fast-toggle");
      if (target === "water") {
        const water = form.elements.water_oz;
        const goal = Number(form.elements.water_goal_oz?.value || 100);
        if (input.checked) {
          water.value = goal;
          autoFilled.water = true;
        } else if (autoFilled.water && Number(water.value || 0) === goal) {
          water.value = 0;
          autoFilled.water = false;
        }
      }
      if (target === "walk") {
        const walk = form.elements.walk_minutes;
        if (input.checked && Number(walk.value || 0) < 20) {
          walk.value = 20;
          autoFilled.walk = true;
        } else if (!input.checked && autoFilled.walk && Number(walk.value || 0) === 20) {
          walk.value = 0;
          autoFilled.walk = false;
        }
      }
      if (target === "workout") form.elements.workout_completed.checked = input.checked;
      if (target === "devotion") form.elements.devotion_completed.checked = input.checked;
      if (target === "journal") form.elements.journal_completed.checked = input.checked;
      updateTodayUi(currentEntryPreview());
    });
  });

  form?.addEventListener("input", () => {
    updateFastToggles(currentEntryPreview());
    updateTodayUi(currentEntryPreview());
  });

  form?.addEventListener("change", () => {
    updateFastToggles(currentEntryPreview());
    updateTodayUi(currentEntryPreview());
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await save();
      showToast();
    } catch (error) {
      showError(error.message || "Could not save alignment. Try again.");
      showToast("Could not save alignment. Try again.");
    }
  });

  fastSaveButton?.addEventListener("click", async () => {
    try {
      await save();
      showToast();
    } catch (error) {
      showError(error.message || "Could not save alignment. Try again.");
      showToast("Could not save alignment. Try again.");
    }
  });

  lockButton?.addEventListener("click", async () => {
    try {
      await save("lock");
      showToast("✅ Professional Day Complete. Final score locked.");
    } catch (error) {
      showError(error.message || "Could not save alignment. Try again.");
      showToast("Could not save alignment. Try again.");
    }
  });

  unlockButton?.addEventListener("click", async () => {
    if (!window.confirm("Unlock today and allow edits?")) return;
    try {
      await save("unlock");
      showToast("Day unlocked. Edit carefully.");
    } catch (error) {
      showError(error.message || "Could not save alignment. Try again.");
      showToast("Could not save alignment. Try again.");
    }
  });

  function rotateMessage() {
    const primary = document.getElementById("lifeAccountabilityPrimary");
    const support = document.getElementById("lifeAccountabilitySupport");
    if (!primary || !support) return;
    const next = accountabilityMessages[Math.floor(Math.random() * accountabilityMessages.length)];
    const parts = next.includes(" — ")
      ? next.split(" — ")
      : next.includes(". ")
        ? next.split(". ")
        : [next, ""];
    primary.textContent = parts[0].replace(/\.$/, "");
    support.textContent = (parts.slice(1).join(". ") || "Keep the standard.").replace(/\.$/, ".");
  }
  document.getElementById("lifeMessageRefresh")?.addEventListener("click", rotateMessage);
  rotateMessage();

  refresh().catch((error) => showError(error.message || "Could not load Life Alignment."));
})();
