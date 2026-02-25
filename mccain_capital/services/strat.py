"""The Strat page service."""

from __future__ import annotations

from mccain_capital.services.ui import render_page


def strat_page():
    content = r"""
    <div class="stratWrap">
      <section class="stratHero">
        <h2 class="stratTitle">🧠 The Strat Core Playbook</h2>
        <div class="stratSub">
          Quick reference for <b>candle types</b>, <b>combo patterns</b>, <b>universal truths</b>, and <b>stop structure</b>.
          Use this as your pre-trade quality gate before entry.
        </div>
        <div class="stratPills">
          <span class="stratPill">🕯️ Structure</span>
          <span class="stratPill">🔁 Patterns</span>
          <span class="stratPill">🧭 Context</span>
          <span class="stratPill">🛡️ Risk</span>
        </div>
      </section>

      <section class="stratGrid3">
        <article class="stratCard">
          <h3>🕯️ Candle Types</h3>
          <div class="meta">The 1-2-3 language</div>
          <ul>
            <li><b>1</b> = inside bar (range contraction)</li>
            <li><b>2</b> = directional break (higher high or lower low)</li>
            <li><b>3</b> = outside bar (breaks both sides)</li>
          </ul>
        </article>

        <article class="stratCard">
          <h3>🔁 Core Combos</h3>
          <div class="meta">Common setups</div>
          <ul>
            <li><b>2-1-2</b> continuation after pause</li>
            <li><b>3-1-2</b> volatility → pause → break</li>
            <li><b>2-2</b> reversal (your main trigger)</li>
          </ul>
        </article>

        <article class="stratCard">
          <h3>🧭 Timeframe Continuity</h3>
          <div class="meta">Context matters</div>
          <ul>
            <li>Trade <b>with</b> higher timeframe intent</li>
            <li>Expect cleaner moves when HTF aligns</li>
            <li>Be selective when HTF disagrees</li>
          </ul>
        </article>
      </section>

      <section class="stratGrid2">
        <article class="stratCard">
          <h3>🌎 Universal Truths</h3>
          <div class="meta">Keep these on your screen</div>
          <ul>
            <li><b>Location is king:</b> levels and liquidity drive decisions.</li>
            <li><b>Direction needs proof:</b> break plus follow-through beats hoping.</li>
            <li><b>Range = risk:</b> mid-range trades are hardest to manage.</li>
            <li><b>Losses are part of the plan:</b> define risk before entry.</li>
            <li><b>Your edge is repetition:</b> same process, same sizing.</li>
          </ul>
        </article>

        <article class="stratCard">
          <h3>🛡️ Stop Loss Structure</h3>
          <div class="meta">Simple, consistent, non-negotiable</div>
          <div class="stratStopBody">
            <div><b>Default rule:</b> stop goes beyond the level that invalidates the setup.</div>
            <div class="meta stack6">
              Beyond reversal-candle extreme, key level (PDH/PDL), or HTF swing.
            </div>
            <div class="stack12"><b>Options risk cap:</b> keep premium risk in plan (e.g. 20-25%).</div>
            <div class="meta stack6">If your real stop exceeds cap, reduce size or pass.</div>
          </div>
        </article>
      </section>

      <section class="stratCard">
        <div class="stratChecklistTop">
          <div>
            <h3 class="stratChecklistTitle">✅ Pre-Trade Checklist</h3>
            <div class="meta stack6">Saved locally in your browser.</div>
          </div>
          <div class="stratProgress">
            <div class="stratProgressBar"><div id="stratProgressFill" class="stratProgressFill"></div></div>
            <div id="stratProgressText" class="stratProgressText">0/6 complete</div>
          </div>
        </div>

        <div class="checklist stack10">
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="level" />
            <div class="checkText"><b>Location:</b> at PDH/PDL, CDH/CDL, HTF swing, or VWAP zone</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="htf" />
            <div class="checkText"><b>HTF intent:</b> 45m/1h agrees (or explicit fade at major level)</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="structure" />
            <div class="checkText"><b>Structure:</b> 30m defines box, range, and pivots clearly</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="trigger" />
            <div class="checkText"><b>Trigger:</b> 15m 2-2 reversal with 5m expansion confirmation</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="risk" />
            <div class="checkText"><b>Risk:</b> stop defined, size fixed, premium cap respected</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="plan" />
            <div class="checkText"><b>Plan:</b> targets chosen and no revenge re-entry rule acknowledged</div>
          </label>
        </div>

        <div class="stratActions">
          <button class="btn" type="button" onclick="stratChecklistClear()">🧹 Clear</button>
          <button class="btn primary" type="button" onclick="window.location.href='/trades'">📒 Go to Trades</button>
        </div>
      </section>

      <section class="stratCard">
        <h3>🧩 Combo Quick Reference</h3>
        <div class="meta stack6">Use this like a decision tree.</div>
        <div class="stratTableWrap">
          <table class="table">
          <thead>
            <tr>
              <th>Pattern</th>
              <th>Meaning</th>
              <th>What you want to see</th>
              <th>Invalidation / stop anchor</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><b>2-1-2</b></td>
              <td>Continuation after contraction</td>
              <td>Break → inside → break in same direction</td>
              <td>Beyond the <b>1</b> range / setup level</td>
            </tr>
            <tr>
              <td><b>3-1-2</b></td>
              <td>Expansion then decision</td>
              <td>Outside bar sets both sides → inside bar → break with intent</td>
              <td>Beyond the inside bar / opposite side of 3</td>
            </tr>
            <tr>
              <td><b>2-2</b></td>
              <td>Reversal / failed direction</td>
              <td>Push fails at key level → reverse break + follow-through</td>
              <td>Beyond the reversal extreme (the “failed direction” point)</td>
            </tr>
          </tbody>
        </table>
        </div>
      </section>
    </div>

    <script>
      (function initStratChecklist(){
        try{
          const key = "strat_checklist_v1";
          const saved = JSON.parse(localStorage.getItem(key) || "{}");
          const checks = Array.from(document.querySelectorAll(".strat-check"));
          const progressFill = document.getElementById("stratProgressFill");
          const progressText = document.getElementById("stratProgressText");

          function syncProgress(){
            const total = checks.length;
            const done = checks.filter(cb => cb.checked).length;
            const pct = total ? Math.round((done / total) * 100) : 0;
            if (progressFill) progressFill.style.width = pct + "%";
            if (progressText) progressText.textContent = done + "/" + total + " complete";
            checks.forEach(cb => {
              const row = cb.closest(".checkRow");
              if (row) row.classList.toggle("checked", cb.checked);
            });
          }

          checks.forEach(cb=>{
            const k = cb.getAttribute("data-key");
            cb.checked = !!saved[k];
            cb.addEventListener("change", ()=>{
              const next = JSON.parse(localStorage.getItem(key) || "{}");
              next[k] = cb.checked;
              localStorage.setItem(key, JSON.stringify(next));
              syncProgress();
            });
          });
          syncProgress();
          window.stratChecklistClear = function(){
            localStorage.removeItem(key);
            checks.forEach(cb => cb.checked = false);
            syncProgress();
          }
        }catch(e){
          // ignore localStorage issues
          window.stratChecklistClear = function(){
            document.querySelectorAll(".strat-check").forEach(cb => {
              cb.checked = false;
              const row = cb.closest(".checkRow");
              if (row) row.classList.remove("checked");
            });
            const progressFill = document.getElementById("stratProgressFill");
            const progressText = document.getElementById("stratProgressText");
            if (progressFill) progressFill.style.width = "0%";
            if (progressText) progressText.textContent = "0/6 complete";
          }
        }
      })();
    </script>
    """
    return render_page(content, active="strat", title="🧠 The Strat")
