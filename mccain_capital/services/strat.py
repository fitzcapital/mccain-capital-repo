"""The Strat page service."""

from __future__ import annotations

from mccain_capital.services.ui import render_page


def strat_page():
    content = r"""
    <div class="stratWrap stratLabWrap">
      <section class="stratHero stratHeroLab">
        <div class="stratHeroStatusRow">
          <div class="stratHeroIdentity">
            <span class="pill">🧠 The Strat Lab</span>
            <span class="stratHeroStatus">Interactive Learning Mode</span>
          </div>
          <div class="stratHeroQuickStats">
            <span class="stratHeroStat"><b id="stratHeroProgressPct">0%</b> Progress</span>
            <span class="stratHeroStat"><b id="stratHeroDrillScore">0/4</b> Drill Score</span>
            <span class="stratHeroStat"><b id="stratHeroFocus">Patterns + Gamma</b> Focus</span>
          </div>
        </div>
        <div class="stratHeroMain">
          <div class="stratHeroCopy">
            <h2 class="stratTitle">The Strat Core Playbook</h2>
            <div class="stratSub">
              Master structure, combo behavior, gamma context, and trade logic through one repeatable learning room.
            </div>
            <div class="stratPills stratModePills">
              <button class="stratPill is-active" type="button" data-strat-module="basics">🕯️ Basics</button>
              <button class="stratPill" type="button" data-strat-module="patterns">🔁 Patterns</button>
              <button class="stratPill" type="button" data-strat-module="gamma">⚡ Gamma</button>
              <button class="stratPill" type="button" data-strat-module="context">🧭 Context</button>
              <button class="stratPill" type="button" data-strat-module="risk">🛡️ Risk</button>
              <button class="stratPill" type="button" data-strat-module="drills">🎯 Drills</button>
              <button class="stratPill" type="button" data-strat-module="glossary">🧾 Glossary</button>
            </div>
          </div>
          <aside class="stratFocusCard">
            <div class="stratFocusEyebrow">Today’s Focus</div>
            <div class="stratFocusTitle" id="stratFocusTitle">Patterns + Gamma Context</div>
            <div class="stratFocusBody" id="stratFocusBody">
              See how the same Strat trigger changes in positive gamma, negative gamma, and flip-zone conditions.
            </div>
            <div class="stratLearningMeter">
              <div class="stratLearningMeterTop">
                <span>Learning Progress</span>
                <strong id="stratLearningMeterText">0 / 14 concepts</strong>
              </div>
              <div class="stratProgressBar stratProgressBar-hero">
                <div id="stratHeroProgressFill" class="stratProgressFill"></div>
              </div>
            </div>
          </aside>
        </div>
      </section>

      <section class="stratGrid3 stratLabSection" id="stratSection-basics" data-strat-section="basics">
        <article class="stratCard stratCardGlow stratCard-blue">
          <div class="stratCardHead">
            <div>
              <h3>🕯️ Candle Types</h3>
              <div class="meta">Read the candle first. Then read the story.</div>
            </div>
            <span class="trendChip info">Core Memory</span>
          </div>
          <div class="stratPatternGrid">
            <div class="stratPatternTile tone-neutral">
              <div class="stratPatternIcon">📦</div>
              <div class="stratPatternCode">1</div>
              <div class="stratPatternTitle">Inside Bar</div>
              <div class="stratPatternBody">Range contracts. Energy stores. The break matters more than the bar.</div>
            </div>
            <div class="stratPatternTile tone-positive">
              <div class="stratPatternIcon">⬆️</div>
              <div class="stratPatternCode">2U</div>
              <div class="stratPatternTitle">Directional Up</div>
              <div class="stratPatternBody">Higher high. Upward pressure is active. Continuation still needs follow-through.</div>
            </div>
            <div class="stratPatternTile tone-negative">
              <div class="stratPatternIcon">⬇️</div>
              <div class="stratPatternCode">2D</div>
              <div class="stratPatternTitle">Directional Down</div>
              <div class="stratPatternBody">Lower low. Downside is active. Failed pushes can reverse hard at real levels.</div>
            </div>
            <div class="stratPatternTile tone-caution">
              <div class="stratPatternIcon">🔄</div>
              <div class="stratPatternCode">3</div>
              <div class="stratPatternTitle">Outside Bar</div>
              <div class="stratPatternBody">Both sides break. Volatility expands. The next decision often matters more.</div>
            </div>
          </div>
        </article>

        <article class="stratCard stratCardGlow stratCard-cyan" id="stratSection-patterns" data-strat-section="patterns">
          <div class="stratCardHead">
            <div>
              <h3>🔁 Core Combos</h3>
              <div class="meta">Open each combo for meaning, failure, and best context.</div>
            </div>
            <span class="trendChip info">Setup Engine</span>
          </div>
          <div class="stratComboStack">
            <details class="stratComboCard" open>
              <summary>
                <span class="stratComboCode">2-1-2</span>
                <span class="stratComboTitle">Continuation</span>
                <span class="stratComboSummary">Break → pause → break</span>
              </summary>
              <div class="stratComboBody">
                <div><b>Meaning:</b> trend pauses, then reasserts.</div>
                <div><b>Want:</b> clean inside bar, support, aligned continuity.</div>
                <div><b>Fails:</b> pause loses location or the break has no expansion.</div>
                <div><b>Best:</b> above local pivot / support / continuation day.</div>
              </div>
            </details>
            <details class="stratComboCard">
              <summary>
                <span class="stratComboCode">3-1-2</span>
                <span class="stratComboTitle">Expansion Reload</span>
                <span class="stratComboSummary">Outside bar → pause → go</span>
              </summary>
              <div class="stratComboBody">
                <div><b>Meaning:</b> volatility expands, compresses, then resolves.</div>
                <div><b>Want:</b> clear outside range and a strong reclaim.</div>
                <div><b>Fails:</b> traded midrange with no anchor.</div>
                <div><b>Best:</b> after macro impulse or a clean range reset.</div>
              </div>
            </details>
            <details class="stratComboCard">
              <summary>
                <span class="stratComboCode">2-2</span>
                <span class="stratComboTitle">Reversal Trigger</span>
                <span class="stratComboSummary">Failed push → reverse break</span>
              </summary>
              <div class="stratComboBody">
                <div><b>Meaning:</b> one side fails and the other takes control.</div>
                <div><b>Want:</b> level rejection, failed continuation, real trigger.</div>
                <div><b>Fails:</b> forced midrange or into strong momentum.</div>
                <div><b>Best:</b> near walls, local flip, HTF rejection, trapped move.</div>
              </div>
            </details>
          </div>
        </article>

        <article class="stratCard stratCardGlow stratCard-green" id="stratSection-context" data-strat-section="context">
          <div class="stratCardHead">
            <div>
              <h3>🧭 Timeframe Continuity</h3>
              <div class="meta">Context decides whether the same trigger is clean, mixed, or dangerous.</div>
            </div>
            <span class="trendChip positive">Context</span>
          </div>
          <div class="stratContinuityGrid">
            <div class="stratContinuityState tone-positive">
              <div class="stratContinuityTitle">Aligned</div>
              <div class="stratContinuityBody">HTF agrees. Cleaner moves, less friction, better continuation.</div>
            </div>
            <div class="stratContinuityState tone-caution">
              <div class="stratContinuityTitle">Mixed</div>
              <div class="stratContinuityBody">Two-way tape. Shorten expectations. Levels matter more than pattern alone.</div>
            </div>
            <div class="stratContinuityState tone-negative">
              <div class="stratContinuityTitle">Conflict</div>
              <div class="stratContinuityBody">HTF disagrees. Reversals need proof. Avoid forcing a clean story.</div>
            </div>
          </div>
        </article>
      </section>

      <section class="stratGrid2 stratLabSection" data-strat-section="risk" id="stratSection-risk">
        <article class="stratCard stratCardGlow stratCard-ice">
          <div class="stratCardHead">
            <div>
              <h3>🌎 Universal Truths</h3>
              <div class="meta">Treat these as laws, not tips.</div>
            </div>
            <span class="trendChip">Foundations</span>
          </div>
          <div class="stratTruthStack">
            <div class="stratTruthRow"><span class="stratTruthIcon">📍</span><div><b>Location is king.</b><span>Patterns matter more at real levels than in empty space.</span></div></div>
            <div class="stratTruthRow"><span class="stratTruthIcon">✅</span><div><b>Direction needs proof.</b><span>Break plus follow-through beats prediction.</span></div></div>
            <div class="stratTruthRow"><span class="stratTruthIcon">🧨</span><div><b>Midrange kills discipline.</b><span>Most weak trades come from forcing action where nothing matters.</span></div></div>
            <div class="stratTruthRow"><span class="stratTruthIcon">🛡️</span><div><b>Risk is part of entry.</b><span>If invalidation is unclear, the trade is unclear.</span></div></div>
            <div class="stratTruthRow"><span class="stratTruthIcon">🔁</span><div><b>Edge is repetition.</b><span>Same process. Same sizing. Same rules.</span></div></div>
          </div>
        </article>

        <article class="stratCard stratCardGlow stratCard-risk">
          <div class="stratCardHead">
            <div>
              <h3>🛡️ Stop Loss Structure</h3>
              <div class="meta">Stops belong beyond invalidation, not where you hope price turns.</div>
            </div>
            <span class="trendChip warn">Risk</span>
          </div>
          <div class="stratAnchorGrid">
            <div class="stratAnchorTile"><b>Reversal extreme</b><span>Best for 2-2 reversals and failed-direction setups.</span></div>
            <div class="stratAnchorTile"><b>Key level</b><span>PDH, PDL, local flip, gamma walls, or session edge.</span></div>
            <div class="stratAnchorTile"><b>HTF swing</b><span>Use when lower-timeframe noise sits inside a larger map.</span></div>
            <div class="stratAnchorTile"><b>Premium cap</b><span>If the real stop is too large, reduce size or pass.</span></div>
          </div>
          <div class="stratStopBody">
            <div><b>Default rule:</b> your stop goes beyond the level that proves your idea wrong.</div>
            <div class="meta stack6">If the stop cannot fit the plan, the setup does not earn risk.</div>
          </div>
        </article>
      </section>

      <section class="stratSectionCard stratGammaSection stratSectionSecondary stratLabSection" id="stratSection-gamma" data-strat-section="gamma">
        <div class="stratSectionHeader">
          <div>
            <div class="pill">⚡ Gamma / GEX Context</div>
            <h3>Gamma behavior changes how Strat setups actually respond.</h3>
            <div class="meta">Same candle. Different dealer regime. Different trade quality.</div>
          </div>
          <div class="stratSectionBadge">Context Engine</div>
        </div>
        <div class="stratGammaGrid">
          <article class="stratGammaCard tone-positive">
            <div class="stratGammaIcon">🟢</div>
            <h4>Positive Gamma</h4>
            <p><b>Plain English:</b> movement is more responsive and often mean-reverting around levels.</p>
            <ul>
              <li><b>Behavior:</b> dips can bounce, rips can fade.</li>
              <li><b>Takeaway:</b> don’t chase far from structure.</li>
              <li><b>Mistake:</b> treating positive gamma as automatic bullish direction.</li>
              <li><b>Scenario:</b> 2-2 reversals at support can clean up.</li>
            </ul>
          </article>
          <article class="stratGammaCard tone-negative">
            <div class="stratGammaIcon">🔴</div>
            <h4>Negative Gamma</h4>
            <p><b>Plain English:</b> movement can expand and failed reversals get punished faster.</p>
            <ul>
              <li><b>Behavior:</b> momentum stretches and volatility bites harder.</li>
              <li><b>Takeaway:</b> respect continuation pressure.</li>
              <li><b>Mistake:</b> buying every dip because it looks extended.</li>
              <li><b>Scenario:</b> failed bounces below resistance can unwind quickly.</li>
            </ul>
          </article>
          <article class="stratGammaCard tone-caution">
            <div class="stratGammaIcon">🟠</div>
            <h4>Gamma Flip</h4>
            <p><b>Plain English:</b> the broad behavior line where tape character can change and edge usually drops.</p>
            <ul>
              <li><b>Behavior:</b> mixed price action and false starts.</li>
              <li><b>Takeaway:</b> wait for confirmation near the flip.</li>
              <li><b>Mistake:</b> treating flip-zone action like clean direction.</li>
              <li><b>Scenario:</b> reclaim attempts often need proof before size increases.</li>
            </ul>
          </article>
          <article class="stratGammaCard tone-call">
            <div class="stratGammaIcon">🔺</div>
            <h4>Call Wall</h4>
            <p><b>Plain English:</b> an overhead pressure zone that often matters as resistance or slowdown.</p>
            <ul>
              <li><b>Behavior:</b> price can reject, compress, or hesitate.</li>
              <li><b>Takeaway:</b> don’t chase directly into the wall.</li>
              <li><b>Mistake:</b> assuming it always rejects.</li>
              <li><b>Scenario:</b> continuation into the wall often needs trim logic or proof.</li>
            </ul>
          </article>
          <article class="stratGammaCard tone-put">
            <div class="stratGammaIcon">🟩</div>
            <h4>Put Wall</h4>
            <p><b>Plain English:</b> a support-like area that can stabilize price or matter sharply if lost.</p>
            <ul>
              <li><b>Behavior:</b> bounces can form here, but breaks can matter fast.</li>
              <li><b>Takeaway:</b> support matters only if price responds.</li>
              <li><b>Mistake:</b> buying blindly because the wall exists.</li>
              <li><b>Scenario:</b> buy dips only when response confirms.</li>
            </ul>
          </article>
        </div>
      </section>

      <section class="stratSectionCard stratFusionSection stratSectionSecondary">
        <div class="stratSectionHeader">
          <div>
            <div class="pill">🧠 Strat + Gamma Together</div>
            <h3>Same pattern, different environment, different outcome quality.</h3>
            <div class="meta">Use context to decide whether a setup is clean, conditional, or dangerous.</div>
          </div>
        </div>
        <div class="stratScenarioGrid">
          <article class="stratScenarioCard tone-positive">
            <h4>2-2 reversal in positive gamma</h4>
            <div class="stratScenarioBody">Cleaner at support. Better for responsive fades and disciplined buy-dip logic.</div>
          </article>
          <article class="stratScenarioCard tone-negative">
            <h4>2-2 reversal in negative gamma</h4>
            <div class="stratScenarioBody">More dangerous against momentum. Needs stronger level quality and sharper confirmation.</div>
          </article>
          <article class="stratScenarioCard tone-cyan">
            <h4>Continuation above flip</h4>
            <div class="stratScenarioBody">Pullbacks are cleaner if local support and continuity hold after reclaim.</div>
          </article>
          <article class="stratScenarioCard tone-danger">
            <h4>Failed bounce below flip</h4>
            <div class="stratScenarioBody">Sell-rip logic improves when the bounce cannot reclaim local structure.</div>
          </article>
        </div>
        <div class="stratIfThenRail">
          <div class="stratIfThenCard"><b>If</b> below flip + failed bounce + reversal trigger <b>then</b> sell rip bias.</div>
          <div class="stratIfThenCard"><b>If</b> above flip + pullback holds + continuation trigger <b>then</b> buy dip bias.</div>
          <div class="stratIfThenCard"><b>If</b> near the flip + mixed candles <b>then</b> wait for resolution.</div>
        </div>
      </section>

      <section class="stratCard stratChecklistCard">
        <div class="stratChecklistTop">
          <div>
            <h3 class="stratChecklistTitle">✅ Pre-Trade Checklist</h3>
            <div class="meta stack6">Saved locally. Use it as a readiness gate.</div>
          </div>
          <div class="stratProgress">
            <div class="stratProgressBlock">
              <div class="stratProgressBar"><div id="stratProgressFill" class="stratProgressFill"></div></div>
              <div id="stratProgressText" class="stratProgressText">0/6 complete</div>
            </div>
            <div class="stratReadinessState" id="stratReadinessState">Not Ready</div>
          </div>
        </div>

        <div class="checklist stack10">
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="level" />
            <div class="checkText"><b>Location:</b> price is at a decision area, not dead center.</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="htf" />
            <div class="checkText"><b>HTF intent:</b> higher timeframe agrees or the fade is clearly level-based.</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="structure" />
            <div class="checkText"><b>Structure:</b> box, range, walls, or flip context is mapped.</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="trigger" />
            <div class="checkText"><b>Trigger:</b> the pattern has confirmation, not anticipation.</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="risk" />
            <div class="checkText"><b>Risk:</b> invalidation is real, size is fixed, premium cap is respected.</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="plan" />
            <div class="checkText"><b>Plan:</b> target, stop, and no-revenge rule are set before entry.</div>
          </label>
        </div>

        <div class="stratActions">
          <button class="btn" type="button" onclick="stratChecklistClear()">🧹 Clear</button>
          <button class="btn primary" type="button" onclick="window.location.href='/market-pulse?refresh=1'">⚡ Open Market Pulse</button>
        </div>
      </section>

      <section class="stratCard stratQuickRefCard">
        <div class="stratCardHead">
            <div>
              <h3>🧩 Combo Quick Reference</h3>
              <div class="meta stack6">Fast reference for pattern meaning, confirmation, and invalidation.</div>
            </div>
          <span class="trendChip info">Fast Reference</span>
        </div>
        <div class="stratTableWrap">
          <table class="table stratQuickTable">
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
                <td><b>2-1-2</b><span class="stratTableChip">Continuation</span></td>
                <td>Directional break pauses, then resumes.</td>
                <td>Inside bar holds structure and the next break expands with intent.</td>
                <td>Beyond the <b>1</b> range or the support / resistance that defines the pause.</td>
              </tr>
              <tr>
                <td><b>3-1-2</b><span class="stratTableChip">Expansion</span></td>
                <td>Outside bar creates energy, inside bar organizes the next decision.</td>
                <td>Clear reclaim / rejection after the outside range establishes both sides.</td>
                <td>Beyond the inside bar or the opposite side of the 3.</td>
              </tr>
              <tr>
                <td><b>2-2</b><span class="stratTableChip">Reversal</span></td>
                <td>One direction fails and reverses through the opposite side.</td>
                <td>Key level rejection plus follow-through, not just a pretty candle shape.</td>
                <td>Beyond the failed direction extreme.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="stratGrid2 stratSectionTertiary">
        <article class="stratCard stratQuizCard stratLabSection" id="stratSection-drills" data-strat-section="drills">
          <div class="stratCardHead">
            <div>
              <h3>🎯 Quiz / Drills</h3>
              <div class="meta">Answer, score, repeat.</div>
            </div>
            <span class="trendChip info" id="stratQuizScoreChip">0/4</span>
          </div>
          <div class="stratQuizStack">
            <div class="stratQuizQuestion" data-question-id="q1" data-answer="b">
              <div class="stratQuizPrompt">What does positive gamma usually suggest?</div>
              <div class="stratQuizOptions">
                <button type="button" class="stratQuizOption" data-choice="a">Every dip is bullish</button>
                <button type="button" class="stratQuizOption" data-choice="b">More responsive / mean-reverting reactions</button>
                <button type="button" class="stratQuizOption" data-choice="c">Guaranteed trend day</button>
              </div>
              <div class="stratQuizFeedback"></div>
            </div>
            <div class="stratQuizQuestion" data-question-id="q2" data-answer="c">
              <div class="stratQuizPrompt">Below flip + failed bounce usually favors what bias?</div>
              <div class="stratQuizOptions">
                <button type="button" class="stratQuizOption" data-choice="a">Buy dips immediately</button>
                <button type="button" class="stratQuizOption" data-choice="b">Trade midrange</button>
                <button type="button" class="stratQuizOption" data-choice="c">Sell rips / reject pops</button>
              </div>
              <div class="stratQuizFeedback"></div>
            </div>
            <div class="stratQuizQuestion" data-question-id="q3" data-answer="a">
              <div class="stratQuizPrompt">What is a 2-2 reversal really showing?</div>
              <div class="stratQuizOptions">
                <button type="button" class="stratQuizOption" data-choice="a">One direction failed and the other side took control</button>
                <button type="button" class="stratQuizOption" data-choice="b">Random candle noise</button>
                <button type="button" class="stratQuizOption" data-choice="c">Guaranteed continuation</button>
              </div>
              <div class="stratQuizFeedback"></div>
            </div>
            <div class="stratQuizQuestion" data-question-id="q4" data-answer="b">
              <div class="stratQuizPrompt">What is the call wall most similar to?</div>
              <div class="stratQuizOptions">
                <button type="button" class="stratQuizOption" data-choice="a">Guaranteed upside breakout</button>
                <button type="button" class="stratQuizOption" data-choice="b">An overhead resistance / pressure zone</button>
                <button type="button" class="stratQuizOption" data-choice="c">The same thing as gamma flip</button>
              </div>
              <div class="stratQuizFeedback"></div>
            </div>
          </div>
        </article>

        <article class="stratCard stratGlossaryCard stratLabSection" id="stratSection-glossary" data-strat-section="glossary">
          <div class="stratCardHead">
            <div>
              <h3>🧾 Glossary / Memory Tools</h3>
              <div class="meta">Use quick chips to load the exact term.</div>
            </div>
            <span class="trendChip">Memory</span>
          </div>
          <div class="stratGlossaryChips">
            <button type="button" class="stratGlossaryChip is-active" data-term="gamma">Gamma</button>
            <button type="button" class="stratGlossaryChip" data-term="gex">GEX</button>
            <button type="button" class="stratGlossaryChip" data-term="positive">Positive Gamma</button>
            <button type="button" class="stratGlossaryChip" data-term="negative">Negative Gamma</button>
            <button type="button" class="stratGlossaryChip" data-term="flip">Gamma Flip</button>
            <button type="button" class="stratGlossaryChip" data-term="call_wall">Call Wall</button>
            <button type="button" class="stratGlossaryChip" data-term="put_wall">Put Wall</button>
            <button type="button" class="stratGlossaryChip" data-term="twotwo">2-2</button>
            <button type="button" class="stratGlossaryChip" data-term="threetwelve">3-1-2</button>
            <button type="button" class="stratGlossaryChip" data-term="continuity">Timeframe Continuity</button>
            <button type="button" class="stratGlossaryChip" data-term="midrange">Midrange</button>
            <button type="button" class="stratGlossaryChip" data-term="invalidation">Invalidation</button>
          </div>
          <div class="stratGlossaryDisplay" id="stratGlossaryDisplay">
            <div class="stratGlossaryTerm">Gamma</div>
            <div class="stratGlossaryDefinition">Dealer positioning sensitivity that often changes how price responds around levels and liquidity.</div>
          </div>
        </article>
      </section>

      <section class="stratSectionCard stratTrapSection stratSectionTertiary">
        <div class="stratSectionHeader">
          <div>
            <div class="pill">⚠️ Common Traps</div>
            <h3>Common mistakes that make a good-looking page useless in live trading.</h3>
            <div class="meta">Crisp warnings. Memorize them.</div>
          </div>
        </div>
        <div class="stratTrapGrid">
          <div class="stratTrapCard">Trading midrange because you want action, not because price is at a decision level.</div>
          <div class="stratTrapCard">Confusing a candle pattern with a valid setup when location is weak.</div>
          <div class="stratTrapCard">Assuming positive gamma means bullish direction instead of responsive behavior.</div>
          <div class="stratTrapCard">Assuming the call wall always rejects or the put wall always holds.</div>
          <div class="stratTrapCard">Taking reversals below flip or in negative gamma without confirmation.</div>
          <div class="stratTrapCard">Trading the flip without proof and calling it conviction.</div>
        </div>
      </section>
    </div>

    <script>
      (function initStratLab(){
        const storageKey = "strat_lab_state_v2";
        const glossary = {
          gamma: ["Gamma", "Dealer sensitivity that changes how price often reacts around structure."],
          gex: ["GEX", "Gamma exposure. A way of framing whether dealer positioning may dampen or amplify movement."],
          positive: ["Positive Gamma", "More responsive / mean-reverting behavior. Dips can bounce and rips can fade."],
          negative: ["Negative Gamma", "More expansion / momentum risk. Trends can extend and failed reversals can get punished."],
          flip: ["Gamma Flip", "The broad regime line where behavior can shift and clean edge often drops."],
          call_wall: ["Call Wall", "An overhead pressure zone that often acts like resistance or slowdown."],
          put_wall: ["Put Wall", "A support-like zone that can stabilize price or matter sharply if lost."],
          twotwo: ["2-2", "A reversal trigger showing one direction failed and the other side took control."],
          threetwelve: ["3-1-2", "Expansion, pause, then directional decision. Great when structure is clear."],
          continuity: ["Timeframe Continuity", "When higher timeframes agree with the setup, moves often clean up."],
          midrange: ["Midrange", "The dead center of structure where most forced trades go wrong."],
          invalidation: ["Invalidation", "The level that proves your idea is wrong. The stop belongs beyond it."],
        };
        const moduleFocus = {
          basics: ["Structure + Candle Language", "Build the 1-2-3 vocabulary and keep location first."],
          patterns: ["Pattern Selection", "Focus on continuation, expansion, and reversal logic."],
          gamma: ["Gamma Context", "Learn how positive, negative, and flip-zone behavior changes pattern quality."],
          context: ["Context First", "Use timeframe continuity and level location to filter signals."],
          risk: ["Risk + Invalidation", "Map stops beyond invalidation and protect premium risk."],
          drills: ["Quiz Reinforcement", "Use quick drills to lock in the mental model."],
          glossary: ["Memory Tools", "Rehearse the language until the terms feel automatic."],
        };

        let state;
        try {
          state = JSON.parse(localStorage.getItem(storageKey) || "{}");
        } catch (_err) {
          state = {};
        }
        state.checklist = state.checklist || {};
        state.quiz = state.quiz || {};
        state.activeModule = state.activeModule || "gamma";
        state.glossaryTerm = state.glossaryTerm || "gamma";

        const checks = Array.from(document.querySelectorAll(".strat-check"));
        const progressFill = document.getElementById("stratProgressFill");
        const progressText = document.getElementById("stratProgressText");
        const readinessState = document.getElementById("stratReadinessState");
        const heroProgressFill = document.getElementById("stratHeroProgressFill");
        const heroProgressPct = document.getElementById("stratHeroProgressPct");
        const heroDrillScore = document.getElementById("stratHeroDrillScore");
        const heroFocus = document.getElementById("stratHeroFocus");
        const learningMeterText = document.getElementById("stratLearningMeterText");
        const focusTitle = document.getElementById("stratFocusTitle");
        const focusBody = document.getElementById("stratFocusBody");
        const quizScoreChip = document.getElementById("stratQuizScoreChip");
        const glossaryDisplay = document.getElementById("stratGlossaryDisplay");
        const moduleButtons = Array.from(document.querySelectorAll("[data-strat-module]"));
        const glossaryButtons = Array.from(document.querySelectorAll("[data-term]"));
        const quizQuestions = Array.from(document.querySelectorAll(".stratQuizQuestion"));

        function persist(){
          try {
            localStorage.setItem(storageKey, JSON.stringify(state));
          } catch (_err) {
            // ignore local storage errors
          }
        }

        function checklistTotals(){
          const total = checks.length;
          const done = checks.filter((cb) => cb.checked).length;
          return { total, done };
        }

        function quizTotals(){
          const total = quizQuestions.length;
          let correct = 0;
          quizQuestions.forEach((question) => {
            const id = question.getAttribute("data-question-id");
            const answer = state.quiz[id];
            const expected = question.getAttribute("data-answer");
            if (answer && answer === expected) correct += 1;
          });
          return { total, correct };
        }

        function readinessLabel(done, total){
          if (!total || done === 0) return "Not Ready";
          if (done === total) return "Ready to Review";
          if (done >= Math.max(4, total - 1)) return "Building";
          return "Not Ready";
        }

        function syncHeroProgress(){
          const { total, done } = checklistTotals();
          const quiz = quizTotals();
          const conceptTotal = total + quiz.total + Object.keys(glossary).length;
          const conceptDone = done + quiz.correct + (state.glossaryTerm ? 1 : 0);
          const pct = conceptTotal ? Math.round((conceptDone / conceptTotal) * 100) : 0;
          if (heroProgressFill) heroProgressFill.style.width = pct + "%";
          if (heroProgressPct) heroProgressPct.textContent = pct + "%";
          if (heroDrillScore) heroDrillScore.textContent = `${quiz.correct}/${quiz.total}`;
          if (learningMeterText) learningMeterText.textContent = `${conceptDone} / ${conceptTotal} concepts`;
        }

        function syncChecklist(){
          const { total, done } = checklistTotals();
          const pct = total ? Math.round((done / total) * 100) : 0;
          if (progressFill) progressFill.style.width = pct + "%";
          if (progressText) progressText.textContent = `${done}/${total} complete`;
          if (readinessState) {
            const label = readinessLabel(done, total);
            readinessState.textContent = label;
            readinessState.dataset.state = label.toLowerCase().replace(/\s+/g, "-");
          }
          checks.forEach((cb) => {
            const row = cb.closest(".checkRow");
            if (row) row.classList.toggle("checked", cb.checked);
          });
          syncHeroProgress();
        }

        function syncModule(){
          const active = state.activeModule || "gamma";
          moduleButtons.forEach((button) => {
            button.classList.toggle("is-active", button.getAttribute("data-strat-module") === active);
          });
          const focus = moduleFocus[active] || moduleFocus.gamma;
          if (heroFocus) heroFocus.textContent = focus[0];
          if (focusTitle) focusTitle.textContent = focus[0];
          if (focusBody) focusBody.textContent = focus[1];
          const target = document.getElementById(`stratSection-${active}`);
          if (target) {
            target.classList.add("is-focused");
            Array.from(document.querySelectorAll("[data-strat-section]")).forEach((section) => {
              if (section !== target) section.classList.remove("is-focused");
            });
          }
        }

        function syncGlossary(){
          const active = state.glossaryTerm || "gamma";
          glossaryButtons.forEach((button) => {
            button.classList.toggle("is-active", button.getAttribute("data-term") === active);
          });
          const entry = glossary[active] || glossary.gamma;
          if (glossaryDisplay) {
            glossaryDisplay.innerHTML = `<div class="stratGlossaryTerm">${entry[0]}</div><div class="stratGlossaryDefinition">${entry[1]}</div>`;
          }
          syncHeroProgress();
        }

        function syncQuiz(){
          const totals = quizTotals();
          if (quizScoreChip) quizScoreChip.textContent = `${totals.correct}/${totals.total}`;
          quizQuestions.forEach((question) => {
            const id = question.getAttribute("data-question-id");
            const expected = question.getAttribute("data-answer");
            const selected = state.quiz[id];
            const feedback = question.querySelector(".stratQuizFeedback");
            const options = Array.from(question.querySelectorAll(".stratQuizOption"));
            options.forEach((button) => {
              const choice = button.getAttribute("data-choice");
              button.classList.toggle("is-correct", !!selected && choice === expected);
              button.classList.toggle("is-wrong", !!selected && choice === selected && selected !== expected);
            });
            if (feedback) {
              if (!selected) {
                feedback.textContent = "";
                feedback.className = "stratQuizFeedback";
              } else if (selected === expected) {
                feedback.textContent = "Correct. Keep that pattern-context link tight.";
                feedback.className = "stratQuizFeedback is-correct";
              } else {
                feedback.textContent = "Not quite. The level + context read matters more than the candle label alone.";
                feedback.className = "stratQuizFeedback is-wrong";
              }
            }
          });
          syncHeroProgress();
        }

        checks.forEach((cb) => {
          const key = cb.getAttribute("data-key");
          cb.checked = !!state.checklist[key];
          cb.addEventListener("change", () => {
            state.checklist[key] = cb.checked;
            persist();
            syncChecklist();
          });
        });

        moduleButtons.forEach((button) => {
          button.addEventListener("click", () => {
            state.activeModule = button.getAttribute("data-strat-module") || "gamma";
            persist();
            syncModule();
          });
        });

        glossaryButtons.forEach((button) => {
          button.addEventListener("click", () => {
            state.glossaryTerm = button.getAttribute("data-term") || "gamma";
            persist();
            syncGlossary();
          });
        });

        quizQuestions.forEach((question) => {
          question.querySelectorAll(".stratQuizOption").forEach((button) => {
            button.addEventListener("click", () => {
              const qid = question.getAttribute("data-question-id");
              state.quiz[qid] = button.getAttribute("data-choice");
              persist();
              syncQuiz();
            });
          });
        });

        window.stratChecklistClear = function(){
          state.checklist = {};
          persist();
          checks.forEach((cb) => { cb.checked = false; });
          syncChecklist();
        };

        syncChecklist();
        syncModule();
        syncGlossary();
        syncQuiz();
      })();
    </script>
    """
    return render_page(content, active="strat", title="🧠 The Strat")
