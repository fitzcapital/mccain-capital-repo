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

      <section class="stratSectionCard stratGammaMasterSection stratSectionSecondary stratLabSection" id="stratSection-gamma" data-strat-section="gamma">
        <div class="stratSectionHeader">
          <div>
            <div class="pill">⚡ Gamma Environment</div>
            <h3>The same Strat setup behaves differently depending on the dealer environment.</h3>
            <div class="meta">Use gamma to judge follow-through quality, reversal odds, and how much confirmation the setup really needs.</div>
          </div>
          <div class="stratSectionBadge">Execution Context</div>
        </div>

        <div class="stratGammaOverviewGrid">
          <article class="stratGammaOverviewCard tone-positive">
            <div class="stratGammaOverviewTop">
              <div class="stratGammaIcon">🟢</div>
              <div>
                <h4>Positive Gamma</h4>
                <div class="stratGammaOverviewKicker">Controlled / mean-reverting tape</div>
              </div>
            </div>
            <p>Dealers tend to dampen moves more. Price often responds to levels instead of running cleanly through them.</p>
            <div class="stratGammaOverviewNote"><b>What price tends to do:</b> chop, pin, fade, reclaim, stall.</div>
            <div class="stratGammaOverviewNote"><b>What this means for Strat:</b> 2-2 reversals and failed moves can clean up, but breakouts need cleaner location and real confirmation.</div>
          </article>

          <article class="stratGammaOverviewCard tone-negative">
            <div class="stratGammaOverviewTop">
              <div class="stratGammaIcon">🔴</div>
              <div>
                <h4>Negative Gamma</h4>
                <div class="stratGammaOverviewKicker">Expansion / momentum tape</div>
              </div>
            </div>
            <p>Moves can travel faster and fail harder. If momentum gets traction, continuation often matters more than hoping for reversion.</p>
            <div class="stratGammaOverviewNote"><b>What price tends to do:</b> trend, accelerate, overshoot, punish late fades.</div>
            <div class="stratGammaOverviewNote"><b>What this means for Strat:</b> breakouts and continuity can run, but weak reversals fail fast unless the level is real and the trigger is sharp.</div>
          </article>

          <article class="stratGammaOverviewCard tone-caution">
            <div class="stratGammaOverviewTop">
              <div class="stratGammaIcon">🟠</div>
              <div>
                <h4>Gamma Flip</h4>
                <div class="stratGammaOverviewKicker">Behavior transition zone</div>
              </div>
            </div>
            <p>This is where tape character can change and edge often degrades. You can still trade it, but you need proof rather than a clean story.</p>
            <div class="stratGammaOverviewNote"><b>What price tends to do:</b> false starts, mixed candles, weak follow-through.</div>
            <div class="stratGammaOverviewNote"><b>What this means for Strat:</b> reduce anticipation, wait for the reclaim or rejection to actually hold, and size down if the market is still indecisive.</div>
          </article>

          <article class="stratGammaOverviewCard tone-call">
            <div class="stratGammaOverviewTop">
              <div class="stratGammaIcon">🧱</div>
              <div>
                <h4>Walls Matter</h4>
                <div class="stratGammaOverviewKicker">Call wall / put wall behavior</div>
              </div>
            </div>
            <p>Walls tell you where continuation can stall and where reversals can actually matter. They are not magic, but they are better than guessing.</p>
            <div class="stratGammaOverviewNote"><b>Call wall:</b> breakouts into it can stall in positive gamma and need stronger proof in negative gamma.</div>
            <div class="stratGammaOverviewNote"><b>Put wall:</b> bounces matter only if buyers respond; if the wall breaks in negative gamma, continuation risk increases fast.</div>
          </article>
        </div>

        <div class="stratGammaCompareShell">
          <div class="stratGammaSubhead">
            <div>
              <h4>Same Setup, Different Environment</h4>
              <div class="meta">This is the big idea: pattern name stays the same, but expected quality changes with gamma.</div>
            </div>
          </div>
          <div class="stratGammaCompareGrid">
            <article class="stratGammaCompareCard tone-positive">
              <div class="stratGammaCompareEyebrow">2-2 reversal · Positive Gamma</div>
              <div class="stratGammaCompareTitle">Responsive mean-reversion setup</div>
              <div class="stratGammaCompareBody">At support or the local flip, the failed push can reverse cleanly because the tape is more controlled. You still want location, but you can trust the level response more.</div>
              <div class="stratGammaCompareFooter"><b>Execution read:</b> fade with confirmation, don't chase after the first push away from the level.</div>
            </article>

            <article class="stratGammaCompareCard tone-negative">
              <div class="stratGammaCompareEyebrow">2-2 reversal · Negative Gamma</div>
              <div class="stratGammaCompareTitle">Needs sharper proof or it can fail fast</div>
              <div class="stratGammaCompareBody">The same 2-2 can look attractive and still get steamrolled if momentum is active. If the trigger is weak or the level is sloppy, continuation can overwhelm the reversal quickly.</div>
              <div class="stratGammaCompareFooter"><b>Execution read:</b> demand rejection plus follow-through and use tighter discipline on size and invalidation.</div>
            </article>

            <article class="stratGammaCompareCard tone-cyan">
              <div class="stratGammaCompareEyebrow">Breakout / continuity · Positive Gamma</div>
              <div class="stratGammaCompareTitle">Breakouts can stall without real expansion</div>
              <div class="stratGammaCompareBody">A clean 2-1-2 or 3-1-2 above support can still fail if it runs directly into a wall or extended location. Positive gamma often rewards buying pullbacks better than chasing the initial break.</div>
              <div class="stratGammaCompareFooter"><b>Execution read:</b> don’t overpay for the breakout; wait for reclaim, hold, or pullback acceptance.</div>
            </article>

            <article class="stratGammaCompareCard tone-danger">
              <div class="stratGammaCompareEyebrow">Breakout / failed bounce · Negative Gamma</div>
              <div class="stratGammaCompareTitle">Continuation and failure both carry more force</div>
              <div class="stratGammaCompareBody">Above the flip, breakouts can actually run. Below the flip, a failed bounce or failed broadening formation can accelerate lower because the tape is less forgiving.</div>
              <div class="stratGammaCompareFooter"><b>Execution read:</b> respect momentum, especially when continuity agrees and the bounce cannot reclaim structure.</div>
            </article>
          </div>
        </div>

        <div class="stratGammaRulesShell">
          <div class="stratGammaSubhead">
            <div>
              <h4>Gamma-Aware Execution Notes</h4>
              <div class="meta">Use these as live trading rules, not as abstract market theory.</div>
            </div>
          </div>
          <div class="stratGammaRulesGrid">
            <div class="stratGammaRuleCard"><b>Positive gamma:</b> demand clean location and don’t overpay for breakouts. Reversion usually beats blind chase.</div>
            <div class="stratGammaRuleCard"><b>Negative gamma:</b> respect momentum and tighten risk because failed entries can accelerate immediately.</div>
            <div class="stratGammaRuleCard"><b>Reversals:</b> a 2-2 near support in positive gamma is not the same quality as a 2-2 fading trend in negative gamma.</div>
            <div class="stratGammaRuleCard"><b>Continuity:</b> when timeframe continuity lines up with negative gamma, breakout follow-through quality improves.</div>
            <div class="stratGammaRuleCard"><b>Walls and flip:</b> these levels change whether a setup is responsive, conditional, or dangerous.</div>
            <div class="stratGammaRuleCard"><b>Stop discipline:</b> gamma changes how fast price can move, so the same candle pattern may need very different risk tolerance.</div>
          </div>
        </div>

        <div class="stratGammaCheatShell">
          <div class="stratGammaSubhead">
            <div>
              <h4>Quick Read / Cheat Sheet</h4>
              <div class="meta">Fast scan for how local flip location and gamma combine.</div>
            </div>
          </div>
          <div class="stratGammaCheatGrid">
            <div class="stratGammaCheatCard tone-positive">
              <div class="stratGammaCheatTitle">Above LF + Positive Gamma</div>
              <div class="stratGammaCheatBody">Controlled bullish. Buy dips carefully. Expect better level response than runaway continuation.</div>
            </div>
            <div class="stratGammaCheatCard tone-caution">
              <div class="stratGammaCheatTitle">Below LF + Positive Gamma</div>
              <div class="stratGammaCheatBody">Weak but more contained. Reversion is possible, but only if support actually responds.</div>
            </div>
            <div class="stratGammaCheatCard tone-cyan">
              <div class="stratGammaCheatTitle">Above LF + Negative Gamma</div>
              <div class="stratGammaCheatBody">Unstable bullish. Breakout prone. Continuation can work, but sloppy pullbacks get punished fast.</div>
            </div>
            <div class="stratGammaCheatCard tone-danger">
              <div class="stratGammaCheatTitle">Below LF + Negative Gamma</div>
              <div class="stratGammaCheatBody">Bearish acceleration risk. Failed bounces and weak reclaim attempts can unwind quickly.</div>
            </div>
          </div>
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
        state.quiz = state.quiz || {};
        state.activeModule = state.activeModule || "gamma";
        state.glossaryTerm = state.glossaryTerm || "gamma";

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

        function syncHeroProgress(){
          const quiz = quizTotals();
          const conceptTotal = quiz.total + Object.keys(glossary).length;
          const conceptDone = quiz.correct + (state.glossaryTerm ? 1 : 0);
          const pct = conceptTotal ? Math.round((conceptDone / conceptTotal) * 100) : 0;
          if (heroProgressFill) heroProgressFill.style.width = pct + "%";
          if (heroProgressPct) heroProgressPct.textContent = pct + "%";
          if (heroDrillScore) heroDrillScore.textContent = `${quiz.correct}/${quiz.total}`;
          if (learningMeterText) learningMeterText.textContent = `${conceptDone} / ${conceptTotal} concepts`;
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

        syncModule();
        syncGlossary();
        syncQuiz();
      })();
    </script>
    """
    return render_page(content, active="strat", title="🧠 The Strat")
