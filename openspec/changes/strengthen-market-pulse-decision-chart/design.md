## Context

The Market Pulse page already has a canonical four-part execution narrative and a Lightweight
Charts hero chart. The first narrative card currently receives most visual emphasis. Candle
direction uses white and blue even though blue is also a structural-level color. Strat numbers
and arrows are meaningful user annotations and must remain intact.

## Goals / Non-Goals

**Goals:**

- Make the four execution decisions scan as a single, equally important sequence.
- Give each decision a restrained semantic identity without turning the strip into four alerts.
- Separate candle direction from structural-level color semantics.
- Strengthen active/invalidation context and session awareness using existing data.
- Preserve responsive layout, incremental refresh, and accessibility.

**Non-Goals:**

- No strategy, permission, refresh, API, or market-data changes.
- No removal or reinterpretation of Strat numbers, arrows, or toggles.
- No new chart dependency or animation-heavy effects.

## Decisions

1. Use a four-stage decision rail. Each card receives the same elevation and minimum height,
   with a semantic edge/accent: permission violet, active level cyan, evidence amber, and
   invalidation rose. State-specific emphasis is additive rather than making other cards flat.
2. Keep one shared legend on the strip. Its language explains each card's decision purpose and
   execution-state meanings so help does not compete with live values.
3. Use soft-white `#F4F6FA` bullish bodies, deep-blue `#1F4ACB` bearish bodies, silver `#B9C1CD`
   and electric-blue `#2F6BFF` borders, and cool-gray `#D7DCE5` wicks. Prior-session variants are
   translucent. Strat numerals remain unchanged; directional arrows use coordinated white/blue
   colors. Marker generation and classification remain unchanged.
4. Preserve existing level values and line construction. Primary spot, active, and invalidation
   labels receive stronger opacity and concise signed-distance context; other levels are dimmed
   to maintain reference without competing.
5. Use existing session boundaries to add restrained after-hours context. The treatment must not
   recolor candles or imply a different directional meaning.
6. Invalidation becomes proximity-aware. Its baseline remains legible, with stronger emphasis
   only near or beyond the threshold.
7. Automatic refresh communicates through the existing dot, tooltip, and accessible label only.
   Manual refresh retains its labeled button but does not replace its label with transient status
   prose. Market Pulse CSS and hero-chart scripts receive a new explicit revision token.
8. Gamma Ladder presents requested and effective expiry separately when after-hours `0DTE` falls
   forward to the next expiry, and visually selects the effective preset.
9. Gamma depth bars continue scaling against the largest call/put component. Displayed net
   strength uses a separate maximum absolute net-GEX scale and the labels Dominant, Major,
   Moderate, and Minor.
10. Ladder rows show strike, one signed distance, one role, the depth bar, net GEX, and one strength
   label. Repeated direction, rank, and per-row NEG/POS language is removed. The selected-level
   panel remains compact until selection, then shows behavior, failure context, and chart linkage;
   selection persists across refresh when the strike remains available.

## Risks / Trade-offs

- [Four colored cards could become noisy] -> Keep backgrounds dark and use color primarily on
  edges, labels, and restrained shadows.
- [Red/green direction can be inaccessible] -> Retain candle body/wick geometry and high contrast;
  never encode direction through color alone.
- [Chart overlays could hide candles] -> Keep session treatment behind series and distance labels
  on existing price lines/rail.
- [Dynamic proximity could imply trading permission] -> Treat it solely as distance emphasis;
  Execution Read remains authoritative.
- [Existing screenshot assertions could be brittle] -> Prefer semantic DOM/config assertions and
  preserve stable IDs.
