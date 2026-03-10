(() => {
  "use strict";

  /** @typedef {'at_flip'|'very_close'|'near'|'far'|'unavailable'} FlipProximityState */
  /** @typedef {'at_wall'|'very_close'|'near'|'far'|'unavailable'} WallProximityState */
  /** @typedef {'clear'|'compressed_trap_zone'|'knife_edge_structure'|'unavailable'} TrapZoneState */

  const card = document.getElementById("spxPriorityCard");
  if (!card) return;

  const getJson = (id) => {
    const node = document.getElementById(id);
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "null");
    } catch (_err) {
      return null;
    }
  };

  const initialBasePayload = getJson("spxPriorityBasePayload") || {};

  const asNum = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const formatNumber = (value, digits = 1) => {
    const n = asNum(value);
    return n === null ? "—" : n.toFixed(digits);
  };

  function formatLevelDistance(distance) {
    const value = asNum(distance);
    if (value === null) return "—";
    const pts = Math.abs(value);
    const unit = Math.abs(pts - 1.0) < 1e-9 ? "pt" : "pts";
    if (pts < 0.05) return `0 ${unit}`;
    const side = value > 0 ? "above" : "below";
    return `${pts.toFixed(1)} ${unit} ${side}`;
  }

  function classifyProximity(distance, kind) {
    const value = asNum(distance);
    if (value === null) return "unavailable";
    const absValue = Math.abs(value);
    if (absValue <= 5) return kind === "flip" ? "at_flip" : "at_wall";
    if (absValue <= 10) return "very_close";
    if (absValue <= 20) return "near";
    return "far";
  }

  function classifyTrapZone(spot, gammaFlip, callWall, putWall) {
    const s = asNum(spot);
    const g = asNum(gammaFlip);
    const c = asNum(callWall);
    const p = asNum(putWall);
    if (s === null || g === null || c === null || p === null) return "unavailable";

    const cluster = Math.max(c, p, g) - Math.min(c, p, g);
    if (cluster <= 15) return "knife_edge_structure";

    const betweenFlipCall = s >= Math.min(g, c) && s <= Math.max(g, c);
    const betweenPutFlip = s >= Math.min(p, g) && s <= Math.max(p, g);
    if ((betweenFlipCall && Math.abs(c - g) <= 15) || (betweenPutFlip && Math.abs(g - p) <= 15)) {
      return "compressed_trap_zone";
    }
    return "clear";
  }

  function computeDistanceMetrics(input) {
    const spot = asNum(input.spot);
    const gammaFlip = asNum(input.gammaFlip);
    const callWall = asNum(input.callWall);
    const putWall = asNum(input.putWall);
    const expectedMove = asNum(input.expectedMove);

    const distanceToFlip = spot !== null && gammaFlip !== null ? spot - gammaFlip : null;
    const distanceToCallWall = spot !== null && callWall !== null ? spot - callWall : null;
    const distanceToPutWall = spot !== null && putWall !== null ? spot - putWall : null;

    const nextCallWallAbove = asNum(input.nextCallWallAbove);
    const nextPutWallBelow = asNum(input.nextPutWallBelow);

    const expectedMoveUp = spot !== null && expectedMove !== null ? spot + expectedMove : null;
    const expectedMoveDown = spot !== null && expectedMove !== null ? spot - expectedMove : null;
    const expectedMoveRangeText =
      expectedMoveUp !== null && expectedMoveDown !== null
        ? `${expectedMoveDown.toFixed(1)} - ${expectedMoveUp.toFixed(1)}`
        : expectedMove !== null
          ? `±${expectedMove.toFixed(1)}`
          : "—";

    const flipProximityState = classifyProximity(distanceToFlip, "flip");
    const nearestWallDistance =
      distanceToCallWall !== null && distanceToPutWall !== null
        ? Math.abs(distanceToCallWall) <= Math.abs(distanceToPutWall)
          ? distanceToCallWall
          : distanceToPutWall
        : distanceToCallWall !== null
          ? distanceToCallWall
          : distanceToPutWall;

    const wallProximityState = classifyProximity(nearestWallDistance, "wall");
    const trapZoneState = classifyTrapZone(spot, gammaFlip, callWall, putWall);

    return {
      distance_to_flip: distanceToFlip,
      distance_to_call_wall: distanceToCallWall,
      distance_to_put_wall: distanceToPutWall,
      next_call_wall_above: nextCallWallAbove,
      next_put_wall_below: nextPutWallBelow,
      expected_move_up: expectedMoveUp,
      expected_move_down: expectedMoveDown,
      expected_move_range_text: expectedMoveRangeText,
      flip_proximity_state: flipProximityState,
      wall_proximity_state: wallProximityState,
      trap_zone_state: trapZoneState,
    };
  }

  function buildGammaNarrative(input, metrics) {
    const out = [];
    const distanceToFlip = asNum(metrics.distance_to_flip);
    const distanceToCall = asNum(metrics.distance_to_call_wall);
    const distanceToPut = asNum(metrics.distance_to_put_wall);
    const spot = asNum(input.spot);
    const gammaFlip = asNum(input.gammaFlip);
    const callWall = asNum(input.callWall);
    const putWall = asNum(input.putWall);
    const netGamma = asNum(input.netGamma);

    if (distanceToFlip !== null) {
      const side = distanceToFlip > 0 ? "above" : "below";
      out.push(`SPX is ${Math.abs(distanceToFlip).toFixed(1)} points ${side} gamma flip.`);
    }

    if (spot !== null && gammaFlip !== null && callWall !== null && spot >= Math.min(gammaFlip, callWall) && spot <= Math.max(gammaFlip, callWall)) {
      out.push("Spot is inside the neutral chop zone between gamma flip and call wall.");
    } else if (spot !== null && putWall !== null && gammaFlip !== null && spot >= Math.min(putWall, gammaFlip) && spot <= Math.max(putWall, gammaFlip)) {
      out.push("Spot is inside the lower transition pocket between put wall and gamma flip.");
    }

    if (distanceToCall !== null && Math.abs(distanceToCall) <= 20) {
      out.push("Price is approaching the call wall. Expect resistance or a sweep/reject unless acceptance holds above.");
    } else if (distanceToPut !== null && Math.abs(distanceToPut) <= 20) {
      out.push("Price is approaching the put wall. Expect support response or a flush-and-reclaim test.");
    }

    if (netGamma !== null && distanceToFlip !== null) {
      if (netGamma > 0 && distanceToFlip > 0) {
        out.push("Positive gamma, stabilizing tape. Mean reversion is favored.");
      } else if (netGamma > 0 && distanceToFlip <= 0) {
        out.push("Positive gamma but below flip. Contained downside unless support fails.");
      } else if (netGamma < 0 && distanceToFlip <= 0) {
        out.push("Negative gamma, downside expansion risk.");
      } else if (netGamma < 0 && distanceToFlip > 0) {
        out.push("Negative gamma with upside squeeze risk.");
      }
      if (Math.abs(distanceToFlip) <= 10) {
        out.push("Market is near a regime transition. Expect fakeouts and unstable direction.");
      }
    }

    if (metrics.trap_zone_state === "knife_edge_structure") {
      out.push("Flip and walls are tightly clustered. Knife-edge structure can whip both directions.");
    }

    if (!out.length) {
      out.push("Awaiting complete live gamma context for SPX.");
    }
    if (out.length === 1) {
      out.push("Waiting for full level alignment before issuing directional context.");
    }
    return out.slice(0, 4);
  }

  const levelStrength = (distance, state) => {
    if (asNum(distance) === null || state === "unavailable") return "Unavailable";
    if (state === "at_flip" || state === "at_wall") return "Immediate test";
    if (state === "very_close") return "High reaction odds";
    if (state === "near") return "In play";
    return "Background level";
  };

  const warningBadges = (input, metrics) => {
    const out = [];
    const dFlip = asNum(metrics.distance_to_flip);
    const dCall = asNum(metrics.distance_to_call_wall);
    const dPut = asNum(metrics.distance_to_put_wall);
    const netGamma = asNum(input.netGamma);

    if (metrics.trap_zone_state === "knife_edge_structure") out.push({ label: "Knife Edge", tone: "critical" });
    if (dFlip !== null && Math.abs(dFlip) <= 10) out.push({ label: "Regime Transition", tone: "warn" });
    if (metrics.trap_zone_state === "compressed_trap_zone") out.push({ label: "Neutral Chop", tone: "neutral" });
    if (dCall !== null && Math.abs(dCall) <= 10) out.push({ label: "Call Wall Test", tone: "warn" });
    if (dPut !== null && Math.abs(dPut) <= 10) out.push({ label: "Put Wall Test", tone: "warn" });
    if (netGamma !== null && netGamma < 0) out.push({ label: "Expansion Risk", tone: "critical" });
    if (!out.length) out.push({ label: "Neutral Chop", tone: "neutral" });
    return out;
  };

  const setText = (id, value) => {
    const node = document.getElementById(id);
    if (!node) return;
    const next = String(value ?? "—");
    if (node.textContent !== next) {
      node.textContent = next;
    }
  };

  const setNarrative = (items) => {
    const root = document.getElementById("spxPriorityNarrative");
    if (!root) return;
    const normalized = items.map((s) => String(s || "")).filter(Boolean).slice(0, 4);
    const current = Array.from(root.querySelectorAll("li")).map((li) => (li.textContent || "").trim());
    if (JSON.stringify(current) === JSON.stringify(normalized)) return;
    root.innerHTML = "";
    for (const line of normalized) {
      const li = document.createElement("li");
      li.textContent = line;
      root.appendChild(li);
    }
  };

  const setBadges = (badges) => {
    const root = document.getElementById("spxPriorityWarningBadges");
    if (!root) return;
    const normalized = badges.map((b) => `${b.label}|${b.tone}`);
    const current = Array.from(root.querySelectorAll(".spxPriorityWarningChip")).map((node) => {
      const tone = node.className.includes("tone-critical")
        ? "critical"
        : node.className.includes("tone-warn")
          ? "warn"
          : "neutral";
      return `${(node.textContent || "").trim()}|${tone}`;
    });
    if (JSON.stringify(current) === JSON.stringify(normalized)) return;

    root.innerHTML = "";
    for (const badge of badges) {
      const chip = document.createElement("span");
      chip.className = `trendChip spxPriorityWarningChip tone-${badge.tone}`;
      chip.textContent = badge.label;
      root.appendChild(chip);
    }
  };

  const adaptInput = (base) => {
    const spxQuote = base.spx_quote || {};
    const gamma = base.gamma_snapshot || {};
    return {
      spot: asNum(spxQuote.price),
      gammaFlip: asNum(gamma.gamma_flip),
      callWall: asNum(gamma.call_wall),
      putWall: asNum(gamma.put_wall),
      nextCallWallAbove: asNum(gamma.next_call_wall_above),
      nextPutWallBelow: asNum(gamma.next_put_wall_below),
      netGamma: asNum(gamma.net_gex),
      regime: String(gamma.regime || ""),
      bias: String(gamma.bias || ""),
      expectedMove: asNum(gamma.expected_move),
      updatedAt: gamma.asof || null,
    };
  };

  const applyContext = (input) => {
    const metrics = computeDistanceMetrics(input);
    const narrative = buildGammaNarrative(input, metrics);
    const badges = warningBadges(input, metrics);

    setText("spxPriorityDistanceFlip", formatLevelDistance(metrics.distance_to_flip));
    setText("spxPriorityDistanceCall", formatLevelDistance(metrics.distance_to_call_wall));
    setText("spxPriorityDistancePut", formatLevelDistance(metrics.distance_to_put_wall));
    setText("spxPriorityExpectedMoveRange", metrics.expected_move_range_text || "—");
    setText("spxPriorityExpectedMoveDown", metrics.expected_move_down === null ? "Down —" : `Down ${formatNumber(metrics.expected_move_down, 1)}`);
    setText("spxPriorityExpectedMoveUp", metrics.expected_move_up === null ? "Up —" : `Up ${formatNumber(metrics.expected_move_up, 1)}`);
    setText("spxPriorityFlipState", levelStrength(metrics.distance_to_flip, metrics.flip_proximity_state));
    setText("spxPriorityWallState", levelStrength(metrics.distance_to_call_wall ?? metrics.distance_to_put_wall, metrics.wall_proximity_state));
    setText("spxPriorityTrapZone", String(metrics.trap_zone_state || "unavailable").replace(/_/g, " "));
    setText("spxPriorityNextCallWall", metrics.next_call_wall_above === null ? "—" : formatNumber(metrics.next_call_wall_above, 0));
    setText("spxPriorityNextPutWall", metrics.next_put_wall_below === null ? "—" : formatNumber(metrics.next_put_wall_below, 0));

    setText("spxPriorityContextSummary", `Flip ${metrics.flip_proximity_state || "unavailable"} · Wall ${metrics.wall_proximity_state || "unavailable"} · Trap ${metrics.trap_zone_state || "unavailable"}`);
    setNarrative(narrative);
    setBadges(badges);
  };

  let lastRenderKey = "";
  const renderFromBasePayload = (base) => {
    const input = adaptInput(base);
    const key = JSON.stringify(input);
    if (key === lastRenderKey) return;
    lastRenderKey = key;

    if (input.spot !== null) {
      const spotText = input.spot.toFixed(2);
      setText("spxPrioritySpotValue", spotText);
      setText("spxPrioritySpotLead", `${spotText} · ${base.spx_quote?.market_state || "Unavailable"}`);
    }
    if (input.gammaFlip !== null) setText("spxPriorityGammaFlipValue", formatNumber(input.gammaFlip, 0));
    if (input.callWall !== null) setText("spxPriorityCallWallValue", formatNumber(input.callWall, 0));
    if (input.putWall !== null) setText("spxPriorityPutWallValue", formatNumber(input.putWall, 0));
    if (input.netGamma !== null) setText("spxPriorityNetGammaValue", input.netGamma.toFixed(0));
    if (input.regime) setText("spxPriorityRegimeValue", input.regime.toUpperCase());
    if (input.bias) setText("spxPriorityBiasValue", input.bias);
    applyContext(input);
  };

  renderFromBasePayload(initialBasePayload);

  const stream = new EventSource("/stream/market");
  stream.onmessage = (event) => {
    if (!event || !event.data) return;
    let payload = null;
    try {
      payload = JSON.parse(event.data);
    } catch (_err) {
      return;
    }
    const prices = payload && typeof payload === "object" ? payload.prices || {} : {};
    const spxTick = prices.SPX || prices["^GSPC"] || null;
    const gamma = payload && typeof payload === "object" ? payload.gamma_map || {} : {};

    const nextPayload = {
      spx_quote: {
        ...(initialBasePayload.spx_quote || {}),
        ...(spxTick || {}),
        price: asNum(spxTick && spxTick.price),
      },
      gamma_snapshot: {
        ...(initialBasePayload.gamma_snapshot || {}),
        ...(gamma || {}),
      },
    };
    renderFromBasePayload(nextPayload);
  };
})();
