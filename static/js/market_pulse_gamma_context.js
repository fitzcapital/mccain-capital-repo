(() => {
  "use strict";

  const asNum = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const abs = (value) => {
    const n = asNum(value);
    return n === null ? null : Math.abs(n);
  };

  const formatNumber = (value, digits = 1) => {
    const n = asNum(value);
    return n === null ? "—" : n.toFixed(digits);
  };

  const formatNetGamma = (value) => {
    const n = asNum(value);
    if (n === null) return "—";
    const absN = Math.abs(n);
    const sign = n < 0 ? "-" : n > 0 ? "+" : "";
    if (absN >= 1_000_000_000) return `${sign}${(absN / 1_000_000_000).toFixed(1)} billion`;
    if (absN >= 1_000_000) return `${sign}${(absN / 1_000_000).toFixed(1)} million`;
    if (absN >= 1_000) return `${sign}${(absN / 1_000).toFixed(1)} thousand`;
    return String(Math.round(n));
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

  function inferLevelStep(callWall, putWall, candidates) {
    const vals = (Array.isArray(candidates) ? candidates : []).map(asNum).filter((v) => v !== null);
    const uniq = Array.from(new Set(vals)).sort((a, b) => a - b);
    const diffs = [];
    for (let i = 1; i < uniq.length; i += 1) {
      const d = Math.abs(uniq[i] - uniq[i - 1]);
      if (d > 0.01) diffs.push(d);
    }
    if (diffs.length) {
      diffs.sort((a, b) => a - b);
      return Math.max(5, Math.min(25, diffs[Math.floor(diffs.length / 2)]));
    }
    if (callWall !== null && putWall !== null) {
      const spread = Math.abs(callWall - putWall);
      if (spread > 0) return Math.max(5, Math.min(25, Math.round((spread / 4) / 5) * 5));
    }
    return 10;
  }

  function inferExpectedMove(expectedMove, spot, gammaFlip, callWall, putWall, candidates) {
    const direct = asNum(expectedMove);
    if (direct !== null) return direct;

    const s = asNum(spot);
    const g = asNum(gammaFlip);
    const c = asNum(callWall);
    const p = asNum(putWall);
    const ladder = (Array.isArray(candidates) ? candidates : [])
      .map(asNum)
      .filter((v) => v !== null)
      .sort((a, b) => a - b);

    if (s !== null && c !== null && p !== null) {
      const derived = Math.max(Math.abs(c - s), Math.abs(s - p));
      if (derived > 0) return derived;
    }
    if (c !== null && p !== null) {
      const spread = Math.abs(c - p) / 2;
      if (spread > 0) return spread;
    }
    if (s !== null && g !== null) {
      const flipDist = Math.abs(s - g);
      if (flipDist > 0) return flipDist;
    }
    if (s !== null && ladder.length) {
      const higher = ladder.filter((v) => v > s);
      const lower = ladder.filter((v) => v < s);
      if (higher.length && lower.length) return Math.max(Math.abs(Math.min(...higher) - s), Math.abs(s - Math.max(...lower)));
      if (higher.length) return Math.abs(Math.min(...higher) - s);
      if (lower.length) return Math.abs(s - Math.max(...lower));
    }
    return null;
  }

  function classifyDealerRegime(netGamma) {
    const ng = asNum(netGamma);
    if (ng === null) return "Unavailable";
    return ng >= 0 ? "Positive Gamma / Mean Reverting" : "Negative Gamma / Momentum Amplifying";
  }

  function classifyVolatilityState(input) {
    const vix = asNum(input.vix);
    const dir = String(input.vixDirection || "unavailable").toLowerCase();
    if (vix !== null && (vix >= 26 || (dir === "rising" && vix >= 22))) return "Expansion Risk";
    if (vix !== null && vix >= 22) return "Elevated";
    if (dir === "rising") return "Rising";
    if (dir === "flat") return "Flat";
    if (vix !== null && vix < 16) return "Calm";
    return "Flat";
  }

  function classifyWallStrength(gammaPerPoint) {
    const value = asNum(gammaPerPoint);
    if (value === null) return "Unknown";
    const millionPerPoint = Math.abs(value) / 1_000_000;
    if (millionPerPoint > 2.0) return "Strong";
    if (millionPerPoint >= 1.0) return "Medium";
    return "Weak";
  }

  function classifyStructureType(derived) {
    if (derived.trapZoneState === "knife_edge_structure") return "Knife Edge";
    if (derived.trapZoneState === "compressed_trap_zone") return "Compression Zone";
    if ((abs(derived.distanceToFlip) || 999) <= 10) return "Regime Transition";
    if ((derived.wallSpread || 0) >= 25 && (abs(derived.distanceToFlip) || 0) > 15) return "Clear Structure";
    if (derived.wallSpread !== null) return "Stable Range";
    return "Unavailable";
  }

  function classifySupportResistanceQuality(input, derived) {
    const callStrength = classifyWallStrength(input.callWallGammaPerPoint);
    const putStrength = classifyWallStrength(input.putWallGammaPerPoint);
    const ng = asNum(input.netGamma);
    const vixDir = String(input.vixDirection || "unavailable").toLowerCase();

    const supportStrong = putStrength === "Strong"
      && (ng || 0) >= 0
      && vixDir !== "rising"
      && (asNum(derived.distanceToExpectedMoveLow) || 999) > 5;
    const resistanceStrong = callStrength === "Strong"
      && (ng || 0) >= 0
      && vixDir !== "rising"
      && (asNum(derived.distanceToExpectedMoveHigh) || 999) > 5;

    return {
      supportQuality: supportStrong ? "Robust Support" : "Fragile Floor",
      resistanceQuality: resistanceStrong ? "Strong Ceiling" : "Fragile Ceiling",
    };
  }

  function detectNoTradeZone(derived) {
    const dFlip = abs(derived.distanceToFlip);
    const spread = asNum(derived.wallSpread);
    return Boolean(
      dFlip !== null
      && dFlip <= 5
      && spread !== null
      && spread <= 20
      && (derived.structureType === "Knife Edge" || derived.structureType === "Regime Transition")
    );
  }

  function getSessionWindowState(nowIso) {
    const now = nowIso ? new Date(nowIso) : new Date();
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
    }).formatToParts(now);

    const hh = Number((parts.find((p) => p.type === "hour") || {}).value || "0");
    const mm = Number((parts.find((p) => p.type === "minute") || {}).value || "0");
    const mins = hh * 60 + mm;

    if (mins >= 570 && mins < 585) return "Early Noise";
    if (mins >= 585 && mins < 660) return "Prime Window";
    if (mins >= 660 && mins < 690) return "Secondary Window";
    if (mins >= 690 && mins < 810) return "Midday Chop";
    if (mins >= 870 && mins < 930) return "Late Rebalance";
    return "Off Hours";
  }

  function computeTradeabilityScore(input, derived) {
    let score = 5;
    const dFlip = abs(derived.distanceToFlip) || 999;
    const wallSpread = asNum(derived.wallSpread) || 0;
    const netGamma = asNum(input.netGamma);

    if (dFlip > 15 && derived.structureType === "Clear Structure") score += 2;
    if (wallSpread > 25) score += 1;
    if ((derived.distanceToExpectedMoveHigh || 0) > 8 || (derived.distanceToExpectedMoveLow || 0) > 8) score += 1;
    if ((derived.volatilityState === "Calm" || derived.volatilityState === "Flat") && netGamma !== null && netGamma >= 0) score += 1;
    if (derived.nearMajorWall || derived.nearPDH || derived.nearPDL) score += 1;

    if (dFlip <= 5) score -= 2;
    if (derived.structureType === "Knife Edge") score -= 2;
    if (derived.insideExpectedMove && (derived.distanceToExpectedMoveHigh || 999) <= 5 && (derived.distanceToExpectedMoveLow || 999) <= 5) score -= 1;
    if ((netGamma || 0) < 0 && dFlip <= 10) score -= 1;
    if (["Knife Edge", "Regime Transition"].includes(derived.structureType)) score -= 1;
    if (derived.noTradeCenter) score -= 1;

    score = clamp(Math.round(score), 0, 10);
    const label = score <= 3 ? "No Trade" : score <= 6 ? "Selective" : "Tradeable";
    const explanation = label === "No Trade"
      ? "No-trade center risk. Wait for edge interaction."
      : label === "Selective"
        ? "Structure is mixed. Only take edge-confirmed setups."
        : "Structure supports disciplined edge trades.";

    return { score, label, explanation };
  }

  function classifyReversalSetupFit(input, derived, tradeability) {
    const dFlip = abs(derived.distanceToFlip) || 999;
    const nearFlip = dFlip <= 8;
    const expectedNearlyConsumed = (derived.distanceToExpectedMoveHigh || 999) <= 5 || (derived.distanceToExpectedMoveLow || 999) <= 5;

    const poor = (
      derived.structureType === "Knife Edge"
      || (derived.structureType === "Regime Transition" && nearFlip)
      || (derived.dealerRegime === "Negative Gamma / Momentum Amplifying" && derived.volatilityState === "Expansion Risk")
      || derived.noTradeCenter
      || derived.sessionWindowState === "Midday Chop"
    );

    if (poor || tradeability.label === "No Trade") {
      return { label: "Poor", explanation: "Environment is unstable for reversal traps. Wait for clearer edge." };
    }

    const good = (
      (derived.dealerRegime === "Positive Gamma / Mean Reverting" || ["Stable Range", "Clear Structure"].includes(derived.structureType))
      && derived.nearMajorWall
      && !nearFlip
      && ["Calm", "Flat", "Rising"].includes(derived.volatilityState)
      && derived.sessionWindowState === "Prime Window"
      && !expectedNearlyConsumed
    );

    if (good && tradeability.score >= 7) {
      return { label: "Good", explanation: "Reversal setup has structure, edge, and timing support." };
    }
    return { label: "Caution", explanation: "Setup can work, but needs clean confirmation at edge." };
  }

  function buildAutoRead(input, derived) {
    const bullets = [];
    if (derived.distanceToFlip !== null && Math.abs(derived.distanceToFlip) <= 10) {
      bullets.push("Price is near gamma flip. Expect fakeouts until structure resolves.");
    }
    if (derived.distanceToCallWall !== null && Math.abs(derived.distanceToCallWall) <= 12) {
      bullets.push("Call wall is close overhead. Rejection is likely unless price accepts above.");
    } else if (derived.distanceToPutWall !== null && Math.abs(derived.distanceToPutWall) <= 12) {
      bullets.push("Put wall is close below. Look for hold/reclaim before fading downside.");
    }
    if (derived.dealerRegime === "Negative Gamma / Momentum Amplifying") {
      bullets.push("Negative gamma is active. Accepted breaks can extend faster than normal.");
    }
    if (derived.noTradeCenter) {
      bullets.push("No-trade center detected. Trade the edges only.");
    }
    if (!bullets.length) {
      bullets.push("Structure is balanced. Wait for clean tests at call/put wall edges.");
    }
    return bullets.slice(0, 3);
  }

  function buildWarningBadges(_input, derived) {
    const out = [];
    if (derived.structureType === "Knife Edge") out.push("Knife Edge");
    if (derived.structureType === "Regime Transition") out.push("Regime Transition");
    if (derived.volatilityState === "Expansion Risk") out.push("Expansion Risk");
    if (derived.sessionWindowState === "Midday Chop") out.push("Chop Risk");
    if (derived.distanceToCallWall !== null && Math.abs(derived.distanceToCallWall) <= 10) out.push("Call Wall Test");
    if (derived.distanceToPutWall !== null && Math.abs(derived.distanceToPutWall) <= 10) out.push("Put Wall Test");
    if (derived.distanceToFlip !== null && Math.abs(derived.distanceToFlip) <= 10) out.push("Near Flip");
    if ((derived.distanceToExpectedMoveHigh || 999) <= 2 || (derived.distanceToExpectedMoveLow || 999) <= 2) out.push("Expected Move Exhausted");
    if (derived.noTradeCenter) out.push("No-Trade Center");
    if (derived.supportQuality === "Fragile Floor") out.push("Fragile Floor");
    if (derived.supportQuality === "Robust Support") out.push("Robust Support");
    return Array.from(new Set(out));
  }

  function inferVixDirection(vixQuote, priorVixDirection) {
    const pct = asNum((vixQuote || {}).change_pct);
    if (pct !== null) {
      if (pct > 0.15) return "rising";
      if (pct < -0.15) return "falling";
      return "flat";
    }
    return priorVixDirection || "unavailable";
  }

  function mapDataQualityLabel(input) {
    const state = String(input.dataState || "").toLowerCase();
    const fresh = String(input.freshnessLabel || "").toLowerCase();
    if (state === "cached" || fresh.includes("cached")) return "Cached Fallback";
    if (state === "delayed" || fresh.includes("delayed")) return "Slightly Delayed";
    if (fresh.includes("stale") || fresh.includes("critical")) return "Stale";
    return "Live";
  }

  function computeDistanceMetrics(input) {
    const spot = asNum(input.spot);
    const gammaFlip = asNum(input.gammaFlip);
    const callWall = asNum(input.callWall);
    const putWall = asNum(input.putWall);

    let nextCallWall = asNum(input.nextCallWall);
    let nextPutWall = asNum(input.nextPutWall);
    if ((nextCallWall === null && callWall !== null) || (nextPutWall === null && putWall !== null)) {
      const step = inferLevelStep(callWall, putWall, input.candidateLevels);
      if (nextCallWall === null && callWall !== null) nextCallWall = callWall + step;
      if (nextPutWall === null && putWall !== null) nextPutWall = putWall - step;
    }

    const expectedMove = inferExpectedMove(
      input.expectedMove,
      spot,
      gammaFlip,
      callWall,
      putWall,
      input.candidateLevels
    );
    let emUp = asNum(input.expectedMoveUp);
    let emDown = asNum(input.expectedMoveDown);
    if (spot !== null && expectedMove !== null) {
      if (emUp === null) emUp = spot + expectedMove;
      if (emDown === null) emDown = spot - expectedMove;
    }

    const distanceToFlip = spot !== null && gammaFlip !== null ? spot - gammaFlip : null;
    const distanceToCallWall = spot !== null && callWall !== null ? spot - callWall : null;
    const distanceToPutWall = spot !== null && putWall !== null ? spot - putWall : null;
    const wallSpread = callWall !== null && putWall !== null ? callWall - putWall : null;

    const distanceToExpectedMoveHigh = spot !== null && emUp !== null ? emUp - spot : null;
    const distanceToExpectedMoveLow = spot !== null && emDown !== null ? spot - emDown : null;
    const insideExpectedMove = spot !== null && emUp !== null && emDown !== null ? spot <= emUp && spot >= emDown : null;

    const flipProximityState = classifyProximity(distanceToFlip, "flip");
    const nearestWallDistance =
      distanceToCallWall !== null && distanceToPutWall !== null
        ? (Math.abs(distanceToCallWall) <= Math.abs(distanceToPutWall) ? distanceToCallWall : distanceToPutWall)
        : (distanceToCallWall !== null ? distanceToCallWall : distanceToPutWall);
    const wallProximityState = classifyProximity(nearestWallDistance, "wall");
    const trapZoneState = classifyTrapZone(spot, gammaFlip, callWall, putWall);

    const nearMajorWall = (abs(distanceToCallWall) || 999) <= 10 || (abs(distanceToPutWall) || 999) <= 10;
    const nearVWAP = spot !== null && asNum(input.vwap) !== null && Math.abs(spot - asNum(input.vwap)) <= 6;
    const nearPDH = spot !== null && asNum(input.priorDayHigh) !== null && Math.abs(spot - asNum(input.priorDayHigh)) <= 8;
    const nearPDL = spot !== null && asNum(input.priorDayLow) !== null && Math.abs(spot - asNum(input.priorDayLow)) <= 8;

    const sessionWindowState = getSessionWindowState(input.marketTimeIso || null);
    const dealerRegime = classifyDealerRegime(input.netGamma);
    const volatilityState = classifyVolatilityState(input);

    const structureType = classifyStructureType({ distanceToFlip, wallSpread, trapZoneState });
    const noTradeCenter = detectNoTradeZone({ distanceToFlip, wallSpread, structureType });

    const callWallStrength = classifyWallStrength(input.callWallGammaPerPoint);
    const putWallStrength = classifyWallStrength(input.putWallGammaPerPoint);
    const sr = classifySupportResistanceQuality(input, { distanceToExpectedMoveLow, distanceToExpectedMoveHigh });

    const derived = {
      distanceToFlip,
      distanceToCallWall,
      distanceToPutWall,
      wallSpread,
      distanceToExpectedMoveHigh,
      distanceToExpectedMoveLow,
      aboveOrBelowFlip: distanceToFlip === null ? "unavailable" : (distanceToFlip > 0 ? "above" : distanceToFlip < 0 ? "below" : "at"),
      insideExpectedMove,
      nearMajorWall,
      nearVWAP,
      nearPDH,
      nearPDL,
      sessionWindowState,
      flipProximityState,
      wallProximityState,
      trapZoneState,
      dealerRegime,
      volatilityState,
      structureType,
      callWallStrength,
      putWallStrength,
      supportQuality: sr.supportQuality,
      resistanceQuality: sr.resistanceQuality,
      noTradeCenter,
      dataQualityLabel: mapDataQualityLabel(input),
      nextCallWall,
      nextPutWall,
      expectedMoveRangeText: emUp !== null && emDown !== null ? `${emDown.toFixed(1)} - ${emUp.toFixed(1)}` : "—",
    };

    derived.tradeability = computeTradeabilityScore(input, {
      distanceToFlip: derived.distanceToFlip,
      wallSpread: derived.wallSpread,
      distanceToExpectedMoveHigh: derived.distanceToExpectedMoveHigh,
      distanceToExpectedMoveLow: derived.distanceToExpectedMoveLow,
      structureType: derived.structureType,
      volatilityState: derived.volatilityState,
      nearMajorWall: derived.nearMajorWall,
      nearPDH: derived.nearPDH,
      nearPDL: derived.nearPDL,
      insideExpectedMove: derived.insideExpectedMove,
      noTradeCenter: derived.noTradeCenter,
    });

    derived.reversalSetupFit = classifyReversalSetupFit(input, {
      dealerRegime: derived.dealerRegime,
      structureType: derived.structureType,
      distanceToFlip: derived.distanceToFlip,
      nearMajorWall: derived.nearMajorWall,
      volatilityState: derived.volatilityState,
      sessionWindowState: derived.sessionWindowState,
      distanceToExpectedMoveHigh: derived.distanceToExpectedMoveHigh,
      distanceToExpectedMoveLow: derived.distanceToExpectedMoveLow,
      noTradeCenter: derived.noTradeCenter,
    }, derived.tradeability);

    return derived;
  }

  const exported = {
    formatNetGamma,
    formatLevelDistance,
    classifyProximity,
    classifyTrapZone,
    classifyDealerRegime,
    classifyVolatilityState,
    classifyStructureType,
    classifyWallStrength,
    classifySupportResistanceQuality,
    detectNoTradeZone,
    computeTradeabilityScore,
    classifyReversalSetupFit,
    buildAutoRead,
    buildWarningBadges,
    getSessionWindowState,
    computeDistanceMetrics,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = exported;
  }
  if (typeof window !== "undefined") {
    window.marketPulseGammaContext = exported;
  }

  if (typeof document === "undefined") return;

  const card = document.getElementById("spxPriorityCard");
  if (!card) return;
  const shell = card.closest(".spxPriorityShell");
  const spotPanel = document.getElementById("spxPrioritySpotPanel");

  const toneForBadge = (label) => {
    const l = String(label || "").toLowerCase();
    if (l.includes("knife") || l.includes("expansion") || l.includes("no-trade")) return "critical";
    if (l.includes("transition") || l.includes("test") || l.includes("near flip") || l.includes("chop")) return "warn";
    return "neutral";
  };

  const setText = (id, value) => {
    const node = document.getElementById(id);
    if (!node) return;
    const next = String(value ?? "—");
    if (node.textContent !== next) node.textContent = next;
  };

  const buildTickPingLabel = (asOfIso, provider) => {
    const ts = typeof asOfIso === "string" ? Date.parse(asOfIso) : NaN;
    if (!Number.isFinite(ts)) return "No tick timestamp";
    const ageMs = Math.max(0, Date.now() - ts);
    const ageS = ageMs / 1000;
    const state = ageS <= 1.5 ? "Live" : ageS <= 4 ? "Lagging" : "Stale";
    const p = String(provider || "").trim();
    return `${state} · ${ageS.toFixed(1)}s old${p ? ` · ${p}` : ""}`;
  };

  const setBullets = (id, items) => {
    const root = document.getElementById(id);
    if (!root) return;
    const normalized = (Array.isArray(items) ? items : []).map((s) => String(s || "").trim()).filter(Boolean).slice(0, 3);
    const current = Array.from(root.querySelectorAll("li")).map((li) => (li.textContent || "").trim());
    if (JSON.stringify(current) === JSON.stringify(normalized)) return;
    root.innerHTML = "";
    normalized.forEach((line) => {
      const li = document.createElement("li");
      li.textContent = line;
      root.appendChild(li);
    });
  };

  const setBadges = (id, labels) => {
    const root = document.getElementById(id);
    if (!root) return;
    const normalized = (Array.isArray(labels) ? labels : []).map((label) => String(label || "")).filter(Boolean);
    const current = Array.from(root.querySelectorAll(".spxPriorityWarningChip")).map((node) => (node.textContent || "").trim());
    if (JSON.stringify(current) === JSON.stringify(normalized)) return;

    root.innerHTML = "";
    normalized.forEach((label) => {
      const chip = document.createElement("span");
      chip.className = `trendChip spxPriorityWarningChip tone-${toneForBadge(label)}`;
      chip.textContent = label;
      root.appendChild(chip);
    });
  };

  const tapeCards = Array.from(document.querySelectorAll(".marketPulseTapeCard[data-symbol]"));

  const dispatchStreamStatus = (status, detail) => {
    window.dispatchEvent(new CustomEvent("market-pulse-stream-status", { detail: { status, detail } }));
  };

  const formatSigned = (value, digits = 2) => {
    const n = asNum(value);
    if (n === null) return "—";
    return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}`;
  };

  const inferAbsoluteChange = (price, pctChange) => {
    const p = asNum(price);
    const pct = asNum(pctChange);
    if (p === null || pct === null) return null;
    const prior = p / (1 + (pct / 100));
    if (!Number.isFinite(prior)) return null;
    return p - prior;
  };

  const formatEtLabel = (iso) => {
    const ts = typeof iso === "string" ? Date.parse(iso) : NaN;
    if (!Number.isFinite(ts)) return "Awaiting live refresh";
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
      timeZoneName: "short",
    }).format(new Date(ts));
  };

  const formatRange = (points) => {
    const values = (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : asNum(row)))
      .filter((value) => value !== null);
    if (!values.length) return "—";
    const low = Math.min(...values);
    const high = Math.max(...values);
    return Math.abs(high - low) < 0.01 ? high.toFixed(2) : `${low.toFixed(2)} to ${high.toFixed(2)}`;
  };

  const sparkTone = (pctChange) => {
    const pct = asNum(pctChange);
    if (pct === null) return "flat";
    if (pct > 0) return "up";
    if (pct < 0) return "down";
    return "flat";
  };

  const buildSparklineSvg = (points, tone) => {
    const values = (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : asNum(row)))
      .filter((value) => value !== null);
    if (values.length < 2) {
      return '<div class="marketMiniSparkEmpty">No trend</div>';
    }
    const width = 120;
    const height = 28;
    let minV = Math.min(...values);
    let maxV = Math.max(...values);
    if (Math.abs(maxV - minV) < 1e-9) maxV = minV + 1;
    const step = width / Math.max(values.length - 1, 1);
    const pts = values.map((value, index) => {
      const x = index * step;
      const y = ((maxV - value) / (maxV - minV)) * (height - 2) + 1;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });
    return (
      `<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true">`
      + `<polyline class="marketMiniSparkLine ${tone}" points="${pts.join(" ")}" />`
      + `</svg>`
    );
  };

  const deriveDataState = (quote) => {
    const price = asNum((quote || {}).price);
    if (price === null) return "missing";
    const provider = String((quote || {}).provider || "").toLowerCase();
    const reason = String((quote || {}).reason || (quote || {}).data_reason || "").toLowerCase();
    if (reason.includes("cached")) return "cached";
    if (provider === "tradier" && reason.startsWith("tradier_")) return "live";
    if (
      reason.includes("fallback")
      || reason.includes("close")
      || reason.includes("snapshot")
      || reason.includes("intraday")
      || reason.includes("prev_close")
    ) {
      return "delayed";
    }
    if (provider && provider !== "tradier") return "delayed";
    return "live";
  };

  const dataStateLabel = (state) => {
    if (state === "live") return "Live";
    if (state === "delayed") return "Delayed";
    if (state === "cached") return "Cached";
    return "Missing";
  };

  const updateStateChip = (node, state) => {
    if (!node) return;
    const nextState = String(state || "missing");
    node.textContent = dataStateLabel(nextState);
    node.classList.remove("state-live", "state-delayed", "state-cached", "state-missing");
    node.classList.add(`state-${nextState}`);
  };

  const updateTextNode = (node, value) => {
    if (!node) return;
    const next = String(value ?? "—");
    if (node.textContent !== next) node.textContent = next;
  };

  const updateSparkNode = (node, points, tone) => {
    if (!node) return;
    node.innerHTML = buildSparklineSvg(points, tone);
  };

  const applyGlowState = (nodes, pctChange) => {
    const pct = asNum(pctChange);
    const positive = pct !== null && pct > 0;
    const negative = pct !== null && pct < 0;
    (Array.isArray(nodes) ? nodes : []).forEach((node) => {
      if (!node) return;
      node.classList.toggle("glow-green", positive);
      node.classList.toggle("glow-red", negative);
    });
  };

  const updateTapeSummary = (prices) => {
    const tracked = tapeCards
      .map((card) => String(card.dataset.symbol || "").toUpperCase())
      .filter(Boolean);
    let advancers = 0;
    let decliners = 0;
    let missing = 0;
    let biggestLabel = "—";
    let biggestMove = null;

    tracked.forEach((symbol) => {
      const quote = ((prices || {})[symbol] || {});
      const pct = asNum(quote.pct_change);
      const price = asNum(quote.price);
      if (price === null || pct === null) {
        missing += 1;
        return;
      }
      if (pct > 0) advancers += 1;
      else if (pct < 0) decliners += 1;
      if (biggestMove === null || Math.abs(pct) > Math.abs(biggestMove)) {
        biggestMove = pct;
        biggestLabel = symbol;
      }
    });

    setText("marketPulseAdvancers", String(advancers));
    setText("marketPulseDecliners", String(decliners));
    setText("marketPulseMissing", String(missing));
    setText(
      "marketPulseBiggestMove",
      biggestMove === null ? "—" : `${biggestLabel} ${formatSigned(biggestMove, 2)}%`
    );
  };

  const applyTapeCardUpdate = (card, quote, points) => {
    if (!card || !quote || typeof quote !== "object") return;
    const price = asNum(quote.price);
    const pct = asNum(quote.pct_change);
    const state = deriveDataState(quote);
    const tone = sparkTone(pct);
    const reason = String(quote.reason || "").trim();
    const asOf = String(quote.as_of || quote.asof || "").trim();

    updateStateChip(card.querySelector('[data-role="state-chip"]'), state);
    updateTextNode(card.querySelector('[data-role="freshness"]'), formatEtLabel(asOf));
    updateTextNode(card.querySelector('[data-role="price"]'), price === null ? "—" : price.toFixed(2));
    updateTextNode(
      card.querySelector('[data-role="change-line"]'),
      `${formatSigned(inferAbsoluteChange(price, pct), 2)} · ${formatSigned(pct, 2)}%`
    );
    updateSparkNode(card.querySelector('[data-role="sparkline"]'), points, tone);
    updateTextNode(card.querySelector('[data-role="range-line"]'), `Range: ${formatRange(points)}`);

    const reasonNode = card.querySelector('[data-role="reason-line"]');
    if (reasonNode) {
      if (state !== "live" && reason) {
        reasonNode.hidden = false;
        updateTextNode(reasonNode, `Reason: ${reason}`);
      } else {
        reasonNode.hidden = true;
        updateTextNode(reasonNode, "");
      }
    }

    card.classList.toggle("glow-green", (pct || 0) > 0);
    card.classList.toggle("glow-red", (pct || 0) < 0);
  };

  const adaptInput = (base) => {
    const spx = (base && base.spx_quote) || {};
    const gamma = (base && base.gamma_snapshot) || {};
    const vixQuote = (base && base.vix_quote) || {};

    const candidateLevels = [];
    const top3 = Array.isArray(gamma.gamma_walls_top3) ? gamma.gamma_walls_top3 : [];
    top3.forEach((v) => {
      const n = asNum(v);
      if (n !== null) candidateLevels.push(n);
    });
    const xs = ((((gamma.chart_json || {}).gex || {}).data || [])[0] || {}).x;
    if (Array.isArray(xs)) {
      xs.forEach((v) => {
        const n = asNum(v);
        if (n !== null) candidateLevels.push(n);
      });
    }

    const nowIso = base.market_now_iso || null;
    const vixDirection = inferVixDirection(vixQuote, base.vix_direction || "unavailable");

    const dayOpen = asNum(spx.day_open);
    const sessionHigh = asNum(spx.day_high);
    const sessionLow = asNum(spx.day_low);

    return {
      spot: asNum(spx.price),
      dayOpen,
      sessionHigh,
      sessionLow,
      priorDayHigh: asNum(spx.prior_day_high), // TODO(api): wire prior_day_high in quote payload when available.
      priorDayLow: asNum(spx.prior_day_low), // TODO(api): wire prior_day_low in quote payload when available.
      vwap: asNum(spx.vwap), // TODO(api): wire session VWAP from provider stream when available.
      overnightHigh: asNum(spx.overnight_high), // TODO(api): wire overnight levels when available.
      overnightLow: asNum(spx.overnight_low), // TODO(api): wire overnight levels when available.
      vix: asNum(vixQuote.price),
      vixDirection,

      gammaFlip: asNum(gamma.gamma_flip),
      callWall: asNum(gamma.call_wall),
      putWall: asNum(gamma.put_wall),
      nextCallWall: asNum(gamma.next_call_wall_above), // TODO(api): preferred backend field for next call wall.
      nextPutWall: asNum(gamma.next_put_wall_below), // TODO(api): preferred backend field for next put wall.
      netGamma: asNum(gamma.net_gex),
      callWallGammaPerPoint: asNum(gamma.call_wall_gamma_per_point), // TODO(api): wire gamma/point from backend.
      putWallGammaPerPoint: asNum(gamma.put_wall_gamma_per_point), // TODO(api): wire gamma/point from backend.
      expectedMove: asNum(gamma.expected_move), // TODO(api): backend should provide explicit expected_move_up/down when available.
      expectedMoveUp: asNum(gamma.expected_move_up ?? gamma.expected_move_high),
      expectedMoveDown: asNum(gamma.expected_move_down ?? gamma.expected_move_low),

      regime: String(gamma.regime || ""),
      bias: String(gamma.bias || ""),
      candidateLevels,
      updatedAt: gamma.asof || null,
      marketTimeIso: nowIso,
      dataState: String(spx.data_state || ""),
      freshnessLabel: String(spx.freshness_label || ""),
      freshnessReason: String(spx.data_reason || ""),
    };
  };

  const render = (base) => {
    const input = adaptInput(base);
    const derived = computeDistanceMetrics(input);
    const bullets = buildAutoRead(input, derived);
    const badges = buildWarningBadges(input, derived);
    const spxQuote = (base && base.spx_quote) || {};
    const spxPoints = (
      (((base || {}).series_points || {}).SPX)
      || (((base || {}).series_points || {})["^GSPC"])
      || (Array.isArray(spxQuote.series) ? spxQuote.series : [])
      || (Array.isArray(spxQuote.mini_series) ? spxQuote.mini_series : [])
    );
    const spxState = deriveDataState(spxQuote);
    const spxAbsChange = inferAbsoluteChange(spxQuote.price, spxQuote.change_pct);

    setText("spxPrioritySpotValue", formatNumber(input.spot, 2));
    setText(
      "spxPrioritySpotLead",
      input.spot === null
        ? "SPX quote unavailable"
        : `${formatNumber(input.spot, 2)} · ${formatSigned(spxAbsChange, 2)} · ${formatSigned(spxQuote.change_pct, 2)}%`
    );
    setText("spxPriorityGammaFlipValue", formatNumber(input.gammaFlip, 0));
    setText("spxPriorityCallWallValue", formatNumber(input.callWall, 0));
    setText("spxPriorityPutWallValue", formatNumber(input.putWall, 0));
    setText("spxPriorityNextCallWall", formatNumber(derived.nextCallWall, 0));
    setText("spxPriorityNextPutWall", formatNumber(derived.nextPutWall, 0));
    setText("spxPriorityExpectedMoveRange", derived.expectedMoveRangeText || "—");

    setText("spxPriorityDealerRegime", derived.dealerRegime);
    setText("spxPriorityVolatilityState", derived.volatilityState);
    setText("spxPriorityStructureType", derived.structureType);
    setText("spxPriorityTradeabilityScore", `${derived.tradeability.score}/10 · ${derived.tradeability.label}`);
    setText("spxPriorityTradeabilityLine", derived.tradeability.explanation);
    setText("spxPriorityReversalFit", `${derived.reversalSetupFit.label} · ${derived.reversalSetupFit.explanation}`);

    setText("spxPriorityCallWallStrength", derived.callWallStrength);
    setText("spxPriorityPutWallStrength", derived.putWallStrength);
    setText("spxPrioritySupportQuality", derived.supportQuality);
    setText("spxPriorityResistanceQuality", derived.resistanceQuality);

    setText("spxPriorityDistanceFlip", formatLevelDistance(derived.distanceToFlip));
    setText("spxPriorityDistanceCall", formatLevelDistance(derived.distanceToCallWall));
    setText("spxPriorityDistancePut", formatLevelDistance(derived.distanceToPutWall));
    setText("spxPriorityWallSpread", asNum(derived.wallSpread) === null ? "—" : `${formatNumber(derived.wallSpread, 1)} pts`);
    setText("spxPrioritySessionWindow", derived.sessionWindowState);
    setText("spxPriorityNoTradeState", derived.noTradeCenter ? "No-Trade Center · Trade the edges only" : "Center is tradeable with confirmation");

    setText("spxPriorityDataQuality", derived.dataQualityLabel);
    setText("spxPriorityVixRaw", asNum(input.vix) === null ? "—" : formatNumber(input.vix, 2));
    setText("spxPriorityVixDirection", input.vixDirection || "unavailable");
    setText("spxPriorityDayOpen", asNum(input.dayOpen) === null ? "—" : formatNumber(input.dayOpen, 2));
    setText(
      "spxPrioritySessionRange",
      asNum(input.sessionHigh) !== null && asNum(input.sessionLow) !== null
        ? `${formatNumber(input.sessionLow, 2)} - ${formatNumber(input.sessionHigh, 2)}`
        : "—"
    );
    setText("spxPriorityVWAP", asNum(input.vwap) === null ? "—" : formatNumber(input.vwap, 2));
    setText(
      "spxPriorityPriorRange",
      asNum(input.priorDayHigh) !== null && asNum(input.priorDayLow) !== null
        ? `${formatNumber(input.priorDayLow, 2)} - ${formatNumber(input.priorDayHigh, 2)}`
        : "—"
    );
    setText("spxPriorityNetGammaValue", formatNetGamma(input.netGamma));
    setText("spxPriorityCallGammaPerPoint", asNum(input.callWallGammaPerPoint) === null ? "—" : formatNetGamma(input.callWallGammaPerPoint));
    setText("spxPriorityPutGammaPerPoint", asNum(input.putWallGammaPerPoint) === null ? "—" : formatNetGamma(input.putWallGammaPerPoint));
    const tickTimeRaw =
      (base.spx_quote || {}).as_of
      || (base.spx_quote || {}).asof
      || base.updated_at
      || base.server_ts
      || base.market_now_iso
      || null;
    setText(
      "spxPriorityTickPing",
      buildTickPingLabel(
        tickTimeRaw,
        (base.spx_quote || {}).provider || (base.spx_quote || {}).data_reason
      )
    );
    updateStateChip(document.getElementById("spxPriorityStateChip"), spxState);
    setText("spxPriorityFreshness", formatEtLabel(tickTimeRaw));
    setText(
      "spxPriorityFreshnessLine",
      `${formatEtLabel(tickTimeRaw)}${spxQuote.provider ? ` · ${spxQuote.provider}` : ""}`
    );
    setText("spxPriorityChangeLine", `${formatSigned(spxAbsChange, 2)} · ${formatSigned(spxQuote.change_pct, 2)}%`);
    setText("spxPriorityRangeLine", `Range: ${formatRange(spxPoints)}`);
    updateSparkNode(document.querySelector("#spxPriorityCard .marketMiniSparkWrap"), spxPoints, sparkTone(spxQuote.change_pct));
    applyGlowState([shell, spotPanel], spxQuote.change_pct);
    setText("marketPulseFetchedAt", formatEtLabel(base.updated_at || base.server_ts || base.market_now_iso));

    setText("spxPriorityExpectedMoveHighDist", asNum(derived.distanceToExpectedMoveHigh) === null ? "—" : `${formatNumber(derived.distanceToExpectedMoveHigh, 1)} pts`);
    setText("spxPriorityExpectedMoveLowDist", asNum(derived.distanceToExpectedMoveLow) === null ? "—" : `${formatNumber(derived.distanceToExpectedMoveLow, 1)} pts`);
    setText("spxPriorityTrap", String(derived.trapZoneState || "unavailable").replace(/_/g, " "));

    setBullets("spxPriorityNarrative", bullets);
    setBadges("spxPriorityWarningBadges", badges);
  };

  const getJson = (id) => {
    const node = document.getElementById(id);
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "null");
    } catch (_err) {
      return null;
    }
  };

  const base = getJson("spxPriorityBasePayload") || {};
  let current = JSON.parse(JSON.stringify(base));
  render(current);
  dispatchStreamStatus("Live stream connecting", "Waiting for first tick…");

  const connectStream = () => {
    const stream = new EventSource("/stream/market");
    stream.onopen = () => {
      dispatchStreamStatus("Live stream connected", "Listening for fresh ticks…");
    };
    stream.onmessage = (event) => {
      if (!event || !event.data) return;
      let payload = null;
      try {
        payload = JSON.parse(event.data);
      } catch (_err) {
        return;
      }

      const prices = payload && typeof payload === "object" ? (payload.prices || {}) : {};
      const seriesPoints = payload && typeof payload === "object" ? (payload.series_points || {}) : {};
      const gamma = payload && typeof payload === "object" ? (payload.gamma_map || {}) : {};
      const spxTick = prices.SPX || prices["^GSPC"] || null;
      const vixTick = prices.VIX || prices["^VIX"] || null;

      const nextSpx = { ...(current.spx_quote || {}) };
      if (spxTick && typeof spxTick === "object") {
        const tickPrice = asNum(spxTick.price);
        if (tickPrice !== null) nextSpx.price = tickPrice;
        const tickPct = asNum(spxTick.pct_change);
        if (tickPct !== null) nextSpx.change_pct = tickPct;
        if (typeof spxTick.as_of === "string" && spxTick.as_of) nextSpx.as_of = spxTick.as_of;
        if (typeof spxTick.as_of === "string" && spxTick.as_of) nextSpx.asof = spxTick.as_of;
        if (typeof spxTick.provider === "string") nextSpx.provider = spxTick.provider;
        if (typeof spxTick.reason === "string") nextSpx.data_reason = spxTick.reason;
      }

      const nextVix = { ...(current.vix_quote || {}) };
      if (vixTick && typeof vixTick === "object") {
        const vixPrice = asNum(vixTick.price);
        if (vixPrice !== null) nextVix.price = vixPrice;
        const vixPct = asNum(vixTick.pct_change);
        if (vixPct !== null) nextVix.change_pct = vixPct;
        if (typeof vixTick.provider === "string") nextVix.provider = vixTick.provider;
        if (typeof vixTick.reason === "string") nextVix.data_reason = vixTick.reason;
      }

      current = {
        ...(current || {}),
        spx_quote: nextSpx,
        vix_quote: nextVix,
        market_now_iso: new Date().toISOString(),
        updated_at: payload.updated_at || (current || {}).updated_at || null,
        server_ts: payload.server_ts || null,
        series_points: {
          ...((current || {}).series_points || {}),
          ...(seriesPoints || {}),
        },
        gamma_snapshot: {
          ...(current.gamma_snapshot || {}),
          ...(gamma || {}),
        },
      };
      render(current);
      tapeCards.forEach((card) => {
        const symbol = String(card.dataset.symbol || "").toUpperCase();
        if (!symbol) return;
        applyTapeCardUpdate(card, prices[symbol] || {}, seriesPoints[symbol] || []);
      });
      updateTapeSummary(prices);
      dispatchStreamStatus(
        "Live stream on",
        buildTickPingLabel(
          (spxTick || {}).as_of || payload.updated_at || payload.server_ts || new Date().toISOString(),
          (spxTick || {}).provider || ""
        )
      );
    };

    stream.onerror = () => {
      dispatchStreamStatus("Live stream retrying", "Connection dropped. Reconnecting to market feed…");
      try {
        stream.close();
      } catch (_err) {
        // no-op
      }
      window.setTimeout(connectStream, 3000);
    };
  };

  connectStream();
})();
