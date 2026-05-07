(() => {
  "use strict";

  const asNum = (value) => {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
  const LOCAL_FLIP_NONE_LABEL = "No Local Flip between Put Wall and Call Wall";
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

  const localFlipFromSnapshot = (gammaSnapshot, fallback = null) => {
    const snapshot = gammaSnapshot && typeof gammaSnapshot === "object" ? gammaSnapshot : {};
    if (Object.prototype.hasOwnProperty.call(snapshot, "local_flip_aggregated_gamma")) {
      return asNum(snapshot.local_flip_aggregated_gamma);
    }
    if (Object.prototype.hasOwnProperty.call(snapshot, "local_flip")) {
      return asNum(snapshot.local_flip);
    }
    return fallback;
  };

  const localFlipMissingInBand = (gammaSnapshot) => {
    const snapshot = gammaSnapshot && typeof gammaSnapshot === "object" ? gammaSnapshot : {};
    const status = String(snapshot.snapshot_status || "").toLowerCase();
    return ["healthy", "degraded", "stale"].includes(status) && snapshot.local_flip_found === false;
  };

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
    const localFlip = asNum(input.localFlip);
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
    const distanceToLocalFlip = spot !== null && localFlip !== null ? spot - localFlip : null;
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
      distanceToLocalFlip,
      distanceToCallWall,
      distanceToPutWall,
      wallSpread,
      distanceToExpectedMoveHigh,
      distanceToExpectedMoveLow,
      aboveOrBelowFlip: distanceToFlip === null ? "unavailable" : (distanceToFlip > 0 ? "above" : distanceToFlip < 0 ? "below" : "at"),
      aboveOrBelowLocalFlip: distanceToLocalFlip === null ? "unavailable" : (distanceToLocalFlip > 0 ? "above" : distanceToLocalFlip < 0 ? "below" : "at"),
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

  function bestInvalidationLevel(levels) {
    const usable = (Array.isArray(levels) ? levels : [])
      .filter((row) => row && asNum(row.value) !== null);
    if (!usable.length) return null;
    usable.sort((a, b) => Math.abs(asNum(a.distance) || 999) - Math.abs(asNum(b.distance) || 999));
    return usable[0];
  }

  function buildLiquidityPath(spot, levels) {
    const s = asNum(spot);
    const usable = (Array.isArray(levels) ? levels : [])
      .map((row) => ({ ...row, value: asNum(row.value) }))
      .filter((row) => row.value !== null);
    if (s === null || !usable.length) return "Await the next clean interaction.";
    const higher = usable.filter((row) => row.value >= s).sort((a, b) => a.value - b.value);
    const lower = usable.filter((row) => row.value <= s).sort((a, b) => b.value - a.value);
    const upPath = higher.slice(0, 2).map((row) => `${row.label} ${formatNumber(row.value, 0)}`).join(" -> ");
    const downPath = lower.slice(0, 2).map((row) => `${row.label} ${formatNumber(row.value, 0)}`).join(" -> ");
    if (upPath && downPath) return `${downPath} | ${upPath}`;
    return upPath || downPath || "Await the next clean interaction.";
  }

  function buildTriggerState(trigger, tone) {
    const triggerText = String(trigger || "").toLowerCase();
    if (tone === "warn") {
      return {
        label: "Blocked until edge",
        line: "No valid 5m trigger matters until price reaches a real boundary.",
      };
    }
    if (triggerText.includes("bearish")) {
      return {
        label: "Armed for bearish confirmation",
        line: "Only actionable if the 5m reversal or continuation confirms at the level.",
      };
    }
    if (triggerText.includes("bullish")) {
      return {
        label: "Armed for bullish confirmation",
        line: "Only actionable if the 5m reversal or continuation confirms at the level.",
      };
    }
    if (triggerText.includes("wait")) {
      return {
        label: "Waiting for clean confirmation",
        line: "The structure is mapped, but the entry has not earned itself yet.",
      };
    }
    if (triggerText.includes("stand down")) {
      return {
        label: "Stand down",
        line: "No trigger quality right now.",
      };
    }
    return {
      label: "Context only",
      line: "Use the level interaction first, then let the 5m chart confirm.",
    };
  }

  function buildExecutionPlan(input, derived) {
    const spot = asNum(input.spot);
    const gammaFlip = asNum(input.gammaFlip);
    const localFlip = asNum(input.localFlip);
    const callWall = asNum(input.callWall);
    const putWall = asNum(input.putWall);
    const nextCallWall = asNum(derived.nextCallWall);
    const nextPutWall = asNum(derived.nextPutWall);
    const nearCall = (abs(derived.distanceToCallWall) || 999) <= 8;
    const nearPut = (abs(derived.distanceToPutWall) || 999) <= 8;
    const nearFlip = (abs(derived.distanceToFlip) || 999) <= 6;
    const nearLocalFlip = (abs(derived.distanceToLocalFlip) || 999) <= 6;
    const aboveFlip = derived.aboveOrBelowFlip === "above";
    const belowFlip = derived.aboveOrBelowFlip === "below";
    const aboveLocalFlip = derived.aboveOrBelowLocalFlip === "above";
    const belowLocalFlip = derived.aboveOrBelowLocalFlip === "below";
    const bearishExpansion = derived.dealerRegime === "Negative Gamma / Momentum Amplifying";
    const levelStack = [
      { label: "Put Wall", value: putWall, distance: derived.distanceToPutWall },
      { label: "Gamma Flip", value: gammaFlip, distance: derived.distanceToFlip },
      { label: "Local Flip", value: localFlip, distance: derived.distanceToLocalFlip },
      { label: "Call Wall", value: callWall, distance: derived.distanceToCallWall },
      { label: "Next Call", value: nextCallWall, distance: spot !== null && nextCallWall !== null ? spot - nextCallWall : null },
      { label: "Next Put", value: nextPutWall, distance: spot !== null && nextPutWall !== null ? spot - nextPutWall : null },
      { label: "Day Open", value: asNum(input.dayOpen), distance: spot !== null && asNum(input.dayOpen) !== null ? spot - asNum(input.dayOpen) : null },
      { label: "VWAP", value: asNum(input.vwap), distance: spot !== null && asNum(input.vwap) !== null ? spot - asNum(input.vwap) : null },
    ];

    if (derived.noTradeCenter) {
      return {
        tone: "warn",
        headline: "No trade in the center.",
        subline: `Compressed between ${formatNumber(putWall, 0)} and ${formatNumber(callWall, 0)} near gamma flip.`,
        location: "Inside the no-trade center",
        locationLine: "Price is too close to the flip/wall cluster to force an intraday read.",
        bias: "Neutral / responsive",
        biasLine: "Do not pick a side until price reaches a real liquidity boundary.",
        trigger: "Stand down",
        triggerLine: "Wait for a 5m Strat trigger only after price tags put wall, call wall, or cleanly accepts away from the flip.",
        target: "Nearest edge first",
        targetLine: `Watch for movement toward ${formatNumber(putWall, 0)} or ${formatNumber(callWall, 0)} before building a path.`,
        invalidation: "No thesis yet",
        invalidationLine: "If you cannot define the edge first, there is no valid entry.",
        plan: "Wait for wall interaction first.",
        doThis: "Wait for a real edge first.",
        doThisLine: "Reassess only after put wall or call wall interaction.",
        avoidThis: "Do not trade the center.",
        avoidThisLine: "Mid-range triggers do not count.",
      };
    }

    if (nearCall && derived.reversalSetupFit.label !== "Poor") {
      return {
        tone: "negative",
        headline: "Call wall test. Fade only if the 5m trigger confirms.",
        subline: bearishExpansion
          ? "Negative gamma can still squeeze overhead. Require rejection before acting."
          : "This is only a reversal setup if price cannot accept above the wall.",
        location: `At Call Wall ${formatNumber(callWall, 0)}`,
        locationLine: `Spot is ${formatLevelDistance(derived.distanceToCallWall)} from the ceiling.`,
        bias: "Responsive short bias",
        biasLine: "Only trade the fade if price rejects the wall and loses micro structure.",
        trigger: "5m bearish Strat",
        triggerLine: "Wait for a 2-1-2 down or 3-1-2 reversal back under the wall.",
        target: gammaFlip !== null ? `Gamma Flip ${formatNumber(gammaFlip, 0)}` : "Back to the flip",
        targetLine: buildLiquidityPath(spot, [
          { label: "Call Wall", value: callWall },
          { label: "Gamma Flip", value: gammaFlip },
          { label: "Put Wall", value: putWall },
        ]),
        invalidation: nextCallWall !== null ? `Acceptance above ${formatNumber(nextCallWall, 0)}` : `Acceptance above ${formatNumber(callWall, 0)}`,
        invalidationLine: "If price holds above the wall, the fade thesis is wrong.",
        plan: "Reversal only. No blind shorting into a wall without rejection.",
        doThis: "Let the wall reject first, then short the confirmed 5m reversal.",
        doThisLine: "You want the failure under resistance, not a front-run into strength.",
        avoidThis: "Avoid fading a clean acceptance above the wall.",
        avoidThisLine: "If price is holding above the wall, the short idea is dead.",
      };
    }

    if (nearPut && derived.reversalSetupFit.label !== "Poor") {
      return {
        tone: "positive",
        headline: "Put wall support. Buy only if the 5m trigger confirms.",
        subline: bearishExpansion
          ? "Negative gamma can still flush support. Require hold/reclaim before acting."
          : "This is only a reversal setup if price respects the wall and reclaims structure.",
        location: `At Put Wall ${formatNumber(putWall, 0)}`,
        locationLine: `Spot is ${formatLevelDistance(derived.distanceToPutWall)} from the floor.`,
        bias: "Responsive long bias",
        biasLine: "Only trade the bounce if support holds and price reclaims the level cleanly.",
        trigger: "5m bullish Strat",
        triggerLine: "Wait for a 2-1-2 up or 3-1-2 reversal off the wall.",
        target: gammaFlip !== null ? `Gamma Flip ${formatNumber(gammaFlip, 0)}` : "Back to the flip",
        targetLine: buildLiquidityPath(spot, [
          { label: "Put Wall", value: putWall },
          { label: "Gamma Flip", value: gammaFlip },
          { label: "Call Wall", value: callWall },
        ]),
        invalidation: nextPutWall !== null ? `Acceptance below ${formatNumber(nextPutWall, 0)}` : `Acceptance below ${formatNumber(putWall, 0)}`,
        invalidationLine: "If price loses the wall and cannot reclaim it, the bounce thesis is wrong.",
        plan: "Support reclaim only. No catching a falling knife under support.",
        doThis: "Wait for the hold or reclaim, then buy the confirmed 5m reversal.",
        doThisLine: "You want proof that support is working before the long exists.",
        avoidThis: "Avoid catching breakdowns under support.",
        avoidThisLine: "If price accepts below the wall, the bounce setup is gone.",
      };
    }

    if (aboveFlip && !nearFlip) {
      return {
        tone: "positive",
        headline: aboveLocalFlip ? "Above both flips. Momentum long is the cleaner plan." : "Above main flip. Reclaim local to strengthen longs.",
        subline: aboveLocalFlip
          ? "Structure favors continuation if price holds above gamma and local control pivots."
          : "Main structure is supportive, but local control is not fully reclaimed yet.",
        location: gammaFlip !== null ? `Above Gamma Flip ${formatNumber(gammaFlip, 0)}` : "Above core support",
        locationLine: `Spot is ${formatLevelDistance(derived.distanceToFlip)} from the flip with ${derived.structureType.toLowerCase()} structure.`,
        bias: "Continuation long",
        biasLine: bearishExpansion
          ? "Breaks can run harder in negative gamma. Respect speed and avoid late entries."
          : aboveLocalFlip
            ? "Positive gamma and local control both support cleaner continuation."
            : "Positive gamma is supportive, but local reclaim still matters.",
        trigger: aboveLocalFlip ? "5m bullish Strat" : "Reclaim local flip first",
        triggerLine: aboveLocalFlip
          ? "Wait for a 2-1-2 up / 3-1-2 continuation after hold or reclaim above the flip."
          : "Wait for price to hold back above local flip before treating dips as cleaner longs.",
        target: callWall !== null && (spot === null || spot < callWall) ? `Call Wall ${formatNumber(callWall, 0)}` : `Next Call ${formatNumber(nextCallWall, 0)}`,
        targetLine: buildLiquidityPath(spot, [
          { label: "Gamma Flip", value: gammaFlip },
          { label: "Call Wall", value: callWall },
          { label: "Next Call", value: nextCallWall },
        ]),
        invalidation: gammaFlip !== null ? `Lose ${formatNumber(gammaFlip, 0)}` : "Lose the reclaim",
        invalidationLine: "If price slips back through the flip and cannot reclaim it, reset the long thesis.",
        plan: "Longs only after the 5m trigger confirms acceptance above the working level.",
        doThis: "Buy continuation only after the 5m bullish confirmation above structure.",
        doThisLine: "Hold/reclaim above the flip first, then trade the move toward the next liquidity shelf.",
        avoidThis: "Avoid chasing extended upside away from the level.",
        avoidThisLine: "If the entry is late and the invalidation is wide, skip it.",
      };
    }

    if (belowFlip && !nearFlip) {
      return {
        tone: "negative",
        headline: belowLocalFlip ? "Below both flips. Momentum short is the cleaner plan." : "Below main flip. Local reclaim can still bounce.",
        subline: belowLocalFlip
          ? "Structure favors continuation lower while price stays below both gamma and intraday control."
          : "Main structure is weak, but local reclaim can still produce responsive bounces.",
        location: gammaFlip !== null ? `Below Gamma Flip ${formatNumber(gammaFlip, 0)}` : "Below core resistance",
        locationLine: `Spot is ${formatLevelDistance(derived.distanceToFlip)} from the flip with ${derived.structureType.toLowerCase()} structure.`,
        bias: "Continuation short",
        biasLine: bearishExpansion
          ? "Negative gamma supports faster downside extension after accepted breaks."
          : belowLocalFlip
            ? "Below the flip and local control, failed bounces deserve more respect than impulsive longs."
            : "Below the main flip, but local reclaim can still create responsive long bounces.",
        trigger: belowLocalFlip ? "5m bearish Strat" : "Watch local reclaim first",
        triggerLine: belowLocalFlip
          ? "Wait for a 2-1-2 down / 3-1-2 continuation after failure under the flip."
          : "If price reclaims local flip while staying below main flip, treat it as bounce risk first.",
        target: putWall !== null && (spot === null || spot > putWall) ? `Put Wall ${formatNumber(putWall, 0)}` : `Next Put ${formatNumber(nextPutWall, 0)}`,
        targetLine: buildLiquidityPath(spot, [
          { label: "Gamma Flip", value: gammaFlip },
          { label: "Put Wall", value: putWall },
          { label: "Next Put", value: nextPutWall },
        ]),
        invalidation: gammaFlip !== null ? `Reclaim ${formatNumber(gammaFlip, 0)}` : "Reclaim the failed level",
        invalidationLine: "If price reclaims the flip and holds, the short thesis is wrong.",
        plan: "Shorts only after the 5m trigger confirms rejection under the working level.",
        doThis: "Short continuation only after the 5m bearish confirmation under structure.",
        doThisLine: "Let the failed bounce prove itself, then trade toward the next lower liquidity pocket.",
        avoidThis: "Avoid pressing shorts into support without confirmation.",
        avoidThisLine: "If price reclaims the flip, the downside thesis is wrong.",
      };
    }

    const invalidation = bestInvalidationLevel(levelStack);
    return {
      tone: "neutral",
      headline: "Responsive only. Let the level decide the trade.",
      subline: "The page has context, but structure is still mixed. Wait for a cleaner interaction before committing.",
      location: nearFlip ? "At the flip" : "Between major levels",
      locationLine: nearFlip
        ? "Price is sitting too close to gamma flip for blind momentum."
        : buildLiquidityPath(spot, [
            { label: "Put Wall", value: putWall },
            { label: "Gamma Flip", value: gammaFlip },
            { label: "Call Wall", value: callWall },
          ]),
      bias: "Two-way",
      biasLine: "Trade the reaction, not a prediction. Let the next level interaction choose the side.",
      trigger: "Wait for 5m confirmation",
      triggerLine: "Bullish only on acceptance above structure. Bearish only on rejection below it.",
      target: "Nearest liquidity",
      targetLine: buildLiquidityPath(spot, [
        { label: "Put Wall", value: putWall },
        { label: "Gamma Flip", value: gammaFlip },
        { label: "Call Wall", value: callWall },
      ]),
      invalidation: invalidation ? `${invalidation.label} ${formatNumber(invalidation.value, 0)}` : "Reset if thesis breaks",
      invalidationLine: "If the level you are trading off stops mattering, the trade is over.",
      plan: "No forcing. Wait for price to interact with a wall or cleanly accept away from the flip first.",
      doThis: "Stay responsive and let the next touched level pick the side.",
      doThisLine: "The next good trade comes from interaction, not prediction.",
      avoidThis: "Avoid pre-committing in mixed structure.",
      avoidThisLine: "No entry deserves size until the board simplifies.",
    };
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
    buildExecutionPlan,
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
  const uiFX = typeof window !== "undefined" ? window.mcUIFX : null;
  const LIVE_VALUE_IDS = new Set([
    "marketPulseHeroSpot",
    "spxPrioritySpot",
    "spxPriorityChangeLine",
  ]);
  const STATUS_IDS = new Set([
    "marketPulseHeroStateChip",
    "marketPulseHeroStateContext",
    "spxPriorityStateChip",
  ]);

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
    const previous = String(node.textContent || "");
    if (previous === next) return;
    node.textContent = next;
    if (LIVE_VALUE_IDS.has(id)) {
      const prevNumeric = Number(previous.replace(/[^0-9+\-.]/g, ""));
      const nextNumeric = Number(next.replace(/[^0-9+\-.]/g, ""));
      const direction = Number.isFinite(prevNumeric) && Number.isFinite(nextNumeric)
        ? nextNumeric > prevNumeric ? "up" : nextNumeric < prevNumeric ? "down" : "neutral"
        : "neutral";
      uiFX?.flashValue?.(node, direction, { essential: true });
      return;
    }
    if (STATUS_IDS.has(id)) {
      uiFX?.pulseNode?.(node, "info");
    }
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
  const mobilePulseQuery = window.matchMedia("(max-width: 640px)");
  const scrollRoot = document.scrollingElement || document.documentElement;
  const DESKTOP_STREAM_FLUSH_MS = 500;
  const MOBILE_STREAM_FLUSH_MS = 1200;
  const RESIZE_RENDER_DEBOUNCE_MS = 180;
  let pendingStreamPayload = null;
  let pendingStreamTimer = null;
  let pendingRenderTimer = null;
  let lastStreamApplyAt = 0;
  let pageVisible = document.visibilityState !== "hidden";
  let stream = null;
  let reconnectTimer = null;

  const dispatchStreamStatus = (status, detail) => {
    window.dispatchEvent(new CustomEvent("market-pulse-stream-status", { detail: { status, detail } }));
  };

  const scheduleRender = (delay = 0) => {
    if (!pageVisible) return;
    if (pendingRenderTimer !== null) {
      window.clearTimeout(pendingRenderTimer);
    }
    pendingRenderTimer = window.setTimeout(() => {
      pendingRenderTimer = null;
      window.requestAnimationFrame(() => render(current));
    }, Math.max(0, delay));
  };

  const preserveMobileScroll = (beforeBottomGap) => {
    if (!mobilePulseQuery.matches || beforeBottomGap === null) return;
    window.requestAnimationFrame(() => {
      const currentTop = scrollRoot.scrollTop;
      const currentBottomGap = scrollRoot.scrollHeight - (currentTop + window.innerHeight);
      if (beforeBottomGap > 140 && currentBottomGap > 140) return;
      const nextTop = Math.max(0, scrollRoot.scrollHeight - window.innerHeight - Math.max(0, beforeBottomGap));
      if (Math.abs(currentTop - nextTop) > 2) {
        scrollRoot.scrollTop = nextTop;
      }
    });
  };

  const applyStreamPayload = (payload) => {
    if (!payload || typeof payload !== "object") return;
    const beforeBottomGap = mobilePulseQuery.matches
      ? Math.max(0, scrollRoot.scrollHeight - (scrollRoot.scrollTop + window.innerHeight))
      : null;

    const prices = payload.prices || {};
    const seriesPoints = payload.series_points || {};
    const gamma = payload.gamma_map || {};
    const activeTicker = String(((current || {}).ticker) || "QQQ").toUpperCase();
    const playbookTick = prices[activeTicker] || null;
    const vixTick = prices.VIX || prices["^VIX"] || null;

    const nextQuotesMap = {
      ...((current || {}).quotes_map || {}),
    };
    Object.entries(prices || {}).forEach(([symbol, tick]) => {
      const key = String(symbol || "").toUpperCase();
      if (!key || !tick || typeof tick !== "object") return;
      const existing = { ...(nextQuotesMap[key] || {}) };
      const nextQuote = { ...existing };
      const tickPrice = asNum(tick.price);
      const tickPct = asNum(tick.pct_change ?? tick.change_pct);
      const tickChange = asNum(tick.change);
      if (tickPrice !== null) nextQuote.price = tickPrice;
      if (tickPct !== null) nextQuote.change_pct = tickPct;
      if (tickChange !== null) nextQuote.change = tickChange;
      if (typeof tick.as_of === "string" && tick.as_of) {
        nextQuote.as_of = tick.as_of;
        nextQuote.asof = tick.as_of;
      }
      if (typeof tick.provider === "string" && tick.provider) nextQuote.provider = tick.provider;
      if (typeof tick.reason === "string" && tick.reason) {
        nextQuote.reason = tick.reason;
        nextQuote.data_reason = tick.reason;
      }
      nextQuote.symbol = String(nextQuote.symbol || key).toUpperCase();
      nextQuote.label = String(nextQuote.label || nextQuote.symbol || key).toUpperCase();
      nextQuote.data_state = deriveDataState(nextQuote);
      nextQuote.data_status_label = dataStateLabel(nextQuote.data_state);
      nextQuote.source_badge_label = sourceBadgeLabel(nextQuote);
      nextQuotesMap[key] = nextQuote;
    });

    const nextPlaybookQuote = { ...((current.playbook_quote || current.spx_quote) || {}) };
    if (playbookTick && typeof playbookTick === "object") {
      const tickPrice = asNum(playbookTick.price);
      if (tickPrice !== null) nextPlaybookQuote.price = tickPrice;
      const tickPct = asNum(playbookTick.pct_change);
      if (tickPct !== null) nextPlaybookQuote.change_pct = tickPct;
      const tickVwap = asNum(playbookTick.vwap);
      if (tickVwap !== null) nextPlaybookQuote.vwap = tickVwap;
      if (typeof playbookTick.as_of === "string" && playbookTick.as_of) nextPlaybookQuote.as_of = playbookTick.as_of;
      if (typeof playbookTick.as_of === "string" && playbookTick.as_of) nextPlaybookQuote.asof = playbookTick.as_of;
      if (typeof playbookTick.provider === "string") nextPlaybookQuote.provider = playbookTick.provider;
      if (typeof playbookTick.reason === "string") {
        nextPlaybookQuote.reason = playbookTick.reason;
        nextPlaybookQuote.data_reason = playbookTick.reason;
      }
      nextPlaybookQuote.symbol = activeTicker;
      nextPlaybookQuote.label = activeTicker;
      nextPlaybookQuote.data_state = deriveDataState(nextPlaybookQuote);
      nextPlaybookQuote.data_status_label = dataStateLabel(nextPlaybookQuote.data_state);
      nextPlaybookQuote.source_badge_label = sourceBadgeLabel(nextPlaybookQuote);
    }

    const nextVix = { ...(current.vix_quote || {}) };
    if (vixTick && typeof vixTick === "object") {
      const vixPrice = asNum(vixTick.price);
      if (vixPrice !== null) nextVix.price = vixPrice;
      const vixPct = asNum(vixTick.pct_change);
      if (vixPct !== null) nextVix.change_pct = vixPct;
      if (typeof vixTick.provider === "string") nextVix.provider = vixTick.provider;
      if (typeof vixTick.reason === "string") {
        nextVix.reason = vixTick.reason;
        nextVix.data_reason = vixTick.reason;
      }
      nextVix.data_state = deriveDataState(nextVix);
      nextVix.data_status_label = dataStateLabel(nextVix.data_state);
      nextVix.source_badge_label = sourceBadgeLabel(nextVix);
    }

    const nextGammaSnapshot = {
      ...(current.gamma_snapshot || {}),
      ...(gamma || {}),
    };
    current = {
      ...(current || {}),
      playbook_quote: nextPlaybookQuote,
      spx_quote: nextPlaybookQuote,
      vix_quote: nextVix,
      market_now_iso: new Date().toISOString(),
      updated_at: payload.updated_at || (current || {}).updated_at || null,
      server_ts: payload.server_ts || null,
      series_points: {
        ...((current || {}).series_points || {}),
        ...(seriesPoints || {}),
      },
      quotes_map: nextQuotesMap,
      gamma_snapshot: nextGammaSnapshot,
      execution_model: patchExecutionModelForStream(
        payload.execution_model || current.execution_model,
        nextPlaybookQuote,
        nextGammaSnapshot
      ),
    };
    scheduleRender();
    tapeCards.forEach((card) => {
      const symbol = String(card.dataset.symbol || "").toUpperCase();
      if (!symbol) return;
      applyTapeCardUpdate(
        card,
        nextQuotesMap[symbol] || {},
        seriesPoints[symbol] || (((current || {}).series_points || {})[symbol]) || []
      );
    });
    updateTapeSummary(nextQuotesMap);
    dispatchStreamStatus(
      "Live stream on",
      buildTickPingLabel(
        (spxTick || {}).as_of || payload.updated_at || payload.server_ts || new Date().toISOString(),
        (spxTick || {}).provider || ""
      )
    );
    window.dispatchEvent(new CustomEvent("market-pulse-stream-payload", { detail: payload }));
    preserveMobileScroll(beforeBottomGap);
    lastStreamApplyAt = Date.now();
  };

  const queueStreamPayload = (payload) => {
    const flushMs = mobilePulseQuery.matches ? MOBILE_STREAM_FLUSH_MS : DESKTOP_STREAM_FLUSH_MS;
    if (!pageVisible) {
      pendingStreamPayload = payload;
      return;
    }
    if (flushMs <= 0) {
      applyStreamPayload(payload);
      return;
    }
    pendingStreamPayload = payload;
    const elapsed = Date.now() - lastStreamApplyAt;
    if (elapsed >= flushMs && pendingStreamTimer === null) {
      const nextPayload = pendingStreamPayload;
      pendingStreamPayload = null;
      applyStreamPayload(nextPayload);
      return;
    }
    if (pendingStreamTimer !== null) return;
    pendingStreamTimer = window.setTimeout(() => {
      pendingStreamTimer = null;
      const nextPayload = pendingStreamPayload;
      pendingStreamPayload = null;
      applyStreamPayload(nextPayload);
    }, Math.max(0, flushMs - elapsed));
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

  const seriesValueCount = (points) => (
    (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : asNum(row)))
      .filter((value) => value !== null)
      .length
  );

  const pickBestSeries = (...candidates) => {
    let fallback = [];
    for (const candidate of candidates) {
      if (!Array.isArray(candidate)) continue;
      const count = seriesValueCount(candidate);
      if (count >= 2) return candidate;
      if (!fallback.length && count >= 1) fallback = candidate;
    }
    return fallback;
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

  const deriveSessionPhase = (iso) => {
    const source = iso ? new Date(iso) : new Date();
    if (Number.isNaN(source.getTime())) return "closed";
    const weekday = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      weekday: "short",
    }).format(source);
    if (weekday === "Sat" || weekday === "Sun") return "closed";
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
    }).formatToParts(source);
    const hh = Number((parts.find((p) => p.type === "hour") || {}).value || "0");
    const mm = Number((parts.find((p) => p.type === "minute") || {}).value || "0");
    const mins = hh * 60 + mm;
    if (mins >= 570 && mins < 960) return "open";
    if (mins >= 240 && mins < 570) return "premarket";
    if (mins >= 960 && mins < 1200) return "afterhours";
    return "closed";
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

  const tapeStateFor = (symbol, pctChange) => {
    const ticker = String(symbol || "").toUpperCase();
    const pct = asNum(pctChange);
    if (["SPX", "SPY", "QQQ", "IWM"].includes(ticker)) {
      if (pct !== null && pct >= 0.35) {
        return { label: "Risk-On", tone: "positive", title: "Broad tape supports long risk." };
      }
      if (pct !== null && pct <= -0.35) {
        return {
          label: "Risk-Off",
          tone: "negative",
          title: "Broad tape is defensive; long risk needs extra confirmation.",
        };
      }
    }
    if (pct !== null && pct >= 0.75) {
      return {
        label: "Strong",
        tone: "positive",
        title: "Symbol is leading or showing strong upside pressure.",
      };
    }
    if (pct !== null && pct <= -0.75) {
      return {
        label: "Weak",
        tone: "negative",
        title: "Symbol is lagging or under downside pressure.",
      };
    }
    return { label: "Mixed", tone: "neutral", title: "No clean tape edge yet." };
  };

  const buildSparklineSvg = (points, tone) => {
    const values = (Array.isArray(points) ? points : [])
      .map((row) => (row && typeof row === "object" ? asNum(row.v) : asNum(row)))
      .filter((value) => value !== null);
    if (values.length < 4) {
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
    const areaPoints = `0.00,28.00 ${pts.join(" ")} 120.00,28.00`;
    const baselineY = (((maxV - values[0]) / (maxV - minV)) * (height - 2) + 1).toFixed(2);
    const markerStride = Math.max(1, Math.floor(pts.length / 8));
    const selected = pts
      .filter((_point, index) => index === 0 || index === pts.length - 1 || index % markerStride === 0);
    const markers = selected
      .map((point, index) => {
        const [x, y] = point.split(",");
        const endpoint = index === 0 ? " start" : index === selected.length - 1 ? " end" : "";
        return `<circle class="marketMiniSparkPoint ${tone}${endpoint}" cx="${x}" cy="${y}" r="1.55" />`;
      })
      .join("");
    return (
      `<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true">`
      + `<line class="marketMiniSparkGuide" x1="0" y1="7" x2="120" y2="7" />`
      + `<line class="marketMiniSparkGuide marketMiniSparkBaseline" x1="0" y1="${baselineY}" x2="120" y2="${baselineY}" />`
      + `<line class="marketMiniSparkGuide" x1="0" y1="21" x2="120" y2="21" />`
      + `<polygon class="marketMiniSparkArea ${tone}" points="${areaPoints}" />`
      + `<polyline class="marketMiniSparkLine ${tone}" points="${pts.join(" ")}" />`
      + markers
      + `</svg>`
    );
  };

  const deriveDataState = (quote) => {
    const provider = String((quote || {}).provider || "").toLowerCase();
    const reason = String((quote || {}).reason || (quote || {}).data_reason || "").toLowerCase();
    const explicit = String((quote || {}).data_state || "").trim().toLowerCase();
    if (!provider && !reason && ["live", "delayed", "cached", "missing"].includes(explicit)) return explicit;
    const price = asNum((quote || {}).price);
    if (price === null) return "missing";
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

  const sourceBadgeLabel = (quote) => {
    const provider = String((quote || {}).provider || "").toLowerCase();
    const reason = String((quote || {}).reason || (quote || {}).data_reason || "").toLowerCase();
    const explicit = String((quote || {}).source_badge_label || "").trim();
    if (!provider && !reason && explicit) return explicit;
    if (provider === "tradier" && reason.startsWith("tradier_stream_")) return "Tradier Stream";
    if (provider === "tradier" && reason.startsWith("tradier_live")) return "Tradier Live Quote";
    if (provider === "tradier" && reason.startsWith("tradier_close")) return "Tradier Close";
    if (provider === "massive") return "Massive Fallback";
    if (provider === "yfinance") return "Yahoo Fallback";
    if (provider) return `${provider[0].toUpperCase()}${provider.slice(1)} Fallback`;
    return "Feed unavailable";
  };

  const compactAgeLabel = (ageS) => {
    const seconds = Math.max(0, Math.floor(Number(ageS) || 0));
    if (seconds >= 72 * 3600) return "72h+ old";
    if (seconds >= 3600) return `${Math.floor(seconds / 3600)}h old`;
    if (seconds >= 60) return `${Math.floor(seconds / 60)}m old`;
    return `${seconds}s old`;
  };

  const dashboardTapeFreshnessLabel = (label, state, asOf) => {
    const stateLabel = dataStateLabel(state);
    const ts = typeof asOf === "string" ? Date.parse(asOf) : NaN;
    if (Number.isFinite(ts)) {
      const ageS = Math.max(0, (Date.now() - ts) / 1000);
      const band = ageS >= 900 ? "Critical" : stateLabel;
      return `${band} · ${compactAgeLabel(ageS)}`;
    }
    let text = String(label || "").trim();
    if (!text) return state === "live" ? "Live" : "Awaiting refresh";
    text = text
      .replace(/^Closed\s*·\s*last quote\s*/i, "Critical · ")
      .replace(/^Closed\s*·\s*last snapshot\s*/i, "Critical · ")
      .replace(/last quote\s*/i, "");
    return text.trim() || stateLabel;
  };

  const updateStateChip = (node, state, labelOverride = "") => {
    if (!node) return;
    const nextState = String(state || "missing");
    const nextLabel = String(labelOverride || dataStateLabel(nextState));
    const changed = String(node.textContent || "") !== nextLabel || !node.classList.contains(`state-${nextState}`);
    node.textContent = nextLabel;
    node.classList.remove("state-live", "state-delayed", "state-cached", "state-missing");
    node.classList.add(`state-${nextState}`);
    if (changed) {
      const tone = nextState === "live" ? "positive" : nextState === "missing" ? "negative" : "warning";
      uiFX?.pulseNode?.(node, tone);
    }
  };

  const updateTextNode = (node, value, options = {}) => {
    if (!node) return;
    const next = String(value ?? "—");
    const previous = String(node.textContent || "");
    if (previous === next) return false;
    node.textContent = next;
    if (options.live) {
      const prevNumeric = Number(previous.replace(/[^0-9+\-.]/g, ""));
      const nextNumeric = Number(next.replace(/[^0-9+\-.]/g, ""));
      const direction = Number.isFinite(prevNumeric) && Number.isFinite(nextNumeric)
        ? nextNumeric > prevNumeric ? "up" : nextNumeric < prevNumeric ? "down" : "neutral"
        : (options.direction || "neutral");
      uiFX?.flashValue?.(node, direction, { essential: true });
    } else if (options.pulse) {
      uiFX?.pulseNode?.(node, options.tone || "info");
    }
    return true;
  };

  const updateSparkNode = (node, points, tone) => {
    if (!node) return;
    node.classList.remove("spark-pos", "spark-neg", "spark-flat");
    node.classList.add(tone === "up" ? "spark-pos" : tone === "down" ? "spark-neg" : "spark-flat");
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

  const setChipTone = (id, label, tone) => {
    const node = document.getElementById(id);
    if (!node) return;
    const next = String(label || "—");
    const changed = String(node.textContent || "") !== next || (tone && !node.classList.contains(tone));
    node.textContent = next;
    node.classList.remove("positive", "negative", "warn", "critical", "neutral");
    if (tone) node.classList.add(tone);
    if (changed) {
      const pulseTone = tone === "positive" ? "positive" : tone === "negative" || tone === "critical" ? "negative" : tone === "warn" ? "warning" : "info";
      uiFX?.pulseNode?.(node, pulseTone);
    }
  };

  const gradeTradeability = (score) => {
    const n = asNum(score);
    if (n === null) return "—";
    if (n >= 90) return "A";
    if (n >= 82) return "A-";
    if (n >= 74) return "B";
    if (n >= 66) return "B-";
    if (n >= 58) return "C";
    if (n >= 48) return "C-";
    return "D";
  };

  const patchExecutionModelForStream = (model, spxQuote, gammaSnapshot) => {
    if (!model || typeof model !== "object") return model;
    const next = {
      ...model,
      levels: { ...((model && model.levels) || {}) },
      distances: { ...((model && model.distances) || {}) },
      location: { ...((model && model.location) || {}) },
    };
    const spot = asNum((spxQuote || {}).price) ?? asNum(next.levels.spot);
    const mainFlip = asNum((gammaSnapshot || {}).gamma_flip_combined_basket) ?? asNum(next.levels.main_flip);
    const localFlip = localFlipFromSnapshot(gammaSnapshot, asNum(next.levels.local_flip));
    const callWall = asNum((gammaSnapshot || {}).call_wall_aggregated_gamma) ?? asNum(next.levels.call_wall);
    const putWall = asNum((gammaSnapshot || {}).put_wall_aggregated_gamma) ?? asNum(next.levels.put_wall);

    next.levels = {
      ...next.levels,
      spot,
      main_flip: mainFlip,
      local_flip: localFlip,
      call_wall: callWall,
      put_wall: putWall,
    };

    const nextDistances = {
      to_main_flip: spot !== null && mainFlip !== null ? spot - mainFlip : null,
      to_local_flip: spot !== null && localFlip !== null ? spot - localFlip : null,
      to_call_wall: spot !== null && callWall !== null ? spot - callWall : null,
      to_put_wall: spot !== null && putWall !== null ? spot - putWall : null,
    };
    next.distances = { ...next.distances, ...nextDistances };

    const wallSpan = (() => {
      if (callWall !== null && putWall !== null) return Math.max(20, Math.abs(callWall - putWall));
      const numeric = [spot, mainFlip, localFlip, callWall, putWall].filter((value) => value !== null);
      return numeric.length >= 2 ? Math.max(20, Math.max(...numeric) - Math.min(...numeric)) : 20;
    })();

    if (Array.isArray(model.distance_rows)) {
      next.distance_rows = model.distance_rows.map((row) => {
        const key = String((row && row.key) || "");
        const signedValue =
          key === "main_flip" ? nextDistances.to_main_flip
            : key === "local_flip" ? nextDistances.to_local_flip
              : key === "call_wall" ? nextDistances.to_call_wall
                : key === "put_wall" ? nextDistances.to_put_wall
                  : null;
        return {
          ...row,
          value: signedValue === null ? null : Math.abs(signedValue),
          signed_value: signedValue,
          pct: signedValue === null ? 0 : Math.min(100, (Math.abs(signedValue) / wallSpan) * 100),
          direction: signedValue === null ? "flat" : signedValue > 0 ? "up" : signedValue < 0 ? "down" : "flat",
        };
      });
    }

    if (Array.isArray(model.ladder_rows)) {
      const valuesByKey = {
        call_wall: callWall,
        main_flip: mainFlip,
        local_flip: localFlip,
        price: spot,
        put_wall: putWall,
      };
      const signedByKey = {
        call_wall: nextDistances.to_call_wall,
        main_flip: nextDistances.to_main_flip,
        local_flip: nextDistances.to_local_flip,
        price: null,
        put_wall: nextDistances.to_put_wall,
      };
      next.ladder_rows = model.ladder_rows
        .map((row) => ({
          ...row,
          value: Object.prototype.hasOwnProperty.call(valuesByKey, String(row.key || ""))
            ? valuesByKey[String(row.key || "")]
            : asNum(row.value),
          distance_points: Object.prototype.hasOwnProperty.call(signedByKey, String(row.key || ""))
            ? signedByKey[String(row.key || "")]
            : asNum(row.distance_points),
        }))
        .filter((row) => asNum(row.value) !== null)
        .sort((a, b) => asNum(b.value) - asNum(a.value));
    }

    const nearestCandidates = [
      ["Main Flip", mainFlip],
      ["Local Flip", localFlip],
      ["Call Wall", callWall],
      ["Put Wall", putWall],
    ].filter(([, value]) => value !== null && spot !== null);
    let nearestLevelName = "";
    let nearestLevelValue = null;
    let nearestDistance = null;
    nearestCandidates.forEach(([label, value]) => {
      const distance = Math.abs(spot - value);
      if (nearestDistance === null || distance < nearestDistance) {
        nearestLevelName = label;
        nearestLevelValue = value;
        nearestDistance = distance;
      }
    });
    const insideRange = spot !== null && callWall !== null && putWall !== null && spot >= putWall && spot <= callWall;
    const localBand = Math.max(2.5, asNum(next.neutral_band_local) || 2.5);
    next.location = {
      ...next.location,
      nearest_level_name: nearestLevelName || next.location.nearest_level_name || "",
      nearest_level_value: nearestLevelValue !== null ? nearestLevelValue : next.location.nearest_level_value,
      distance_points: nearestDistance !== null ? nearestDistance : next.location.distance_points,
      inside_range: insideRange,
      midrange: Boolean(insideRange && nearestDistance !== null && nearestDistance > Math.max(localBand * 1.5, 22)),
    };

    return next;
  };

  const updateTradeReadState = (tradeability, executionPlan) => {
    const card = document.getElementById("marketPulseTradeReadCard");
    const chip = document.getElementById("marketPulseTradeabilityBadge");
    const label = String((tradeability || {}).label || "").toLowerCase();

    let stateClass = "tradeRead-conditional";
    let chipTone = "tone-warn";
    let chipLabel = "CONDITIONAL";

    if (label === "no trade") {
      stateClass = "tradeRead-stand-down";
      chipTone = "tone-negative";
      chipLabel = "NO TRADE";
    } else if (label === "tradeable") {
      stateClass = "tradeRead-tradeable";
      chipTone = "tone-positive";
      chipLabel = "TRADEABLE";
    }

    if (card) {
      card.classList.remove("tradeRead-stand-down", "tradeRead-wait", "tradeRead-tradeable", "tradeRead-conditional");
      card.classList.add(stateClass);
    }
    if (chip) {
      chip.textContent = chipLabel;
      chip.classList.remove("tone-positive", "tone-warn", "tone-negative");
      chip.classList.add(chipTone);
    }
  };

  const updateStructureZoneBar = (input, derived, model) => {
    const shell = document.getElementById("marketPulseZoneBarShell");
    const axis = shell ? shell.querySelector(".marketPulseExecBarAxis") : null;
    const priceMarker = document.getElementById("marketPulseZonePriceMarker");
    const flipMarker = document.getElementById("marketPulseZoneFlipMarker");
    const localMarker = document.getElementById("marketPulseZoneLocalMarker");
    const priceLabel = document.getElementById("marketPulseZonePriceLabel");
    if (!shell || !priceMarker || !flipMarker || !priceLabel) return;

    const levels = (model && model.levels) || {};
    const location = (model && model.location) || {};
    const spot = asNum(levels.spot ?? input.spot);
    const flip = asNum(levels.main_flip ?? input.gammaFlip);
    const local = asNum(levels.local_flip);
    const call = asNum(levels.call_wall ?? input.callWall);
    const put = asNum(levels.put_wall ?? input.putWall);
    const available = [spot, flip, call, put].every((value) => value !== null);
    const localAvailable = local !== null;
    shell.classList.toggle("is-empty", !available);
    shell.classList.remove("is-near-level", "is-near-flip", "is-near-call", "is-near-put");
    if (localMarker) localMarker.hidden = !localAvailable;

    if (!available) {
      setText("marketPulseZoneLabel", "Unavailable");
      setText("marketPulseZoneNearest", "—");
      setText("marketPulseZoneStatus", "Awaiting structure");
      setText("marketPulseZoneRead", "Wait for live structure");
      return;
    }

    const values = [spot, flip, call, put];
    const lo = Math.min(...values);
    const hi = Math.max(...values);
    const pad = Math.max(6, (hi - lo) * 0.12);
    const domainMin = lo - pad;
    const domainMax = hi + pad;
    const pct = (value) => clamp(((value - domainMin) / Math.max(1, domainMax - domainMin)) * 100, 2, 98);

    priceMarker.style.left = `${pct(spot)}%`;
    flipMarker.style.left = `${pct(flip)}%`;
    if (localMarker && localAvailable) localMarker.style.left = `${pct(local)}%`;
    priceLabel.style.left = `${pct(spot)}%`;
    setText("marketPulseZonePriceLabel", `Price ${formatNumber(spot, 0)}`);
    setText("marketPulseZonePutLabel", `PW ${formatNumber(put, 0)}`);
    setText("marketPulseZoneFlipLabel", `Main ${formatNumber(flip, 0)}`);
    setText("marketPulseZoneCallLabel", `CW ${formatNumber(call, 0)}`);
    if (axis) {
      const putLabel = document.getElementById("marketPulseZonePutLabel");
      const callLabel = document.getElementById("marketPulseZoneCallLabel");
      const flipLabel = document.getElementById("marketPulseZoneFlipLabel");
      const labels = [putLabel, callLabel, flipLabel].filter(Boolean);
      labels.forEach((node) => node.style.setProperty("--label-row", "0"));

      const metrics = labels.map((node) => {
        const leftPct = parseFloat(node.style.left || "50");
        return {
          node,
          leftPct: clamp(leftPct, 2, 98),
        };
      });

      const findMetric = (node) => metrics.find((item) => item.node === node) || null;
      const putMetric = findMetric(putLabel);
      const callMetric = findMetric(callLabel);
      const flipMetric = findMetric(flipLabel);
      let maxRow = 0;

      const overlaps = (left, right, minPctGap = 15) => {
        if (!left || !right) return false;
        return Math.abs(right.leftPct - left.leftPct) < minPctGap;
      };

      if (putLabel && callLabel && overlaps(putMetric, callMetric, 16)) {
        callLabel.style.setProperty("--label-row", "1");
        maxRow = Math.max(maxRow, 1);
      }

      if (callLabel && flipLabel) {
        const callRow = Number(callLabel.style.getPropertyValue("--label-row") || 0);
        if (overlaps(callMetric, flipMetric, 16)) {
          callLabel.style.setProperty("--label-row", callRow > 0 ? "2" : "1");
          maxRow = Math.max(maxRow, Number(callLabel.style.getPropertyValue("--label-row") || 1));
        }
      }

      axis.style.setProperty("--axis-rows", String(maxRow + 1));
    }

    const candidates = [
      { key: "flip", label: "Flip", distance: abs(derived.distanceToFlip) },
      { key: "call", label: "Call Wall", distance: abs(derived.distanceToCallWall) },
      { key: "put", label: "Put Wall", distance: abs(derived.distanceToPutWall) },
    ].filter((row) => row.distance !== null);
    candidates.sort((a, b) => a.distance - b.distance);
    const nearest = candidates[0] || null;

    let zone = "Outside Range";
    if (abs(derived.distanceToFlip) !== null && abs(derived.distanceToFlip) <= 5) {
      zone = "At Flip Decision Zone";
    } else if (abs(derived.distanceToCallWall) !== null && abs(derived.distanceToCallWall) <= 12) {
      zone = "Near Call Wall";
    } else if (abs(derived.distanceToPutWall) !== null && abs(derived.distanceToPutWall) <= 12) {
      zone = "Near Put Wall";
    } else if (spot >= put && spot <= call) {
      zone = `Inside Range · ${spot >= flip ? "Above Flip" : "Below Flip"}`;
    } else if (spot > call) {
      zone = "Above Call Wall";
    } else if (spot < put) {
      zone = "Below Put Wall";
    }

    let status = "In neutral zone";
    if (zone === "At Flip Decision Zone") status = "Near breakout decision";
    else if (zone === "Near Call Wall") status = "Approaching resistance";
    else if (zone === "Near Put Wall") status = "Approaching support";
    else if (spot > call) status = "Above resistance";
    else if (spot < put) status = "Below support";

    let actionRead = "Avoid mid-range entries";
    if (zone === "At Flip Decision Zone") actionRead = "Wait near flip for confirmation";
    else if (zone === "Near Call Wall") actionRead = String(derived.dealerRegime || "").toLowerCase().includes("positive")
      ? "Sell rips near resistance"
      : "Fade failed breakout only";
    else if (zone === "Near Put Wall") actionRead = String(derived.dealerRegime || "").toLowerCase().includes("positive")
      ? "Buy dips near support"
      : "Wait for support reclaim";
    else if (zone === "Inside Range · Below Flip") actionRead = "Sell rips below flip";
    else if (zone === "Inside Range · Above Flip") actionRead = "Buy dips above flip";
    else if (zone === "Above Call Wall") actionRead = "Avoid chasing above resistance";
    else if (zone === "Below Put Wall") actionRead = "Wait for support reclaim";

    if (nearest && nearest.distance <= 15) {
      shell.classList.add("is-near-level");
      if (nearest.key === "flip") shell.classList.add("is-near-flip");
      if (nearest.key === "call") shell.classList.add("is-near-call");
      if (nearest.key === "put") shell.classList.add("is-near-put");
    }

    setText("marketPulseZoneLabel", String(location.zone || zone));
    setText(
      "marketPulseZoneNearest",
      location.nearest_level_name
        ? `${location.nearest_level_name} (${formatNumber(location.distance_points, 0)} pts)`
        : nearest
          ? `${nearest.label} (${formatNumber(nearest.distance, 0)} pts)`
          : "—"
    );
    setText("marketPulseZoneStatus", String(location.status || status));
    setText("marketPulseZoneRead", String(location.read || actionRead));
  };

  const summarizeStateLine = (input, derived, panelMode) => {
    const quality = String(derived.dataQualityLabel || "").trim();
    const regime = String(derived.dealerRegime || "").toLowerCase().includes("negative")
      ? "Negative gamma"
      : "Positive gamma";
    if (panelMode === "replay") return `${derived.structureType} / replay reference`;
    if (quality && quality !== "Live") return `${quality} / ${regime}`;
    return `${regime} / ${derived.volatilityState}`;
  };

  const summarizeStateSubline = (derived, panelMode) => {
    if (panelMode === "replay") return "Replay reference";
    if (derived.noTradeCenter) return "Inside no-trade center";
    return String(derived.structureType || "Structure pending");
  };

  const summarizeActionLine = (executionPlan, panelMode) => {
    const tone = String((executionPlan || {}).tone || "");
    if (panelMode === "replay") return "Review edge behavior before the next open";
    if (tone === "warn") return "WAIT for confirmed edge interaction";
    if (tone === "negative") return "SHORT only on confirmed breakdown";
    if (tone === "positive") return "LONG only on confirmed reclaim";
    return "RESPONSIVE only at key levels";
  };

  const summarizeRuleLine = (executionPlan, derived) => {
    if (derived.noTradeCenter) return "Do not trade the center";
    const raw = String((executionPlan || {}).avoidThis || "").toLowerCase();
    if (raw.includes("support")) return "Do not press into support without confirmation";
    if (raw.includes("wall")) return "Do not force trades through the wall";
    if (raw.includes("center")) return "Do not force entries in the middle";
    return "Only act after confirmed structure response";
  };

  const summarizeBiasSubline = (executionPlan, derived) => {
    if (derived.noTradeCenter) return "Range first";
    const raw = String((executionPlan || {}).biasLine || "");
    if (raw.toLowerCase().includes("mean reversion")) return "Mean reversion";
    if (raw.toLowerCase().includes("expansion")) return "Expansion risk";
    if (raw.toLowerCase().includes("reaction")) return "Reaction, not prediction";
    return String(derived.volatilityState || "Structure-led");
  };

  const summarizeEdgeSubline = (executionPlan) => {
    const trigger = String((executionPlan || {}).trigger || "");
    if (!trigger) return "Confirm on 5m";
    return trigger.length > 26 ? "Confirm on 5m" : trigger;
  };

  const summarizeAvoidSubline = (executionPlan, derived) => {
    if (derived.noTradeCenter) return "Mid-range is dead space";
    const raw = String((executionPlan || {}).avoidThisLine || "");
    if (!raw) return "Skip weak location";
    if (raw.toLowerCase().includes("wide")) return "Skip wide invalidation";
    if (raw.toLowerCase().includes("wrong")) return "Reset if thesis breaks";
    if (raw.length > 34) return "Skip weak location";
    return raw;
  };

  const buildIfThenLine = (input, derived) => {
    const spot = asNum(input.spot);
    const local = asNum(input.localFlip);
    const call = asNum(input.callWall);
    const put = asNum(input.putWall);
    const nextCall = asNum(derived.nextCallWall);
    const nextPut = asNum(derived.nextPutWall);
    if (spot === null) return "If live price is unavailable, stand down.";
    if (call !== null && spot > call) {
      return `If ${formatNumber(call, 0)} holds on retest, continuation can stretch toward ${formatNumber(nextCall, 0)}.`;
    }
    if (put !== null && spot < put) {
      return `If ${formatNumber(put, 0)} fails to reclaim, downside can continue toward ${formatNumber(nextPut, 0)}.`;
    }
    if (local !== null && spot > local) {
      return `If dips hold above ${formatNumber(local, 0)}, continuation can press toward ${formatNumber(call, 0)}.`;
    }
    if (local !== null && spot < local) {
      return `If pops fail below ${formatNumber(local, 0)}, pressure can rotate back toward ${formatNumber(put, 0)}.`;
    }
    return "If the level confirms, act. If not, wait.";
  };

  const computeHeroState = (input, derived, executionPlan) => {
    const spot = asNum(input.spot);
    const local = asNum(input.localFlip);
    const call = asNum(input.callWall);
    const put = asNum(input.putWall);
    const nextCall = asNum(derived.nextCallWall);
    const nextPut = asNum(derived.nextPutWall);
    const negativeGamma = String(derived.dealerRegime || "").toLowerCase().includes("negative");
    const extensionThreshold = 15;

    if (spot === null || (local === null && call === null && put === null)) {
      return {
        state: "NO TRADE",
        tone: "warn",
        bestLook: "Unavailable",
        destination: "Await live levels",
        invalidation: "No structure",
        trigger: "Await data",
        note: "Execution stays blocked until spot and core levels print cleanly.",
      };
    }

    if (derived.noTradeCenter) {
      return {
        state: "NO TRADE",
        tone: "negative",
        bestLook: "No trade in the center",
        destination: "Wait for a wall test",
        invalidation: "Center is invalid",
        trigger: "Edge interaction first",
        note: "Flip and wall cluster are too compressed. Trade only after price tags a real boundary.",
      };
    }

    if (call !== null && spot > call) {
      const extensionDistance = spot - call;
      const extended = extensionDistance <= extensionThreshold;
      return {
        state: extended ? "NO TRADE" : "WAIT",
        tone: extended ? "negative" : "warn",
        bestLook: "Wait for pullback into Call Wall",
        destination: nextCall !== null ? `NCW ${formatNumber(nextCall, 0)}` : "Expansion zone",
        invalidation: local !== null ? `Lose Local Flip ${formatNumber(local, 0)}` : `Lose Call Wall ${formatNumber(call, 0)}`,
        trigger: "Sweep + reclaim + 2-2 + volume",
        note: negativeGamma
          ? "Macro negative, local bullish. Avoid chasing extension and require pullback confirmation."
          : "Above call wall. Momentum can continue, but the only valid long is a confirmed retest.",
      };
    }

    if (put !== null && spot < put) {
      return {
        state: "WAIT",
        tone: "negative",
        bestLook: "Short bounces after failed reclaim",
        destination: nextPut !== null ? `NPW ${formatNumber(nextPut, 0)}` : `Press below PW ${formatNumber(put, 0)}`,
        invalidation: local !== null ? `Reclaim Local Flip ${formatNumber(local, 0)}` : `Reclaim Put Wall ${formatNumber(put, 0)}`,
        trigger: "Failed reclaim + 2-2 + volume",
        note: negativeGamma
          ? "Put wall is lost in a fast tape. Do not front-run; wait for failed reclaim or clean continuation."
          : "Put wall is lost. Wait for failed reclaim before pressing downside.",
      };
    }

    if (local !== null && spot > local) {
      return {
        state: (abs(derived.distanceToLocalFlip) || 999) <= 10 && (call === null || spot < call) ? "READY" : "WAIT",
        tone: "positive",
        bestLook: "Buy dip after sweep + reclaim",
        destination: call !== null ? `CW ${formatNumber(call, 0)}` : "Press toward next upside shelf",
        invalidation: `Lose Local Flip ${formatNumber(local, 0)}`,
        trigger: "Sweep + reclaim + 2-2 + volume",
        note: negativeGamma
          ? "Above Local Flip in negative gamma. Long bias is valid, but no lazy entries and no chasing."
          : "Above Local Flip. Buy dips only and keep continuation longs tied to real confirmation.",
      };
    }

    if (local !== null && spot < local) {
      return {
        state: (abs(derived.distanceToLocalFlip) || 999) <= 10 && (put === null || spot > put) ? "READY" : "WAIT",
        tone: "negative",
        bestLook: "Sell failed bounce below Local Flip",
        destination: put !== null && spot >= put ? `PW ${formatNumber(put, 0)}` : nextPut !== null ? `NPW ${formatNumber(nextPut, 0)}` : "Press toward next downside shelf",
        invalidation: `Reclaim Local Flip ${formatNumber(local, 0)}`,
        trigger: "Pop + fail + 2-2 + volume",
        note: negativeGamma
          ? "Below Local Flip in negative gamma. Failed pops can resolve fast, but only after confirmation."
          : "Below Local Flip. Sell rips only and require the failed reclaim first.",
      };
    }

    return {
      state: "WAIT",
      tone: executionPlan.tone === "positive" ? "positive" : executionPlan.tone === "negative" ? "negative" : "warn",
      bestLook: executionPlan.doThis || executionPlan.headline || "Wait for clean structure",
      destination: executionPlan.target || "Await next level",
      invalidation: executionPlan.invalidation || "Reset if thesis breaks",
      trigger: executionPlan.trigger || "Confirmation required",
      note: executionPlan.subline || "Wait for clean structure before committing size.",
    };
  };

  const buildHeroMapSummary = (input, derived) => {
    const spot = asNum(input.spot);
    const local = asNum(input.localFlip);
    const call = asNum(input.callWall);
    const put = asNum(input.putWall);
    const nextCall = asNum(derived.nextCallWall);
    const nextPut = asNum(derived.nextPutWall);

    if (spot === null) {
      return {
        currentRead: "Unavailable",
        pullbackLevel: "Unavailable",
        nextDestination: "Await live levels",
      };
    }

    if (call !== null && spot > call) {
      return {
        currentRead: "Above Call Wall",
        pullbackLevel: `CW ${formatNumber(call, 0)}`,
        nextDestination: nextCall !== null ? `NCW ${formatNumber(nextCall, 0)}` : "Expansion zone",
      };
    }

    if (local !== null && call !== null && spot >= local && spot <= call) {
      return {
        currentRead: "Above Local Flip",
        pullbackLevel: `Local ${formatNumber(local, 0)}`,
        nextDestination: `CW ${formatNumber(call, 0)}`,
      };
    }

    if (local !== null && spot < local) {
      return {
        currentRead: "Below Local Flip",
        pullbackLevel: `Local ${formatNumber(local, 0)}`,
        nextDestination: put !== null && spot >= put ? `PW ${formatNumber(put, 0)}` : nextPut !== null ? `NPW ${formatNumber(nextPut, 0)}` : put !== null ? `PW ${formatNumber(put, 0)}` : "Downside shelf",
      };
    }

    return {
      currentRead: "Responsive / rotational",
      pullbackLevel: local !== null ? `Local ${formatNumber(local, 0)}` : "Working level",
      nextDestination: call !== null ? `CW ${formatNumber(call, 0)}` : "Await next level",
    };
  };

  const updateHeroRail = (input, derived) => {
    const track = document.getElementById("marketPulseExecutionHeroRail");
    if (!track) return;
    const levels = [
      { id: "marketPulseHeroMarkerNextPut", labelId: "marketPulseHeroNextPut", keyId: "marketPulseHeroLevelKeyNextPut", prefix: "NPW", short: "NPW", value: asNum(derived.nextPutWall) },
      { id: "marketPulseHeroMarkerPut", labelId: "marketPulseHeroPutWall", keyId: "marketPulseHeroLevelKeyPut", prefix: "PW", short: "PW", value: asNum(input.putWall) },
      { id: "marketPulseHeroMarkerLocal", labelId: "marketPulseHeroLocalFlip", keyId: "marketPulseHeroLevelKeyLocal", prefix: "LF", short: "LF", value: asNum(input.localFlip), allowNoneText: input.localFlipMissingInBand },
      { id: "marketPulseHeroMarkerMain", labelId: "marketPulseHeroMainFlip", keyId: null, prefix: "Main", short: "Main", value: asNum(input.gammaFlip) },
      { id: "marketPulseHeroMarkerSpot", labelId: "marketPulseHeroSpotLabel", keyId: "marketPulseHeroLevelKeySpot", prefix: "Spot", short: "Spot", value: asNum(input.spot), digits: 2 },
      { id: "marketPulseHeroMarkerCall", labelId: "marketPulseHeroCallWall", keyId: "marketPulseHeroLevelKeyCall", prefix: "CW", short: "CW", value: asNum(input.callWall) },
      { id: "marketPulseHeroMarkerNextCall", labelId: "marketPulseHeroNextCall", keyId: "marketPulseHeroLevelKeyNextCall", prefix: "NCW", short: "NCW", value: asNum(derived.nextCallWall) },
    ];
    const numeric = levels.map((row) => row.value).filter((value) => value !== null);
    if (!numeric.length) return;
    const lo = Math.min(...numeric);
    const hi = Math.max(...numeric);
    const pad = Math.max(8, (hi - lo) * 0.1);
    const domainMin = lo - pad;
    const domainMax = hi + pad;
    const pct = (value) => clamp(((value - domainMin) / Math.max(1, domainMax - domainMin)) * 100, 3, 97);

    levels.forEach((row) => {
      const marker = document.getElementById(row.id);
      const label = document.getElementById(row.labelId);
      const key = row.keyId ? document.getElementById(row.keyId) : null;
      if (!marker || !label) return;
      if (row.value === null) {
        marker.hidden = true;
        label.textContent = row.short;
        if (key) {
          key.hidden = false;
          key.textContent = row.allowNoneText ? `${row.prefix} ${LOCAL_FLIP_NONE_LABEL}` : `${row.prefix} Unavailable`;
        }
        return;
      }
      marker.hidden = false;
      marker.style.left = `${pct(row.value)}%`;
      const position = pct(row.value);
      marker.classList.toggle("is-edge-left", position <= 9);
      marker.classList.toggle("is-edge-right", position >= 91);
      label.textContent = row.short;
      if (key) {
        key.hidden = false;
        key.textContent = `${row.prefix} ${formatNumber(row.value, row.digits || 0)}`;
      }
    });
  };

  const updateExecutionHero = (input, derived, executionPlan, model) => {
    if (window.__mcHeroApiDriven) return;
    const hero = computeHeroState(input, derived, executionPlan);
    const mapSummary = buildHeroMapSummary(input, derived);
    const biasLine = input.localFlip === null
      ? (input.localFlipMissingInBand ? LOCAL_FLIP_NONE_LABEL : "Unavailable")
      : input.spot !== null && input.localFlip !== null && input.spot >= input.localFlip
        ? `Bullish above Local Flip ${formatNumber(input.localFlip, 0)}`
        : `Bearish below Local Flip ${formatNumber(input.localFlip, 0)}`;

    setText("marketPulseHeroSpot", formatNumber(input.spot, 2));
    setText("marketPulseHeroBias", biasLine);
    const tradeabilityLabel = String(derived.tradeability.label || "Unavailable")
      .replace(/_/g, " ")
      .trim();
    setText("marketPulseHeroTradeability", tradeabilityLabel || "Unavailable");
    setText("marketPulseHeroMacroFlip", formatNumber(input.gammaFlip, 0));
    setText("marketPulseHeroRailContext", (model && model.posture_summary) || executionPlan.subline);
    setText("marketPulseHeroRailSummary", executionPlan.locationLine || executionPlan.subline);
    setText("marketPulseHeroRailFootState", mapSummary.currentRead);
    setText("marketPulseHeroPullbackLevel", mapSummary.pullbackLevel);
    setText("marketPulseHeroDestinationInline", mapSummary.nextDestination);
    setText("marketPulseHeroStateContext", hero.state);
    setText("marketPulseHeroStateChip", hero.state);
    setText("marketPulseHeroTradeState", hero.state);
    setText("marketPulseHeroBestLook", hero.bestLook);
    setText("marketPulseHeroInvalidation", hero.invalidation);
    setText("marketPulseHeroRequiredTrigger", hero.trigger);

    const stateChip = document.getElementById("marketPulseHeroStateChip");
    const stateContext = document.getElementById("marketPulseHeroStateContext");
    [stateChip, stateContext].forEach((node) => {
      if (!node) return;
      node.classList.remove("tone-positive", "tone-warn", "tone-negative");
      node.classList.add(hero.tone === "positive" ? "tone-positive" : hero.tone === "negative" ? "tone-negative" : "tone-warn");
    });

    updateHeroRail(input, derived);
  };

  const updateTriggerValidation = (heroState, derived, triggerValidation = null) => {
    const backendItems = (triggerValidation && triggerValidation.items) || {};
    const nearLong = derived.aboveOrBelowLocalFlip === "above";
    const nearShort = derived.aboveOrBelowLocalFlip === "below";
    const setItem = (id, lineId, text, active = false) => {
      const node = document.getElementById(id);
      if (node) node.classList.toggle("is-active", Boolean(active));
      setText(lineId, text);
      const stateNode = document.getElementById(`${id}State`);
      if (stateNode) stateNode.textContent = active ? "Ready" : "Pending";
    };

    setItem(
      "marketPulseTriggerSweep",
      "marketPulseTriggerSweepLine",
      String(backendItems.sweep?.line || (nearLong ? "Need sweep into support or Local Flip before the long exists." : nearShort ? "Need pop into resistance or Local Flip before the short exists." : "Need sweep into the working level before entry is considered.")),
      Boolean(backendItems.sweep?.active)
    );
    setItem(
      "marketPulseTriggerReclaim",
      "marketPulseTriggerReclaimLine",
      String(backendItems.reclaim?.line || (nearLong ? "Need reclaim back above the working level after the sweep." : nearShort ? "Need failed reclaim back under the working level after the pop." : "Wait for the level to prove itself after the interaction.")),
      Boolean(backendItems.reclaim?.active)
    );
    setItem(
      "marketPulseTriggerReversal",
      "marketPulseTriggerReversalLine",
      String(backendItems.reversal?.line || (nearLong ? "Need 5m 2-2 up / 3-1-2 continuation before the long is valid." : nearShort ? "Need 5m 2-2 down / 3-1-2 continuation before the short is valid." : "A clean 5m reversal or continuation trigger still has to print.")),
      Boolean(backendItems.reversal?.active)
    );
    setItem(
      "marketPulseTriggerVolume",
      "marketPulseTriggerVolumeLine",
      String(backendItems.volume?.line || (String(derived.dealerRegime || "").toLowerCase().includes("negative") ? "Fast tape is active. Volume confirmation is mandatory." : "Require real participation before calling the move valid.")),
      Boolean(backendItems.volume?.active)
    );

    setText(
      "marketPulseTriggerHeaderLine",
      String(triggerValidation?.header_line || (heroState.state === "READY" ? "Ready location, but still trigger-gated." : heroState.state === "BLOCKED" ? "Blocked until a real edge appears." : "No trigger = no trade."))
    );
    setText(
      "marketPulseTriggerStatus",
      String(triggerValidation?.status_line || (heroState.state === "BLOCKED" ? "BLOCKED — WAIT FOR EDGE" : heroState.state === "READY" ? "READY LOCATION — TRIGGER STILL REQUIRED" : "NO TRIGGER — NO TRADE"))
    );
    setText("marketPulseTriggerFooterLine", String(triggerValidation?.footer_line || heroState.note));
    syncTriggerChecklistUi();
  };

  const triggerChecklistItems = Array.from(document.querySelectorAll(".marketPulseTriggerItem[data-trigger-step]"));
  const triggerProgressFill = document.getElementById("marketPulseTriggerProgressFill");
  const triggerProgressLabel = document.getElementById("marketPulseTriggerProgressLabel");
  const triggerProgressNext = document.getElementById("marketPulseTriggerProgressNext");
  let selectedTriggerStep = null;

  const selectTriggerChecklistStep = (step) => {
    selectedTriggerStep = step || null;
    triggerChecklistItems.forEach((item) => {
      const selected = step && item.dataset.triggerStep === step;
      item.classList.toggle("is-selected", selected);
      item.setAttribute("aria-pressed", selected ? "true" : "false");
    });
  };

  const syncTriggerChecklistUi = () => {
    if (!triggerChecklistItems.length) return;
    const activeItems = triggerChecklistItems.filter((item) => item.classList.contains("is-active"));
    const completeCount = activeItems.length;
    const nextItem = triggerChecklistItems.find((item) => !item.classList.contains("is-active"));
    if (triggerProgressFill) {
      triggerProgressFill.style.width = `${(completeCount / triggerChecklistItems.length) * 100}%`;
    }
    if (triggerProgressLabel) {
      triggerProgressLabel.textContent = `${completeCount}/${triggerChecklistItems.length} complete`;
    }
    if (triggerProgressNext) {
      triggerProgressNext.textContent = nextItem
        ? `Next: ${String(nextItem.dataset.triggerTitle || "Checklist step")}`
        : "Checklist complete";
    }
    const validSelected = triggerChecklistItems.some((item) => item.dataset.triggerStep === selectedTriggerStep);
    if (!validSelected) {
      selectTriggerChecklistStep((activeItems[0] || nextItem || triggerChecklistItems[0]).dataset.triggerStep);
      return;
    }
    selectTriggerChecklistStep(selectedTriggerStep);
  };

  triggerChecklistItems.forEach((item) => {
    item.addEventListener("click", () => {
      selectTriggerChecklistStep(item.dataset.triggerStep);
    });
  });

  const updateTapeSummary = (quotesBySymbol, tapeMeta = {}) => {
    const tracked = tapeCards
      .map((card) => String(card.dataset.symbol || "").toUpperCase())
      .filter(Boolean);
    let advancers = 0;
    let decliners = 0;
    let missing = 0;
    let biggestLabel = "—";
    let biggestMove = null;

    tracked.forEach((symbol) => {
      const quote = ((quotesBySymbol || {})[symbol] || {});
      const pct = asNum(quote.pct_change ?? quote.change_pct);
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
      String(tapeMeta.drag || "").trim()
        || (biggestMove === null ? "—" : `${biggestLabel} ${formatSigned(biggestMove, 2)}%`)
    );
    if (String(tapeMeta.risk || "").trim()) setText("marketPulseRiskLabel", tapeMeta.risk);
    if (Array.isArray(tapeMeta.leaders)) {
      setText("marketPulseLeaders", tapeMeta.leaders.length ? tapeMeta.leaders.join(", ") : "—");
    }
  };

  const applyCoreTapeSnapshot = (quotesBySymbol, seriesBySymbol = {}, tapeMeta = {}) => {
    const quotes = quotesBySymbol && typeof quotesBySymbol === "object" ? quotesBySymbol : {};
    const series = seriesBySymbol && typeof seriesBySymbol === "object" ? seriesBySymbol : {};
    tapeCards.forEach((card) => {
      const symbol = String(card.dataset.symbol || "").toUpperCase();
      if (!symbol) return;
      applyTapeCardUpdate(card, quotes[symbol] || {}, series[symbol] || []);
    });
    updateTapeSummary(quotes, tapeMeta);
  };

  const applyTapeCardUpdate = (card, quote, points) => {
    if (!card || !quote || typeof quote !== "object") return;
    const price = asNum(quote.price);
    const pct = asNum(quote.pct_change ?? quote.change_pct);
    const state = deriveDataState(quote);
    const tone = sparkTone(pct);
    const reason = String(quote.reason || "").trim();
    const asOf = String(quote.as_of || quote.asof || "").trim();
    const symbol = String(card.dataset.symbol || "").toUpperCase();
    const watchState = tapeStateFor(symbol, pct);

    const chip = card.querySelector('[data-role="state-chip"]');
    if (chip) {
      const chipChanged = String(chip.textContent || "") !== watchState.label;
      chip.textContent = watchState.label;
      chip.title = watchState.title;
      const watchTone = `tone-${watchState.tone}`;
      chip.classList.remove("tone-positive", "tone-negative", "tone-neutral");
      chip.classList.add(watchTone);
      if (chipChanged) {
        uiFX?.pulseNode?.(
          chip,
          watchTone === "tone-positive"
            ? "positive"
            : watchTone === "tone-negative"
              ? "negative"
              : "info"
        );
      }
    }
    updateTextNode(
      card.querySelector('[data-role="freshness"]'),
      dashboardTapeFreshnessLabel(
        String(quote.freshness_label || "").trim() || (state === "live" ? "Live" : formatEtLabel(asOf)),
        state,
        asOf
      )
    );
    updateTextNode(card.querySelector('[data-role="price"]'), price === null ? "—" : price.toFixed(2), {
      live: true,
      direction: pct > 0 ? "up" : pct < 0 ? "down" : "neutral",
    });
    updateTextNode(
      card.querySelector('[data-role="change-line"]'),
      `${formatSigned(pct, 2)}%`,
      {
        live: true,
        direction: pct > 0 ? "up" : pct < 0 ? "down" : "neutral",
      }
    );
    updateTextNode(card.querySelector('[data-role="source-badge"]'), sourceBadgeLabel(quote));
    const hasSeriesPoints = Array.isArray(points) && seriesValueCount(points) >= 4;
    if (hasSeriesPoints) {
      updateSparkNode(card.querySelector('[data-role="sparkline"]'), points, tone);
      updateTextNode(card.querySelector('[data-role="range-line"]'), formatRange(points));
    } else {
      const sparkNode = card.querySelector('[data-role="sparkline"]');
      if (sparkNode) {
        sparkNode.classList.remove("spark-pos", "spark-neg", "spark-flat");
        sparkNode.classList.add(tone === "up" ? "spark-pos" : tone === "down" ? "spark-neg" : "spark-flat");
      }
    }

    const reasonNode = card.querySelector('[data-role="reason-line"]');
    if (reasonNode) {
      reasonNode.hidden = true;
      updateTextNode(reasonNode, "");
    }

    card.classList.toggle("glow-green", (pct || 0) > 0);
    card.classList.toggle("glow-red", (pct || 0) < 0);
    card.classList.toggle("tone-positive", pct !== null && pct > 0);
    card.classList.toggle("tone-negative", pct !== null && pct < 0);
    card.classList.toggle("tone-neutral", pct === null || pct === 0);
  };

  const adaptInput = (base) => {
    const quote = (base && (base.playbook_quote || base.spx_quote)) || {};
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

    const dayOpen = asNum(quote.day_open);
    const sessionHigh = asNum(quote.day_high);
    const sessionLow = asNum(quote.day_low);

    return {
      spot: asNum(quote.price),
      dayOpen,
      sessionHigh,
      sessionLow,
      priorDayHigh: asNum(quote.prior_day_high), // TODO(api): wire prior_day_high in quote payload when available.
      priorDayLow: asNum(quote.prior_day_low), // TODO(api): wire prior_day_low in quote payload when available.
      vwap: asNum(quote.vwap), // TODO(api): wire session VWAP from provider stream when available.
      localFlip: localFlipFromSnapshot(gamma),
      localFlipFound: gamma.local_flip_found === true,
      localFlipMissingInBand: localFlipMissingInBand(gamma),
      overnightHigh: asNum(quote.overnight_high), // TODO(api): wire overnight levels when available.
      overnightLow: asNum(quote.overnight_low), // TODO(api): wire overnight levels when available.
      vix: asNum(vixQuote.price),
      vixDirection,

      gammaFlip: asNum(gamma.gamma_flip_combined_basket),
      callWall: asNum(gamma.call_wall_aggregated_gamma),
      putWall: asNum(gamma.put_wall_aggregated_gamma),
      nextCallWall: asNum(gamma.next_call_wall_above), // TODO(api): preferred backend field for next call wall.
      nextPutWall: asNum(gamma.next_put_wall_below), // TODO(api): preferred backend field for next put wall.
      netGamma: asNum(gamma.net_gex),
      callWallGammaPerPoint: asNum(gamma.call_wall_gamma_per_point), // TODO(api): wire gamma/point from backend.
      putWallGammaPerPoint: asNum(gamma.put_wall_gamma_per_point), // TODO(api): wire gamma/point from backend.
      expectedMove: asNum(gamma.gamma_range_estimate),
      expectedMoveUp: asNum(gamma.gamma_range_high),
      expectedMoveDown: asNum(gamma.gamma_range_low),

      regime: String(gamma.regime || ""),
      bias: String(gamma.bias || ""),
      candidateLevels,
      updatedAt: gamma.asof || null,
      marketTimeIso: nowIso,
      dataState: String(quote.data_state || ""),
      freshnessLabel: String(quote.freshness_label || ""),
      freshnessReason: String(quote.data_reason || ""),
    };
  };

  const render = (base) => {
    const activeTicker = String((base && base.ticker) || "QQQ").toUpperCase();
    const input = adaptInput(base);
    const derived = computeDistanceMetrics(input);
    const executionPlan = buildExecutionPlan(input, derived);
    const model = (base && base.execution_model) || {};
    const modelPlaybook = (model && model.playbook) || {};
    const structureSnapshot = ((base && base.market_structure_snapshot) || base || {});
    const backendTriggerValidation =
      (structureSnapshot || {}).trigger_validation
      || (base && base.trigger_validation)
      || null;
    const modelTone = String(structureSnapshot.context_tone || modelPlaybook.tone || "");
    const modelStatus = String(structureSnapshot.context_status || modelPlaybook.status || "");
    const triggerState = buildTriggerState(executionPlan.trigger, executionPlan.tone);
    const snapshotNarrative = (((base || {}).gamma_snapshot || {}).narrative) || {};
    const bullets = Array.isArray(snapshotNarrative.auto_read) && snapshotNarrative.auto_read.length
      ? snapshotNarrative.auto_read
      : buildAutoRead(input, derived);
    const badges = Array.isArray(snapshotNarrative.warning_badges) && snapshotNarrative.warning_badges.length
      ? snapshotNarrative.warning_badges
      : buildWarningBadges(input, derived);
    const playbookQuote = (base && (base.playbook_quote || base.spx_quote)) || {};
    const quotePoints = pickBestSeries(
      (((base || {}).series_points || {})[activeTicker]),
      (Array.isArray(playbookQuote.series) ? playbookQuote.series : []),
      (Array.isArray(playbookQuote.mini_series) ? playbookQuote.mini_series : [])
    );
    const quoteState = deriveDataState(playbookQuote);
    const quoteAbsChange = inferAbsoluteChange(playbookQuote.price, playbookQuote.change_pct);
    const sessionPhase = deriveSessionPhase(base.market_now_iso || base.server_ts || base.updated_at || null);
    const panelMode = seriesValueCount(quotePoints) >= 2
      ? (sessionPhase === "open" ? "live" : "replay")
      : "reference";
    const panelState = panelMode === "live" ? quoteState : panelMode === "replay" ? "delayed" : "cached";
    const panelStateLabel = panelMode === "live" ? dataStateLabel(panelState) : panelMode === "replay" ? "Replay" : "Reference";
    const panelPriceLabel = panelMode === "live" ? "Live Price" : panelMode === "replay" ? "Last Session Close" : "Reference Price";
    const panelRange = formatRange(quotePoints);
    const tickTimeRaw =
      (playbookQuote || {}).as_of
      || (playbookQuote || {}).asof
      || base.updated_at
      || base.server_ts
      || base.market_now_iso
      || null;
    const footerTime = panelMode === "live"
      ? String(spxQuote.freshness_label || "Updated just now")
      : formatEtLabel(tickTimeRaw);
    const footerLabel = panelMode === "live" ? "Live" : panelMode === "replay" ? "Replay" : "Reference";
    const gammaLabel = String(
      structureSnapshot.gamma_regime_label
      || ((base || {}).gamma_snapshot || {}).regime_label
      || structureSnapshot.gamma_regime
      || "REGIME UNAVAILABLE"
    );
    const gammaSub = String(
      structureSnapshot.gamma_regime_reason_label
      || structureSnapshot.gamma_regime_subtitle
      || "Gamma snapshot unavailable"
    );
    const biasContext = String(structureSnapshot.bias_context || structureSnapshot.planning_bias_label || "Awaiting valid structure");
    const biasShort = String(structureSnapshot.bias_label || structureSnapshot.trade_state_label || "WAIT");
    const biasState = String(structureSnapshot.bias_state || "").toLowerCase();

    setText("spxPrioritySpotValue", formatNumber(input.spot, 2));
    setText("spxPriorityPriceLabel", panelPriceLabel);
    setText("spxPriorityGammaFlipValue", formatNumber(input.gammaFlip, 0));
    setText("spxPriorityCallWallValue", formatNumber(input.callWall, 0));
    setText("spxPriorityPutWallValue", formatNumber(input.putWall, 0));
    setText("spxPriorityNextCallWall", formatNumber(derived.nextCallWall, 0));
    setText("spxPriorityNextPutWall", formatNumber(derived.nextPutWall, 0));
    setText("spxPriorityExpectedMoveRange", derived.expectedMoveRangeText || "—");

    setText("spxPriorityDealerRegime", derived.dealerRegime);
    setText("spxPriorityVolatilityState", derived.volatilityState);
    setText("spxPriorityStructureType", derived.structureType);
    const tradeabilityScore100 = clamp(Math.round(asNum(structureSnapshot.context_score) || asNum(modelPlaybook.score) || 0), 0, 100);
    const toneClass = modelTone === "positive" ? "tone-positive" : modelTone === "negative" ? "tone-negative" : "tone-warn";
    const bestLook = String(structureSnapshot.best_look || modelPlaybook.best_look || "Wait for cleaner structure");
    const whyLine = String(modelPlaybook.why || structureSnapshot.plan_note || (model && model.posture_summary) || "Context is mixed.");
    const executionStatusLine = String(backendTriggerValidation?.manual_label || backendTriggerValidation?.header_line || "Waiting for manual confirmation");
    const needLine = String(structureSnapshot.required_trigger || modelPlaybook.need || "Need confirmation");
    const ifThenLine = String(structureSnapshot.invalidation || modelPlaybook.avoid || buildIfThenLine(input, derived));
    const heroState = computeHeroState(input, derived, executionPlan);

    updateStructureZoneBar(input, derived, model);
    updateExecutionHero(input, derived, executionPlan, model);
    updateTriggerValidation(heroState, derived, backendTriggerValidation);

    setText("marketPulseTradeabilityScore", `${tradeabilityScore100}`);
    setText("marketPulseTradeabilityGrade", String(structureSnapshot.context_grade || modelPlaybook.grade || gradeTradeability(tradeabilityScore100)));
    setText("marketPulseActionLead", whyLine);
    setText("marketPulseBestLook", bestLook);
    setText("marketPulseEnvironment", whyLine);
    setText("marketPulseExecutionStatus", executionStatusLine);
    setText("marketPulseNeed", needLine);
    setText("marketPulseIfThen", ifThenLine);
    const scoreFill = document.getElementById("marketPulseTradeabilityBarFill");
    if (scoreFill) {
      scoreFill.style.width = `${tradeabilityScore100}%`;
      scoreFill.classList.remove("tone-positive", "tone-warn", "tone-negative");
      scoreFill.classList.add(toneClass);
    }
    const card = document.getElementById("marketPulseTradeReadCard");
    const chip = document.getElementById("marketPulseTradeabilityBadge");
    if (card) {
      card.classList.remove("tradeRead-tradeable", "tradeRead-conditional", "tradeRead-stand-down");
      card.classList.add(modelTone === "positive" ? "tradeRead-tradeable" : modelStatus === "CAUTION" || modelStatus === "WATCH" ? "tradeRead-conditional" : "tradeRead-stand-down");
    }
    if (chip) {
      const statusBadge = String(backendTriggerValidation?.status_badge || modelStatus || "WATCH");
      chip.textContent = statusBadge;
      chip.classList.remove("tone-positive", "tone-warn", "tone-negative");
      chip.classList.add(
        statusBadge.includes("NO TRIGGER") || statusBadge.includes("UNAVAILABLE")
          ? "tone-negative"
          : statusBadge.includes("WAIT") || statusBadge.includes("PLAN") || statusBadge.includes("CAUTION") || statusBadge.includes("WATCH")
            ? "tone-warn"
            : toneClass
      );
    }

    setText("spxPriorityTradeabilityScore", `${derived.tradeability.score}/10 · ${derived.tradeability.label}`);
    setText("spxPriorityTradeabilityLine", derived.tradeability.explanation);
    setText("spxPriorityReversalFit", `${derived.reversalSetupFit.label} · ${derived.reversalSetupFit.explanation}`);

    setText("spxPriorityCallWallStrength", derived.callWallStrength);
    setText("spxPriorityPutWallStrength", derived.putWallStrength);
    setText("spxPrioritySupportQuality", derived.supportQuality);
    setText("spxPriorityResistanceQuality", derived.resistanceQuality);

    const localDistance = asNum(((model && model.distances) || {}).to_local_flip);
    setText("spxPriorityDistanceFlip", formatLevelDistance(asNum(((model && model.distances) || {}).to_main_flip)));
    setText(
      "spxPriorityDistanceLocal",
      localDistance === null && input.localFlipMissingInBand
        ? LOCAL_FLIP_NONE_LABEL
        : formatLevelDistance(localDistance)
    );
    setText("spxPriorityDistanceCall", formatLevelDistance(asNum(((model && model.distances) || {}).to_call_wall)));
    setText("spxPriorityDistancePut", formatLevelDistance(asNum(((model && model.distances) || {}).to_put_wall)));
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
    setText(
      "spxPriorityTickPing",
      buildTickPingLabel(
        tickTimeRaw,
        (playbookQuote || {}).provider || (playbookQuote || {}).data_reason
      )
    );
    const stateChip = document.getElementById("spxPriorityStateChip");
    updateStateChip(stateChip, panelState, panelStateLabel);
    if (stateChip) {
      stateChip.classList.toggle("is-hidden", panelMode === "live");
    }
    setText("spxPrioritySourceBadge", sourceBadgeLabel(playbookQuote));
    setText("marketPulseSourceMode", sourceBadgeLabel(playbookQuote));
    setText("spxPriorityChangeLine", `${formatSigned(quoteAbsChange, 2)} · ${formatSigned(playbookQuote.change_pct, 2)}%`);
    setText("spxPriorityFooterMeta", String((model && model.posture_summary) || `${footerLabel} • ${formatNumber(input.spot, 2)} • ${footerTime}`));
    updateSparkNode(document.querySelector("#spxPriorityCard .marketMiniSparkWrap"), quotePoints, sparkTone(playbookQuote.change_pct));
    applyGlowState([shell, spotPanel], playbookQuote.change_pct);
    setText("marketPulseFetchedAt", formatEtLabel(base.updated_at || base.server_ts || base.market_now_iso));
    setText("marketPulseHeaderGammaLabel", gammaLabel);
    setText("marketPulseHeaderGammaSub", gammaSub);
    setText("marketPulseHeaderBiasPrimary", biasContext);
    setText("marketPulseHeaderBiasSecondary", biasShort);

    const gammaCard = document.getElementById("marketPulseHeaderGammaCard");
    if (gammaCard) {
      gammaCard.classList.remove("is-positive", "is-negative", "is-neutral");
      gammaCard.classList.add(
        derived.dealerRegime === "Positive Gamma / Mean Reverting"
          ? "is-positive"
          : derived.dealerRegime === "Negative Gamma / Momentum Amplifying"
            ? "is-negative"
            : "is-neutral"
      );
    }
    const biasCard = document.getElementById("marketPulseHeaderBiasCard");
    if (biasCard) {
      biasCard.classList.remove("is-positive", "is-negative", "is-neutral");
      biasCard.classList.add(
        biasState === "above_local"
          ? "is-positive"
          : biasState === "below_local"
            ? "is-negative"
            : "is-neutral"
      );
    }
    const aboveNode = document.getElementById("marketPulseHeaderBiasAbove");
    const belowNode = document.getElementById("marketPulseHeaderBiasBelow");
    if (aboveNode) aboveNode.classList.toggle("is-active", biasState === "above_local");
    if (belowNode) belowNode.classList.toggle("is-active", biasState === "below_local");

    setText("spxPriorityExpectedMoveHighDist", asNum(derived.distanceToExpectedMoveHigh) === null ? "—" : `${formatNumber(derived.distanceToExpectedMoveHigh, 1)} pts`);
    setText("spxPriorityExpectedMoveLowDist", asNum(derived.distanceToExpectedMoveLow) === null ? "—" : `${formatNumber(derived.distanceToExpectedMoveLow, 1)} pts`);
    setText("spxPriorityTrap", String(derived.trapZoneState || "unavailable").replace(/_/g, " "));

    setText("marketPulseSetupHeadline", summarizeStateLine(input, derived, panelMode));
    setText("marketPulseSetupSubline", "");
    setText("marketPulseSetupLocation", executionPlan.location);
    setText("marketPulseSetupLocationLine", summarizeStateSubline(derived, panelMode));
    setText("marketPulseSetupBias", executionPlan.bias);
    setText("marketPulseSetupBiasLine", summarizeBiasSubline(executionPlan, derived));
    setText("marketPulseSetupTrigger", executionPlan.trigger);
    setText("marketPulseSetupTriggerLine", executionPlan.triggerLine);
    setText("marketPulseSetupTriggerState", triggerState.label);
    setText("marketPulseSetupTriggerStateLine", triggerState.line);
    setText("marketPulseSetupTarget", executionPlan.target);
    setText("marketPulseSetupTargetLine", executionPlan.targetLine);
    setText("marketPulseSetupInvalidation", executionPlan.invalidation);
    setText("marketPulseSetupInvalidationLine", executionPlan.invalidationLine);
    if (!modelPlaybook.status) updateTradeReadState(derived.tradeability, executionPlan);

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
  const mergePayload = (incoming) => {
    if (!incoming || typeof incoming !== "object") return current;
    current = {
      ...current,
      ...incoming,
      playbook_quote: {
        ...(current.playbook_quote || current.spx_quote || {}),
        ...(incoming.playbook_quote || incoming.spx_quote || {}),
      },
      spx_quote: {
        ...(current.playbook_quote || current.spx_quote || {}),
        ...(incoming.playbook_quote || incoming.spx_quote || {}),
      },
      vix_quote: { ...(current.vix_quote || {}), ...(incoming.vix_quote || {}) },
      gamma_snapshot: { ...(current.gamma_snapshot || {}), ...(incoming.gamma_snapshot || {}) },
      execution_model: { ...(current.execution_model || {}), ...(incoming.execution_model || {}) },
      market_structure_snapshot: {
        ...(current.market_structure_snapshot || {}),
        ...(incoming.market_structure_snapshot || {}),
      },
      playbook_view: { ...(current.playbook_view || {}), ...(incoming.playbook_view || {}) },
      playbook_priority_context: {
        ...(current.playbook_priority_context || current.spx_priority_context || {}),
        ...(incoming.playbook_priority_context || incoming.spx_priority_context || {}),
      },
      spx_priority_context: {
        ...(current.playbook_priority_context || current.spx_priority_context || {}),
        ...(incoming.playbook_priority_context || incoming.spx_priority_context || {}),
      },
      execution_chart: { ...(current.execution_chart || {}), ...(incoming.execution_chart || {}) },
      series_points: { ...(current.series_points || {}), ...(incoming.series_points || {}) },
      quotes_map: { ...(current.quotes_map || {}), ...(incoming.quotes_map || {}) },
    };
    render(current);
    applyCoreTapeSnapshot(current.quotes_map || {}, current.series_points || {});
    window.dispatchEvent(new CustomEvent("market-pulse-core-ready"));
    return current;
  };
  window.applyMarketPulseContextPayload = mergePayload;
  window.applyMarketPulseTapePayload = (payload) => {
    const nextPayload = payload && typeof payload === "object" ? payload : {};
    current = {
      ...(current || {}),
      server_ts: nextPayload.server_ts || (current || {}).server_ts || null,
      quotes_map: {
        ...((current || {}).quotes_map || {}),
        ...(nextPayload.quotes_map || {}),
      },
      series_points: {
        ...((current || {}).series_points || {}),
        ...(nextPayload.series_points || {}),
      },
    };
    applyCoreTapeSnapshot(
      current.quotes_map || {},
      current.series_points || {},
      nextPayload.tape_meta || {}
    );
    return current;
  };
  window.addEventListener("resize", () => {
    if (pendingRenderTimer !== null) {
      window.clearTimeout(pendingRenderTimer);
    }
    pendingRenderTimer = window.setTimeout(() => {
      pendingRenderTimer = null;
      scheduleRender();
    }, RESIZE_RENDER_DEBOUNCE_MS);
  });
  window.dispatchEvent(new CustomEvent("market-pulse-core-ready"));
  dispatchStreamStatus("Live stream connecting", "Waiting for first tick…");

  const closeStream = () => {
    if (reconnectTimer !== null) {
      window.clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (!stream) return;
    try {
      stream.close();
    } catch (_err) {
      // no-op
    }
    stream = null;
  };

  const connectStream = () => {
    closeStream();
    if (!pageVisible) return;
    const streamUrl = new URL("/stream/market", window.location.origin);
    streamUrl.searchParams.set("ticker", String((state.base && state.base.ticker) || "QQQ").toUpperCase());
    stream = new EventSource(streamUrl.toString());
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
      queueStreamPayload(payload);
    };

    stream.onerror = () => {
      dispatchStreamStatus("Live stream retrying", "Connection dropped. Reconnecting to market feed…");
      closeStream();
      if (!pageVisible) return;
      reconnectTimer = window.setTimeout(connectStream, 3000);
    };
  };

  connectStream();
  document.addEventListener("visibilitychange", () => {
    pageVisible = document.visibilityState !== "hidden";
    if (!pageVisible) {
      closeStream();
      if (pendingStreamTimer !== null) {
        window.clearTimeout(pendingStreamTimer);
        pendingStreamTimer = null;
      }
      dispatchStreamStatus("Live stream paused", "Background tab");
      return;
    }
    scheduleRender();
    if (pendingStreamPayload) {
      const nextPayload = pendingStreamPayload;
      pendingStreamPayload = null;
      applyStreamPayload(nextPayload);
    }
    dispatchStreamStatus("Live stream connecting", "Restoring live feed…");
    connectStream();
  });
})();
