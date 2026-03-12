export type FlipProximityState = "at_flip" | "very_close" | "near" | "far" | "unavailable";
export type WallProximityState = "at_wall" | "very_close" | "near" | "far" | "unavailable";
export type TrapZoneState = "clear" | "compressed_trap_zone" | "knife_edge_structure" | "unavailable";

export type DealerRegime = "Positive Gamma / Mean Reverting" | "Negative Gamma / Momentum Amplifying" | "Unavailable";
export type VolatilityState = "Calm" | "Flat" | "Rising" | "Elevated" | "Expansion Risk";
export type StructureType =
  | "Stable Range"
  | "Clear Structure"
  | "Regime Transition"
  | "Knife Edge"
  | "Compression Zone"
  | "Unavailable";
export type SetupFitLabel = "Good" | "Caution" | "Poor";
export type SessionWindowState =
  | "Early Noise"
  | "Prime Window"
  | "Secondary Window"
  | "Midday Chop"
  | "Late Rebalance"
  | "Off Hours";
export type WallStrength = "Strong" | "Medium" | "Weak" | "Unknown";

export interface GammaContext {
  spot: number | null;
  dayOpen?: number | null;
  sessionHigh?: number | null;
  sessionLow?: number | null;
  priorDayHigh?: number | null;
  priorDayLow?: number | null;
  vwap?: number | null;
  overnightHigh?: number | null;
  overnightLow?: number | null;
  vix?: number | null;
  vixDirection?: "rising" | "flat" | "falling" | "unavailable";

  gammaFlip: number | null;
  callWall: number | null;
  putWall: number | null;
  nextCallWall?: number | null;
  nextPutWall?: number | null;
  netGamma: number | null;
  callWallGammaPerPoint?: number | null;
  putWallGammaPerPoint?: number | null;
  expectedMove?: number | null;
  expectedMoveUp?: number | null;
  expectedMoveDown?: number | null;

  regime?: string;
  bias?: string;
  candidateLevels?: number[];
  updatedAt?: string | number | null;
  marketTimeIso?: string | null;
  dataState?: string | null;
  freshnessLabel?: string | null;
  freshnessReason?: string | null;
}

export interface TradeabilityResult {
  score: number;
  label: "No Trade" | "Selective" | "Tradeable";
  explanation: string;
}

export interface SetupFitResult {
  label: SetupFitLabel;
  explanation: string;
}

export interface DerivedGammaContext {
  distanceToFlip: number | null;
  distanceToCallWall: number | null;
  distanceToPutWall: number | null;
  wallSpread: number | null;
  distanceToExpectedMoveHigh: number | null;
  distanceToExpectedMoveLow: number | null;
  aboveOrBelowFlip: "above" | "below" | "at" | "unavailable";
  insideExpectedMove: boolean | null;
  nearMajorWall: boolean;
  nearVWAP: boolean;
  nearPDH: boolean;
  nearPDL: boolean;
  sessionWindowState: SessionWindowState;

  flipProximityState: FlipProximityState;
  wallProximityState: WallProximityState;
  trapZoneState: TrapZoneState;

  dealerRegime: DealerRegime;
  volatilityState: VolatilityState;
  structureType: StructureType;
  tradeability: TradeabilityResult;
  reversalSetupFit: SetupFitResult;

  callWallStrength: WallStrength;
  putWallStrength: WallStrength;
  supportQuality: "Fragile Floor" | "Robust Support";
  resistanceQuality: "Fragile Ceiling" | "Strong Ceiling";

  noTradeCenter: boolean;
  dataQualityLabel: "Live" | "Slightly Delayed" | "Stale" | "Cached Fallback";
}

const asNum = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const clamp = (value: number, min: number, max: number): number => Math.min(max, Math.max(min, value));

const abs = (value: number | null | undefined): number | null => {
  const n = asNum(value);
  return n === null ? null : Math.abs(n);
};

export function formatNetGamma(value: number | null | undefined): string {
  const n = asNum(value);
  if (n === null) return "—";
  const absN = Math.abs(n);
  const sign = n < 0 ? "-" : n > 0 ? "+" : "";
  if (absN >= 1_000_000_000) return `${sign}${(absN / 1_000_000_000).toFixed(1)} billion`;
  if (absN >= 1_000_000) return `${sign}${(absN / 1_000_000).toFixed(1)} million`;
  if (absN >= 1_000) return `${sign}${(absN / 1_000).toFixed(1)} thousand`;
  return String(Math.round(n));
}

export function formatLevelDistance(distance: number | null | undefined): string {
  const value = asNum(distance);
  if (value === null) return "—";
  const pts = Math.abs(value);
  const unit = Math.abs(pts - 1.0) < 1e-9 ? "pt" : "pts";
  if (pts < 0.05) return `0 ${unit}`;
  const side = value > 0 ? "above" : "below";
  return `${pts.toFixed(1)} ${unit} ${side}`;
}

export function classifyProximity(distance: number | null | undefined, kind: "flip" | "wall"): FlipProximityState | WallProximityState {
  const value = asNum(distance);
  if (value === null) return "unavailable";
  const absValue = Math.abs(value);
  if (absValue <= 5) return kind === "flip" ? "at_flip" : "at_wall";
  if (absValue <= 10) return "very_close";
  if (absValue <= 20) return "near";
  return "far";
}

export function classifyTrapZone(
  spot: number | null | undefined,
  gammaFlip: number | null | undefined,
  callWall: number | null | undefined,
  putWall: number | null | undefined,
): TrapZoneState {
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

function inferLevelStep(callWall: number | null, putWall: number | null, candidates?: number[]): number {
  const vals = (Array.isArray(candidates) ? candidates : []).map(asNum).filter((v): v is number => v !== null);
  const uniq = Array.from(new Set(vals)).sort((a, b) => a - b);
  const diffs: number[] = [];
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

function inferExpectedMove(
  expectedMove: number | null | undefined,
  spot: number | null,
  gammaFlip: number | null,
  callWall: number | null,
  putWall: number | null,
  candidates?: number[],
): number | null {
  const direct = asNum(expectedMove);
  if (direct !== null) return direct;

  const s = asNum(spot);
  const g = asNum(gammaFlip);
  const c = asNum(callWall);
  const p = asNum(putWall);
  const ladder = (Array.isArray(candidates) ? candidates : [])
    .map((v) => asNum(v))
    .filter((v): v is number => v !== null)
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

export function classifyDealerRegime(netGamma: number | null | undefined): DealerRegime {
  const ng = asNum(netGamma);
  if (ng === null) return "Unavailable";
  return ng >= 0 ? "Positive Gamma / Mean Reverting" : "Negative Gamma / Momentum Amplifying";
}

export function classifyVolatilityState(input: GammaContext): VolatilityState {
  const vix = asNum(input.vix);
  const dir = String(input.vixDirection || "unavailable").toLowerCase();
  if (vix !== null && (vix >= 26 || (dir === "rising" && vix >= 22))) return "Expansion Risk";
  if (vix !== null && vix >= 22) return "Elevated";
  if (dir === "rising") return "Rising";
  if (dir === "flat") return "Flat";
  if (vix !== null && vix < 16) return "Calm";
  return "Flat";
}

export function classifyWallStrength(gammaPerPoint: number | null | undefined): WallStrength {
  const value = asNum(gammaPerPoint);
  if (value === null) return "Unknown";
  const millionPerPoint = Math.abs(value) / 1_000_000;
  if (millionPerPoint > 2.0) return "Strong";
  if (millionPerPoint >= 1.0) return "Medium";
  return "Weak";
}

export function classifyStructureType(input: GammaContext, derived: Pick<DerivedGammaContext, "distanceToFlip" | "wallSpread" | "trapZoneState">): StructureType {
  if (derived.trapZoneState === "knife_edge_structure") return "Knife Edge";
  if (derived.trapZoneState === "compressed_trap_zone") return "Compression Zone";
  if (abs(derived.distanceToFlip) !== null && (abs(derived.distanceToFlip) as number) <= 10) return "Regime Transition";
  if ((derived.wallSpread ?? 0) >= 25 && (abs(derived.distanceToFlip) ?? 0) > 15) return "Clear Structure";
  if (derived.wallSpread !== null) return "Stable Range";
  return "Unavailable";
}

export function classifySupportResistanceQuality(input: GammaContext, derived: Pick<DerivedGammaContext, "distanceToExpectedMoveLow" | "distanceToExpectedMoveHigh">): {
  supportQuality: "Fragile Floor" | "Robust Support";
  resistanceQuality: "Fragile Ceiling" | "Strong Ceiling";
} {
  const callStrength = classifyWallStrength(input.callWallGammaPerPoint);
  const putStrength = classifyWallStrength(input.putWallGammaPerPoint);
  const ng = asNum(input.netGamma);
  const vixDir = String(input.vixDirection || "unavailable").toLowerCase();

  const supportStrong = putStrength === "Strong" && (ng ?? 0) >= 0 && vixDir !== "rising" && ((derived.distanceToExpectedMoveLow ?? 999) > 5);
  const resistanceStrong = callStrength === "Strong" && (ng ?? 0) >= 0 && vixDir !== "rising" && ((derived.distanceToExpectedMoveHigh ?? 999) > 5);

  return {
    supportQuality: supportStrong ? "Robust Support" : "Fragile Floor",
    resistanceQuality: resistanceStrong ? "Strong Ceiling" : "Fragile Ceiling",
  };
}

export function detectNoTradeZone(derived: Pick<DerivedGammaContext, "distanceToFlip" | "wallSpread" | "structureType">): boolean {
  const dFlip = abs(derived.distanceToFlip);
  const spread = asNum(derived.wallSpread);
  const structure = derived.structureType;
  return Boolean(
    dFlip !== null
    && dFlip <= 5
    && spread !== null
    && spread <= 20
    && (structure === "Knife Edge" || structure === "Regime Transition"),
  );
}

export function getSessionWindowState(nowIso?: string | null): SessionWindowState {
  const now = nowIso ? new Date(nowIso) : new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  }).formatToParts(now);

  const hh = Number(parts.find((p) => p.type === "hour")?.value ?? "0");
  const mm = Number(parts.find((p) => p.type === "minute")?.value ?? "0");
  const mins = hh * 60 + mm;

  if (mins >= 570 && mins < 585) return "Early Noise";
  if (mins >= 585 && mins < 660) return "Prime Window";
  if (mins >= 660 && mins < 690) return "Secondary Window";
  if (mins >= 690 && mins < 810) return "Midday Chop";
  if (mins >= 870 && mins < 930) return "Late Rebalance";
  return "Off Hours";
}

function nearLevel(spot: number | null, level: number | null, threshold = 8): boolean {
  return spot !== null && level !== null && Math.abs(spot - level) <= threshold;
}

export function computeTradeabilityScore(input: GammaContext, derived: Pick<DerivedGammaContext,
  | "distanceToFlip"
  | "wallSpread"
  | "distanceToExpectedMoveHigh"
  | "distanceToExpectedMoveLow"
  | "structureType"
  | "volatilityState"
  | "nearMajorWall"
  | "nearPDH"
  | "nearPDL"
  | "insideExpectedMove"
  | "noTradeCenter"
>): TradeabilityResult {
  let score = 5;
  const dFlip = abs(derived.distanceToFlip) ?? 999;
  const wallSpread = asNum(derived.wallSpread) ?? 0;
  const netGamma = asNum(input.netGamma);
  const vState = derived.volatilityState;

  if (dFlip > 15 && derived.structureType === "Clear Structure") score += 2;
  if (wallSpread > 25) score += 1;
  if ((derived.distanceToExpectedMoveHigh ?? 0) > 8 || (derived.distanceToExpectedMoveLow ?? 0) > 8) score += 1;
  if ((vState === "Calm" || vState === "Flat") && netGamma !== null && netGamma >= 0) score += 1;
  if (derived.nearMajorWall || derived.nearPDH || derived.nearPDL) score += 1;

  if (dFlip <= 5) score -= 2;
  if (derived.structureType === "Knife Edge") score -= 2;
  if (derived.insideExpectedMove && (derived.distanceToExpectedMoveHigh ?? 999) <= 5 && (derived.distanceToExpectedMoveLow ?? 999) <= 5) score -= 1;
  if ((netGamma ?? 0) < 0 && dFlip <= 10) score -= 1;
  if (["Knife Edge", "Regime Transition"].includes(derived.structureType)) score -= 1;
  if (derived.noTradeCenter) score -= 1;

  score = clamp(Math.round(score), 0, 10);
  const label: TradeabilityResult["label"] = score <= 3 ? "No Trade" : score <= 6 ? "Selective" : "Tradeable";
  const explanation = label === "No Trade"
    ? "No-trade center risk. Wait for edge interaction."
    : label === "Selective"
      ? "Structure is mixed. Only take edge-confirmed setups."
      : "Structure supports disciplined edge trades.";
  return { score, label, explanation };
}

export function classifyReversalSetupFit(input: GammaContext, derived: Pick<DerivedGammaContext,
  | "dealerRegime"
  | "structureType"
  | "distanceToFlip"
  | "nearMajorWall"
  | "volatilityState"
  | "sessionWindowState"
  | "distanceToExpectedMoveHigh"
  | "distanceToExpectedMoveLow"
  | "noTradeCenter"
>, tradeability: TradeabilityResult): SetupFitResult {
  const dFlip = abs(derived.distanceToFlip) ?? 999;
  const nearFlip = dFlip <= 8;
  const expectedNearlyConsumed = (derived.distanceToExpectedMoveHigh ?? 999) <= 5 || (derived.distanceToExpectedMoveLow ?? 999) <= 5;
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
    (derived.dealerRegime === "Positive Gamma / Mean Reverting" || derived.structureType === "Stable Range" || derived.structureType === "Clear Structure")
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

export function buildAutoRead(input: GammaContext, derived: DerivedGammaContext): string[] {
  const bullets: string[] = [];
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

export function buildWarningBadges(input: GammaContext, derived: DerivedGammaContext): string[] {
  const badges: string[] = [];
  if (derived.structureType === "Knife Edge") badges.push("Knife Edge");
  if (derived.structureType === "Regime Transition") badges.push("Regime Transition");
  if (derived.volatilityState === "Expansion Risk") badges.push("Expansion Risk");
  if (derived.sessionWindowState === "Midday Chop") badges.push("Chop Risk");
  if (derived.distanceToCallWall !== null && Math.abs(derived.distanceToCallWall) <= 10) badges.push("Call Wall Test");
  if (derived.distanceToPutWall !== null && Math.abs(derived.distanceToPutWall) <= 10) badges.push("Put Wall Test");
  if (derived.distanceToFlip !== null && Math.abs(derived.distanceToFlip) <= 10) badges.push("Near Flip");
  if ((derived.distanceToExpectedMoveHigh ?? 999) <= 2 || (derived.distanceToExpectedMoveLow ?? 999) <= 2) {
    badges.push("Expected Move Exhausted");
  }
  if (derived.noTradeCenter) badges.push("No-Trade Center");
  if (derived.supportQuality === "Fragile Floor") badges.push("Fragile Floor");
  if (derived.supportQuality === "Robust Support") badges.push("Robust Support");

  return Array.from(new Set(badges));
}

export function computeDistanceMetrics(input: GammaContext): DerivedGammaContext {
  const spot = asNum(input.spot);
  const gammaFlip = asNum(input.gammaFlip);
  const callWall = asNum(input.callWall);
  const putWall = asNum(input.putWall);

  let nextCall = asNum(input.nextCallWall);
  let nextPut = asNum(input.nextPutWall);
  if ((nextCall === null && callWall !== null) || (nextPut === null && putWall !== null)) {
    const step = inferLevelStep(callWall, putWall, input.candidateLevels);
    if (nextCall === null && callWall !== null) nextCall = callWall + step;
    if (nextPut === null && putWall !== null) nextPut = putWall - step;
  }

  const expectedMove = inferExpectedMove(
    input.expectedMove,
    spot,
    gammaFlip,
    callWall,
    putWall,
    input.candidateLevels,
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

  const flipState = classifyProximity(distanceToFlip, "flip") as FlipProximityState;
  const wallState = classifyProximity(
    distanceToCallWall !== null && distanceToPutWall !== null
      ? (Math.abs(distanceToCallWall) <= Math.abs(distanceToPutWall) ? distanceToCallWall : distanceToPutWall)
      : (distanceToCallWall ?? distanceToPutWall),
    "wall",
  ) as WallProximityState;
  const trapZoneState = classifyTrapZone(spot, gammaFlip, callWall, putWall);

  const aboveOrBelowFlip = distanceToFlip === null ? "unavailable" : distanceToFlip > 0 ? "above" : distanceToFlip < 0 ? "below" : "at";
  const nearMajorWall = (abs(distanceToCallWall) ?? 999) <= 10 || (abs(distanceToPutWall) ?? 999) <= 10;
  const nearVWAP = nearLevel(spot, asNum(input.vwap), 6);
  const nearPDH = nearLevel(spot, asNum(input.priorDayHigh), 8);
  const nearPDL = nearLevel(spot, asNum(input.priorDayLow), 8);

  const dealerRegime = classifyDealerRegime(input.netGamma);
  const volatilityState = classifyVolatilityState(input);
  const sessionWindowState = getSessionWindowState(input.marketTimeIso || null);

  const structureType = classifyStructureType(input, { distanceToFlip, wallSpread, trapZoneState });

  const callWallStrength = classifyWallStrength(input.callWallGammaPerPoint);
  const putWallStrength = classifyWallStrength(input.putWallGammaPerPoint);
  const levelQuality = classifySupportResistanceQuality(input, { distanceToExpectedMoveLow, distanceToExpectedMoveHigh });

  const noTradeCenter = detectNoTradeZone({ distanceToFlip, wallSpread, structureType });

  const qualityRaw = String(input.dataState || "").toLowerCase();
  const freshness = String(input.freshnessLabel || "").toLowerCase();
  const dataQualityLabel: DerivedGammaContext["dataQualityLabel"] = qualityRaw === "cached" || freshness.includes("cached")
    ? "Cached Fallback"
    : qualityRaw === "delayed" || freshness.includes("delayed")
      ? "Slightly Delayed"
      : freshness.includes("stale") || freshness.includes("critical")
        ? "Stale"
        : "Live";

  const tradeability = computeTradeabilityScore(input, {
    distanceToFlip,
    wallSpread,
    distanceToExpectedMoveHigh,
    distanceToExpectedMoveLow,
    structureType,
    volatilityState,
    nearMajorWall,
    nearPDH,
    nearPDL,
    insideExpectedMove,
    noTradeCenter,
  });

  const reversalSetupFit = classifyReversalSetupFit(input, {
    dealerRegime,
    structureType,
    distanceToFlip,
    nearMajorWall,
    volatilityState,
    sessionWindowState,
    distanceToExpectedMoveHigh,
    distanceToExpectedMoveLow,
    noTradeCenter,
  }, tradeability);

  return {
    distanceToFlip,
    distanceToCallWall,
    distanceToPutWall,
    wallSpread,
    distanceToExpectedMoveHigh,
    distanceToExpectedMoveLow,
    aboveOrBelowFlip,
    insideExpectedMove,
    nearMajorWall,
    nearVWAP,
    nearPDH,
    nearPDL,
    sessionWindowState,
    flipProximityState: flipState,
    wallProximityState: wallState,
    trapZoneState,
    dealerRegime,
    volatilityState,
    structureType,
    tradeability,
    reversalSetupFit,
    callWallStrength,
    putWallStrength,
    supportQuality: levelQuality.supportQuality,
    resistanceQuality: levelQuality.resistanceQuality,
    noTradeCenter,
    dataQualityLabel,
  };
}

export function buildGammaNarrative(input: GammaContext, derived: DerivedGammaContext): string[] {
  return buildAutoRead(input, derived);
}
