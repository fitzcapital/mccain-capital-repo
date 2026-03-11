export type FlipProximityState = "at_flip" | "very_close" | "near" | "far" | "unavailable";
export type WallProximityState = "at_wall" | "very_close" | "near" | "far" | "unavailable";
export type TrapZoneState = "clear" | "compressed_trap_zone" | "knife_edge_structure" | "unavailable";

export interface GammaContextInput {
  spot: number | null;
  gammaFlip: number | null;
  callWall: number | null;
  putWall: number | null;
  nextCallWallAbove?: number | null;
  nextPutWallBelow?: number | null;
  netGamma: number | null;
  regime?: string;
  bias?: string;
  expectedMove?: number | null;
  candidateLevels?: number[];
  updatedAt?: string | number | null;
}

export interface DistanceMetrics {
  distance_to_flip: number | null;
  distance_to_call_wall: number | null;
  distance_to_put_wall: number | null;
  next_call_wall_above: number | null;
  next_put_wall_below: number | null;
  expected_move_up: number | null;
  expected_move_down: number | null;
  expected_move_range_text: string;
  flip_proximity_state: FlipProximityState;
  wall_proximity_state: WallProximityState;
  trap_zone_state: TrapZoneState;
}

const asNum = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
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

export function classifyProximity(
  distance: number | null | undefined,
  kind: "flip" | "wall",
): FlipProximityState | WallProximityState {
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

export function computeDistanceMetrics(input: GammaContextInput): DistanceMetrics {
  const spot = asNum(input.spot);
  const gammaFlip = asNum(input.gammaFlip);
  const callWall = asNum(input.callWall);
  const putWall = asNum(input.putWall);
  const providedExpectedMove = asNum(input.expectedMove);
  const expectedMove =
    providedExpectedMove !== null
      ? providedExpectedMove
      : spot !== null && callWall !== null && putWall !== null
        ? Math.max(Math.abs(callWall - spot), Math.abs(spot - putWall))
        : callWall !== null && putWall !== null
          ? Math.abs(callWall - putWall) / 2.0
          : spot !== null && gammaFlip !== null
            ? Math.abs(spot - gammaFlip)
            : null;

  const distanceToFlip = spot !== null && gammaFlip !== null ? spot - gammaFlip : null;
  const distanceToCallWall = spot !== null && callWall !== null ? spot - callWall : null;
  const distanceToPutWall = spot !== null && putWall !== null ? spot - putWall : null;

  const expectedMoveUp = spot !== null && expectedMove !== null ? spot + expectedMove : null;
  const expectedMoveDown = spot !== null && expectedMove !== null ? spot - expectedMove : null;

  const step =
    callWall !== null && putWall !== null
      ? Math.max(5, Math.min(25, Math.round((Math.abs(callWall - putWall) / 4.0) / 5.0) * 5.0))
      : 10;

  return {
    distance_to_flip: distanceToFlip,
    distance_to_call_wall: distanceToCallWall,
    distance_to_put_wall: distanceToPutWall,
    next_call_wall_above:
      asNum(input.nextCallWallAbove) !== null
        ? asNum(input.nextCallWallAbove)
        : callWall !== null
          ? callWall + step
          : null,
    next_put_wall_below:
      asNum(input.nextPutWallBelow) !== null
        ? asNum(input.nextPutWallBelow)
        : putWall !== null
          ? putWall - step
          : null,
    expected_move_up: expectedMoveUp,
    expected_move_down: expectedMoveDown,
    expected_move_range_text:
      expectedMoveUp !== null && expectedMoveDown !== null
        ? `${expectedMoveDown.toFixed(1)} - ${expectedMoveUp.toFixed(1)}`
        : expectedMove !== null
          ? `±${expectedMove.toFixed(1)}`
          : "—",
    flip_proximity_state: classifyProximity(distanceToFlip, "flip") as FlipProximityState,
    wall_proximity_state: classifyProximity(
      distanceToCallWall !== null && distanceToPutWall !== null
        ? Math.abs(distanceToCallWall) <= Math.abs(distanceToPutWall)
          ? distanceToCallWall
          : distanceToPutWall
        : distanceToCallWall !== null
          ? distanceToCallWall
          : distanceToPutWall,
      "wall",
    ) as WallProximityState,
    trap_zone_state: classifyTrapZone(spot, gammaFlip, callWall, putWall),
  };
}

export function buildGammaNarrative(input: GammaContextInput, metrics: DistanceMetrics): string[] {
  const notes: string[] = [];
  if (metrics.distance_to_flip !== null) {
    const side = metrics.distance_to_flip > 0 ? "above" : "below";
    notes.push(`SPX is ${Math.abs(metrics.distance_to_flip).toFixed(1)} points ${side} gamma flip.`);
  }
  if (input.netGamma !== null && input.netGamma !== undefined && metrics.distance_to_flip !== null) {
    if (input.netGamma > 0 && metrics.distance_to_flip > 0) {
      notes.push("Positive gamma, stabilizing tape. Mean reversion is favored.");
    } else if (input.netGamma > 0 && metrics.distance_to_flip <= 0) {
      notes.push("Positive gamma but below flip. Contained downside unless support fails.");
    } else if (input.netGamma < 0 && metrics.distance_to_flip <= 0) {
      notes.push("Negative gamma, downside expansion risk.");
    } else if (input.netGamma < 0 && metrics.distance_to_flip > 0) {
      notes.push("Negative gamma with upside squeeze risk.");
    }
    if (Math.abs(metrics.distance_to_flip) <= 10) {
      notes.push("Market is near a regime transition. Expect fakeouts and unstable direction.");
    }
  }
  if (!notes.length) {
    notes.push("Awaiting complete live gamma context for SPX.");
  }
  if (notes.length === 1) {
    notes.push("Waiting for full level alignment before issuing directional context.");
  }
  return notes.slice(0, 4);
}
