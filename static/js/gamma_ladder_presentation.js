(function (rootScope, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (rootScope) rootScope.GammaLadderPresentation = api;
})(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  const numeric = (value) => {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const strikeKey = (value) => {
    const parsed = numeric(value);
    return parsed === null ? "" : String(parsed);
  };

  const contextKey = (context) => {
    const source = context && typeof context === "object" ? context : {};
    return [
      String(source.symbol || "").toUpperCase(),
      String(source.dte || ""),
      String(source.expiration || ""),
      String(source.window || "").toLowerCase(),
    ].join("|");
  };

  const rowRole = (row) => {
    const source = row && typeof row === "object" ? row : {};
    if (source.is_spot_nearest) return "current";
    if (source.is_strongest) return "magnet";
    if (source.is_flip) return "flip";
    return String(source.level_type || source.classification || source.role || "level")
      .trim()
      .toLowerCase();
  };

  const createPresentationSnapshot = (payload, context) => {
    const source = payload && typeof payload === "object" ? payload : {};
    const rows = Array.isArray(source.rows) ? source.rows : [];
    const normalizedRows = {};
    const order = [];
    rows.forEach((row) => {
      const key = strikeKey(row && row.strike);
      if (!key) return;
      const netGex = numeric(row.net_gex) || 0;
      const role = rowRole(row);
      order.push(key);
      normalizedRows[key] = {
        strike: numeric(row.strike),
        callGex: numeric(row.call_gex) || 0,
        putGex: numeric(row.put_gex) || 0,
        netGex,
        sign: netGex > 0 ? "positive" : netGex < 0 ? "negative" : "neutral",
        classification: String(row.classification || "unclassified").trim().toLowerCase(),
        role,
        structural: Boolean(
          row.is_flip || row.is_strongest || [
            "flip",
            "magnet",
            "support",
            "resistance",
            "acceleration",
            "acceleration zone",
          ].includes(role),
        ),
        isFlip: Boolean(row.is_flip),
        isSpot: Boolean(row.is_spot_nearest),
        isStrongest: Boolean(row.is_strongest),
      };
    });
    const strongestStrike = order.find((key) => normalizedRows[key].isStrongest) || "";
    return {
      contextKey: contextKey(context),
      symbol: String(context?.symbol || source.symbol || "").toUpperCase(),
      spot: numeric(source.spot),
      regime: String(source.regime || "mixed_gamma"),
      strongestStrike,
      order,
      rows: normalizedRows,
    };
  };

  const sameRow = (before, after) => Boolean(
    before && after &&
    before.callGex === after.callGex &&
    before.putGex === after.putGex &&
    before.netGex === after.netGex &&
    before.sign === after.sign &&
    before.classification === after.classification &&
    before.role === after.role &&
    before.isFlip === after.isFlip &&
    before.isSpot === after.isSpot &&
    before.isStrongest === after.isStrongest
  );

  const crossedStrikes = (before, after) => {
    if (!before || !after || before.spot === null || after.spot === null) return [];
    if (before.spot === after.spot) return [];
    return after.order.filter((key) => {
      const row = after.rows[key];
      if (!row || !row.structural) return false;
      const strike = row.strike;
      return (
        (before.spot < strike && after.spot >= strike) ||
        (before.spot > strike && after.spot <= strike)
      );
    });
  };

  const diffPresentationSnapshots = (before, after) => {
    const previous = before && typeof before === "object" ? before : null;
    const next = after && typeof after === "object" ? after : null;
    if (!next) {
      return {
        contextChanged: false,
        inserted: [],
        removed: [],
        reordered: [],
        changed: [],
        crossed: [],
        spotMoved: false,
        regimeChanged: false,
        strongestChanged: false,
        unchanged: true,
      };
    }
    if (!previous || previous.contextKey !== next.contextKey) {
      return {
        contextChanged: Boolean(previous),
        inserted: [...next.order],
        removed: previous ? [...previous.order] : [],
        reordered: [],
        changed: [],
        crossed: [],
        spotMoved: false,
        regimeChanged: false,
        strongestChanged: false,
        unchanged: next.order.length === 0,
      };
    }
    const inserted = next.order.filter((key) => !previous.rows[key]);
    const removed = previous.order.filter((key) => !next.rows[key]);
    const reordered = next.order.filter((key, index) => (
      Boolean(previous.rows[key]) && previous.order.indexOf(key) !== index
    ));
    const changed = next.order.filter((key) => (
      Boolean(previous.rows[key]) && !sameRow(previous.rows[key], next.rows[key])
    ));
    const crossed = crossedStrikes(previous, next);
    const spotMoved = previous.spot !== next.spot;
    const regimeChanged = previous.regime !== next.regime;
    const strongestChanged = previous.strongestStrike !== next.strongestStrike;
    const unchanged = !(
      inserted.length || removed.length || reordered.length || changed.length || crossed.length ||
      spotMoved || regimeChanged || strongestChanged
    );
    return {
      contextChanged: false,
      inserted,
      removed,
      reordered,
      changed,
      crossed,
      spotMoved,
      regimeChanged,
      strongestChanged,
      unchanged,
    };
  };

  const shouldAcceptPayload = (input) => {
    const source = input && typeof input === "object" ? input : {};
    if (Number(source.requestId) !== Number(source.latestRequestId)) return false;
    if (String(source.symbol || "").toUpperCase() !== String(source.activeSymbol || "").toUpperCase()) {
      return false;
    }
    if (source.contextKey && source.activeContextKey && source.contextKey !== source.activeContextKey) {
      return false;
    }
    const timestamp = Number(source.timestamp) || 0;
    const lastTimestamp = Number(source.lastTimestamp) || 0;
    if (timestamp && lastTimestamp && timestamp < lastTimestamp) return false;
    return true;
  };

  const shouldAnimate = (input) => {
    const source = input && typeof input === "object" ? input : {};
    return source.accepted === true && source.reducedMotion !== true && source.unchanged !== true;
  };

  return {
    contextKey,
    createPresentationSnapshot,
    diffPresentationSnapshots,
    shouldAcceptPayload,
    shouldAnimate,
  };
});
