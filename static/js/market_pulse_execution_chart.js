(() => {
  "use strict";

  if (typeof document === "undefined") return;

  const chartHost = document.getElementById("spxExecutionHeroChart");
  const chartSvg = document.getElementById("spxExecutionHeroChartSvg");
  const chartEmpty = document.getElementById("spxExecutionHeroChartEmpty");

  if (!chartHost || !chartSvg) {
    window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
    return;
  }

  const SVG_NS = "http://www.w3.org/2000/svg";
  const VIEWBOX = { width: 960, height: 400 };
  const MARGIN = { top: 20, right: 116, bottom: 26, left: 18 };
  const LABEL_GAP = 22;

  const asNum = (value) => {
    if (value === null || value === undefined || value === "") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const getJson = (id) => {
    const node = document.getElementById(id);
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "null");
    } catch (_err) {
      return null;
    }
  };

  const formatNumber = (value, digits = 0) => {
    const n = asNum(value);
    return n === null
      ? "Unavailable"
      : n.toLocaleString("en-US", {
          minimumFractionDigits: digits,
          maximumFractionDigits: digits,
        });
  };

  const normalizePoints = (rows) => {
    const out = [];
    const seen = new Set();
    for (const row of Array.isArray(rows) ? rows : []) {
      if (!row || typeof row !== "object") continue;
      const ts = String(row.ts || "").trim();
      const price = asNum(row.price ?? row.v ?? row.close);
      if (!ts || price === null || seen.has(ts)) continue;
      const stamp = Date.parse(ts);
      if (!Number.isFinite(stamp)) continue;
      seen.add(ts);
      out.push({ ts, stamp, price });
    }
    return out.sort((a, b) => a.stamp - b.stamp);
  };

  const normalizeLevels = (rows) =>
    (Array.isArray(rows) ? rows : [])
      .map((row) => ({
        key: String((row && row.key) || "").trim(),
        value: asNum(row && row.value),
      }))
      .filter((row) => row.key && row.value !== null);

  const localFlipFromGamma = (gamma, fallback = null) => {
    const snapshot = gamma && typeof gamma === "object" ? gamma : {};
    if (Object.prototype.hasOwnProperty.call(snapshot, "local_flip_aggregated_gamma")) {
      return asNum(snapshot.local_flip_aggregated_gamma);
    }
    if (Object.prototype.hasOwnProperty.call(snapshot, "local_flip")) {
      return asNum(snapshot.local_flip);
    }
    return fallback;
  };

  const createSvg = (tag, attrs = {}) => {
    const node = document.createElementNS(SVG_NS, tag);
    Object.entries(attrs).forEach(([key, value]) => {
      if (value !== null && value !== undefined) node.setAttribute(key, String(value));
    });
    return node;
  };

  const clearNode = (node) => {
    while (node.firstChild) node.removeChild(node.firstChild);
  };

  const fallbackLevel = (name) => asNum(chartHost.dataset[name]);

  const resolveLevels = (payload, points) => {
    const modelLevels =
      payload && payload.execution_model && payload.execution_model.levels && typeof payload.execution_model.levels === "object"
        ? payload.execution_model.levels
        : {};
    const levelMap = new Map(normalizeLevels(payload && payload.levels).map((level) => [level.key, level.value]));
    const lastPoint = points.length ? points[points.length - 1] : null;
    const gammaMap = payload && payload.gamma_map && typeof payload.gamma_map === "object" ? payload.gamma_map : {};

    return {
      spot:
        (lastPoint && lastPoint.price)
        ?? asNum(payload && payload.latest_price)
        ?? asNum(modelLevels.spot)
        ?? fallbackLevel("spot"),
      mainFlip:
        levelMap.get("gamma_flip")
        ?? asNum(modelLevels.main_flip)
        ?? fallbackLevel("mainFlip"),
      localFlip:
        levelMap.get("local_flip")
        ?? localFlipFromGamma(gammaMap, asNum(modelLevels.local_flip))
        ?? fallbackLevel("localFlip"),
      callWall:
        levelMap.get("call_wall")
        ?? asNum(modelLevels.call_wall)
        ?? fallbackLevel("callWall"),
      putWall:
        levelMap.get("put_wall")
        ?? asNum(modelLevels.put_wall)
        ?? fallbackLevel("putWall"),
      nextCallWall:
        asNum(payload && payload.next_call_wall_above)
        ?? asNum(gammaMap.next_call_wall_above)
        ?? asNum(modelLevels.next_call_wall_above)
        ?? asNum(modelLevels.next_call_wall)
        ?? fallbackLevel("nextCallWall"),
      nextPutWall:
        asNum(payload && payload.next_put_wall_below)
        ?? asNum(gammaMap.next_put_wall_below)
        ?? asNum(modelLevels.next_put_wall_below)
        ?? asNum(modelLevels.next_put_wall)
        ?? fallbackLevel("nextPutWall"),
    };
  };

  const buildSummary = (levels) => {
    const spot = asNum(levels.spot);
    const local = asNum(levels.localFlip);
    const call = asNum(levels.callWall);
    const put = asNum(levels.putWall);
    const nextCall = asNum(levels.nextCallWall);
    const nextPut = asNum(levels.nextPutWall);

    if (spot === null) {
      return {
        currentRead: "Unavailable",
        pullbackLevel: "Unavailable",
        nextDestination: "Await live levels",
        banner: "WAIT — AWAITING LIVE LEVELS",
        bannerSub: "Chart is mounted, but spot and gamma levels have not populated yet.",
      };
    }

    if (call !== null && spot > call) {
      return {
        currentRead: "Above Call Wall",
        pullbackLevel: `CW ${formatNumber(call, 0)}`,
        nextDestination: nextCall !== null ? `NCW ${formatNumber(nextCall, 0)}` : "Expansion zone",
        banner: "NO TRADE — EXTENDED ABOVE CALL WALL",
        bannerSub: `Wait for pullback into ${formatNumber(call, 0)} and confirmation.`,
      };
    }

    if (local !== null && call !== null && spot >= local && spot <= call) {
      return {
        currentRead: "Above Local Flip",
        pullbackLevel: `LF ${formatNumber(local, 0)}`,
        nextDestination: `CW ${formatNumber(call, 0)}`,
        banner: "WAIT — ABOVE LOCAL FLIP",
        bannerSub: `Buy dips only. Pullback needs to hold ${formatNumber(local, 0)} before continuation can press ${formatNumber(call, 0)}.`,
      };
    }

    if (put !== null && spot < put) {
      return {
        currentRead: "Below Put Wall",
        pullbackLevel: `PW ${formatNumber(put, 0)}`,
        nextDestination: nextPut !== null ? `NPW ${formatNumber(nextPut, 0)}` : "Downside expansion zone",
        banner: "WAIT — BELOW PUT WALL",
        bannerSub: `Downside is active. Only press after failed reclaim or clean continuation below ${formatNumber(put, 0)}.`,
      };
    }

    if (local !== null && spot < local) {
      return {
        currentRead: "Below Local Flip",
        pullbackLevel: `LF ${formatNumber(local, 0)}`,
        nextDestination: put !== null && spot >= put ? `PW ${formatNumber(put, 0)}` : nextPut !== null ? `NPW ${formatNumber(nextPut, 0)}` : "Downside shelf",
        banner: "WAIT — BELOW LOCAL FLIP",
        bannerSub: `Sell rips only. Failed reclaim into ${formatNumber(local, 0)} is the short trigger area.`,
      };
    }

    return {
      currentRead: "Responsive / rotational",
      pullbackLevel: local !== null ? `LF ${formatNumber(local, 0)}` : "Working level",
      nextDestination: call !== null ? `CW ${formatNumber(call, 0)}` : "Await next level",
      banner: "WAIT — RESPONSIVE TAPE",
      bannerSub: "Price is between major walls. Wait for the next clean level interaction.",
    };
  };

  const setText = (id, value) => {
    const node = document.getElementById(id);
    if (node) node.textContent = value;
  };

  const setStateTone = (id, state) => {
    const node = document.getElementById(id);
    if (!node) return;
    node.classList.remove("tone-positive", "tone-warn", "tone-negative");
    node.classList.add(
      state === "READY" ? "tone-positive" : state === "NO TRADE" ? "tone-negative" : "tone-warn"
    );
  };

  const deriveState = (levels) => {
    const spot = asNum(levels.spot);
    const local = asNum(levels.localFlip);
    const call = asNum(levels.callWall);
    if (spot === null) return "WAIT";
    if (call !== null && spot > call) return "NO TRADE";
    if (local !== null && spot > local) return "WAIT";
    if (local !== null && spot < local) return "WAIT";
    return "WAIT";
  };

  const stripAction = (summary, levels) => {
    const pullback = String(summary.pullbackLevel || "").replace(/^LF\s+/, "").replace(/^CW\s+/, "").replace(/^PW\s+/, "");
    if (summary.currentRead === "Above Call Wall" && pullback) return `Wait for pullback into ${pullback}`;
    if (summary.currentRead === "Above Local Flip" && pullback) return `Buy dips into ${pullback}`;
    if (summary.currentRead === "Below Local Flip" && pullback) return `Sell failed pops into ${pullback}`;
    if (summary.currentRead === "Below Put Wall" && pullback) return `Wait for failed reclaim into ${pullback}`;
    return "Wait for clean interaction";
  };

  const buildPath = (points, xFor, yFor) =>
    points
      .map((point, index) => `${index === 0 ? "M" : "L"}${xFor(point.stamp).toFixed(2)},${yFor(point.price).toFixed(2)}`)
      .join(" ");

  const buildAreaPath = (points, xFor, yFor, baselineY) => {
    if (points.length < 2) return "";
    const line = buildPath(points, xFor, yFor);
    const last = points[points.length - 1];
    const first = points[0];
    return `${line} L${xFor(last.stamp).toFixed(2)},${baselineY.toFixed(2)} L${xFor(first.stamp).toFixed(2)},${baselineY.toFixed(2)} Z`;
  };

  const mergeLivePoints = (currentPoints, streamPoints) => {
    const byTs = new Map(normalizePoints(currentPoints).map((point) => [point.ts, point]));
    normalizePoints(streamPoints).forEach((point) => {
      byTs.set(point.ts, point);
    });
    return Array.from(byTs.values()).sort((a, b) => a.stamp - b.stamp).slice(-240);
  };

  const mergePayload = (current, streamPayload) => {
    const next = { ...current };
    const streamPoints =
      (((streamPayload || {}).series_points || {}).SPX)
      || (((streamPayload || {}).series_points || {})["^GSPC"])
      || [];
    const normalizedStreamPoints = normalizePoints(streamPoints);

    if (normalizedStreamPoints.length >= 2) {
      next.points = mergeLivePoints(next.points, normalizedStreamPoints);
    }

    const gamma = streamPayload && streamPayload.gamma_map && typeof streamPayload.gamma_map === "object"
      ? streamPayload.gamma_map
      : null;
    if (gamma) {
      const levels = new Map(normalizeLevels(next.levels).map((level) => [level.key, level]));
      const patch = {
        gamma_flip: gamma.gamma_flip_combined_basket,
        local_flip: localFlipFromGamma(gamma),
        call_wall: gamma.call_wall_aggregated_gamma,
        put_wall: gamma.put_wall_aggregated_gamma,
      };
      Object.entries(patch).forEach(([key, value]) => {
        const n = asNum(value);
        if (n === null) return;
        levels.set(key, { key, value: n });
      });
      next.levels = Array.from(levels.values());
      next.next_call_wall_above = asNum(gamma.next_call_wall_above) ?? next.next_call_wall_above ?? null;
      next.next_put_wall_below = asNum(gamma.next_put_wall_below) ?? next.next_put_wall_below ?? null;
    }

    if (streamPayload && streamPayload.execution_model && typeof streamPayload.execution_model === "object") {
      next.execution_model = streamPayload.execution_model;
    }

    return next;
  };

  const drawZoneBand = (group, x, width, yTop, yBottom, className) => {
    if (!Number.isFinite(yTop) || !Number.isFinite(yBottom)) return;
    const top = Math.min(yTop, yBottom);
    const height = Math.max(0, Math.abs(yBottom - yTop));
    if (height < 2) return;
    group.appendChild(
      createSvg("rect", {
        x,
        y: top,
        width,
        height,
        rx: 14,
        class: `marketPulseExecutionHeroChartZone ${className}`,
      })
    );
  };

  const drawLabel = (group, plotRight, y, text, className = "") => {
    const safeText = String(text || "");
    const approxWidth = Math.max(52, Math.min(104, (safeText.length * 6.5) + 16));
    const height = 18;
    const x = plotRight + 8;
    const pill = createSvg("rect", {
      x,
      y: y - (height / 2),
      width: approxWidth,
      height,
      rx: 9,
      class: `marketPulseExecutionHeroChartLabelPill${className ? ` ${className}` : ""}`,
    });
    const label = createSvg("text", {
      x: x + (approxWidth / 2),
      y: y + 3.5,
      "text-anchor": "middle",
      class: "marketPulseExecutionHeroChartLabelText",
    });
    label.textContent = safeText;
    group.append(pill, label);
  };

  const render = (payload) => {
    const points = normalizePoints(payload && payload.points);
    const levels = resolveLevels(payload || {}, points);
    const summary = buildSummary(levels);
    const state = deriveState(levels);

    setText("marketPulseHeroChartBanner", summary.banner);
    setText("marketPulseHeroChartBannerSub", summary.bannerSub);
    setText("marketPulseHeroRailFootState", summary.currentRead);
    setText("marketPulseHeroPullbackLevel", summary.pullbackLevel);
    setText("marketPulseHeroDestinationInline", summary.nextDestination);
    setText("marketPulseHeroChartStateChip", state);
    setText("marketPulseHeroChartStateRead", summary.currentRead);
    setText("marketPulseHeroChartStateAction", stripAction(summary, levels));
    setStateTone("marketPulseHeroChartStateChip", state);

    const numericLevels = [
      levels.spot,
      levels.mainFlip,
      levels.localFlip,
      levels.callWall,
      levels.putWall,
      levels.nextCallWall,
      levels.nextPutWall,
    ].filter((value) => value !== null);

    if (!points.length || !numericLevels.length) {
      clearNode(chartSvg);
      if (chartEmpty) chartEmpty.hidden = false;
      window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
      return;
    }

    if (chartEmpty) chartEmpty.hidden = true;
    clearNode(chartSvg);

    const plot = {
      x: MARGIN.left,
      y: MARGIN.top,
      width: VIEWBOX.width - MARGIN.left - MARGIN.right,
      height: VIEWBOX.height - MARGIN.top - MARGIN.bottom,
    };

    const prices = points.map((point) => point.price);
    const yValues = prices.concat(numericLevels);
    const minY = Math.min(...yValues);
    const maxY = Math.max(...yValues);
    const yPad = Math.max(8, (maxY - minY) * 0.14);
    const domainMin = minY - yPad;
    const domainMax = maxY + yPad;
    const xMin = points[0].stamp;
    const xMax = points[points.length - 1].stamp;
    const xSpan = Math.max(1, xMax - xMin);
    const ySpan = Math.max(1, domainMax - domainMin);
    const xFor = (stamp) => plot.x + (((stamp - xMin) / xSpan) * plot.width);
    const yFor = (value) => plot.y + (plot.height - (((value - domainMin) / ySpan) * plot.height));

    const zones = createSvg("g");
    const grid = createSvg("g");
    const levelLines = createSvg("g");
    const pathLayer = createSvg("g");
    const labels = createSvg("g");
    const defs = createSvg("defs");

    const areaGradient = createSvg("linearGradient", {
      id: "marketPulseExecutionHeroAreaGradient",
      x1: "0",
      y1: "0",
      x2: "0",
      y2: "1",
    });
    areaGradient.append(
      createSvg("stop", { offset: "0%", "stop-color": "#76d6ff", "stop-opacity": ".24" }),
      createSvg("stop", { offset: "55%", "stop-color": "#57aae1", "stop-opacity": ".12" }),
      createSvg("stop", { offset: "100%", "stop-color": "#0c1524", "stop-opacity": "0" }),
    );
    defs.appendChild(areaGradient);

    for (let index = 0; index <= 4; index += 1) {
      const y = plot.y + ((plot.height / 4) * index);
      grid.appendChild(createSvg("line", {
        x1: plot.x,
        y1: y,
        x2: plot.x + plot.width,
        y2: y,
        class: "marketPulseExecutionHeroChartGridLine",
      }));
      const value = domainMax - ((ySpan / 4) * index);
      const axis = createSvg("text", {
        x: plot.x + plot.width + 10,
        y: y + 4,
        class: "marketPulseExecutionHeroChartAxisLabel",
      });
      axis.textContent = formatNumber(value, 0);
      labels.appendChild(axis);
    }
    for (let index = 0; index <= 5; index += 1) {
      const x = plot.x + ((plot.width / 5) * index);
      grid.appendChild(createSvg("line", {
        x1: x,
        y1: plot.y,
        x2: x,
        y2: plot.y + plot.height,
        class: "marketPulseExecutionHeroChartGridLine is-vertical",
      }));
    }

    const localY = levels.localFlip !== null ? yFor(levels.localFlip) : null;
    const callY = levels.callWall !== null ? yFor(levels.callWall) : null;
    const putY = levels.putWall !== null ? yFor(levels.putWall) : null;
    const plotBottom = plot.y + plot.height;
    const plotTop = plot.y;

    if (putY !== null) drawZoneBand(zones, plot.x, plot.width, putY, plotBottom, "marketPulseExecutionHeroChartZone-put");
    if (localY !== null) {
      const sellBottom = putY !== null ? putY : plotBottom;
      drawZoneBand(zones, plot.x, plot.width, localY, sellBottom, "marketPulseExecutionHeroChartZone-sell");
    }
    if (localY !== null) {
      const buyTop = callY !== null ? callY : plotTop;
      drawZoneBand(zones, plot.x, plot.width, buyTop, localY, "marketPulseExecutionHeroChartZone-buy");
    }
    if (callY !== null) drawZoneBand(zones, plot.x, plot.width, plotTop, callY, "marketPulseExecutionHeroChartZone-extension");

    const lineDefs = [
      { key: "nextPutWall", label: "NPW", value: levels.nextPutWall, className: "marketPulseExecutionHeroChartLevelLine-next-put" },
      { key: "putWall", label: "PW", value: levels.putWall, className: "marketPulseExecutionHeroChartLevelLine-put" },
      { key: "localFlip", label: "LF", value: levels.localFlip, className: "marketPulseExecutionHeroChartLevelLine-local" },
      { key: "mainFlip", label: "Main", value: levels.mainFlip, className: "marketPulseExecutionHeroChartLevelLine-main" },
      { key: "callWall", label: "CW", value: levels.callWall, className: "marketPulseExecutionHeroChartLevelLine-call" },
      { key: "nextCallWall", label: "NCW", value: levels.nextCallWall, className: "marketPulseExecutionHeroChartLevelLine-next-call" },
      { key: "spot", label: "SPX", value: levels.spot, className: "marketPulseExecutionHeroChartPriceLine", isPrice: true },
    ]
      .filter((row) => row.value !== null)
      .map((row) => {
        const activePullback = row.key === "callWall" && summary.currentRead === "Above Call Wall";
        const isNextTarget =
          (row.key === "nextCallWall" && String(summary.nextDestination).startsWith("NCW"))
          || (row.key === "nextPutWall" && String(summary.nextDestination).startsWith("NPW"));
        const labelTone = row.key === "callWall"
          ? "is-call"
          : row.key === "localFlip"
            ? "is-local"
            : row.key === "nextCallWall" || row.key === "nextPutWall"
              ? "is-next"
              : row.isPrice
                ? "is-price"
                : "";
        return {
          ...row,
          y: yFor(row.value),
          activePullback,
          isNextTarget,
          labelTone,
        };
      })
      .sort((a, b) => a.y - b.y);

    const labelRows = [];
    lineDefs.forEach((row) => {
      const y = clamp(row.y, plot.y + 12, plotBottom - 12);
      const prior = labelRows[labelRows.length - 1];
      row.labelY = prior ? Math.max(y, prior + LABEL_GAP) : y;
      labelRows.push(row.labelY);
    });
    for (let index = lineDefs.length - 2; index >= 0; index -= 1) {
      const next = lineDefs[index + 1];
      const current = lineDefs[index];
      if ((next.labelY - current.labelY) < LABEL_GAP) {
        current.labelY = clamp(next.labelY - LABEL_GAP, plot.y + 12, plotBottom - 12);
      }
    }

    lineDefs.forEach((row) => {
      levelLines.appendChild(createSvg("line", {
        x1: plot.x,
        y1: row.y,
        x2: plot.x + plot.width,
        y2: row.y,
        class: `${row.className}${row.activePullback ? " is-active-level" : ""}${row.isNextTarget ? " is-next-target" : ""}`,
      }));
      drawLabel(
        labels,
        plot.x + plot.width,
        row.labelY,
        `${row.label} ${formatNumber(row.value, row.isPrice ? 2 : 0)}`,
        `${row.labelTone}${row.activePullback ? " is-active-level" : ""}`.trim()
      );
    });

    if (points.length >= 2) {
      pathLayer.appendChild(createSvg("path", {
        d: buildAreaPath(points, xFor, yFor, plotBottom),
        class: "marketPulseExecutionHeroChartArea",
      }));
      pathLayer.appendChild(createSvg("path", {
        d: buildPath(points, xFor, yFor),
        class: "marketPulseExecutionHeroChartGlow",
      }));
      pathLayer.appendChild(createSvg("path", {
        d: buildPath(points, xFor, yFor),
        class: "marketPulseExecutionHeroChartPath",
      }));
    }

    const lastPoint = points[points.length - 1];
    if (lastPoint) {
      pathLayer.appendChild(createSvg("circle", {
        cx: xFor(lastPoint.stamp),
        cy: yFor(lastPoint.price),
        r: 5.5,
        class: "marketPulseExecutionHeroChartPriceDot",
      }));
    }

    const firstTsLabel = createSvg("text", {
      x: plot.x,
      y: VIEWBOX.height - 8,
      class: "marketPulseExecutionHeroChartDomainNote",
    });
    firstTsLabel.textContent = new Date(points[0].stamp).toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      timeZone: "America/New_York",
    });

    const lastTsLabel = createSvg("text", {
      x: plot.x + plot.width,
      y: VIEWBOX.height - 8,
      class: "marketPulseExecutionHeroChartDomainNote",
      "text-anchor": "end",
    });
    lastTsLabel.textContent = new Date(points[points.length - 1].stamp).toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      timeZone: "America/New_York",
    });

    chartSvg.append(defs, zones, grid, levelLines, pathLayer, labels, firstTsLabel, lastTsLabel);
    window.dispatchEvent(new CustomEvent("market-pulse-chart-ready"));
  };

  let current = getJson("spxExecutionChartPayload") || {};
  render(current);

  window.addEventListener("market-pulse-stream-payload", (event) => {
    current = mergePayload(current, (event && event.detail) || {});
    render(current);
  });

  window.addEventListener("resize", () => {
    render(current);
  });
})();
