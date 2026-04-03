(function () {
  const app = document.getElementById("analyticsDashboardApp");
  if (!app) return;

  const apiUrl = app.dataset.analyticsApi || "/api/analytics/dashboard";
  const initialNode = document.getElementById("analyticsDashboardInitial");
  const filterForm = document.getElementById("analyticsDashboardFilters");
  const scopeInput = document.getElementById("analyticsScopeInput");

  const state = {
    charts: new Map(),
    hasRendered: false,
    requestId: 0,
  };

  const CHART_FONT = '"Space Grotesk", "Sora", "Segoe UI", system-ui, sans-serif';
  const ACCENT_CHART_FONT = '"Architects Daughter", "Space Grotesk", "Sora", "Segoe UI", system-ui, sans-serif';

  const destroyAllCharts = () => {
    Array.from(state.charts.keys()).forEach((key) => destroyChart(key));
  };

  const parseInitialPayload = () => {
    if (!initialNode) return null;
    try {
      return JSON.parse(initialNode.textContent || "null");
    } catch (_error) {
      return null;
    }
  };

  const formatMoney = (value) => {
    const amount = Number(value || 0);
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(amount);
  };

  const formatMoneyPrecise = (value) => {
    const amount = Number(value || 0);
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(amount);
  };

  const formatPercent = (value, digits = 1) =>
    `${Number(value || 0).toFixed(digits)}%`;

  const AXIS_DATE_FORMATTER = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
  });

  const formatWindow = (start, end) => {
    const left = String(start || "").trim();
    const right = String(end || "").trim();
    if (left && right) return `${left} to ${right}`;
    if (left) return `From ${left}`;
    if (right) return `Through ${right}`;
    return "All dates";
  };

  const byId = (id) => document.getElementById(id);

  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

  const mixHex = (source, target, ratio) => {
    const parse = (hex) => {
      const clean = String(hex || "").replace("#", "");
      if (clean.length !== 6) return null;
      const int = Number.parseInt(clean, 16);
      if (Number.isNaN(int)) return null;
      return {
        r: (int >> 16) & 255,
        g: (int >> 8) & 255,
        b: int & 255,
      };
    };
    const left = parse(source);
    const right = parse(target);
    if (!left || !right) return source;
    const t = clamp(Number(ratio || 0), 0, 1);
    const channel = (a, b) => Math.round(a + (b - a) * t);
    return `#${[channel(left.r, right.r), channel(left.g, right.g), channel(left.b, right.b)]
      .map((value) => value.toString(16).padStart(2, "0"))
      .join("")}`;
  };

  const radialEndColor = (color) => mixHex(color, "#a7bbd0", 0.18);

  const parseAxisDate = (raw) => {
    const text = String(raw || "").trim();
    const match = text.match(/(\d{4}-\d{2}-\d{2})/);
    if (!match) return null;
    const date = new Date(`${match[1]}T00:00:00`);
    return Number.isNaN(date.getTime()) ? null : date;
  };

  const formatAxisLabel = (raw) => {
    const date = parseAxisDate(raw);
    if (date) return AXIS_DATE_FORMATTER.format(date);
    const text = String(raw || "").trim();
    return text.length > 8 ? text.slice(0, 8) : text;
  };

  const sparseAxisFormatter = (totalPoints, stride = 1) => (value, _timestamp, opts) => {
    const index = Number(opts?.dataPointIndex ?? opts?.index ?? 0);
    if (totalPoints <= 1) return formatAxisLabel(value);
    const interval = Math.max(stride, Math.ceil(totalPoints / 5));
    const isEdge = index === 0 || index === totalPoints - 1;
    return isEdge || index % interval === 0 ? formatAxisLabel(value) : "";
  };

  const setText = (id, value) => {
    const node = byId(id);
    if (node) node.textContent = value == null ? "" : String(value);
  };

  const setHTML = (id, html) => {
    const node = byId(id);
    if (node) node.innerHTML = html;
  };

  const setProgress = (id, value) => {
    const node = byId(id);
    if (node) node.style.width = `${Math.max(0, Math.min(100, Number(value || 0))).toFixed(1)}%`;
  };

  const toneClass = (node, tone) => {
    if (!node) return;
    node.classList.remove("metaRed", "metaGreen");
    if (tone === "critical" || tone === "negative") node.classList.add("metaRed");
    if (tone === "positive") node.classList.add("metaGreen");
  };

  const escapeHtml = (value) =>
    String(value == null ? "" : value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

  const tooltipShell = (title, rows) => `
    <div class="analyticsApexTooltip">
      ${title ? `<div class="analyticsApexTooltipTitle">${escapeHtml(title)}</div>` : ""}
      <div class="analyticsApexTooltipRows">
        ${rows.join("")}
      </div>
    </div>
  `;

  const tooltipRow = (label, value, color, tone = "") => `
    <div class="analyticsApexTooltipRow${tone ? ` is-${escapeHtml(tone)}` : ""}">
      <span class="analyticsApexTooltipKey">
        <i style="--tooltip-swatch:${escapeHtml(color || "#53d7ff")}"></i>
        ${escapeHtml(label)}
      </span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;

  const destroyChart = (key) => {
    const chart = state.charts.get(key);
    if (chart) {
      try {
        chart.destroy();
      } catch (_error) {
        // Ignore teardown errors during rerender.
      }
      state.charts.delete(key);
    }
  };

  const renderEmptyState = (key, container, title, detail) => {
    destroyChart(key);
    if (!container) return;
    container.innerHTML = `
      <div class="analyticsChartEmpty">
        <div class="analyticsChartEmptyTitle">${title}</div>
        <div class="analyticsChartEmptyDetail">${detail}</div>
      </div>
    `;
  };

  const mountChart = (key, container, options) => {
    if (!container) return;
    destroyChart(key);
    container.innerHTML = "";
    if (typeof window.ApexCharts !== "function") {
      renderEmptyState(
        key,
        container,
        "Chart library unavailable",
        "ApexCharts did not load, so the reactive analytics workspace cannot render yet."
      );
      return;
    }
    const chart = new window.ApexCharts(container, options);
    chart.render();
    state.charts.set(key, chart);
  };

  const tooltipBase = () => ({
    shared: false,
    intersect: false,
    followCursor: true,
    fillSeriesColor: false,
    theme: false,
    style: { fontSize: "12px", fontFamily: CHART_FONT },
    marker: { show: false },
    custom: undefined,
  });

  const positionTooltipFromCursor = (event, chartContext) => {
    const host = chartContext?.el;
    const canvas = host?.querySelector(".apexcharts-canvas");
    const tooltip = host?.querySelector(".apexcharts-tooltip.apexcharts-active");
    if (!canvas || !tooltip || !event) return;
    const frame = canvas.getBoundingClientRect();
    const bubble = tooltip.getBoundingClientRect();
    if (!frame.width || !frame.height || !bubble.width || !bubble.height) return;
    const gap = 16;
    let left = event.clientX - frame.left + gap;
    let top = event.clientY - frame.top - bubble.height - 14;
    if (left + bubble.width > frame.width - gap) left = frame.width - bubble.width - gap;
    if (left < gap) left = gap;
    if (top < gap) top = event.clientY - frame.top + gap;
    if (top + bubble.height > frame.height - gap) top = frame.height - bubble.height - gap;
    if (top < gap) top = gap;
    tooltip.style.left = `${Math.round(left)}px`;
    tooltip.style.top = `${Math.round(top)}px`;
    tooltip.style.right = "auto";
    tooltip.style.bottom = "auto";
    tooltip.style.transform = "none";
  };

  const baseChartOptions = (height) => ({
    chart: {
      background: "transparent",
      foreColor: "#9fb3c8",
      toolbar: { show: false },
      animations: {
        enabled: !state.hasRendered,
        easing: "easeinout",
        speed: state.hasRendered ? 220 : 520,
        dynamicAnimation: { enabled: true, speed: 220 },
      },
      fontFamily: CHART_FONT,
      height,
      zoom: { enabled: false },
      parentHeightOffset: 0,
      events: {
        mouseMove(event, chartContext) {
          requestAnimationFrame(() => positionTooltipFromCursor(event, chartContext));
        },
        mouseLeave(_event, chartContext) {
          const tooltip = chartContext?.el?.querySelector(".apexcharts-tooltip");
          if (!tooltip) return;
          tooltip.style.left = "";
          tooltip.style.top = "";
          tooltip.style.right = "";
          tooltip.style.bottom = "";
          tooltip.style.transform = "";
        },
      },
    },
    dataLabels: { enabled: false },
    grid: {
      borderColor: "rgba(120, 154, 191, 0.08)",
      strokeDashArray: 4,
      padding: { left: 4, right: 10, top: 10, bottom: 4 },
    },
    theme: { mode: "dark" },
    states: {
      normal: { filter: { type: "none" } },
      hover: { filter: { type: "none" } },
      active: { filter: { type: "none" } },
    },
    legend: {
      labels: { colors: "#9fb3c8" },
      fontFamily: CHART_FONT,
      markers: { width: 8, height: 8, radius: 8, offsetX: -1 },
      itemMargin: { horizontal: 14, vertical: 4 },
    },
    tooltip: tooltipBase(),
    xaxis: {
      labels: {
        style: { colors: "#6f88a4", fontSize: "10px", fontWeight: 600 },
        rotate: 0,
        trim: false,
        hideOverlappingLabels: true,
        showDuplicates: false,
        maxHeight: 36,
      },
      axisBorder: { color: "rgba(146, 173, 204, 0.08)" },
      axisTicks: { color: "rgba(146, 173, 204, 0.08)" },
      tooltip: { enabled: false },
      crosshairs: { show: false },
    },
    yaxis: {
      labels: {
        style: { colors: "#6f88a4", fontSize: "10px", fontWeight: 600 },
        formatter: (value) => formatMoney(value),
        offsetX: -2,
      },
      tickAmount: 4,
    },
    stroke: { lineCap: "round" },
  });

  const hoverMarkers = (points, color) => ({
    size: points.length === 1 ? 5 : 0,
    strokeWidth: 0,
    hover: {
      size: 5,
      sizeOffset: 2,
    },
    discrete: lastPointMarker(points, color),
  });

  const pointSeries = (points) =>
    Array.isArray(points) ? points.map((point) => ({ x: point.x, y: Number(point.y || 0) })) : [];

  const tooltipDate = (point) => point.day || point.x || "";

  const lastPointMarker = (points, color, seriesIndex = 0) => {
    if (!points.length) return [];
    return [
      {
        seriesIndex,
        dataPointIndex: points.length - 1,
        fillColor: color,
        strokeColor: "#04111e",
        size: 6,
        shape: "circle",
      },
    ];
  };

  const meaningfulPointMarkers = (points, color, seriesIndex = 0, size = 4) =>
    points
      .map((point, dataPointIndex) => ({
        point,
        dataPointIndex,
      }))
      .filter(({ point }) => Math.abs(Number(point?.y || 0)) > 0.0001)
      .map(({ dataPointIndex }) => ({
        seriesIndex,
        dataPointIndex,
        fillColor: color,
        strokeColor: "#0a1321",
        strokeWidth: 2,
        size,
        shape: "circle",
      }));

  const renderDonutCenterOverlay = (container, payload) => {
    if (!container || !payload || payload.empty) return;
    const total = (payload.series || []).reduce((sum, value) => sum + Number(value || 0), 0);
    const dominantValue = Math.max(...(payload.series || [0]));
    const dominantPct = total ? (dominantValue / total) * 100 : 0;
    const dominantLabel = String(payload.dominant || payload.center_secondary || "Breakdown");
    const valueClass = dominantPct >= 100 ? "is-full" : dominantPct >= 10 ? "" : "is-fractional";
    const labelClass = dominantLabel.length > 12 ? "is-long" : "";
    const overlay = document.createElement("div");
    overlay.className = "analyticsDonutCenter";
    overlay.innerHTML = `
      <div class="analyticsDonutCenterValue ${valueClass}">${formatPercent(dominantPct, dominantPct >= 10 ? 0 : 1)}</div>
      <div class="analyticsDonutCenterLabel ${labelClass}">${escapeHtml(dominantLabel)}</div>
      <div class="analyticsDonutCenterMeta">${escapeHtml(payload.center_primary)} ${escapeHtml(payload.center_secondary || "")}</div>
    `;
    container.appendChild(overlay);
    const syncPosition = () => {
      const legend = container.querySelector(".apexcharts-legend");
      const legendHeight = legend ? legend.getBoundingClientRect().height : 0;
      const availableHeight = Math.max(120, container.clientHeight - legendHeight - 6);
      overlay.style.top = `${Math.round(availableHeight / 2) + 2}px`;
    };
    requestAnimationFrame(syncPosition);
    window.setTimeout(syncPosition, 60);
  };

  const renderEquityChart = (container, payload) => {
    const points = pointSeries(payload?.points || []);
    if (!points.length) {
      renderEmptyState("equity", container, "No equity curve yet", "Log trades inside this range to build the curve.");
      return;
    }
    const lastPoint = points[points.length - 1];
    mountChart("equity", container, {
      ...baseChartOptions(340),
      chart: { ...baseChartOptions(340).chart, type: "area" },
      grid: {
        ...baseChartOptions(340).grid,
        padding: { left: 4, right: 28, top: 12, bottom: 6 },
      },
      series: [{ name: "Equity", data: points }],
      xaxis: {
        ...baseChartOptions(340).xaxis,
        tickAmount: Math.min(5, points.length),
        labels: {
          ...baseChartOptions(340).xaxis.labels,
          formatter: sparseAxisFormatter(points.length),
        },
      },
      colors: ["#41d8ff"],
      stroke: { curve: "smooth", width: 3.4 },
      fill: {
        type: "gradient",
        gradient: {
          shade: "dark",
          type: "vertical",
          shadeIntensity: 0.2,
          gradientToColors: ["#0f4c73"],
          opacityFrom: 0.42,
          opacityTo: 0.04,
          stops: [0, 90, 100],
        },
      },
      markers: {
        ...hoverMarkers(points, "#41d8ff"),
      },
      tooltip: {
        ...tooltipBase(),
        custom: ({ dataPointIndex }) => {
          const point = payload.points[dataPointIndex] || {};
          const current = Number(point.y || 0);
          const baseline = Number(payload.points?.[0]?.y || 0);
          return tooltipShell(tooltipDate(point), [
            tooltipRow("Equity", formatMoneyPrecise(current), "#41d8ff"),
            tooltipRow("Net From Start", formatMoneyPrecise(current - baseline), "#7ce9ff"),
          ]);
        },
      },
      annotations: points.length > 1 ? {
        points: [
          {
            x: lastPoint.x,
            y: lastPoint.y,
            marker: {
              size: 0,
            },
            label: {
              offsetX: -104,
              offsetY: -14,
              borderColor: "rgba(65, 216, 255, 0.35)",
              style: {
                background: "rgba(11, 25, 40, 0.94)",
                color: "#dff8ff",
                fontSize: "11px",
                padding: {
                  left: 10,
                  right: 10,
                  top: 5,
                  bottom: 5,
                },
              },
              text: `Latest ${formatMoney(lastPoint.y)}`,
            },
          },
        ],
      } : {},
    });
  };

  const renderDrawdownChart = (container, payload) => {
    const points = pointSeries(payload?.points || []);
    if (!points.length) {
      renderEmptyState("drawdown", container, "No drawdown curve yet", "The range has no drawdown data to plot.");
      return;
    }
    mountChart("drawdown", container, {
      ...baseChartOptions(300),
      chart: { ...baseChartOptions(300).chart, type: "area" },
      series: [{ name: "Drawdown", data: points }],
      xaxis: {
        ...baseChartOptions(300).xaxis,
        tickAmount: Math.min(5, points.length),
        labels: {
          ...baseChartOptions(300).xaxis.labels,
          formatter: sparseAxisFormatter(points.length),
        },
      },
      colors: ["#ff6d8c"],
      stroke: { curve: "straight", width: 3.4 },
      fill: {
        type: "gradient",
        gradient: {
          shade: "dark",
          type: "vertical",
          shadeIntensity: 0.15,
          gradientToColors: ["#4f1628"],
          opacityFrom: 0.34,
          opacityTo: 0.03,
          stops: [0, 90, 100],
        },
      },
      markers: {
        size: 0,
        strokeWidth: 0,
        hover: {
          size: 6,
          sizeOffset: 0,
        },
        discrete: [
          ...meaningfulPointMarkers(points, "#ff6d8c", 0, 4),
          ...lastPointMarker(points, "#ff6d8c"),
        ],
      },
      tooltip: {
        ...tooltipBase(),
        intersect: true,
        followCursor: true,
        custom: ({ dataPointIndex }) => {
          const point = payload.points[dataPointIndex] || {};
          return tooltipShell(tooltipDate(point), [
            tooltipRow("Drawdown", formatMoneyPrecise(point.y), "#ff6d8c", Number(point.y || 0) < 0 ? "negative" : ""),
          ]);
        },
      },
    });
  };

  const renderBenchmarkChart = (container, payload) => {
    const strategy = pointSeries(payload?.strategy || []);
    const benchmark = pointSeries(payload?.benchmark || []);
    if (!strategy.length) {
      renderEmptyState("benchmark", container, "No benchmark comparison yet", "The strategy curve needs at least one point first.");
      return;
    }
    if (!payload?.has_benchmark || !benchmark.length) {
      renderEmptyState("benchmark", container, payload.empty_title, payload.empty_detail);
      return;
    }
    mountChart("benchmark", container, {
      ...baseChartOptions(320),
      chart: { ...baseChartOptions(320).chart, type: "line" },
      series: [
        { name: "Strategy", data: strategy },
        { name: "SPX Benchmark", data: benchmark },
      ],
      colors: ["#41d8ff", "#f1c45a"],
      xaxis: {
        ...baseChartOptions(320).xaxis,
        tickAmount: Math.min(5, strategy.length),
        labels: {
          ...baseChartOptions(320).xaxis.labels,
          formatter: sparseAxisFormatter(strategy.length),
        },
      },
      stroke: { curve: "smooth", width: [3, 2.6], dashArray: [0, 6] },
      markers: {
        size: 0,
        strokeWidth: 0,
        hover: { size: 5, sizeOffset: 1 },
        discrete: [
          ...lastPointMarker(strategy, "#41d8ff", 0),
          ...lastPointMarker(benchmark, "#f1c45a", 1),
        ],
      },
      legend: {
        show: true,
        position: "top",
        horizontalAlign: "right",
        fontSize: "10px",
      },
      tooltip: {
        ...tooltipBase(),
        shared: true,
        intersect: false,
        custom: ({ series, dataPointIndex, w }) => {
          const point = payload.strategy[dataPointIndex] || payload.benchmark[dataPointIndex] || {};
          const rows = w.globals.seriesNames.map((name, index) =>
            tooltipRow(name, formatMoneyPrecise(series[index][dataPointIndex]), w.globals.colors[index])
          );
          return tooltipShell(tooltipDate(point), rows);
        },
      },
    });
  };

  const renderPnLByDayChart = (container, payload) => {
    const points = pointSeries(payload?.points || []);
    if (!points.length) {
      renderEmptyState("pnl-day", container, "No daily PnL yet", "The selected range does not contain any closed trade days.");
      return;
    }
    mountChart("pnl-day", container, {
      ...baseChartOptions(300),
      chart: { ...baseChartOptions(300).chart, type: "bar" },
      series: [{ name: "PnL", data: points }],
      xaxis: {
        ...baseChartOptions(300).xaxis,
        tickAmount: Math.min(6, points.length),
        labels: {
          ...baseChartOptions(300).xaxis.labels,
          formatter: sparseAxisFormatter(points.length, 2),
        },
      },
      colors: ["#66d9a6"],
      plotOptions: {
        bar: {
          borderRadius: 10,
          columnWidth: "54%",
          distributed: false,
          colors: {
            ranges: [
              { from: -1000000, to: -0.0001, color: "#ff7a8b" },
              { from: 0, to: 1000000, color: "#56d6a2" },
            ],
          },
        },
      },
      tooltip: {
        ...tooltipBase(),
        custom: ({ dataPointIndex }) => {
          const point = payload.points[dataPointIndex] || {};
          const tone = Number(point.y || 0) < 0 ? "negative" : "positive";
          const color = tone === "negative" ? "#ff7a8b" : "#56d6a2";
          return tooltipShell(String(point.x || ""), [
            tooltipRow("Net P/L", formatMoneyPrecise(point.y), color, tone),
          ]);
        },
      },
    });
  };

  const renderDonutChart = (key, container, payload, colors) => {
    if (!payload || payload.empty) {
      renderEmptyState(key, container, payload?.empty_title || "No data", payload?.empty_detail || "No data available.");
      return;
    }
    const total = (payload.series || []).reduce((sum, value) => sum + Number(value || 0), 0);
    const denseLegend = (payload.labels || []).length >= 5;
    mountChart(key, container, {
      chart: {
        type: "donut",
        background: "transparent",
        foreColor: "#9fb3c8",
        toolbar: { show: false },
        animations: {
          enabled: !state.hasRendered,
          speed: state.hasRendered ? 200 : 500,
        },
        fontFamily: CHART_FONT,
        height: 280,
      },
      series: payload.series,
      labels: payload.labels,
      colors,
      legend: {
        show: true,
        position: "bottom",
        fontSize: denseLegend ? "8px" : "9px",
        labels: { colors: "#8ea6bf" },
        itemMargin: { horizontal: denseLegend ? 8 : 10, vertical: denseLegend ? 2 : 4 },
        formatter: (seriesName, opts) => {
          const value = Number(payload.series[opts.seriesIndex] || 0);
          const pct = total ? (Number(value || 0) / total) * 100 : 0;
          return `${seriesName} · ${formatPercent(pct, pct >= 10 ? 0 : 1)}`;
        },
      },
      stroke: { width: 0, lineCap: "round" },
      dataLabels: { enabled: false },
      plotOptions: {
        pie: {
          expandOnClick: false,
          offsetY: 0,
          donut: {
            size: denseLegend ? "78%" : "80%",
            labels: { show: false },
          },
        },
      },
      tooltip: {
        ...tooltipBase(),
        custom: ({ seriesIndex }) => {
          const value = Number(payload.series[seriesIndex] || 0);
          const pct = total ? (value / total) * 100 : 0;
          return tooltipShell(payload.labels[seriesIndex] || "", [
            tooltipRow("Count", `${value}`, colors[seriesIndex] || "#53d7ff"),
            tooltipRow("Share", formatPercent(pct, pct >= 10 ? 0 : 1), colors[seriesIndex] || "#53d7ff"),
          ]);
        },
      },
    });
    renderDonutCenterOverlay(container, payload);
  };

  const renderRadialKpi = (key, container, payload, color) => {
    if (!container) return;
    const centerText = String(payload.center || "");
    const valueFontSize =
      centerText.length >= 11 ? "21px" :
      centerText.length >= 8 ? "24px" :
      centerText.length >= 6 ? "27px" :
      "30px";
    mountChart(key, container, {
      chart: {
        type: "radialBar",
        background: "transparent",
        foreColor: "#9fb3c8",
        toolbar: { show: false },
        animations: {
          enabled: !state.hasRendered,
          speed: state.hasRendered ? 180 : 520,
        },
        height: 250,
        fontFamily: CHART_FONT,
      },
      series: [Number(payload.progress || 0)],
      colors: [color],
      fill: {
        type: "gradient",
        gradient: {
          shade: "dark",
          type: "horizontal",
          shadeIntensity: 0.15,
          gradientToColors: [radialEndColor(color)],
          inverseColors: false,
          opacityFrom: 1,
          opacityTo: 1,
          stops: [0, 100],
        },
      },
      plotOptions: {
        radialBar: {
          startAngle: -135,
          endAngle: 135,
          offsetY: -8,
          hollow: {
            size: "74%",
            background: "rgba(3, 10, 18, 0.9)",
            dropShadow: {
              enabled: true,
              top: 0,
              left: 0,
              blur: 16,
              opacity: 0.24,
            },
          },
          track: {
            background: "rgba(68, 86, 111, 0.22)",
            strokeWidth: "100%",
            margin: 0,
            dropShadow: {
              enabled: true,
              top: 0,
              left: 0,
              blur: 10,
              opacity: 0.08,
            },
          },
          dataLabels: {
            name: {
              show: true,
              offsetY: 58,
              color: "#86a0ba",
              fontSize: "9px",
              fontWeight: 700,
              fontFamily: ACCENT_CHART_FONT,
            },
            value: {
              show: true,
              offsetY: -6,
              fontSize: valueFontSize,
              fontWeight: 800,
              color: "#f5fbff",
              fontFamily: ACCENT_CHART_FONT,
              formatter: () => payload.center,
            },
          },
        },
      },
      labels: [payload.subtitle],
      stroke: { lineCap: "round" },
      tooltip: { enabled: false },
    });
  };

  const renderSparkline = (key, container, points, color) => {
    if (!container) return;
    const series = pointSeries(points || []);
    if (!series.length) {
      renderEmptyState(key, container, "No sparkline yet", "No trend data in this range.");
      return;
    }
    mountChart(key, container, {
      chart: {
        type: "area",
        sparkline: { enabled: true },
        background: "transparent",
        animations: {
          enabled: !state.hasRendered,
          speed: state.hasRendered ? 160 : 420,
        },
        height: 74,
        fontFamily: CHART_FONT,
      },
      series: [{ data: series }],
      colors: [color],
      stroke: { curve: "smooth", width: 2.4 },
      fill: {
        type: "gradient",
        gradient: {
          shade: "dark",
          opacityFrom: 0.28,
          opacityTo: 0.02,
          stops: [0, 100],
        },
      },
      tooltip: {
        ...tooltipBase(),
        custom: ({ dataPointIndex }) => {
          const point = points[dataPointIndex] || {};
          return tooltipShell(tooltipDate(point), [
            tooltipRow("Trend", `${Number(point.y || 0).toFixed(1)}`, color),
          ]);
        },
      },
    });
  };

  const renderExpectancyTrendChart = (container, points) => {
    if (!container) return;
    const series = pointSeries(points || []);
    if (!series.length) {
      renderEmptyState(
        "expectancy-trend",
        container,
        "No expectancy trend yet",
        "The selected granularity does not have enough periods in this range."
      );
      return;
    }
    mountChart("expectancy-trend", container, {
      ...baseChartOptions(280),
      chart: { ...baseChartOptions(280).chart, type: "line" },
      series: [{ name: "Expectancy", data: series }],
      colors: ["#8be06a"],
      stroke: { curve: "smooth", width: 2.8 },
      markers: {
        ...hoverMarkers(series, "#8be06a"),
      },
      tooltip: {
        ...tooltipBase(),
        custom: ({ dataPointIndex }) => {
          const point = series[dataPointIndex] || {};
          return tooltipShell(String(point.x || ""), [
            tooltipRow("Expectancy", formatMoneyPrecise(point.y), "#8be06a"),
          ]);
        },
      },
    });
  };

  const updateFilterChips = (chips) => {
    const host = byId("analyticsFilterChips");
    if (!host) return;
    host.innerHTML = "";
    (chips || []).forEach((chip) => {
      const span = document.createElement("span");
      span.className = "trendChip";
      span.textContent = chip;
      host.appendChild(span);
    });
  };

  const updateDayLines = (lines) => {
    const host = byId("analyticsDayLines");
    if (!host) return;
    host.innerHTML = "";
    (lines || []).forEach((line) => {
      const row = document.createElement("div");
      row.className = "tiny stack8 line16";
      row.textContent = `• ${line}`;
      host.appendChild(row);
    });
  };

  const updateTrustLink = (payload) => {
    const link = byId("analyticsTrustCta");
    const wrapper = link ? link.parentElement : null;
    if (!payload?.data_trust_href || !payload?.data_trust_label) {
      if (wrapper) wrapper.style.display = "none";
      return;
    }
    if (link) {
      link.href = payload.data_trust_href;
      link.textContent = payload.data_trust_label;
    }
    if (wrapper) wrapper.style.display = "";
  };

  const updateScopeButtons = (scope) => {
    document.querySelectorAll(".analyticsScopeBtn").forEach((button) => {
      button.classList.toggle("is-active", button.dataset.scopeValue === scope);
    });
    if (scopeInput) scopeInput.value = scope || "all";
  };

  const renderDashboard = (payload) => {
    if (!payload) return;

    setText("analyticsBalanceValue", payload.summary.balance_display);
    setText("analyticsNetStat", payload.summary.net_display);
    setText("analyticsWinRateStat", payload.summary.win_rate_display);
    setText("analyticsExpectancyStat", payload.summary.expectancy_display);
    setText("analyticsDrawdownStat", payload.summary.drawdown_display);
    setText("analyticsSessionTrades", payload.summary.total_trades);
    setText("analyticsSessionProfitFactor", payload.summary.profit_factor_display);
    setText("analyticsSessionWindow", formatWindow(payload.meta.start_date, payload.meta.end_date));
    setText("analyticsSessionScopeText", payload.meta.scope_label || "All completed trades in range");
    setText("analyticsSessionExplainDay", payload.meta.explain_day || "Auto");
    setText("analyticsPriorityText", payload.coach.next_action);
    setText("analyticsChangedText", payload.coach.changed);
    setText("analyticsRiskText", payload.coach.risk_now);
    setText("analyticsActionText", payload.coach.next_action);
    setText("analyticsBestSetupHeadline", payload.coach.best_setup_headline);
    setText("analyticsBestSetupBody", payload.coach.best_setup_body);
    setText("analyticsLeakHeadline", payload.coach.biggest_leak_headline);
    setText("analyticsLeakBody", payload.coach.biggest_leak_body);
    setText("analyticsTrustMessage", payload.coach.data_trust_message);
    setText("analyticsDayTitle", payload.coach.day_title);
    setText("analyticsSizingLead", payload.coach.sizing_action);
    setText("analyticsSizingBody", payload.coach.sizing_note);
    setText("analyticsSizingAction", payload.coach.sizing_action);
    setText("analyticsSizingRegime", `Regime ${payload.coach.sizing_regime}`);
    setText("analyticsFitzStatus", `Fitz 22 ${payload.coach.fitz_status}`);
    setText("analyticsEquitySubtitle", payload.charts.equity.subtitle);
    setText("analyticsDrawdownSubtitle", payload.charts.drawdown.subtitle);
    setText("analyticsPnlDaySubtitle", payload.charts.pnl_day.subtitle);
    setText("analyticsWinRateFoot", payload.kpis.win_rate.footnote);
    setText("analyticsExpectancyFoot", payload.kpis.expectancy.footnote);
    setText("analyticsRiskControlFoot", payload.kpis.risk_control.footnote);
    setText("analyticsReviewCoverageFoot", payload.kpis.review_coverage.footnote);
    setText("analyticsReviewMicroValue", payload.micro.review.value);
    setText("analyticsReviewMicroNote", payload.micro.review.note);
    setText("analyticsTrustMicroValue", payload.micro.trust.value);
    setText("analyticsTrustMicroNote", payload.micro.trust.note);
    setText("analyticsEdgeMicroValue", payload.micro.edge.value);
    setText("analyticsEdgeMicroNote", payload.micro.edge.note);
    updateFilterChips(payload.meta.filter_chips);
    updateDayLines(payload.coach.day_lines);
    updateTrustLink(payload.coach);
    updateScopeButtons(payload.meta.scope);

    const trustMessage = byId("analyticsTrustMessage");
    toneClass(trustMessage, payload.coach.data_trust_tone);
    setProgress("analyticsReviewMicroProgress", payload.micro.review.progress);
    setProgress("analyticsTrustMicroProgress", payload.micro.trust.progress);
    setProgress("analyticsEdgeMicroProgress", payload.micro.edge.progress);

    renderRadialKpi("win-rate-kpi", byId("analyticsWinRateChart"), payload.kpis.win_rate, "#53d7ff");
    renderRadialKpi("expectancy-kpi", byId("analyticsExpectancyChart"), payload.kpis.expectancy, "#8de76d");
    renderRadialKpi("risk-kpi", byId("analyticsRiskControlChart"), payload.kpis.risk_control, "#ff8a74");
    renderRadialKpi("review-kpi", byId("analyticsReviewCoverageChart"), payload.kpis.review_coverage, "#f2c96a");

    renderEquityChart(byId("analyticsEquityChart"), payload.charts.equity);
    renderDrawdownChart(byId("analyticsDrawdownChart"), payload.charts.drawdown);
    renderBenchmarkChart(byId("analyticsBenchmarkChart"), payload.charts.benchmark);
    renderPnLByDayChart(byId("analyticsPnlDayChart"), payload.charts.pnl_day);

    renderDonutChart(
      "outcomes-donut",
      byId("analyticsOutcomeBreakdownChart"),
      payload.charts.breakdowns.outcomes,
      ["#55d6a3", "#ff7a8b", "#7d8ea2"]
    );
    renderDonutChart(
      "setups-donut",
      byId("analyticsSetupBreakdownChart"),
      payload.charts.breakdowns.setups,
      ["#53d7ff", "#7f9bff", "#f1c45a", "#8de76d", "#ff9e63", "#889ab3"]
    );
    renderDonutChart(
      "mistakes-donut",
      byId("analyticsMistakeBreakdownChart"),
      payload.charts.breakdowns.mistakes,
      ["#ff8a74", "#ffbc5f", "#f1c45a", "#7d8ea2", "#53d7ff", "#8e6bf2"]
    );
    renderDonutChart(
      "regimes-donut",
      byId("analyticsRegimeBreakdownChart"),
      payload.charts.breakdowns.regimes,
      ["#56d6a2", "#53d7ff", "#ff8a74"]
    );

    renderSparkline("review-spark", byId("analyticsReviewSparkChart"), payload.charts.review_spark, "#53d7ff");
    renderSparkline("trust-spark", byId("analyticsTrustSparkChart"), payload.charts.trust_spark, "#f1c45a");
    renderSparkline("edge-spark", byId("analyticsEdgeSparkChart"), payload.charts.edge_spark, "#8de76d");
    renderExpectancyTrendChart(byId("analyticsExpectancyTrendChart"), payload.charts.edge_spark);

    if (filterForm) {
      const granularity = filterForm.elements.namedItem("expectancy_granularity");
      if (granularity) granularity.value = payload.meta.expectancy_granularity;
    }

    state.hasRendered = true;
  };

  const serializeForm = () => {
    const params = new URLSearchParams();
    if (!filterForm) return params;
    const formData = new FormData(filterForm);
    formData.forEach((value, key) => {
      const text = String(value || "").trim();
      if (text) {
        params.set(key, text);
      }
    });
    return params;
  };

  const fetchAndRender = async (pushHistory) => {
    const requestId = ++state.requestId;
    app.dataset.loading = "1";
    const params = serializeForm();
    try {
      const response = await fetch(`${apiUrl}?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      if (requestId !== state.requestId) return;
      renderDashboard(payload);
      if (pushHistory) {
        const url = `${window.location.pathname}?${params.toString()}`;
        window.history.replaceState({}, "", url);
      }
    } catch (error) {
      console.error("Analytics workspace refresh failed", error);
    } finally {
      if (requestId === state.requestId) app.dataset.loading = "0";
    }
  };

  if (filterForm) {
    filterForm.addEventListener("submit", (event) => {
      event.preventDefault();
      fetchAndRender(true);
    });

    filterForm.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.matches("select") || target.matches('input[type="date"]')) {
        fetchAndRender(true);
      }
    });
  }

  document.querySelectorAll(".analyticsScopeBtn").forEach((button) => {
    button.addEventListener("click", () => {
      if (scopeInput) scopeInput.value = button.dataset.scopeValue || "all";
      updateScopeButtons(button.dataset.scopeValue || "all");
      fetchAndRender(true);
    });
  });

  window.addEventListener("pagehide", destroyAllCharts);

  renderDashboard(parseInitialPayload());
})();
