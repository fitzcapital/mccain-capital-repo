(function () {
  const node = document.getElementById("tradesReplayChart");
  if (!node) return;

  let payload = {};
  try {
    payload = JSON.parse(node.dataset.chart || "{}");
  } catch (error) {
    payload = {};
  }

  const points = Array.isArray(payload.series) ? payload.series : [];
  if (!points.length) {
    node.innerHTML = '<div class="chartEmpty"><div class="chartEmptyTitle">No replay data available</div><div class="chartEmptySub">This trade does not have enough price context to build a replay map.</div></div>';
    return;
  }

  const width = 920;
  const height = 320;
  const pad = { top: 18, right: 42, bottom: 34, left: 54 };
  const values = points.map((point) => Number(point.close || 0));
  const minValue = Math.min(...values, Number(payload.stop_price || values[0] || 0), Number(payload.target_price || values[0] || 0));
  const maxValue = Math.max(...values, Number(payload.stop_price || values[0] || 0), Number(payload.target_price || values[0] || 0));
  const range = Math.max(maxValue - minValue, 1);
  const innerWidth = width - pad.left - pad.right;
  const innerHeight = height - pad.top - pad.bottom;

  const mapX = (index) => pad.left + (points.length === 1 ? innerWidth / 2 : (index / (points.length - 1)) * innerWidth);
  const mapY = (value) => pad.top + innerHeight - (((value - minValue) / range) * innerHeight);
  const coords = points.map((point, index) => ({
    ...point,
    x: mapX(index),
    y: mapY(Number(point.close || 0)),
  }));
  const path = coords.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(" ");
  const area = `${path} L ${coords[coords.length - 1].x.toFixed(1)} ${(height - pad.bottom).toFixed(1)} L ${coords[0].x.toFixed(1)} ${(height - pad.bottom).toFixed(1)} Z`;
  const gridValues = Array.from({ length: 4 }, (_, index) => minValue + ((range * index) / 3));
  const yLines = gridValues.map((value) => {
    const y = mapY(value);
    return `
      <line x1="${pad.left}" y1="${y.toFixed(1)}" x2="${(width - pad.right).toFixed(1)}" y2="${y.toFixed(1)}" class="tradesChartGridLine"></line>
      <text x="${(pad.left - 10).toFixed(1)}" y="${(y + 4).toFixed(1)}" text-anchor="end" class="tradesChartAxisLabel">${value.toFixed(2)}</text>
    `;
  }).join("");
  const xLabels = coords.map((point) => `<text x="${point.x.toFixed(1)}" y="${(height - 8).toFixed(1)}" text-anchor="middle" class="tradesChartAxisLabel">${String(point.label || "").slice(0, 8)}</text>`).join("");
  const markerForMinute = (minute) => {
    const sorted = coords.slice().sort((a, b) => Math.abs((a.minute || 0) - minute) - Math.abs((b.minute || 0) - minute));
    return sorted[0] || coords[0];
  };
  const entry = markerForMinute(Number(payload.entry_minute || 0));
  const exit = markerForMinute(Number(payload.exit_minute || 0));
  const stopLine = payload.stop_price ? `<line x1="${pad.left}" y1="${mapY(Number(payload.stop_price)).toFixed(1)}" x2="${(width - pad.right).toFixed(1)}" y2="${mapY(Number(payload.stop_price)).toFixed(1)}" class="tradesReplayLevelLine is-stop"></line>` : "";
  const targetLine = payload.target_price ? `<line x1="${pad.left}" y1="${mapY(Number(payload.target_price)).toFixed(1)}" x2="${(width - pad.right).toFixed(1)}" y2="${mapY(Number(payload.target_price)).toFixed(1)}" class="tradesReplayLevelLine is-target"></line>` : "";

  node.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" class="tradesMiniChartSvg" aria-hidden="true">
      <defs>
        <linearGradient id="tradesReplayFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="rgba(107, 196, 255, .26)"></stop>
          <stop offset="100%" stop-color="rgba(107, 196, 255, 0)"></stop>
        </linearGradient>
      </defs>
      ${yLines}
      ${stopLine}
      ${targetLine}
      <path d="${area}" fill="url(#tradesReplayFill)"></path>
      <path d="${path}" class="tradesChartLine"></path>
      <circle cx="${entry.x.toFixed(1)}" cy="${entry.y.toFixed(1)}" r="6" class="tradesChartMarkerDot is-positive"></circle>
      <circle cx="${exit.x.toFixed(1)}" cy="${exit.y.toFixed(1)}" r="6" class="tradesChartMarkerDot is-negative"></circle>
      <text x="${entry.x.toFixed(1)}" y="${(entry.y - 12).toFixed(1)}" text-anchor="middle" class="tradesChartValueLabel">Entry</text>
      <text x="${exit.x.toFixed(1)}" y="${(exit.y - 12).toFixed(1)}" text-anchor="middle" class="tradesChartValueLabel">Exit</text>
      ${xLabels}
    </svg>
  `;
})();
