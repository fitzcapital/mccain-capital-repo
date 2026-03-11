const test = require('node:test');
const assert = require('node:assert/strict');

const gamma = require('../static/js/market_pulse_gamma_context.js');

test('wall strength mapping', () => {
  assert.equal(gamma.classifyWallStrength(2_500_000), 'Strong');
  assert.equal(gamma.classifyWallStrength(1_500_000), 'Medium');
  assert.equal(gamma.classifyWallStrength(500_000), 'Weak');
});

test('structure classification', () => {
  assert.equal(gamma.classifyStructureType({ distanceToFlip: 1, wallSpread: 8, trapZoneState: 'knife_edge_structure' }), 'Knife Edge');
  assert.equal(gamma.classifyStructureType({ distanceToFlip: 7, wallSpread: 30, trapZoneState: 'clear' }), 'Regime Transition');
  assert.equal(gamma.classifyStructureType({ distanceToFlip: 22, wallSpread: 35, trapZoneState: 'clear' }), 'Clear Structure');
});

test('no-trade zone detection', () => {
  assert.equal(gamma.detectNoTradeZone({ distanceToFlip: 3, wallSpread: 14, structureType: 'Knife Edge' }), true);
  assert.equal(gamma.detectNoTradeZone({ distanceToFlip: 18, wallSpread: 40, structureType: 'Clear Structure' }), false);
});

test('tradeability score labels', () => {
  const input = {
    netGamma: 120_000_000,
  };
  const low = gamma.computeTradeabilityScore(input, {
    distanceToFlip: 2,
    wallSpread: 10,
    distanceToExpectedMoveHigh: 3,
    distanceToExpectedMoveLow: 3,
    structureType: 'Knife Edge',
    volatilityState: 'Expansion Risk',
    nearMajorWall: false,
    nearPDH: false,
    nearPDL: false,
    insideExpectedMove: true,
    noTradeCenter: true,
  });
  assert.equal(low.label, 'No Trade');

  const high = gamma.computeTradeabilityScore(input, {
    distanceToFlip: 25,
    wallSpread: 40,
    distanceToExpectedMoveHigh: 18,
    distanceToExpectedMoveLow: 12,
    structureType: 'Clear Structure',
    volatilityState: 'Calm',
    nearMajorWall: true,
    nearPDH: false,
    nearPDL: false,
    insideExpectedMove: false,
    noTradeCenter: false,
  });
  assert.equal(high.label, 'Tradeable');
});

test('auto-read is capped at 3 bullets', () => {
  const bullets = gamma.buildAutoRead(
    { netGamma: -1 },
    {
      distanceToFlip: 3,
      distanceToCallWall: 5,
      distanceToPutWall: 30,
      dealerRegime: 'Negative Gamma / Momentum Amplifying',
      noTradeCenter: true,
    }
  );
  assert.equal(Array.isArray(bullets), true);
  assert.equal(bullets.length, 3);
});
