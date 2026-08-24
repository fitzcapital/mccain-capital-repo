## REMOVED Requirements

### Requirement: Regular-session VWAP calculation
**Reason**: VWAP is being removed entirely from Market Pulse because it is not a dependable execution
aid for this page.

**Migration**: No replacement indicator is introduced; existing structural, Gamma, candle, and Strat
evidence remain authoritative.

### Requirement: VWAP source integrity
**Reason**: Market Pulse will no longer request or serialize any VWAP price/volume source.

**Migration**: Remove SPX/SPY VWAP provider-leg selection and provenance fields.

### Requirement: VWAP chart overlay
**Reason**: The chart must not display a VWAP series, label, badge, or visibility toggle.

**Migration**: Existing chart state continues with candles and non-VWAP overlays only.

### Requirement: VWAP refresh preserves chart state
**Reason**: VWAP no longer participates in canonical refresh.

**Migration**: In-place refresh continues for the remaining canonical chart and decision data.

### Requirement: VWAP participates in confluence transparently
**Reason**: VWAP is removed from scenario evidence and scoring.

**Migration**: Normalize the remaining declared confluence components to a 100-point scale.
