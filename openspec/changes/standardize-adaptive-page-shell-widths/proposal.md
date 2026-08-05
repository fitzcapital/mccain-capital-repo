## Why

Navigation currently shifts between a 1280px shared shell and wider 1440px or 1600px
workspaces, making otherwise related pages feel visually disconnected. The application needs a
consistent outer frame while preserving narrower readable content for forms and text-heavy work.

## What Changes

- Introduce an adaptive page-shell width system with explicit standard, wide, and dense modes.
- Align the shared header and primary navigation to a consistent 1440px desktop frame.
- Use a 1280px content frame for reading, form, profile, journal, and configuration workflows.
- Use a 1440px content frame for data-heavy workspaces including Trading Dashboard, Executive,
  Market Pulse, Candle Opens, and Analytics.
- Preserve the 1500-1600px dense-mode contract for the dormant standalone Budget template without
  changing the current `/budget` redirect to Executive.
- Allow designated charts, ladders, calendars, and tables to use the available wide content frame
  without stretching ordinary text or form controls.
- Preserve all routes, controls, data hooks, form behavior, and responsive/mobile layouts.
- Add regression coverage for shell-mode assignment, maximum widths, horizontal overflow, and
  representative page functionality.
- Acceptance criteria: desktop headers share aligned 1440px edges; standard pages remain capped at
  1280px; wide pages use 1440px; dense rendering retains its documented wider content cap; tested
  tablet/mobile views have no horizontal overflow; existing controls and forms remain available.
- Non-goals: redesigning individual cards, changing navigation destinations, changing business or
  financial calculations, changing data sources, or forcing text/form cards to fill the viewport.

## Capabilities

### New Capabilities

- `adaptive-page-shell`: Defines shared header alignment, page content density modes, responsive
  width behavior, and functional-parity requirements across menu destinations.

### Modified Capabilities

None. The repository has no existing living capability spec for shared shell width behavior.

## Impact

- Shared Jinja shell markup in `mccain_capital/templates/base.html` may receive stable shell-mode
  hooks where body-page classes are insufficient.
- Shared and page-scoped layout rules in `static/css/app.css` and
  `static/css/app_modern_pages.css` will be consolidated around width tokens.
- Focused pytest contract coverage and representative browser viewport checks will be added or
  updated.
- No API, dependency, database, market-data, or financial-assumption changes are required.
