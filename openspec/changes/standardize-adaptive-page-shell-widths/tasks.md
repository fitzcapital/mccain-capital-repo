## 1. Shell Classification

- [x] 1.1 Add explicit standard, wide, and dense shell-mode hooks to the shared Jinja body contract.
- [x] 1.2 Add focused rendering tests that verify representative routes receive the intended shell mode.

## 2. Adaptive Width Contract

- [x] 2.1 Define shared CSS tokens for the 1440px chrome, 1280px standard, 1440px wide, and up-to-1600px dense frames.
- [x] 2.2 Center the shared header/navigation independently from each page content shell.
- [x] 2.3 Apply mode-specific widths to `#pageShell` and consolidate conflicting legacy page-width overrides.
- [x] 2.4 Preserve readable prose limits and existing visualization/table containment inside wider shells.
- [x] 2.5 Preserve existing tablet/mobile padding, grid collapse, and overflow behavior for every shell mode.

## 3. Verification

- [x] 3.1 Add contract tests for header alignment, content caps, dense exceptions, and functional-parity hooks.
- [x] 3.2 Run focused pytest coverage and CSS/template sanity checks.
- [x] 3.3 Browser-check representative live standard and wide pages plus the synthetic dense contract at desktop, tablet, and mobile viewports for computed widths, visible controls, and horizontal overflow.
- [x] 3.4 Rebuild the container after approval and verify the deployed shell contract plus `/healthz`.
