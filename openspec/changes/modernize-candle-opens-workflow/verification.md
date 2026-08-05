## Verification

### Automated checks

- `pytest -q tests/test_app_core.py -k candle_opens`: 6 passed.
- `pytest -q tests/test_candle_opens_workflow_contract.py`: 3 passed.
- `pytest -q tests/test_application_page_modernization_contract.py`: 21 passed.
- `openspec validate modernize-candle-opens-workflow --strict`: valid.
- `git diff --check`: passed.

### Responsive receiving-surface checks

Authenticated captures were generated for 390px, 768px, 1440px, and 1920px widths under
`artifacts/menu-page-modernization/candle-workflow/`.

- All four viewports reported `horizontalOverflow: false`.
- All four viewports reported `unnamedVisibleControls: 0`.
- The calendar and Day Profile render before catalyst and reference regions.
- The mobile month navigator remains a single segmented control.
- The tablet workflow clears the wrapped application navigation.
- Existing day controls, mobile week folds, macro anchors, and month destinations remain present.

### Container

- Rebuilt with `./scripts/run_podman_app.sh`.
- Container `mccain-capital-app` is running on port 5001.
- `GET /healthz` returned `status: ok`.
- The deployed container contains the workflow stylesheet marker and calendar region hook.
