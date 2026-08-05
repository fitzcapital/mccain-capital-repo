## 1. Server-authoritative partial response

- [x] 1.1 Extract the Dashboard Forward Pace card into a focused Jinja partial without changing its current content, form action, financial labels, or conditional states.
- [x] 1.2 Refactor or expose the smallest reusable Forward Pace view-model builder needed to render the partial after persistence while preserving live/manual sources, timing assumptions, buffer behavior, account allocation, and scoped timeframe links.
- [x] 1.3 Extend `/dashboard/pace` to detect an explicit inline JSON request and return `ok`, server-rendered card markup, and a concise message only after successful validation and persistence.
- [x] 1.4 Preserve the existing flash-and-redirect response for ordinary form submissions and ensure authentication and CSRF behavior remain unchanged.
- [x] 1.5 Ensure invalid dates and handler/rendering failures return a non-success response without presenting unsaved settings as authoritative.
- [x] 1.6 Extend the Forward Pace view model with Reserved Buffer and Available Balance After Buffer values derived directly from projected balance, without changing existing Projected Balance or Projected Profit calculations.

## 2. Dashboard progressive enhancement

- [x] 2.1 Add stable card, form, status, and submit-control hooks with an accessible `aria-live` status region.
- [x] 2.2 Intercept Forward Pace save and reset submissions in Dashboard JavaScript, submit the existing form data with same-origin credentials and the explicit JSON signal, and prevent duplicate requests while pending.
- [x] 2.3 Replace only the Forward Pace card after an explicit successful response and preserve the Dashboard URL, scroll position, and unrelated card state.
- [x] 2.4 Ensure the replacement form supports consecutive inline saves through delegated handling or reliable rebinding.
- [x] 2.5 Retain the current card and entered values on network, parsing, validation, or server failure and display an accessible retryable error state.
- [x] 2.6 Present Projected Balance, Reserved Buffer, and Available Balance After Buffer as distinct values with a concise explanation that the reserve remains in the account.

## 3. Verification

- [x] 3.1 Add focused Flask tests for inline custom saves, live-pace reset, authoritative returned markup, invalid input, and unchanged non-JSON redirect behavior.
- [x] 3.2 Add frontend contract tests for pending/success/error states, fragment-only replacement, duplicate-submit protection, and consecutive-save support.
- [x] 3.3 Add exact numeric regression coverage for zero buffer, a positive buffer, and a buffer larger than gross projected profit across target-date and 5D/10D/20D outputs, including protection against double subtraction.
- [x] 3.4 Run the focused Forward Pace and Dashboard tests, JavaScript syntax checks, `git diff --check`, and strict OpenSpec validation.
- [ ] 3.5 Rebuild the local application and verify authenticated Dashboard saves and resets without full-page navigation on desktop and mobile, including scroll preservation, buffer terminology, calculated values, and failure feedback.
