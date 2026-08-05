## Context

The Dashboard Forward Pace form posts URL-encoded data to `/dashboard/pace`. The handler validates and persists settings, flashes a message, and redirects to the full Dashboard route. The standalone Forward Pace page already demonstrates asynchronous projection updates, but its API and projection model are different from the Dashboard card's live/manual pace model.

The Dashboard calculation remains server-authoritative. Its inputs include the selected Dashboard scope, the live trading pace fallback, an optional manual daily pace, an optional pass buffer, and optional projection dates. The change must not duplicate these rules in JavaScript or overwrite stored manual values after a failed request.

## Goals / Non-Goals

**Goals:**

- Save or reset Dashboard Forward Pace settings without a full-page reload.
- Reuse the existing validation, persistence, and Forward Pace view-model construction.
- Return server-rendered card markup so client and initial-render output cannot drift.
- Present actual projected balance, projected profit after buffer, and available balance after buffer as distinct server-authoritative values.
- Preserve the existing POST-and-redirect behavior as a no-JavaScript and failure fallback.
- Provide accessible pending, success, and error states while preventing duplicate submissions.

**Non-Goals:**

- Reimplementing projection formulas in JavaScript.
- Combining the standalone Forward Pace planner with the Dashboard card.
- Polling, broker synchronization, or refreshing unrelated Dashboard state.
- Changing any financial source, timing assumption, account allocation, persistence format, or the established meaning of Projected Balance and Projected Profit.

## Decisions

### 1. Content negotiation on the existing save route

The existing `/dashboard/pace` POST route will remain the canonical mutation path. Requests that explicitly ask for JSON will receive a JSON response containing success state, a rendered replacement fragment, and a concise message. Ordinary form submissions will keep the current flash-and-redirect response.

This avoids a second mutation endpoint and keeps authorization, CSRF protection, validation, and persistence behavior aligned. A separate API endpoint was considered but rejected because it would duplicate the save contract.

### 2. Extract one server-rendered Forward Pace card partial

The Forward Pace card will be moved into a focused Jinja partial used by both the full Dashboard render and the asynchronous response. The server will rebuild the authoritative Dashboard Forward Pace view model after a successful save and render that partial with the same scope and timeframe context.

Returning raw projection JSON was considered, but rejected because it would require the browser to duplicate a large amount of conditional markup and presentation logic.

### 3. Progressive enhancement in existing Dashboard JavaScript

Dashboard JavaScript will intercept only the identified Forward Pace form. It will submit `FormData` with same-origin credentials, the existing CSRF data, and an explicit JSON response signal. On success, it will replace the card container and rebind the handler to the new form. On failure, it will retain current content and inputs and display an accessible inline error.

The form remains fully functional without JavaScript because its method and action are unchanged.

### 4. Success is the only state allowed to replace the card

The client will replace markup only after an HTTP-success response with an explicit `ok: true` and a non-empty server fragment. Validation, authentication, CSRF, network, parsing, or rendering failures will leave the current card intact. Settings remain whatever the server last persisted; a failed persistence attempt must not be represented as saved in the UI.

### 5. Preserve local interaction context

The current window scroll position will not be changed intentionally. After replacement, focus will move to the compact status region only when necessary for an error; successful saves will keep focus close to the initiating control and announce completion through an `aria-live` status. Duplicate submission controls will be disabled only for the duration of the request.

### 6. Expose actual and available balances separately

The server-side Forward Pace view model will expose three related values without changing the current two calculations:

- `gross projected profit = applied daily pace × projected trading sessions`
- `projected profit after buffer = gross projected profit − pass buffer`
- `projected balance = base balance + gross projected profit`
- `available balance after buffer = projected balance − pass buffer`

Projected Balance remains the expected account balance because the reserved buffer still physically remains in the account. Available Balance After Buffer communicates the amount available above that reserve. The buffer is subtracted exactly once in each net/available output and is never deducted from an already buffer-adjusted value.

Changing Projected Balance itself to a net-of-buffer figure was considered but rejected because it would make the label stop representing the expected account balance and would obscure reconciliation to the starting balance plus gross projection.

## Risks / Trade-offs

- **[Partial markup can lose event handlers]** -> Use delegated handling or explicitly rebind after replacement and cover a second consecutive save in tests.
- **[Full Dashboard context may be expensive to reconstruct]** -> Extract or reuse only the existing Forward Pace view-model dependencies required by the partial rather than rendering the whole page.
- **[Content negotiation could accidentally alter legacy behavior]** -> Require an explicit JSON signal and test the unchanged form redirect path.
- **[External refreshes can make unrelated Dashboard data stale]** -> Refresh only Forward Pace by design; unrelated data remains unchanged until its normal refresh path runs.
- **[Financial display could diverge from persisted state]** -> Render the returned fragment only after successful persistence and rebuild it from server-authoritative settings.
- **[Users could interpret two balance values as contradictory]** -> Label the values explicitly, show the reserved buffer nearby, and retain a concise explanation that the buffer remains in the account but is excluded from available balance.
- **[Buffer could be deducted twice during refactoring]** -> Derive both net outputs directly from their gross bases and add exact numeric regression tests for zero, positive, and larger-than-profit buffers.
- **[Dirty worktree overlap]** -> Limit edits to the Forward Pace handler/view-model, the extracted partial, Dashboard JavaScript, and focused tests.

## Migration Plan

No data migration is required. Deploy the handler negotiation, partial, and JavaScript together. Rollback consists of removing interception and restoring the inline template block; the unchanged form action continues to support the legacy redirect workflow throughout.

## Open Questions

None. The existing Dashboard Forward Pace calculation and persistence behavior remain authoritative.
