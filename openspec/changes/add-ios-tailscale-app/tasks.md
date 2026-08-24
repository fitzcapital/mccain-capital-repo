## 1. Toolchain and Project Foundation

- [ ] 1.1 Record the installed Xcode and Swift versions, confirm the target iPhone/iOS version, and set the supported deployment target without changing unrelated build tooling.
- [x] 1.2 Create the isolated `ios/McCainCapitalMobile/` SwiftUI Xcode project with an iPhone-only target, unit-test target, UI-test target, and Apple-system-framework dependencies only.
- [x] 1.3 Add repository ignore rules for local signing, user-specific Xcode state, build output, TLS material, and other generated iOS files while retaining shared project configuration.
- [x] 1.4 Add non-sensitive application identity, launch screen, accent styling, and placeholder-safe app icons consistent with McCain Capital without changing desktop assets.
- [x] 1.5 Build the empty iOS application and run its unit-test bundle in an available simulator or document the exact physical-device-only blocker.

## 2. Secure Origin Configuration

- [x] 2.1 Implement a pure Swift origin model that trims and normalizes one HTTPS base origin and rejects IPs, credentials, query/fragment values, non-HTTPS schemes, and hosts outside `ts.net`.
- [x] 2.2 Persist only the normalized origin and non-secret preferences in local application settings, with no Flask password, Tailscale credential, certificate, or financial record.
- [x] 2.3 Implement first-run and settings views with validation feedback, save/cancel behavior, host-change confirmation, and old-origin website-data cleanup.
- [x] 2.4 Add focused unit tests for valid origins, case and trailing-slash normalization, malformed input, unsafe schemes, IP literals, embedded credentials, lookalike suffixes, and configuration replacement.

## 3. WebKit Application Shell

- [x] 3.1 Implement a single persistent `WKWebView` bridge and observable coordinator using the default website data store, page progress, title, history state, and lifecycle-safe ownership.
- [x] 3.2 Load the configured origin without injecting credentials, HTML, CSRF tokens, or a parallel API session, and preserve ordinary Flask session cookies across app launches.
- [x] 3.3 Implement exact-origin navigation policy for redirects, clicked links, and new-window requests; route deliberate external HTTP/HTTPS links to the system and reject unapproved schemes or automatic cross-origin navigation.
- [x] 3.4 Add safe-area-aware content presentation, native progress, pull-to-refresh, back/forward actions, settings access, and accessible labels without duplicating the existing responsive navigation hierarchy.
- [x] 3.5 Implement supported download handling or explicit download failure feedback so a file action never disappears silently.
- [x] 3.6 Add focused unit tests for same-origin matching, effective ports, subdomain and suffix attacks, redirect decisions, external user actions, new-window requests, and unsupported schemes.

## 4. Availability and Recovery

- [x] 4.1 Implement explicit configuration, loading, connected, and unavailable states driven by WebKit navigation results and general network-path status.
- [x] 4.2 Add a native unavailable screen with truthful Tailscale, sleeping-Mac, and stopped-service troubleshooting, plus retry and settings actions.
- [x] 4.3 Ensure retry reloads the approved origin without clearing cookies and that settings remains accessible after provisional, committed, TLS, DNS, offline, and timeout failures.
- [x] 4.4 Add deterministic UI-test launch configuration and tests for first run, unsafe configuration, initial loading, unreachable host, retry, and settings recovery without accessing personal runtime data.

## 5. Local Device-Owner Lock

- [x] 5.1 Implement an opt-in `LocalAuthentication` service that verifies device-owner authentication availability and supports Face ID with system passcode fallback.
- [x] 5.2 Add lock state transitions for cold launch and background timeout, with an immediate default unless a different interval is confirmed during device acceptance.
- [x] 5.3 Obscure WebKit content before inactive/background app snapshots and keep it covered after failed, cancelled, or unavailable authentication.
- [x] 5.4 Add settings and retry UI that clearly distinguishes the device privacy lock from Flask login and never collects a Flask credential.
- [ ] 5.5 Add focused tests for disabled lock, successful unlock, failure/cancellation, unavailable authentication, background threshold, relaunch, and privacy-cover transitions.

## 6. Tailscale and Private Installation Documentation

- [x] 6.1 Document prerequisites for Tailscale on Mac and iPhone, a non-sensitive MagicDNS machine name, tailnet HTTPS, and the certificate-transparency naming implication.
- [x] 6.2 Document a persistent, tailnet-only Tailscale Serve HTTPS proxy to `http://127.0.0.1:5001`, status inspection, iPhone `/healthz` verification, and explicit avoidance of Funnel.
- [x] 6.3 Document the Mac availability prerequisites, app start workflow, failure diagnosis, Serve disable/rollback commands, and the continued independent localhost desktop path.
- [x] 6.4 Document Xcode team selection, unique bundle identifier setup, physical-iPhone signing/install steps, developer-signing expiration limitations, and an optional later TestFlight path.
- [x] 6.5 Review all tracked iOS files and documentation for signing credentials, TLS keys, Tailscale credentials, Flask secrets, absolute personal paths, and financial records before acceptance.

## 7. Receiving-Surface Verification

- [ ] 7.1 Run Swift unit tests, UI tests, static analysis, and a clean iPhone-target build with the selected Xcode toolchain.
- [ ] 7.2 On a physical iPhone connected to the authorized tailnet, verify initial setup, trusted HTTPS, Flask login, cookie persistence after relaunch, internal navigation, refresh, history controls, rotation, and settings.
- [ ] 7.3 Verify representative read-only Dashboard and analytics flows plus selected reversible CSRF-protected form and JavaScript mutations against non-personal test data or an explicitly approved safe target.
- [ ] 7.4 Verify deliberate external links, blocked unsafe navigation, supported download behavior, invalid configuration, Tailscale disconnect, sleeping/unreachable Mac, stopped Flask/Podman service, recovery, and app-lock privacy.
- [ ] 7.5 Run focused existing Flask tests, JavaScript syntax checks, `/healthz`, and representative desktop and mobile browser smoke checks to confirm there is no route, financial-rule, persistence, or desktop-layout regression.
- [x] 7.6 Inspect the final diff to confirm the iOS project remains isolated, unrelated dirty-worktree changes are untouched, and no runtime or personal-data file was modified.
- [x] 7.7 Record the tested iPhone/iOS, Xcode/Swift, signing method, configured transport shape, executed checks, known limitations, and rollback result in the change verification notes.
