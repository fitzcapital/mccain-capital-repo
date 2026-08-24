## Why

McCain Capital is usable in a mobile browser, but it does not provide a dedicated iOS app
experience or a secure, stable launch path to the Mac-hosted application while away from the local
network. An iOS-only companion app should make the existing private application feel native on an
iPhone without duplicating its backend, financial logic, or data and without changing the desktop
experience.

## What Changes

- Add a separately contained SwiftUI iOS application that presents the existing responsive Flask
  interface in a managed `WKWebView`.
- Connect the iOS app only to a configured HTTPS `*.ts.net` endpoint exposed privately through
  Tailscale Serve; do not expose McCain Capital to the public internet.
- Add native launch, connection, loading, retry, offline, and host-unavailable states around the web
  experience.
- Add an optional local Face ID or device-passcode app lock without replacing the existing Flask
  authentication, session, or CSRF controls.
- Preserve browser navigation, downloads and external-link handling, session cookies, form
  submissions, JavaScript behavior, safe-area layout, and pull-to-refresh where compatible.
- Add focused setup documentation for the Mac host, Tailscale Serve, Xcode signing, private iPhone
  installation, and connectivity troubleshooting.
- Keep the existing Flask/Podman desktop application, routes, data storage, business rules, and
  localhost workflow behaviorally unchanged.
- Non-goals: Android support, App Store publication in the initial release, public hosting,
  Tailscale Funnel, a second backend, a second database, offline financial-data editing, push
  notifications, or a native rewrite of individual Flask screens.

## Capabilities

### New Capabilities

- `ios-private-app-shell`: Defines the iOS-only native shell, private Tailscale HTTPS connection,
  local app lock, web-session behavior, navigation, availability states, and desktop-preservation
  contract.

### Modified Capabilities

None. The existing application has no main OpenSpec capability requiring a behavioral change for
the initial iOS shell.

## Acceptance Criteria

- On a supported iPhone joined to the authorized tailnet, the installed app reaches the configured
  McCain Capital HTTPS endpoint and displays the existing authenticated mobile interface.
- When the Mac app is stopped, asleep, unreachable, or the iPhone is disconnected from Tailscale,
  the app shows a bounded native recovery state with retry rather than an empty web view.
- Existing Flask login, cookies, CSRF-protected mutations, internal navigation, and supported
  JavaScript workflows operate from the iOS shell.
- Optional device-owner authentication locks access after the configured background interval and
  does not store the Flask password or bypass server authentication.
- External destinations open outside the embedded app, while approved McCain Capital tailnet URLs
  remain in the app.
- The desktop site remains available at its current localhost port with no route, financial-rule,
  database, or desktop-layout regressions.
- The iOS target builds successfully with the repository's documented supported Xcode version and
  runs on a physical iPhone through private signing.
- No secrets, personal financial records, Tailscale keys, generated certificates, or signing
  credentials are committed to source control.

## Impact

- New source area: an iOS Xcode project under `ios/`, isolated from the Python application runtime.
- Host configuration: Tailscale Serve terminates HTTPS and proxies privately to the existing
  loopback Flask/Podman port.
- Existing systems reused without changing their authority: Flask authentication, server-side
  sessions, CSRF protection, responsive Jinja pages, SQLite persistence, and all current financial
  calculations.
- New platform dependencies are limited to Apple system frameworks such as SwiftUI, WebKit,
  LocalAuthentication, and Network; no third-party iOS package is required initially.
- Documentation and focused tests will cover URL allowlisting, navigation decisions, app-lock state,
  configuration validation, and desktop regression boundaries.
