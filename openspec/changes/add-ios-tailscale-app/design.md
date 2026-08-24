## Context

McCain Capital currently runs as a Python 3.11 Flask application in Podman, publishes port 5001,
stores its authoritative data on the Mac, and already has responsive iPhone layouts. The README
documents access through a Tailscale IP over HTTP. A true iOS application needs a native lifecycle
and recovery experience, but rewriting the Flask pages or financial logic would create two clients
with divergent behavior and unnecessary data risk.

The new client must operate only on iPhone, remain private to the user's tailnet, work with the
existing Flask login/session/CSRF model, and have no runtime role in the desktop application. Apple
App Transport Security favors trusted HTTPS, so the host connection will move from an IP-and-port
URL to a Tailscale Serve `*.ts.net` HTTPS origin.

## Goals / Non-Goals

**Goals:**

- Provide an installable SwiftUI iPhone app that makes the existing mobile web interface feel like
  a coherent native application.
- Keep all financial data, calculations, credentials, and mutations authoritative on the existing
  Flask application and Mac storage.
- Use tailnet-only HTTPS with strict origin validation and navigation isolation.
- Provide native configuration, loading, unavailable, retry, settings, and optional device-owner
  lock experiences.
- Keep the desktop Flask/Podman workflow operational and independently removable from the iOS app.
- Support focused unit, UI, physical-device, connectivity, and desktop-regression verification.

**Non-Goals:**

- Native reimplementation of Flask screens, business rules, charts, forms, or persistence.
- Public internet exposure, Tailscale Funnel, App Store submission, Android, iPad-specific layouts,
  offline data editing, background financial synchronization, or push notifications.
- Bundling Tailscale credentials, installing/configuring the Tailscale iOS app programmatically, or
  bypassing Flask authentication.

## Decisions

### 1. Add an isolated SwiftUI application under `ios/`

The implementation will add a conventional Xcode project and Swift source tree under
`ios/McCainCapitalMobile/`. The deployment target will be iOS 17 or later unless the installed
Xcode toolchain requires a compatible adjustment discovered during implementation. The first
release uses only SwiftUI, WebKit, LocalAuthentication, Network, and other Apple frameworks.

This keeps iOS code visible and versioned with the application it presents while preventing it
from entering the Python runtime or container image. A PWA was considered, but it would not provide
the requested true app container, controlled native lock screen, native recovery state, or private
Xcode installation workflow. A full native API client was rejected because it would require a new
API surface and duplicate a large, actively changing UI and financial contract.

### 2. Use Tailscale Serve as the only initial remote transport

The Mac will keep the application bound/published on its established local port. Tailscale Serve
will terminate trusted HTTPS at a stable fully qualified MagicDNS name and reverse-proxy to
`http://127.0.0.1:5001`. Serve, not Funnel, keeps reachability inside the tailnet and avoids broad
App Transport Security exceptions in the iOS target.

Setup documentation will require Tailscale on both devices, MagicDNS/HTTPS enabled for the tailnet,
an intentionally non-sensitive Mac machine name, Serve configured in persistent background mode,
and a health check from the iPhone. The certificate name is publicly logged by the certificate
authority even though the service remains private, so naming guidance is part of the setup.

Direct `http://<Tailscale-IP>:5001` was considered as a fallback but rejected for the application:
it requires weakening transport policy, is less stable than a named origin, and makes precise
origin allowlisting harder.

### 3. Configure and validate one application origin

The app will show first-run settings for a single base origin. A small pure Swift configuration
model will normalize and validate the URL before saving it in `UserDefaults`. Valid configuration
requires HTTPS, no embedded credentials, no query or fragment, and a hostname within the explicit
`ts.net` suffix. The app will store no Tailscale auth key or Flask password.

The normalized scheme, host, and effective port form the navigation allowlist. This is stricter
than allowing all tailnet hosts and prevents an authenticated web session from drifting into an
unrelated site. Changing the origin will require a confirmation and clear the old origin's website
data to prevent cookie confusion across hosts.

A compile-time hard-coded URL was considered but rejected because machine/tailnet names and signing
identities differ between installations. Arbitrary URL support was rejected because the app handles
private financial content and is not intended to become a general browser.

### 4. Keep one persistent `WKWebView` owned by an observable model

SwiftUI will host one `WKWebView` through `UIViewRepresentable`; an observable coordinator/model
will own navigation state, progress, errors, history availability, and retry actions. The default
persistent website data store retains Flask cookies. The app will not inject credentials, rewrite
HTML, manufacture CSRF tokens, or introduce a parallel native API session.

Same-origin navigation stays embedded. User-initiated external HTTP/HTTPS links open through the
system. New-window requests are evaluated through the same policy. Unsupported schemes and
non-user-initiated cross-origin requests are cancelled. Downloads will use the platform download
path where supported or fail with an explicit message rather than silently losing content.

The web view will use safe-area-aware layout, observable load progress, refresh control, and minimal
back/forward controls. Existing responsive navigation remains the primary in-content navigation so
the shell does not create a second menu hierarchy.

### 5. Model availability independently from the web session

An application state model will represent `needsConfiguration`, `locked`, `loading`, `content`, and
`unavailable`. `NWPathMonitor` can identify general network loss, while WebKit provisional and
committed navigation errors determine whether the configured service is reachable. The UI will not
claim that Tailscale specifically is down when the evidence only establishes that the host is
unreachable; it will list Tailscale, sleeping Mac, and stopped app as troubleshooting checks.

Retry reloads the approved origin without clearing cookies. Settings remains reachable from every
failure state. No offline cache or queued mutation is introduced because stale financial screens
and deferred writes would be unsafe and could misrepresent current account state.

### 6. Treat Face ID as a local privacy curtain, not authentication

An opt-in setting will use `LAContext` with device-owner authentication, allowing Face ID with the
system passcode fallback. The lock covers the web view on cold launch and after a conservative
background grace interval. A privacy cover appears as the app resigns active so iOS app-switcher
snapshots do not expose financial information.

Unlocking only reveals the current WebKit state. It neither logs into Flask nor extends the Flask
session. Enabling the lock when no device-owner authentication is available will be rejected with a
clear explanation. The setting and last-background timestamp contain no credential and can remain
in local preferences.

### 7. Verify the receiving experience without disturbing runtime data

Pure Swift models for configuration, navigation policy, and lock transitions will receive focused
unit tests. XCUITest coverage will exercise first-run setup, invalid configuration, unavailable-host
recovery, settings access, and privacy-cover behavior using deterministic app launch arguments or a
local fixture that contains no personal data. Physical-device acceptance will verify Tailscale HTTPS,
Flask login/session persistence, representative read and CSRF-protected write flows, external links,
orientation, relaunch, background locking, and Mac-unavailable recovery.

Desktop verification will use existing focused Flask tests plus `/healthz` and representative
desktop/mobile browser smoke checks. Tests and setup must not modify `journal.db`, `persistent-data/`,
uploads, artifacts, environment secrets, or real signing material.

## Data Flow

```text
SwiftUI shell
  -> validates and loads one HTTPS *.ts.net origin
  -> WKWebView sends normal browser requests and cookies
  -> Tailscale encrypted tailnet + Serve TLS termination
  -> reverse proxy to Mac loopback port 5001
  -> existing Flask auth / CSRF / services / repositories
  -> existing SQLite and persistent data
  -> normal HTML, JSON, static assets, and redirects return to WKWebView
```

No financial record is persisted or calculated by native code. Native storage is limited to the
validated origin and non-secret preferences. WebKit owns its standard cookie/cache data on device.

## Risks / Trade-offs

- [Risk] The Mac is asleep or Podman is stopped while the user is away. → Present a truthful
  unavailable state and document Mac power/runtime prerequisites; do not imply cloud availability.
- [Risk] A broad navigation policy could leak an authenticated browsing context. → Allow embedded
  navigation only for the exact configured origin and send deliberate external links to the system.
- [Risk] WebKit behavior differs from Safari for downloads, pop-ups, authentication, or JavaScript.
  → Test representative current workflows on a physical iPhone and provide explicit unsupported
  behavior instead of silent failure.
- [Risk] A web wrapper can feel insufficiently native. → Add native lifecycle, security, progress,
  error recovery, refresh, history, settings, safe-area handling, launch assets, and app identity
  while retaining one authoritative web UI.
- [Risk] Persistent cookies leave sensitive content available on a lost unlocked phone. → Offer
  device-owner locking, obscure snapshots, retain Flask authentication, and document device security.
- [Risk] HTTPS certificate names are entered in public certificate transparency records. → Require a
  deliberately non-sensitive Tailscale machine name and document that privacy characteristic.
- [Risk] Developer-signed installations can expire depending on Apple account type. → Document the
  distinction and keep TestFlight/App Store distribution as a later choice.
- [Trade-off] The app cannot function when the Mac or tailnet path is unavailable. → Accept this as
  the consequence of keeping data local and avoid unsafe stale/offline financial state.

## Migration Plan

1. Add the isolated iOS project, tests, placeholder-safe app assets, and ignored local signing files.
2. Configure a non-sensitive MagicDNS machine name and Tailscale Serve HTTPS proxy to loopback port
   5001; verify that the endpoint is tailnet-only and `/healthz` responds from the iPhone.
3. Build and run the app in the iOS simulator for deterministic shell tests, then sign and install it
   on the user's physical iPhone through Xcode.
4. Configure the HTTPS origin in-app and verify authentication, read flows, selected safe mutations,
   relaunch, external links, host-unavailable recovery, and local lock behavior.
5. Run focused server tests, `/healthz`, and desktop/mobile browser regression checks before delivery.

Rollback is independent by layer: uninstall the iOS app, disable Tailscale Serve, and remove the
isolated `ios/` source without changing Flask or its data. No data migration or irreversible runtime
step is required.

## Open Questions

- Confirm the exact iPhone model/iOS version and installed Xcode version during implementation; use
  iOS 17 as the baseline unless the user's devices establish a different supported minimum.
- Confirm whether the user's Apple account supports only direct development signing or whether a
  paid Developer Program/TestFlight path is desired after the private first release.
- Confirm the desired app-lock grace interval during physical-device acceptance; default to
  immediate lock after leaving the foreground if no preference is expressed.
