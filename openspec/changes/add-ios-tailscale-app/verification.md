# Verification

## Implemented

- Added an isolated iPhone-only SwiftUI project at `ios/McCainCapitalMobile/` with iOS 17 as the
  minimum deployment target and no third-party runtime dependency.
- Added strict HTTPS `*.ts.net` origin validation, normalized local configuration, persistent
  WebKit website data, exact-origin embedded navigation, external-link routing, explicit download
  behavior, and host-change website-data cleanup.
- Added native setup, loading, connected, unavailable, retry, settings, refresh, web history, and
  safe-area presentation.
- Added an opt-in immediate `LocalAuthentication` privacy lock and foreground/background privacy
  cover without collecting or bypassing Flask credentials.
- Added portable core checks, Xcode unit tests, deterministic XCUITest launch arguments, focused UI
  tests, private Tailscale Serve instructions, signing instructions, troubleshooting, and rollback.
- Reused the existing tracked McCain Capital icon PNGs without changing desktop assets.

## Verified in this environment

- Host: Apple Silicon macOS; Apple Swift 6.4 command-line toolchain.
- Full Xcode/iOS SDK: unavailable. The active developer directory is
  `/Library/Developer/CommandLineTools`; `xcodebuild` reports that full Xcode is required and
  `xcrun --sdk iphoneos` cannot locate the SDK.
- Portable core build/check:
  `swift run --disable-sandbox McCainCapitalCoreChecks` completed and reported all checks passed.
- `swiftc -parse` accepted every application, core, unit-test, and UI-test Swift source.
- `plutil -lint` accepted `Info.plist` and the checked-in `project.pbxproj`.
- `git diff --check` passed for the iOS app, documentation, ignore rules, and change artifacts.
- Focused Flask regression test: `2 passed, 146 deselected` for health/login coverage in
  `tests/test_app_core.py`.
- Existing deployed desktop `/healthz` returned HTTP success with `status: ok` and
  `safe_mode: false` from `http://127.0.0.1:5001`.
- No container rebuild was needed because no Flask runtime, container, financial rule, route, or
  persistence code was changed by this iOS change.

## Remaining physical-device acceptance

These checks require the user's full Xcode installation, Apple signing identity, iPhone, and live
tailnet and therefore are not marked complete without receiving-surface evidence:

- Build/test the iOS target in an installed simulator and on the physical iPhone.
- Select the user's development team and unique bundle identifier; install the signed application.
- Configure and verify Tailscale Serve HTTPS from the iPhone, including `/healthz`.
- Verify real Flask login/cookie persistence, representative read workflows, an approved reversible
  CSRF mutation, external links/downloads, orientation, relaunch, and app-lock behavior.
- Verify Tailscale disconnection, sleeping Mac, stopped service, recovery, and app-switcher privacy.
- Perform final browser visual smoke checks after physical-device acceptance if any web compatibility
  issue requires a server-side adjustment.

## Known limitations

- The Mac must remain awake, online, connected to Tailscale, and running McCain Capital. There is no
  offline financial cache or write queue.
- The first release is iPhone-only and privately signed; it is not an App Store/TestFlight release.
- A free Apple account may require periodic re-signing.
- Tailscale certificate names appear in certificate transparency logs, so the Mac name must remain
  non-sensitive even though Serve access is private.

## Rollback

Uninstalling the iPhone application or running `tailscale serve reset` removes mobile access without
changing the existing desktop app. No database or runtime-data migration was introduced.
