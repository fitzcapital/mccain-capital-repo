# McCain Capital iPhone App

The iPhone app is a private native SwiftUI shell around the existing McCain Capital Flask app. The
Mac remains the server and source of truth. Tailscale Serve provides a trusted HTTPS address that
is reachable only from the authorized tailnet.

## Requirements

- A Mac running the existing McCain Capital Podman app on `127.0.0.1:5001`
- Full Xcode with the iOS SDK (Command Line Tools alone are not enough)
- An iPhone running iOS 17 or later
- Tailscale signed into the same authorized tailnet on the Mac and iPhone
- MagicDNS and HTTPS certificates enabled in the Tailscale DNS settings

Use a non-sensitive Tailscale machine name. HTTPS certificate names appear in public certificate
transparency logs even though access to the service remains private to the tailnet.

## 1. Start McCain Capital on the Mac

From the repository root:

```bash
./scripts/run_podman_app.sh
curl -sf http://127.0.0.1:5001/healthz
```

The desktop app remains available at [http://localhost:5001](http://localhost:5001). The Mac must
remain awake, online, connected to Tailscale, and running the Podman app while the iPhone is away
from the local network.

## 2. Create the private HTTPS address

Check the current machine and tailnet DNS name:

```bash
tailscale status
tailscale dns status
```

Configure Tailscale Serve as a persistent background reverse proxy:

```bash
tailscale serve --bg http://127.0.0.1:5001
tailscale serve status
```

Tailscale reports an address shaped like:

```text
https://your-mac.your-tailnet.ts.net
```

Do not use `tailscale funnel`; Funnel would make the service public. Do not enter the old plain
HTTP Tailscale-IP URL in the iPhone app—the native app intentionally rejects it.

On the iPhone:

1. Open Tailscale and confirm it says **Connected**.
2. Open Safari and visit `https://your-mac.your-tailnet.ts.net/healthz`.
3. Continue only after Safari receives the McCain Capital health response without a certificate
   warning.

If the `tailscale` CLI cannot access the macOS App Store client's preferences, open the Tailscale
menu-bar app, confirm it is signed in, and use the Serve controls exposed by your installed client.
The open-source Tailscale macOS variant offers the most complete CLI integration.

## 3. Install full Xcode

Install Xcode from the Mac App Store, launch it once, accept its license, and allow it to install the
iOS platform components. Then select it for command-line builds:

```bash
sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer
xcodebuild -version
xcrun --sdk iphoneos --show-sdk-path
```

The last two commands must succeed. This repository was prepared for iOS 17 or later and Swift 6.

## 4. Sign and run the iPhone app

Open the checked-in project:

```bash
open ios/McCainCapitalMobile/McCainCapitalMobile.xcodeproj
```

In Xcode:

1. Select the **McCainCapitalMobile** project and app target.
2. Open **Signing & Capabilities**.
3. Enable **Automatically manage signing**.
4. Choose your Apple development team.
5. Replace `com.example.McCainCapitalMobile` with a unique bundle identifier, such as
   `com.yourname.McCainCapitalMobile`.
6. Connect and unlock the iPhone, trust the Mac if prompted, and enable Developer Mode on the
   iPhone if iOS requests it.
7. Choose the physical iPhone as the run destination.
8. Press **Run** (`⌘R`).

A free Apple account can install development builds but they require periodic re-signing. A paid
Apple Developer Program account supports longer-lived development workflows and can later use
TestFlight; TestFlight is not required for this private first release.

## 5. Connect from the app

On first launch, enter only the origin reported by Tailscale Serve:

```text
https://your-mac.your-tailnet.ts.net
```

Do not include `/dashboard`, a port, query parameters, credentials, or a trailing route. Sign into
the existing Flask login page when it appears. The native app does not store or inject the Flask
password. Flask cookies, sessions, CSRF validation, routes, data, and financial calculations remain
authoritative.

In **Settings**, optionally enable **Require Face ID or device passcode**. This covers the web view
immediately whenever the app leaves the foreground; it does not replace Flask login.

## Development checks

The security-critical origin, navigation, and lock logic can be checked without the iOS SDK:

```bash
cd ios/McCainCapitalMobile
SWIFTPM_MODULECACHE_OVERRIDE="$PWD/.build/module-cache" \
CLANG_MODULE_CACHE_PATH="$PWD/.build/clang-cache" \
swift run --disable-sandbox McCainCapitalCoreChecks
```

With full Xcode installed:

```bash
xcodebuild \
  -project ios/McCainCapitalMobile/McCainCapitalMobile.xcodeproj \
  -scheme McCainCapitalMobile \
  -sdk iphonesimulator \
  -destination 'platform=iOS Simulator,name=iPhone 16 Pro' \
  test
```

If that named simulator is unavailable, list installed destinations and select an available iPhone:

```bash
xcodebuild \
  -project ios/McCainCapitalMobile/McCainCapitalMobile.xcodeproj \
  -scheme McCainCapitalMobile \
  -showdestinations
```

## Troubleshooting

### The app says the Mac service is unavailable

Check, in order:

```bash
curl -sf http://127.0.0.1:5001/healthz
tailscale status
tailscale serve status
```

Then confirm Tailscale is connected on the iPhone and test the HTTPS `/healthz` address in Safari.
The Mac may be asleep even when the configuration is correct.

### The app rejects the address

The address must be HTTPS, end in `.ts.net`, use no explicit port, and contain no route, query,
fragment, IP literal, or credentials.

### Xcode reports a signing error

Choose your own team and a globally unique bundle identifier. Never commit provisioning profiles,
certificates, private keys, or Apple account credentials.

### Rollback

Uninstall the iPhone app and disable Serve:

```bash
tailscale serve reset
```

Disabling Serve does not stop or modify the desktop application. McCain Capital remains available
through `http://localhost:5001` whenever its existing Podman container is running.
