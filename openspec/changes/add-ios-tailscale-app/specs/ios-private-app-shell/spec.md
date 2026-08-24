## ADDED Requirements
### Requirement: Private HTTPS host configuration
The iOS application SHALL connect only to a user-configured HTTPS origin whose host is within the
configured Tailscale `ts.net` namespace. It MUST reject insecure, malformed, credential-bearing,
or non-tailnet application origins and MUST NOT contain a public fallback endpoint.

#### Scenario: Configure a valid tailnet origin
- **WHEN** the user enters a normalized `https://<machine>.<tailnet>.ts.net` origin without credentials, query, or fragment
- **THEN** the app saves the origin locally and enables connection to McCain Capital

#### Scenario: Reject an unsafe origin
- **WHEN** the user enters an HTTP URL, an IP address, embedded credentials, or a host outside the allowed `ts.net` namespace
- **THEN** the app leaves the prior valid configuration unchanged and explains why the new value is unsafe

#### Scenario: No host has been configured
- **WHEN** the app launches without a saved valid origin
- **THEN** it presents native setup guidance instead of attempting a public or hard-coded connection

### Requirement: Existing web application remains authoritative
The iOS application SHALL present the existing responsive Flask application through a persistent
`WKWebView` data store. Flask authentication, server sessions, CSRF validation, routes, rendered
content, mutations, financial calculations, and SQLite persistence MUST remain authoritative.

#### Scenario: Authenticate through the iOS shell
- **WHEN** Flask requires authentication and returns its login flow
- **THEN** the iOS app presents that flow in the approved web origin and retains its session cookies across ordinary app launches

#### Scenario: Submit a protected mutation
- **WHEN** the user submits an existing CSRF-protected form or JavaScript request in the web view
- **THEN** the request uses the existing web session and page token without native credential injection or CSRF bypass

#### Scenario: Financial information is displayed or changed
- **WHEN** a financial page, calculation, or mutation is used from the iOS app
- **THEN** the existing Flask service and database determine the result with no native replica or offline write queue

### Requirement: Native availability and recovery states
The iOS application SHALL distinguish initial loading, connected content, offline networking,
unreachable host, and recoverable web-load failure states and SHALL offer a retry path without
discarding the saved origin or valid web session.

#### Scenario: Host becomes reachable
- **WHEN** the iPhone has tailnet connectivity and the configured Mac service responds successfully
- **THEN** the native loading state resolves to the existing McCain Capital web interface

#### Scenario: Tailnet or Mac is unavailable
- **WHEN** the configured endpoint cannot be reached because Tailscale is disconnected, the Mac is asleep, or the local service is stopped
- **THEN** the app presents an actionable unavailable state describing the likely causes and offers retry and settings actions

#### Scenario: Retry succeeds
- **WHEN** the user retries after connectivity or host service is restored
- **THEN** the app reloads the approved origin and returns to connected content without clearing cookies

### Requirement: Origin-bound navigation
The iOS application SHALL keep same-origin McCain Capital navigation inside its web view and SHALL
hand external HTTP or HTTPS destinations to the system browser only after an explicit user action.
Unsupported URL schemes MUST be denied unless they are deliberately handled by the operating
system and initiated by the user.

#### Scenario: Follow internal navigation
- **WHEN** the user follows a URL whose scheme, host, and effective port match the configured origin
- **THEN** the destination remains inside the iOS application

#### Scenario: Follow an external link
- **WHEN** the user deliberately follows an HTTP or HTTPS destination outside the configured origin
- **THEN** the app requests the operating system to open it outside the embedded McCain Capital session

#### Scenario: Page attempts an unapproved navigation
- **WHEN** content initiates an unsupported scheme or cross-origin navigation without a user action
- **THEN** the app cancels the navigation and does not disclose session state to that destination

### Requirement: Local device-owner app lock
The iOS application SHALL offer an optional local app lock using `LocalAuthentication`. When
enabled, protected web content MUST be obscured until the device owner authenticates after a cold
launch or after the configured background interval. The lock MUST NOT collect or store a Flask
password and MUST NOT represent successful device authentication as server authentication.

#### Scenario: Unlock succeeds
- **WHEN** the app lock is required and Face ID or device-passcode authentication succeeds
- **THEN** the existing web view becomes visible without recreating or bypassing its Flask session

#### Scenario: Unlock fails or is cancelled
- **WHEN** device-owner authentication fails, is cancelled, or is unavailable
- **THEN** protected web content remains obscured and the user can retry or leave the app

#### Scenario: App enters the background
- **WHEN** the app lock is enabled and the application crosses the configured background threshold
- **THEN** the application obscures content before it can be viewed in the foreground or app-switcher snapshot

### Requirement: iPhone-native interaction shell
The application SHALL be iOS-only and SHALL provide native safe-area handling, progress feedback,
pull-to-refresh, backward and forward navigation when available, and a settings path without
replacing the existing responsive page controls.

#### Scenario: Use the application on an iPhone
- **WHEN** a supported iPhone displays McCain Capital in portrait or landscape orientation
- **THEN** the web content remains inside safe areas and existing actionable controls are not covered by the native shell

#### Scenario: Refresh current content
- **WHEN** the user performs the native refresh gesture
- **THEN** the current approved web page reloads using the existing session

#### Scenario: Navigate web history
- **WHEN** backward or forward web history is available
- **THEN** the native controls perform the corresponding web-view navigation without leaving the approved origin

### Requirement: Desktop application preservation
The iOS capability SHALL NOT require a second backend, database migration, route replacement,
financial-rule change, or desktop-only layout change. The existing localhost desktop workflow MUST
remain available independently of the iOS client.

#### Scenario: Use the desktop application after installation
- **WHEN** the iOS application and Tailscale Serve configuration have been added
- **THEN** the current Flask/Podman application remains usable through its existing localhost port and data paths

#### Scenario: Remove the iOS application
- **WHEN** the iOS project is not built, the iPhone app is uninstalled, or Tailscale Serve is disabled
- **THEN** the desktop application continues operating without an iOS dependency

### Requirement: Private build and secret hygiene
The repository SHALL document a private Xcode signing and physical-device installation path and
MUST exclude signing credentials, provisioning material, Tailscale credentials, TLS private keys,
Flask secrets, and personal financial data from source control.

#### Scenario: Build for a privately signed device
- **WHEN** a developer selects their own Apple development team and a unique bundle identifier
- **THEN** the documented Xcode workflow produces an installable iPhone build without requiring App Store publication

#### Scenario: Inspect tracked iOS configuration
- **WHEN** the iOS project and setup documentation are reviewed
- **THEN** they contain no reusable secret or personal financial record and identify local-only values that must remain untracked
