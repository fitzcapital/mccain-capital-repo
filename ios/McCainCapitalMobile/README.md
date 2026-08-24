# McCain Capital Mobile

An iPhone-only SwiftUI shell for the existing McCain Capital Flask application. The app connects
to one private Tailscale Serve HTTPS origin, keeps Flask as the source of truth, and adds native
connection recovery plus an optional Face ID/device-passcode privacy lock.

See [../../docs/IOS_APP_SETUP.md](../../docs/IOS_APP_SETUP.md) for setup and run instructions.

Requirements:

- iOS 17 or later
- Full Xcode with the iOS SDK
- Tailscale on the Mac and iPhone
- A private `https://<machine>.<tailnet>.ts.net` Tailscale Serve endpoint

No Flask password, Tailscale key, TLS key, signing credential, or financial record belongs in this
directory.
