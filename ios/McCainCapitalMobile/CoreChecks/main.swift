import Foundation
import McCainCapitalCore

private var failures: [String] = []

@MainActor
private func check(_ condition: @autoclosure () -> Bool, _ message: String) {
    if !condition() { failures.append(message) }
}

do {
    let origin = try AppOrigin("  HTTPS://McCain-Mac.Example.TS.NET/  ")
    check(origin.string == "https://mccain-mac.example.ts.net", "origin normalization")
    check(
        origin.contains(URL(string: "https://mccain-mac.example.ts.net/dashboard")!),
        "same-origin path"
    )
    check(
        !origin.contains(URL(string: "https://other.example.ts.net/dashboard")!),
        "cross-origin rejection"
    )
    check(
        !origin.contains(URL(string: "https://user@mccain-mac.example.ts.net/dashboard")!),
        "credential-bearing navigation rejection"
    )
    let policy = NavigationPolicy(origin: origin)
    check(
        policy.decision(
            for: URL(string: "https://openai.com")!,
            userInitiated: true
        ) == .openExternally,
        "external user navigation"
    )
    check(
        policy.decision(
            for: URL(string: "https://openai.com")!,
            userInitiated: false
        ) == .cancel,
        "automatic cross-origin rejection"
    )
} catch {
    failures.append("valid origin unexpectedly failed: \(error)")
}

for unsafe in [
    "",
    "http://mccain-mac.example.ts.net",
    "https://100.64.0.1",
    "https://user:password@mccain-mac.example.ts.net",
    "https://mccain-mac.example.ts.net:443",
    "https://mccain-mac.example.ts.net/dashboard",
    "https://mccain-mac.example.ts.net?next=/dashboard",
    "https://mccain-mac.example.ts.net.evil.test",
    "https://ts.net",
] {
    do {
        _ = try AppOrigin(unsafe)
        failures.append("unsafe origin accepted: \(unsafe)")
    } catch {}
}

var lock = LockState(enabled: true)
check(lock.isLocked, "enabled lock starts covered")
lock.unlock()
check(!lock.isLocked, "unlock transition")
lock.enteredBackground(at: Date(), enabled: true)
check(lock.isLocked, "background privacy cover")

if failures.isEmpty {
    print("McCainCapitalCoreChecks: all checks passed")
} else {
    for failure in failures { print("FAIL: \(failure)") }
    exit(1)
}
