import Foundation
import XCTest
#if SWIFT_PACKAGE
@testable import McCainCapitalCore
#endif

final class AppOriginTests: XCTestCase {
    func testNormalizesValidTailnetOrigin() throws {
        let origin = try AppOrigin("  HTTPS://McCain-Mac.Example.TS.NET/  ")
        XCTAssertEqual(origin.string, "https://mccain-mac.example.ts.net")
    }

    func testRejectsUnsafeOrigins() {
        let values = [
            "",
            "http://mccain-mac.example.ts.net",
            "https://100.64.0.1",
            "https://user:password@mccain-mac.example.ts.net",
            "https://mccain-mac.example.ts.net:443",
            "https://mccain-mac.example.ts.net/dashboard",
            "https://mccain-mac.example.ts.net?next=/dashboard",
            "https://mccain-mac.example.ts.net.evil.test",
            "https://ts.net",
        ]
        for value in values {
            XCTAssertThrowsError(try AppOrigin(value), "Expected rejection: \(value)")
        }
    }

    func testMatchesOnlyExactOrigin() throws {
        let origin = try AppOrigin("https://mccain-mac.example.ts.net")
        XCTAssertTrue(origin.contains(URL(string: "https://mccain-mac.example.ts.net/dashboard")!))
        XCTAssertFalse(origin.contains(URL(string: "https://other.example.ts.net/dashboard")!))
        XCTAssertFalse(origin.contains(URL(string: "http://mccain-mac.example.ts.net/dashboard")!))
        XCTAssertFalse(origin.contains(URL(string: "https://mccain-mac.example.ts.net:444/dashboard")!))
    }

    func testStoresOnlyNormalizedPreferences() throws {
        let suite = "AppOriginTests-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suite)!
        defer { defaults.removePersistentDomain(forName: suite) }
        let preferences = AppPreferences(defaults: defaults)
        try preferences.saveOrigin("HTTPS://McCain-Mac.Example.TS.NET/")
        XCTAssertEqual(preferences.origin?.string, "https://mccain-mac.example.ts.net")
    }
}
