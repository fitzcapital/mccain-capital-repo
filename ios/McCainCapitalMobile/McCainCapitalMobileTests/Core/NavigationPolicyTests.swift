import Foundation
import XCTest
#if SWIFT_PACKAGE
@testable import McCainCapitalCore
#endif

final class NavigationPolicyTests: XCTestCase {
    func testNavigationDecisionsAreOriginBound() throws {
        let policy = NavigationPolicy(origin: try AppOrigin("https://mccain.example.ts.net"))
        XCTAssertEqual(
            policy.decision(
                for: URL(string: "https://mccain.example.ts.net/dashboard")!,
                userInitiated: false
            ),
            .allowInApp
        )
        XCTAssertEqual(
            policy.decision(for: URL(string: "https://openai.com")!, userInitiated: true),
            .openExternally
        )
        XCTAssertEqual(
            policy.decision(for: URL(string: "https://openai.com")!, userInitiated: false),
            .cancel
        )
        XCTAssertEqual(
            policy.decision(for: URL(string: "javascript:alert(1)")!, userInitiated: true),
            .cancel
        )
    }
}
