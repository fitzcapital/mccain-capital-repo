import Foundation
import XCTest
#if SWIFT_PACKAGE
@testable import McCainCapitalCore
#endif

final class LockStateTests: XCTestCase {
    func testEnabledLockStartsCoveredAndUnlocks() {
        var state = LockState(enabled: true)
        XCTAssertTrue(state.isLocked)
        state.unlock()
        XCTAssertFalse(state.isLocked)
    }

    func testBackgroundRequiresLockWhenEnabled() {
        var state = LockState(enabled: false)
        state.enteredBackground(at: Date(), enabled: true)
        XCTAssertTrue(state.isLocked)
    }

    func testDisabledLockStaysOpenOnForeground() {
        var state = LockState(enabled: true)
        state.enteredForeground(at: Date(), enabled: false, graceInterval: 0)
        XCTAssertFalse(state.isLocked)
    }
}
