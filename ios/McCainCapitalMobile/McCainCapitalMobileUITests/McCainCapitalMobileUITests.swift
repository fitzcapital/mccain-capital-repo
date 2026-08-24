import XCTest

final class McCainCapitalMobileUITests: XCTestCase {
    func testFirstRunRejectsUnsafeOrigin() {
        let app = XCUIApplication()
        app.launchArguments = [
            "-ui-testing-reset",
            "-AppleLanguages", "(en)",
            "-AppleLocale", "en_US",
        ]
        app.launch()

        let field = app.textFields["originField"]
        XCTAssertTrue(field.waitForExistence(timeout: 5))
        field.tap()
        field.typeText("http://100.64.0.1:5001")
        app.buttons["connectButton"].tap()
        XCTAssertTrue(app.staticTexts["The app requires HTTPS through Tailscale Serve."].waitForExistence(timeout: 2))
    }

    func testUnreachableHostOffersRetryAndSettings() {
        let app = XCUIApplication()
        app.launchArguments = [
            "-ui-testing-reset",
            "-ui-testing-origin", "https://unreachable.ui-tests.ts.net",
        ]
        app.launch()

        XCTAssertTrue(app.buttons["retryButton"].waitForExistence(timeout: 20))
        XCTAssertTrue(app.buttons["Connection settings"].exists)
        app.buttons["retryButton"].tap()
    }
}
