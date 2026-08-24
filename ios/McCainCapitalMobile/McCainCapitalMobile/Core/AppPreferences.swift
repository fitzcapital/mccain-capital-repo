import Foundation

public final class AppPreferences {
    private enum Key {
        static let origin = "privateTailnetOrigin"
        static let appLockEnabled = "appLockEnabled"
    }

    private let defaults: UserDefaults

    public init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    public var origin: AppOrigin? {
        guard let value = defaults.string(forKey: Key.origin) else { return nil }
        return try? AppOrigin(value)
    }

    public var appLockEnabled: Bool {
        get { defaults.bool(forKey: Key.appLockEnabled) }
        set { defaults.set(newValue, forKey: Key.appLockEnabled) }
    }

    @discardableResult
    public func saveOrigin(_ input: String) throws -> AppOrigin {
        let origin = try AppOrigin(input)
        defaults.set(origin.string, forKey: Key.origin)
        return origin
    }

    public func clearOrigin() {
        defaults.removeObject(forKey: Key.origin)
    }
}
