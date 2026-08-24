import Foundation

public struct LockState: Equatable, Sendable {
    public private(set) var isLocked: Bool
    public private(set) var backgroundedAt: Date?

    public init(enabled: Bool) {
        isLocked = enabled
    }

    public mutating func enteredBackground(at date: Date, enabled: Bool) {
        backgroundedAt = date
        if enabled { isLocked = true }
    }

    public mutating func enteredForeground(
        at date: Date,
        enabled: Bool,
        graceInterval: TimeInterval
    ) {
        guard enabled else {
            isLocked = false
            backgroundedAt = nil
            return
        }
        if let backgroundedAt, date.timeIntervalSince(backgroundedAt) >= graceInterval {
            isLocked = true
        }
    }

    public mutating func unlock() {
        isLocked = false
        backgroundedAt = nil
    }

    public mutating func requireLock() {
        isLocked = true
    }
}
