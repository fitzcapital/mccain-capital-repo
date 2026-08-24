import Foundation

public enum NavigationDecision: Equatable, Sendable {
    case allowInApp
    case openExternally
    case cancel
}

public struct NavigationPolicy: Sendable {
    public let origin: AppOrigin

    public init(origin: AppOrigin) {
        self.origin = origin
    }

    public func decision(for url: URL, userInitiated: Bool) -> NavigationDecision {
        if origin.contains(url) {
            return .allowInApp
        }
        let scheme = url.scheme?.lowercased()
        if userInitiated, scheme == "https" || scheme == "http" {
            return .openExternally
        }
        return .cancel
    }
}
