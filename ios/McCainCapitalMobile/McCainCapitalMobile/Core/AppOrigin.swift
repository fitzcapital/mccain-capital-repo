import Foundation

public enum AppOriginError: LocalizedError, Equatable {
    case empty
    case malformed
    case requiresHTTPS
    case credentialsNotAllowed
    case portNotAllowed
    case pathNotAllowed
    case queryOrFragmentNotAllowed
    case requiresTailnetHostname

    public var errorDescription: String? {
        switch self {
        case .empty:
            return "Enter your private Tailscale HTTPS address."
        case .malformed:
            return "Enter a complete URL, such as https://mccain-mac.example.ts.net."
        case .requiresHTTPS:
            return "The app requires HTTPS through Tailscale Serve."
        case .credentialsNotAllowed:
            return "Usernames and passwords are not allowed in the address."
        case .portNotAllowed:
            return "Use the standard Tailscale Serve HTTPS port without an explicit port."
        case .pathNotAllowed:
            return "Enter the server origin only, without a path."
        case .queryOrFragmentNotAllowed:
            return "Query strings and fragments are not allowed in the server address."
        case .requiresTailnetHostname:
            return "Use a fully qualified Tailscale hostname ending in .ts.net."
        }
    }
}

public struct AppOrigin: Equatable, Sendable {
    public let url: URL

    public init(_ input: String) throws {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { throw AppOriginError.empty }
        guard var components = URLComponents(string: trimmed), components.url != nil else {
            throw AppOriginError.malformed
        }
        guard components.scheme?.lowercased() == "https" else {
            throw AppOriginError.requiresHTTPS
        }
        guard components.user == nil, components.password == nil else {
            throw AppOriginError.credentialsNotAllowed
        }
        guard components.port == nil else { throw AppOriginError.portNotAllowed }
        guard components.query == nil, components.fragment == nil else {
            throw AppOriginError.queryOrFragmentNotAllowed
        }
        guard let rawHost = components.host?.lowercased(), Self.isTailnetHostname(rawHost) else {
            throw AppOriginError.requiresTailnetHostname
        }
        let path = components.percentEncodedPath
        guard path.isEmpty || path == "/" else { throw AppOriginError.pathNotAllowed }

        components.scheme = "https"
        components.host = rawHost
        components.path = ""
        guard let normalized = components.url else { throw AppOriginError.malformed }
        self.url = normalized
    }

    public var string: String { url.absoluteString }

    public func contains(_ candidate: URL) -> Bool {
        guard let scheme = candidate.scheme?.lowercased(),
              let host = candidate.host?.lowercased(),
              candidate.user == nil,
              candidate.password == nil
        else { return false }
        return scheme == "https" && host == url.host?.lowercased() && candidate.port == nil
    }

    private static func isTailnetHostname(_ host: String) -> Bool {
        guard host.hasSuffix(".ts.net"), !host.hasPrefix("."), !host.contains("..") else {
            return false
        }
        let labels = host.split(separator: ".", omittingEmptySubsequences: false)
        guard labels.count >= 4 else { return false }
        return labels.allSatisfy { label in
            guard let first = label.first, let last = label.last,
                  first.isLetter || first.isNumber,
                  last.isLetter || last.isNumber
            else { return false }
            return label.allSatisfy { $0.isLetter || $0.isNumber || $0 == "-" }
        }
    }
}
