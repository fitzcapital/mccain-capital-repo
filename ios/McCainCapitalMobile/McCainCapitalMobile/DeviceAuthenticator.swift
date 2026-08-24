import LocalAuthentication

struct DeviceAuthenticator {
    enum AuthenticationError: LocalizedError {
        case unavailable(String)

        var errorDescription: String? {
            switch self {
            case .unavailable(let detail):
                return "Device authentication is unavailable: \(detail)"
            }
        }
    }

    func ensureAvailable() throws {
        let context = LAContext()
        var error: NSError?
        guard context.canEvaluatePolicy(.deviceOwnerAuthentication, error: &error) else {
            throw AuthenticationError.unavailable(error?.localizedDescription ?? "not configured")
        }
    }

    func authenticate() async throws {
        let context = LAContext()
        context.localizedCancelTitle = "Keep Locked"
        var error: NSError?
        guard context.canEvaluatePolicy(.deviceOwnerAuthentication, error: &error) else {
            throw AuthenticationError.unavailable(error?.localizedDescription ?? "not configured")
        }
        try await context.evaluatePolicy(
            .deviceOwnerAuthentication,
            localizedReason: "Unlock your private McCain Capital workspace"
        )
    }
}
