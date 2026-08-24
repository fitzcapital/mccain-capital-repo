import SwiftUI
import WebKit

@MainActor
final class AppModel: ObservableObject {
    enum ScreenState: Equatable {
        case needsConfiguration
        case locked
        case loading
        case content
        case unavailable(String)
    }

    @Published private(set) var origin: AppOrigin?
    @Published private(set) var screenState: ScreenState
    @Published var isShowingSettings = false
    @Published var appLockEnabled: Bool
    @Published var lockMessage: String?

    let web: WebViewModel
    let network = NetworkMonitor()
    private let preferences: AppPreferences
    private let authenticator: DeviceAuthenticator
    private var lockState: LockState

    init(
        preferences: AppPreferences = AppPreferences(),
        authenticator: DeviceAuthenticator = DeviceAuthenticator()
    ) {
        self.preferences = preferences
        self.authenticator = authenticator
        let arguments = ProcessInfo.processInfo.arguments
        if arguments.contains("-ui-testing-reset") {
            preferences.clearOrigin()
            preferences.appLockEnabled = false
        }
        if let index = arguments.firstIndex(of: "-ui-testing-origin"),
           arguments.indices.contains(index + 1) {
            _ = try? preferences.saveOrigin(arguments[index + 1])
        }
        origin = preferences.origin
        appLockEnabled = preferences.appLockEnabled
        lockState = LockState(enabled: preferences.appLockEnabled)
        screenState = preferences.origin == nil
            ? .needsConfiguration
            : (preferences.appLockEnabled ? .locked : .loading)
        web = WebViewModel()

        web.onStateChange = { [weak self] state in
            Task { @MainActor in
                guard let self, !self.lockState.isLocked else { return }
                switch state {
                case .idle, .loading:
                    self.screenState = .loading
                case .content:
                    self.screenState = .content
                case .failed(let message):
                    self.screenState = .unavailable(message)
                }
            }
        }

        if let origin, !preferences.appLockEnabled {
            web.configure(origin: origin)
            web.loadHome()
        }
    }

    func saveOrigin(_ input: String) async throws {
        let oldOrigin = origin
        let saved = try preferences.saveOrigin(input)
        if oldOrigin != nil, oldOrigin != saved {
            await WebDataCleaner.clearWebsiteData(for: oldOrigin)
        }
        origin = saved
        web.configure(origin: saved)
        screenState = .loading
        web.loadHome()
    }

    func retry() {
        guard origin != nil else {
            screenState = .needsConfiguration
            return
        }
        screenState = .loading
        web.loadHome()
    }

    func setAppLock(enabled: Bool) async throws {
        if enabled {
            try authenticator.ensureAvailable()
            appLockEnabled = true
            preferences.appLockEnabled = true
            lockState.requireLock()
            screenState = .locked
        } else {
            appLockEnabled = false
            preferences.appLockEnabled = false
            lockState.unlock()
            screenState = web.state == .content ? .content : .loading
        }
    }

    func unlock() async {
        lockMessage = nil
        do {
            try await authenticator.authenticate()
            lockState.unlock()
            if web.origin == nil, let origin {
                web.configure(origin: origin)
            }
            if web.state == .idle {
                screenState = .loading
                web.loadHome()
            } else {
                applyWebState()
            }
        } catch {
            lockMessage = error.localizedDescription
            screenState = .locked
        }
    }

    func handleScenePhase(_ phase: ScenePhase) {
        switch phase {
        case .inactive, .background:
            if appLockEnabled {
                lockState.enteredBackground(at: Date(), enabled: true)
                screenState = .locked
            }
        case .active:
            lockState.enteredForeground(
                at: Date(),
                enabled: appLockEnabled,
                graceInterval: 0
            )
            if lockState.isLocked {
                screenState = .locked
            } else if origin == nil {
                screenState = .needsConfiguration
            } else {
                applyWebState()
            }
        @unknown default:
            break
        }
    }

    private func applyWebState() {
        switch web.state {
        case .idle, .loading: screenState = .loading
        case .content: screenState = .content
        case .failed(let message): screenState = .unavailable(message)
        }
    }
}

private enum WebDataCleaner {
    @MainActor
    static func clearWebsiteData(for origin: AppOrigin?) async {
        guard let host = origin?.url.host else { return }
        let store = WKWebsiteDataStore.default()
        let types = WKWebsiteDataStore.allWebsiteDataTypes()
        let records = await withCheckedContinuation { continuation in
            store.fetchDataRecords(ofTypes: types) { continuation.resume(returning: $0) }
        }
        let matching = records.filter { $0.displayName == host || $0.displayName.hasSuffix(".\(host)") }
        await withCheckedContinuation { continuation in
            store.removeData(ofTypes: types, for: matching) { continuation.resume() }
        }
    }
}
