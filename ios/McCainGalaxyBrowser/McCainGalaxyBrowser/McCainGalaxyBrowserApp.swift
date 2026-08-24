import SwiftUI
import AVFoundation

@main
struct McCainGalaxyBrowserApp: App {
    @StateObject private var browser = BrowserModel()

    init() {
        try? AVAudioSession.sharedInstance().setCategory(.playback, mode: .moviePlayback)
        try? AVAudioSession.sharedInstance().setActive(true)
    }

    var body: some Scene {
        WindowGroup {
            BrowserRootView()
                .environmentObject(browser)
                .preferredColorScheme(.dark)
        }
    }
}
