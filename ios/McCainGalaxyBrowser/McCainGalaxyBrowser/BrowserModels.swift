import Foundation
import WebKit

struct DownloadItem: Identifiable, Codable, Equatable {
    let id: UUID
    let filename: String
    let localURL: URL
    let createdAt: Date
}

struct HistoryItem: Identifiable, Codable, Equatable {
    let id: UUID
    var title: String
    let url: URL
    var visitedAt: Date
}

struct DownloadCandidate: Identifiable {
    let id = UUID()
    let url: URL
    let filename: String?
    let kind: String
}

struct MediaCandidate: Identifiable, Hashable {
    var id: URL { url }
    let url: URL
    let kind: String
    let title: String
}

@MainActor
final class BrowserTab: ObservableObject, Identifiable {
    let id = UUID()
    let webView: WKWebView
    let isPrivate: Bool
    @Published var title = "New Tab"
    @Published var url: URL?
    @Published var progress = 0.0
    @Published var canGoBack = false
    @Published var canGoForward = false
    @Published var media: [MediaCandidate] = []
    var allowNextCrossDomainNavigation = false
    var lastUserGestureAt = Date.distantPast
    var lastUserGestureKind = ""
    var lastUserGestureURL: URL?

    var hasRecentUserGesture: Bool {
        Date().timeIntervalSince(lastUserGestureAt) < 2.5
    }

    init(isPrivate: Bool = false) {
        self.isPrivate = isPrivate
        let configuration = WKWebViewConfiguration()
        configuration.websiteDataStore = isPrivate ? .nonPersistent() : .default()
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true
        configuration.allowsInlineMediaPlayback = true
        configuration.allowsPictureInPictureMediaPlayback = true
        configuration.allowsAirPlayForMediaPlayback = true
        configuration.mediaTypesRequiringUserActionForPlayback = []
        webView = WKWebView(frame: .zero, configuration: configuration)
        webView.allowsBackForwardNavigationGestures = true
    }
}

@MainActor
final class BrowserModel: ObservableObject {
    @Published private(set) var tabs: [BrowserTab] = []
    @Published var selectedTabID: UUID?
    @Published var downloads: [DownloadItem] = []
    @Published private(set) var history: [HistoryItem] = []
    @Published var downloadNotice: String?
    @Published private(set) var blockedEventCount = 0
    @Published private(set) var lastBlockedMessage: String?
    @Published var pendingDownload: DownloadCandidate?
    @Published var showingMedia = false
    @Published var playerItem: MediaCandidate?
    private var adBlockRuleList: WKContentRuleList?
    @Published var showingTabs = false
    @Published var showingDownloads = false
    @Published var showingMenu = false

    var selectedTab: BrowserTab? { tabs.first { $0.id == selectedTabID } }

    init() {
        loadDownloads()
        loadHistory()
        addTab()
        installAdBlocker()
    }

    func addTab(privateMode: Bool = false) {
        let tab = BrowserTab(isPrivate: privateMode)
        if let adBlockRuleList { tab.webView.configuration.userContentController.add(adBlockRuleList) }
        tabs.append(tab)
        selectedTabID = tab.id
        objectWillChange.send()
    }

    func close(_ tab: BrowserTab) {
        let wasSelected = selectedTabID == tab.id
        tabs.removeAll { $0.id == tab.id }
        if wasSelected {
            addTab(privateMode: tab.isPrivate)
        } else if tabs.isEmpty {
            addTab()
        }
    }

    func addMedia(_ candidate: MediaCandidate, to tab: BrowserTab) {
        guard !tab.media.contains(where: { $0.url == candidate.url }) else { return }
        tab.media.append(candidate)
    }

    func recordBlockedEvent(_ message: String) {
        blockedEventCount += 1
        lastBlockedMessage = message
    }

    func select(_ tab: BrowserTab) {
        selectedTabID = tab.id
        showingTabs = false
    }

    func recordDownload(at url: URL) {
        downloads.insert(DownloadItem(id: UUID(), filename: url.lastPathComponent, localURL: url, createdAt: .now), at: 0)
        persistDownloads()
    }

    func recordHistory(title: String, url: URL, isPrivate: Bool) {
        guard !isPrivate, ["http", "https"].contains(url.scheme?.lowercased() ?? "") else { return }
        history.removeAll { $0.url == url }
        history.insert(HistoryItem(id: UUID(), title: title, url: url, visitedAt: .now), at: 0)
        history = Array(history.prefix(250))
        persistHistory()
    }

    func suggestions(for input: String) -> [HistoryItem] {
        let query = input.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard !query.isEmpty else { return Array(history.prefix(5)) }
        return history.filter {
            $0.title.lowercased().contains(query) || $0.url.absoluteString.lowercased().contains(query)
        }.prefix(6).map { $0 }
    }

    func downloadDirect(_ url: URL, suggestedName: String? = nil) {
        guard ["http", "https"].contains(url.scheme?.lowercased() ?? "") else {
            downloadNotice = "This page uses a protected or temporary media address that cannot be downloaded directly."
            return
        }
        downloadNotice = "Starting download…"
        var request = URLRequest(url: url)
        if let userAgent = selectedTab?.webView.customUserAgent {
            request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        }
        URLSession.shared.downloadTask(with: request) { [weak self] temporaryURL, response, error in
            guard let temporaryURL, error == nil else {
                Task { @MainActor in self?.downloadNotice = "Download failed. The website may protect this file." }
                return
            }
            let filename = suggestedName?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
                ?? response?.suggestedFilename
                ?? url.lastPathComponent.nilIfEmpty
                ?? "download"
            do {
                let destination = try Self.downloadDestination(filename: filename)
                try FileManager.default.moveItem(at: temporaryURL, to: destination)
                Task { @MainActor in
                    self?.recordDownload(at: destination)
                    self?.downloadNotice = "Saved \(destination.lastPathComponent)"
                }
            } catch {
                Task { @MainActor in self?.downloadNotice = "Couldn’t save the downloaded file." }
            }
        }.resume()
    }

    func delete(_ item: DownloadItem) {
        try? FileManager.default.removeItem(at: item.localURL)
        downloads.removeAll { $0.id == item.id }
        persistDownloads()
    }

    private var indexURL: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("downloads.json")
    }

    private var historyURL: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("history.json")
    }

    nonisolated private static func downloadDestination(filename: String) throws -> URL {
        let directory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Downloads", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let safe = filename.replacingOccurrences(of: "/", with: "-")
        let original = directory.appendingPathComponent(safe)
        var candidate = original
        var suffix = 2
        while FileManager.default.fileExists(atPath: candidate.path) {
            let stem = original.deletingPathExtension().lastPathComponent
            candidate = directory.appendingPathComponent("\(stem)-\(suffix)")
            if !original.pathExtension.isEmpty { candidate.appendPathExtension(original.pathExtension) }
            suffix += 1
        }
        return candidate
    }

    private func loadDownloads() {
        guard let data = try? Data(contentsOf: indexURL),
              let items = try? JSONDecoder().decode([DownloadItem].self, from: data) else { return }
        downloads = items.filter { FileManager.default.fileExists(atPath: $0.localURL.path) }
    }

    private func persistDownloads() {
        try? FileManager.default.createDirectory(at: indexURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        if let data = try? JSONEncoder().encode(downloads) { try? data.write(to: indexURL, options: .atomic) }
    }


    private func loadHistory() {
        guard let data = try? Data(contentsOf: historyURL),
              let items = try? JSONDecoder().decode([HistoryItem].self, from: data) else { return }
        history = items
    }

    private func persistHistory() {
        try? FileManager.default.createDirectory(at: historyURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        if let data = try? JSONEncoder().encode(history) { try? data.write(to: historyURL, options: .atomic) }
    }

    private func installAdBlocker() {
        let rules = #"""
        [
          {"trigger":{"url-filter":".*","if-domain":["*doubleclick.net","*googlesyndication.com","*googleadservices.com","*adnxs.com","*adsrvr.org","*advertising.com","*amazon-adsystem.com","*criteo.com","*criteo.net","*taboola.com","*outbrain.com","*pubmatic.com","*rubiconproject.com","*openx.net","*casalemedia.com","*indexexchange.com","*smartadserver.com","*yieldmo.com","*moatads.com","*serving-sys.com","*zedo.com","*exoclick.com","*popads.net","*popcash.net","*propellerads.com","*revcontent.com","*mgid.com","*media.net","*scorecardresearch.com","*quantserve.com"]},"action":{"type":"block"}},
          {"trigger":{"url-filter":".*(google-analytics.com|googletagmanager.com/gtag|facebook.com/tr|connect.facebook.net/.*fbevents|analytics.twitter.com|bat.bing.com|hotjar.com|clarity.ms).*"},"action":{"type":"block"}},
          {"trigger":{"url-filter":".*/(ads?|advert|banner|popunder|sponsor)(/|\\?|\\.).*","resource-type":["script","image","style-sheet","raw"]},"action":{"type":"block"}},
          {"trigger":{"url-filter":".*"},"action":{"type":"css-display-none","selector":".adsbygoogle, [id^='google_ads_'], [id^='div-gpt-ad'], [class*=' ad-slot'], [class^='ad-slot'], [class*=' ad-container'], [class^='ad-container'], [data-ad-container], [data-ad-slot], [aria-label='Advertisement'], iframe[src*='doubleclick'], iframe[src*='googlesyndication']"}}
        ]
        """#
        WKContentRuleListStore.default().compileContentRuleList(forIdentifier: "McCainGalaxyPrivacy", encodedContentRuleList: rules) { [weak self] list, _ in
            guard let list else { return }
            Task { @MainActor in
                self?.adBlockRuleList = list
                self?.tabs.forEach { $0.webView.configuration.userContentController.add(list) }
            }
        }
    }
}

private extension String {
    var nilIfEmpty: String? { isEmpty ? nil : self }
}
