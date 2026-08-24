import SwiftUI
import AVKit

struct BrowserRootView: View {
    @EnvironmentObject private var browser: BrowserModel
    @State private var address = ""
    @FocusState private var addressFocused: Bool

    var body: some View {
        ZStack {
            GalaxyTheme.background.ignoresSafeArea()
            VStack(spacing: 0) {
                addressArea
                if let tab = browser.selectedTab {
                    ZStack(alignment: .top) {
                        BrowserWebView(tab: tab)
                            .environmentObject(browser)
                            .opacity(tab.url == nil ? 0 : 1)
                        if tab.url == nil {
                            StartPage(address: $address, submit: navigate)
                                .background(GalaxyTheme.background)
                        }
                        if tab.progress > 0, tab.progress < 1 {
                            ProgressView(value: tab.progress).tint(GalaxyTheme.cyan)
                        }
                    }
                    .id(tab.id)
                }
                toolbar
            }
        }
        .sheet(isPresented: $browser.showingTabs) { TabGridView() }
        .sheet(isPresented: $browser.showingDownloads) { DownloadsView() }
        .sheet(isPresented: $browser.showingMedia) { MediaCenterView() }
        .sheet(item: $browser.playerItem) { MediaPlayerSheet(item: $0) }
        .onChange(of: browser.selectedTab?.url) { _, url in
            if !addressFocused { address = url?.absoluteString ?? "" }
        }
        .confirmationDialog("Download this \(browser.pendingDownload?.kind ?? "item")?", isPresented: pendingDownloadPresented, titleVisibility: .visible, presenting: browser.pendingDownload) { item in
            Button("Download") {
                browser.downloadDirect(item.url, suggestedName: item.filename)
                browser.pendingDownload = nil
            }
            Button("Cancel", role: .cancel) { browser.pendingDownload = nil }
        } message: { item in
            Text(item.filename ?? item.url.lastPathComponent)
        }
        .alert("Downloads", isPresented: downloadNoticePresented) {
            Button("View Downloads") { browser.downloadNotice = nil; browser.showingDownloads = true }
            Button("OK", role: .cancel) { browser.downloadNotice = nil }
        } message: { Text(browser.downloadNotice ?? "") }
    }

    private var pendingDownloadPresented: Binding<Bool> {
        Binding(get: { browser.pendingDownload != nil }, set: { if !$0 { browser.pendingDownload = nil } })
    }

    private var downloadNoticePresented: Binding<Bool> {
        Binding(get: { browser.downloadNotice != nil }, set: { if !$0 { browser.downloadNotice = nil } })
    }

    private var addressArea: some View {
        VStack(spacing: 0) {
            addressBar
            if addressFocused {
                let suggestions = browser.suggestions(for: address)
                if !suggestions.isEmpty {
                    VStack(spacing: 0) {
                        ForEach(suggestions) { item in
                            Button {
                                address = item.url.absoluteString
                                navigate()
                            } label: {
                                HStack(spacing: 12) {
                                    Image(systemName: "clock.arrow.circlepath").foregroundStyle(GalaxyTheme.cyan)
                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(item.title).font(.subheadline.weight(.semibold)).lineLimit(1)
                                        Text(item.url.host ?? item.url.absoluteString).font(.caption).foregroundStyle(.secondary).lineLimit(1)
                                    }
                                    Spacer()
                                    Image(systemName: "arrow.up.left").font(.caption).foregroundStyle(.secondary)
                                }.padding(.horizontal, 18).frame(height: 56)
                            }.buttonStyle(.plain)
                            if item.id != suggestions.last?.id { Divider().opacity(0.22).padding(.leading, 48) }
                        }
                    }
                    .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 20))
                    .overlay(RoundedRectangle(cornerRadius: 20).stroke(.white.opacity(0.1)))
                    .padding(.horizontal, 12).padding(.bottom, 8)
                }
            }
        }
        .animation(.easeOut(duration: 0.18), value: addressFocused)
    }

    private var addressBar: some View {
        HStack(spacing: 10) {
            Image(systemName: browser.selectedTab?.isPrivate == true ? "eye.slash.fill" : "lock.fill")
                .foregroundStyle(browser.selectedTab?.isPrivate == true ? GalaxyTheme.violet : GalaxyTheme.cyan)
            TextField("Search or enter website", text: $address)
                .textInputAutocapitalization(.never).autocorrectionDisabled()
                .keyboardType(.webSearch).submitLabel(.go).focused($addressFocused)
                .onSubmit(navigate)
            if !address.isEmpty {
                Button { address = "" } label: { Image(systemName: "xmark.circle.fill") }
                    .foregroundStyle(.white.opacity(0.45))
            }
            if browser.blockedEventCount > 0 {
                ZStack(alignment: .topTrailing) {
                    Image(systemName: "shield.checkered")
                        .foregroundStyle(GalaxyTheme.cyan)
                    Text(browser.blockedEventCount > 99 ? "99+" : "\(browser.blockedEventCount)")
                        .font(.system(size: 7, weight: .bold))
                        .foregroundStyle(.black)
                        .padding(.horizontal, 3).frame(minWidth: 12, minHeight: 12)
                        .background(GalaxyTheme.gold, in: Capsule())
                        .offset(x: 7, y: -7)
                }
                .padding(.trailing, 4)
                .accessibilityLabel(browser.lastBlockedMessage ?? "Blocked browser redirect")
            }
        }
        .padding(.horizontal, 16).frame(height: 50)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 19))
        .overlay(RoundedRectangle(cornerRadius: 19).stroke(LinearGradient(colors: [GalaxyTheme.cyan.opacity(addressFocused ? 0.8 : 0.28), GalaxyTheme.violet.opacity(0.32)], startPoint: .leading, endPoint: .trailing)))
        .shadow(color: GalaxyTheme.cyan.opacity(addressFocused ? 0.14 : 0), radius: 14)
        .padding(.horizontal, 12).padding(.vertical, 9)
    }

    private var toolbar: some View {
        HStack {
            tool("chevron.backward", enabled: browser.selectedTab?.canGoBack == true) { browser.selectedTab?.webView.goBack() }
            Spacer()
            tool("chevron.forward", enabled: browser.selectedTab?.canGoForward == true) { browser.selectedTab?.webView.goForward() }
            Spacer()
            tool("arrow.clockwise") { browser.selectedTab?.webView.reload() }
            Spacer()
            if browser.selectedTab?.media.isEmpty == false {
                Button { browser.showingMedia = true } label: {
                    Image(systemName: "play.rectangle.on.rectangle.fill")
                        .foregroundStyle(GalaxyTheme.gold)
                }
                Spacer()
            }
            Button { browser.showingDownloads = true } label: { Image(systemName: "arrow.down.circle") }
            Spacer()
            Button { browser.showingTabs = true } label: {
                ZStack { RoundedRectangle(cornerRadius: 4).stroke(lineWidth: 1.5).frame(width: 24, height: 24); Text("\(browser.tabs.count)").font(.caption2.bold()) }
            }
        }
        .font(.title3.weight(.semibold)).foregroundStyle(.white.opacity(0.88))
        .padding(.horizontal, 28).frame(height: 58)
        .background(.ultraThinMaterial)
        .overlay(alignment: .top) { Divider().opacity(0.18) }
    }

    private func tool(_ icon: String, enabled: Bool = true, action: @escaping () -> Void) -> some View {
        Button(action: action) { Image(systemName: icon) }.disabled(!enabled).opacity(enabled ? 1 : 0.3)
    }

    private func navigate() {
        let value = address.trimmingCharacters(in: .whitespacesAndNewlines)
        let url: URL?
        if value.isEmpty {
            url = URL(string: "https://www.google.com")
        } else if value.contains(" ") || (!value.contains(".") && !value.contains(":")) {
            url = URL(string: "https://www.google.com/search?q=\(value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? value)")
        } else if value.contains("://") { url = URL(string: value) }
        else { url = URL(string: "https://\(value)") }
        if let url {
            browser.selectedTab?.allowNextCrossDomainNavigation = true
            browser.selectedTab?.lastUserGestureAt = .now
            browser.selectedTab?.lastUserGestureKind = "address"
            browser.selectedTab?.webView.load(URLRequest(url: url))
            addressFocused = false
        }
    }
}

private struct StartPage: View {
    @Binding var address: String
    let submit: () -> Void
    private let shortcuts = [("Google", "https://google.com", "g.circle.fill"), ("YouTube", "https://youtube.com", "play.rectangle.fill"), ("Reddit", "https://reddit.com", "bubble.left.and.bubble.right.fill"), ("McCain", "https://mccaincapital.com", "chart.line.uptrend.xyaxis")]

    var body: some View {
        ScrollView {
          VStack(spacing: 30) {
            Spacer(minLength: 42)
            ZStack {
                Circle().fill(GalaxyTheme.violet.opacity(0.28)).frame(width: 150, height: 150).blur(radius: 22)
                Circle().stroke(LinearGradient(colors: [GalaxyTheme.cyan, GalaxyTheme.violet, GalaxyTheme.gold], startPoint: .topLeading, endPoint: .bottomTrailing), lineWidth: 2).frame(width: 116, height: 116)
                Image(systemName: "location.north.fill").font(.system(size: 54, weight: .light)).foregroundStyle(LinearGradient(colors: [.white, GalaxyTheme.gold], startPoint: .top, endPoint: .bottom)).rotationEffect(.degrees(42))
            }
            VStack(spacing: 7) {
                Text("McCain Galaxy").font(.system(size: 34, weight: .bold, design: .rounded))
                Text("Your web. Your orbit.").font(.subheadline.weight(.medium)).foregroundStyle(GalaxyTheme.cyan.opacity(0.75))
            }
            Button { address = ""; submit() } label: {
                HStack { Image(systemName: "magnifyingglass"); Text("Search with Google"); Spacer(); Image(systemName: "arrow.right") }
                    .font(.subheadline.weight(.semibold)).padding(.horizontal, 18).frame(height: 54)
                    .background(.white.opacity(0.075), in: RoundedRectangle(cornerRadius: 18))
                    .overlay(RoundedRectangle(cornerRadius: 18).stroke(.white.opacity(0.1)))
            }.buttonStyle(.plain).padding(.horizontal, 20)
            LazyVGrid(columns: [.init(.flexible()), .init(.flexible()), .init(.flexible()), .init(.flexible())], spacing: 18) {
                ForEach(shortcuts, id: \.0) { item in
                    Button { address = item.1; submit() } label: {
                        VStack(spacing: 8) {
                            Image(systemName: item.2).font(.title2).frame(width: 58, height: 58).background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 18)).overlay(RoundedRectangle(cornerRadius: 18).stroke(.white.opacity(0.1))).foregroundStyle(GalaxyTheme.cyan)
                            Text(item.0).font(.caption).foregroundStyle(.white.opacity(0.78))
                        }
                    }
                }
            }.padding(.horizontal, 24)
            Text("Long-press a link, image, or video to download").font(.caption).foregroundStyle(.white.opacity(0.4)).padding(.top, 4)
            Spacer(minLength: 28)
          }
        }.scrollIndicators(.hidden)
    }
}

private struct TabGridView: View {
    @EnvironmentObject private var browser: BrowserModel
    @Environment(\.dismiss) private var dismiss
    var body: some View {
        NavigationStack {
            ScrollView {
                LazyVGrid(columns: [.init(.flexible()), .init(.flexible())], spacing: 14) {
                    ForEach(browser.tabs) { tab in
                        Button { browser.select(tab); dismiss() } label: {
                            VStack(alignment: .leading, spacing: 10) {
                                HStack { Image(systemName: tab.isPrivate ? "eye.slash" : "globe"); Spacer(); Button { browser.close(tab) } label: { Image(systemName: "xmark.circle.fill") }.disabled(browser.tabs.count == 1) }
                                Spacer(); Text(tab.title).font(.headline).lineLimit(2); Text(tab.url?.host ?? "Start page").font(.caption).foregroundStyle(.secondary).lineLimit(1)
                            }.padding().frame(height: 150).background(GalaxyTheme.surface, in: RoundedRectangle(cornerRadius: 18)).overlay(RoundedRectangle(cornerRadius: 18).stroke(tab.id == browser.selectedTabID ? GalaxyTheme.cyan : .white.opacity(0.08)))
                        }.buttonStyle(.plain)
                    }
                }.padding()
            }.background(GalaxyTheme.background.ignoresSafeArea()).navigationTitle("Tabs")
                .toolbar { ToolbarItemGroup(placement: .topBarTrailing) { Button { browser.addTab(privateMode: true) } label: { Image(systemName: "eye.slash") }; Button { browser.addTab() } label: { Image(systemName: "plus") }; Button("Done") { dismiss() } } }
        }
    }
}

private struct DownloadsView: View {
    @EnvironmentObject private var browser: BrowserModel
    @Environment(\.dismiss) private var dismiss
    @State private var showingAddURL = false
    @State private var directURL = ""
    var body: some View {
        NavigationStack {
            Group {
                if browser.downloads.isEmpty { ContentUnavailableView("No Downloads", systemImage: "arrow.down.circle", description: Text("Downloaded files will appear here.")) }
                else { List { ForEach(browser.downloads) { item in HStack { Image(systemName: item.isPlayable ? "play.rectangle.fill" : "doc.fill").foregroundStyle(GalaxyTheme.cyan); VStack(alignment: .leading) { Text(item.filename).lineLimit(1); Text(item.createdAt, style: .date).font(.caption).foregroundStyle(.secondary) }; Spacer(); if item.isPlayable { Button { browser.playerItem = MediaCandidate(url: item.localURL, kind: "video", title: item.filename) } label: { Image(systemName: "play.fill") } }; ShareLink(item: item.localURL) { Image(systemName: "square.and.arrow.up") } }.swipeActions { Button(role: .destructive) { browser.delete(item) } label: { Label("Delete", systemImage: "trash") } } } } }
            }.navigationTitle("Downloads").toolbar { ToolbarItemGroup(placement: .topBarTrailing) { Button { showingAddURL = true } label: { Image(systemName: "plus") }; Button("Done") { dismiss() } } }
            .alert("Download from URL", isPresented: $showingAddURL) {
                TextField("https://example.com/video.mp4", text: $directURL).textInputAutocapitalization(.never).autocorrectionDisabled()
                Button("Download") { if let url = URL(string: directURL) { browser.downloadDirect(url) }; directURL = "" }
                Button("Cancel", role: .cancel) { directURL = "" }
            } message: { Text("Paste a direct file, audio, or video link.") }
            .sheet(item: $browser.playerItem) { MediaPlayerSheet(item: $0) }
        }
    }
}

private struct MediaCenterView: View {
    @EnvironmentObject private var browser: BrowserModel
    @Environment(\.dismiss) private var dismiss
    var body: some View {
        NavigationStack {
            List(browser.selectedTab?.media ?? []) { item in
                VStack(alignment: .leading, spacing: 10) {
                    HStack { Image(systemName: item.kind == "audio" ? "waveform" : "play.rectangle.fill").foregroundStyle(GalaxyTheme.cyan); Text(item.title).font(.headline).lineLimit(2) }
                    Text(item.url.lastPathComponent.isEmpty ? item.url.host ?? "Media" : item.url.lastPathComponent).font(.caption).foregroundStyle(.secondary).lineLimit(1)
                    HStack {
                        Button { browser.playerItem = item } label: { Label("Play", systemImage: "play.fill") }.buttonStyle(.borderedProminent)
                        Button { browser.downloadDirect(item.url, suggestedName: item.url.lastPathComponent) } label: { Label("Download", systemImage: "arrow.down") }.buttonStyle(.bordered)
                    }
                }.padding(.vertical, 6)
            }
            .navigationTitle("Media on This Page")
            .toolbar { Button("Done") { dismiss() } }
            .sheet(item: $browser.playerItem) { MediaPlayerSheet(item: $0) }
        }
    }
}

private struct MediaPlayerSheet: View {
    let item: MediaCandidate
    @Environment(\.dismiss) private var dismiss
    @State private var player: AVPlayer

    init(item: MediaCandidate) {
        self.item = item
        _player = State(initialValue: AVPlayer(url: item.url))
    }

    var body: some View {
        NavigationStack {
            PlayerController(player: player).ignoresSafeArea(edges: .bottom)
                .navigationTitle(item.title).navigationBarTitleDisplayMode(.inline)
                .toolbar { Button("Done") { player.pause(); dismiss() } }
                .onAppear { player.play() }
                .onDisappear { player.pause() }
        }.preferredColorScheme(.dark)
    }
}

private struct PlayerController: UIViewControllerRepresentable {
    let player: AVPlayer
    func makeUIViewController(context: Context) -> AVPlayerViewController {
        let controller = AVPlayerViewController()
        controller.player = player
        controller.allowsPictureInPicturePlayback = true
        controller.canStartPictureInPictureAutomaticallyFromInline = true
        controller.showsPlaybackControls = true
        controller.videoGravity = .resizeAspect
        return controller
    }
    func updateUIViewController(_ controller: AVPlayerViewController, context: Context) { controller.player = player }
}

private extension DownloadItem {
    var isPlayable: Bool {
        ["mp4", "mov", "m4v", "mp3", "m4a", "aac", "wav"].contains(localURL.pathExtension.lowercased())
    }
}
