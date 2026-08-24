import SwiftUI
import WebKit

struct BrowserWebView: UIViewRepresentable {
    @ObservedObject var tab: BrowserTab
    @EnvironmentObject private var browser: BrowserModel

    func makeCoordinator() -> Coordinator { Coordinator(tab: tab, browser: browser) }

    func makeUIView(context: Context) -> WKWebView {
        let view = tab.webView
        view.isOpaque = false
        view.backgroundColor = .clear
        view.scrollView.backgroundColor = .clear
        view.navigationDelegate = context.coordinator
        view.uiDelegate = context.coordinator
        let controller = view.configuration.userContentController
        controller.removeScriptMessageHandler(forName: "galaxyDownload")
        controller.removeScriptMessageHandler(forName: "galaxyMedia")
        controller.removeScriptMessageHandler(forName: "galaxyGesture")
        controller.add(WeakScriptHandler(context.coordinator), name: "galaxyDownload")
        controller.add(WeakScriptHandler(context.coordinator), name: "galaxyMedia")
        controller.add(WeakScriptHandler(context.coordinator), name: "galaxyGesture")
        controller.addUserScript(WKUserScript(source: Self.longPressDownloadScript, injectionTime: .atDocumentEnd, forMainFrameOnly: false))
        controller.addUserScript(WKUserScript(source: Self.mediaDetectionScript, injectionTime: .atDocumentEnd, forMainFrameOnly: false))
        controller.addUserScript(WKUserScript(source: Self.navigationAndAdGuardScript, injectionTime: .atDocumentStart, forMainFrameOnly: false))
        context.coordinator.observe(view)
        return view
    }

    func updateUIView(_ uiView: WKWebView, context: Context) {}

    private static let longPressDownloadScript = #"""
    (() => {
      if (window.__mccainDownloadInstalled) return;
      window.__mccainDownloadInstalled = true;
      document.addEventListener('contextmenu', event => {
        const element = event.target.closest('a, img, video, audio, source');
        if (!element) return;
        let raw = element.currentSrc || element.src || element.href;
        if (!raw && element.closest('a')) raw = element.closest('a').href;
        if (!raw) return;
        event.preventDefault();
        let kind = element.tagName.toLowerCase();
        if (element.closest('video')) kind = 'video';
        if (element.closest('audio')) kind = 'audio';
        const pathname = (() => { try { return new URL(raw, location.href).pathname; } catch (_) { return ''; } })();
        window.webkit.messageHandlers.galaxyDownload.postMessage({
          url: new URL(raw, location.href).href,
          filename: element.getAttribute('download') || pathname.split('/').pop() || '',
          kind
        });
      }, true);
    })();
    """#

    private static let mediaDetectionScript = #"""
    (() => {
      if (window.__mccainMediaInstalled) return;
      window.__mccainMediaInstalled = true;
      const sent = new Set();
      const report = media => {
        const raw = media.currentSrc || media.src || media.querySelector?.('source')?.src;
        if (!raw) return;
        let url;
        try { url = new URL(raw, location.href).href; } catch (_) { return; }
        if (!url.startsWith('http') || sent.has(url)) return;
        sent.add(url);
        window.webkit.messageHandlers.galaxyMedia.postMessage({
          url,
          kind: media.tagName?.toLowerCase() === 'audio' ? 'audio' : 'video',
          title: media.getAttribute('title') || document.title || 'Web media'
        });
      };
      const scan = () => document.querySelectorAll('video, audio').forEach(media => {
        report(media);
        ['loadedmetadata', 'canplay', 'play'].forEach(name => media.addEventListener(name, () => report(media), {passive:true}));
      });
      scan();
      new MutationObserver(scan).observe(document.documentElement, {subtree:true, childList:true, attributes:true, attributeFilter:['src']});
    })();
    """#

    private static let navigationAndAdGuardScript = #"""
    (() => {
      if (window.__mccainGuardInstalled) return;
      window.__mccainGuardInstalled = true;
      let lastGesture = 0;
      let lastGestureKind = '';
      const markGesture = event => {
        lastGesture = Date.now();
        const target = event.target;
        const media = target?.closest?.('video, audio');
        const anchor = target?.closest?.('a[href]');
        const rawURL = media?.currentSrc || media?.src || anchor?.href || '';
        const looksLikeMedia = !!media || /(?:video|player|watch|embed|stream|\.mp4|\.m3u8|\.mov|\.m4v)/i.test(rawURL);
        lastGestureKind = looksLikeMedia ? 'media' : (anchor ? 'link' : 'page');
        window.webkit.messageHandlers.galaxyGesture.postMessage({kind:lastGestureKind, url:rawURL});
      };
      document.addEventListener('pointerdown', markGesture, true);
      document.addEventListener('touchstart', markGesture, {capture:true, passive:true});

      const nativeOpen = window.open.bind(window);
      window.open = (...args) => {
        if (Date.now() - lastGesture <= 2500 && lastGestureKind === 'media') return nativeOpen(...args);
        window.webkit.messageHandlers.galaxyGesture.postMessage('blocked-popup');
        return null;
      };

      const selectors = [
        '.adsbygoogle','[id^="google_ads_"]','[id^="div-gpt-ad"]','[data-ad-container]',
        '[data-ad-slot]','[aria-label="Advertisement"]','iframe[src*="doubleclick"]',
        'iframe[src*="googlesyndication"]','iframe[src*="taboola"]','iframe[src*="outbrain"]'
      ].join(',');
      const clean = () => document.querySelectorAll(selectors).forEach(node => node.remove());
      document.addEventListener('DOMContentLoaded', clean, {once:true});
      const watch = () => {
        if (!document.documentElement) { setTimeout(watch, 25); return; }
        new MutationObserver(clean).observe(document.documentElement, {childList:true, subtree:true});
      };
      watch();
    })();
    """#

    final class Coordinator: NSObject, WKNavigationDelegate, WKUIDelegate, WKDownloadDelegate, WKScriptMessageHandler {
        private let tab: BrowserTab
        private weak var browser: BrowserModel?
        private var observations: [NSKeyValueObservation] = []

        init(tab: BrowserTab, browser: BrowserModel) {
            self.tab = tab
            self.browser = browser
        }

        func observe(_ webView: WKWebView) {
            observations = [
                webView.observe(\.title, options: [.new]) { [weak self] view, _ in
                    Task { @MainActor in self?.tab.title = view.title?.isEmpty == false ? view.title! : "New Tab" }
                },
                webView.observe(\.url, options: [.new]) { [weak self] view, _ in
                    Task { @MainActor in self?.tab.url = view.url }
                },
                webView.observe(\.estimatedProgress, options: [.new]) { [weak self] view, _ in
                    Task { @MainActor in self?.tab.progress = view.estimatedProgress }
                },
                webView.observe(\.canGoBack, options: [.new]) { [weak self] view, _ in
                    Task { @MainActor in self?.tab.canGoBack = view.canGoBack }
                },
                webView.observe(\.canGoForward, options: [.new]) { [weak self] view, _ in
                    Task { @MainActor in self?.tab.canGoForward = view.canGoForward }
                }
            ]
        }

        func webView(_ webView: WKWebView, decidePolicyFor navigationAction: WKNavigationAction,
                     preferences: WKWebpagePreferences,
                     decisionHandler: @escaping (WKNavigationActionPolicy, WKWebpagePreferences) -> Void) {
            guard let destination = navigationAction.request.url else {
                decisionHandler(.cancel, preferences); return
            }
            let scheme = destination.scheme?.lowercased() ?? ""
            if !["http", "https", "about", "data", "blob"].contains(scheme) {
                Task { @MainActor in browser?.recordBlockedEvent("Stopped an external-app redirect (\(scheme):).") }
                decisionHandler(.cancel, preferences)
            } else if navigationAction.shouldPerformDownload {
                decisionHandler(.download, preferences)
            } else if navigationAction.targetFrame == nil {
                let googleResultClick = navigationAction.navigationType == .linkActivated && isGoogleSearchPage(webView.url)
                if googleResultClick {
                    tab.lastUserGestureAt = .now
                    tab.lastUserGestureKind = "searchResult"
                    tab.allowNextCrossDomainNavigation = true
                    webView.load(navigationAction.request)
                    decisionHandler(.cancel, preferences)
                } else if tab.hasRecentUserGesture && tab.lastUserGestureKind == "media" {
                    tab.allowNextCrossDomainNavigation = true
                    webView.load(navigationAction.request)
                    decisionHandler(.cancel, preferences)
                } else {
                    Task { @MainActor in browser?.recordBlockedEvent("Stopped an automatic pop-up from \(webView.url?.host ?? "this page").") }
                    decisionHandler(.cancel, preferences)
                }
            } else if tab.allowNextCrossDomainNavigation {
                tab.allowNextCrossDomainNavigation = false
                decisionHandler(.allow, preferences)
            } else {
                let currentHost = webView.url?.host?.lowercased()
                let destinationHost = destination.host?.lowercased()
                let crossSite = currentHost != nil && destinationHost != nil && !isSameSite(currentHost!, destinationHost!)
                // Google search results legitimately leave google.com. Remember only an
                // explicit result-link activation so Google's short redirect can finish;
                // scripts and clicks on ordinary sites remain subject to the strict rule.
                if navigationAction.navigationType == .linkActivated && isGoogleSearchPage(webView.url) {
                    tab.lastUserGestureAt = .now
                    tab.lastUserGestureKind = "searchResult"
                }
                let trustedGesture = tab.hasRecentUserGesture && ["media", "address", "searchResult"].contains(tab.lastUserGestureKind)
                if crossSite && navigationAction.navigationType != .backForward && !trustedGesture {
                    Task { @MainActor in browser?.recordBlockedEvent("Stopped a cross-site click or redirect to \(destinationHost ?? "another site").") }
                    decisionHandler(.cancel, preferences)
                } else {
                    decisionHandler(.allow, preferences)
                }
            }
        }

        func webView(_ webView: WKWebView, decidePolicyFor navigationResponse: WKNavigationResponse,
                     decisionHandler: @escaping (WKNavigationResponsePolicy) -> Void) {
            decisionHandler(navigationResponse.canShowMIMEType ? .allow : .download)
        }

        func webView(_ webView: WKWebView, navigationAction: WKNavigationAction,
                     didBecome download: WKDownload) { download.delegate = self }

        func webView(_ webView: WKWebView, navigationResponse: WKNavigationResponse,
                     didBecome download: WKDownload) { download.delegate = self }

        func download(_ download: WKDownload, decideDestinationUsing response: URLResponse,
                      suggestedFilename: String,
                      completionHandler: @escaping (URL?) -> Void) {
            let directory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("Downloads", isDirectory: true)
            try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            completionHandler(uniqueURL(in: directory, filename: suggestedFilename))
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            guard let url = webView.url else { return }
            Task { @MainActor in
                browser?.recordHistory(title: webView.title ?? url.host ?? "Web page", url: url, isPrivate: tab.isPrivate)
            }
        }

        func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
            Task { @MainActor in tab.media = [] }
        }

        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            if message.name == "galaxyGesture" {
                if (message.body as? String) == "blocked-popup" {
                    Task { @MainActor in browser?.recordBlockedEvent("Stopped an automatic advertising pop-up.") }
                } else if let body = message.body as? [String: Any] {
                    let kind = body["kind"] as? String ?? "page"
                    let url = (body["url"] as? String).flatMap(URL.init(string:))
                    Task { @MainActor in
                        tab.lastUserGestureAt = .now
                        tab.lastUserGestureKind = kind
                        tab.lastUserGestureURL = url
                    }
                }
                return
            }
            if message.name == "galaxyMedia",
               let body = message.body as? [String: Any],
               let rawURL = body["url"] as? String,
               let url = URL(string: rawURL) {
                let kind = body["kind"] as? String ?? "video"
                let title = body["title"] as? String ?? "Web media"
                Task { @MainActor in browser?.addMedia(MediaCandidate(url: url, kind: kind, title: title), to: tab) }
                return
            }
            guard message.name == "galaxyDownload",
                  let body = message.body as? [String: Any],
                  let rawURL = body["url"] as? String,
                  let url = URL(string: rawURL) else { return }
            let filename = (body["filename"] as? String).flatMap { $0.isEmpty ? nil : $0 }
            let kind = body["kind"] as? String ?? "file"
            Task { @MainActor in browser?.pendingDownload = DownloadCandidate(url: url, filename: filename, kind: kind) }
        }

        func downloadDidFinish(_ download: WKDownload) {
            guard let url = download.progress.fileURL else { return }
            Task { @MainActor in browser?.recordDownload(at: url) }
        }

        private func uniqueURL(in directory: URL, filename: String) -> URL {
            let safeName = filename.replacingOccurrences(of: "/", with: "-")
            var candidate = directory.appendingPathComponent(safeName)
            var suffix = 2
            while FileManager.default.fileExists(atPath: candidate.path) {
                let ext = candidate.pathExtension
                let stem = candidate.deletingPathExtension().lastPathComponent
                candidate = directory.appendingPathComponent("\(stem)-\(suffix)").appendingPathExtension(ext)
                suffix += 1
            }
            return candidate
        }

        private func isSameSite(_ first: String, _ second: String) -> Bool {
            if first == second { return true }
            func registrablePart(_ host: String) -> String {
                let parts = host.split(separator: ".")
                return parts.suffix(2).joined(separator: ".")
            }
            return registrablePart(first) == registrablePart(second)
        }

        private func isGoogleSearchPage(_ url: URL?) -> Bool {
            guard let url, let host = url.host?.lowercased() else { return false }
            let isGoogleHost = host == "google.com" || host.hasSuffix(".google.com") || host.contains(".google.")
            return isGoogleHost && url.path == "/search"
        }

        func webView(_ webView: WKWebView, createWebViewWith configuration: WKWebViewConfiguration,
                     for navigationAction: WKNavigationAction, windowFeatures: WKWindowFeatures) -> WKWebView? {
            if navigationAction.targetFrame == nil, tab.hasRecentUserGesture, tab.lastUserGestureKind == "media" {
                tab.allowNextCrossDomainNavigation = true
                webView.load(navigationAction.request)
            }
            return nil
        }
    }

    private final class WeakScriptHandler: NSObject, WKScriptMessageHandler {
        weak var target: WKScriptMessageHandler?
        init(_ target: WKScriptMessageHandler) { self.target = target }
        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            target?.userContentController(userContentController, didReceive: message)
        }
    }
}
