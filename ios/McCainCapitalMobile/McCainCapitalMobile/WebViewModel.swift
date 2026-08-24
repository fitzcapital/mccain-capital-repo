import Combine
import SwiftUI
import WebKit

@MainActor
final class WebViewModel: NSObject, ObservableObject {
    enum State: Equatable {
        case idle
        case loading
        case content
        case failed(String)
    }

    @Published private(set) var state: State = .idle
    @Published private(set) var progress = 0.0
    @Published private(set) var canGoBack = false
    @Published private(set) var canGoForward = false
    @Published private(set) var title = "McCain Capital"
    @Published var message: String?

    let webView: WKWebView
    private(set) var origin: AppOrigin?
    var onStateChange: ((State) -> Void)?
    private var observations: [NSKeyValueObservation] = []

    override init() {
        let configuration = WKWebViewConfiguration()
        configuration.websiteDataStore = .default()
        configuration.defaultWebpagePreferences.allowsContentJavaScript = true
        webView = WKWebView(frame: .zero, configuration: configuration)
        super.init()
        webView.navigationDelegate = self
        webView.uiDelegate = self
        webView.allowsBackForwardNavigationGestures = true
        webView.scrollView.refreshControl = UIRefreshControl()
        webView.scrollView.refreshControl?.addTarget(
            self,
            action: #selector(refreshRequested),
            for: .valueChanged
        )
        observeWebView()
    }

    func configure(origin: AppOrigin) {
        self.origin = origin
    }

    func loadHome() {
        guard let origin else { return }
        setState(.loading)
        webView.load(URLRequest(url: origin.url, cachePolicy: .useProtocolCachePolicy))
    }

    func goBack() { if webView.canGoBack { webView.goBack() } }
    func goForward() { if webView.canGoForward { webView.goForward() } }

    @objc private func refreshRequested() {
        if webView.url == nil { loadHome() } else { webView.reload() }
    }

    private func observeWebView() {
        observations = [
            webView.observe(\.estimatedProgress, options: [.initial, .new]) { [weak self] view, _ in
                Task { @MainActor in self?.progress = view.estimatedProgress }
            },
            webView.observe(\.canGoBack, options: [.initial, .new]) { [weak self] view, _ in
                Task { @MainActor in self?.canGoBack = view.canGoBack }
            },
            webView.observe(\.canGoForward, options: [.initial, .new]) { [weak self] view, _ in
                Task { @MainActor in self?.canGoForward = view.canGoForward }
            },
            webView.observe(\.title, options: [.new]) { [weak self] view, _ in
                Task { @MainActor in self?.title = view.title ?? "McCain Capital" }
            },
        ]
    }

    private func setState(_ newState: State) {
        state = newState
        onStateChange?(newState)
    }

    private func fail(_ error: Error) {
        webView.scrollView.refreshControl?.endRefreshing()
        let nsError = error as NSError
        if nsError.code == NSURLErrorCancelled { return }
        let detail = nsError.code == NSURLErrorNotConnectedToInternet
            ? "Your iPhone is offline. Reconnect to the network and Tailscale, then retry."
            : "The private Mac service could not be reached. Check Tailscale, Mac wake state, and the McCain Capital container."
        setState(.failed(detail))
    }
}

extension WebViewModel: WKNavigationDelegate {
    func webView(
        _ webView: WKWebView,
        decidePolicyFor navigationAction: WKNavigationAction,
        decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
    ) {
        guard let url = navigationAction.request.url, let origin else {
            decisionHandler(.cancel)
            return
        }
        let decision = NavigationPolicy(origin: origin).decision(
            for: url,
            userInitiated: navigationAction.navigationType == .linkActivated
        )
        switch decision {
        case .allowInApp:
            decisionHandler(.allow)
        case .openExternally:
            decisionHandler(.cancel)
            UIApplication.shared.open(url)
        case .cancel:
            decisionHandler(.cancel)
            message = "Navigation was blocked because it is outside your configured private app."
        }
    }

    func webView(
        _ webView: WKWebView,
        decidePolicyFor navigationResponse: WKNavigationResponse,
        decisionHandler: @escaping (WKNavigationResponsePolicy) -> Void
    ) {
        if !navigationResponse.canShowMIMEType, let url = navigationResponse.response.url {
            decisionHandler(.cancel)
            message = "This download will open outside McCain Capital."
            UIApplication.shared.open(url)
            return
        }
        decisionHandler(.allow)
    }

    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
        setState(.loading)
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        webView.scrollView.refreshControl?.endRefreshing()
        setState(.content)
    }

    func webView(
        _ webView: WKWebView,
        didFailProvisionalNavigation navigation: WKNavigation!,
        withError error: Error
    ) { fail(error) }

    func webView(
        _ webView: WKWebView,
        didFail navigation: WKNavigation!,
        withError error: Error
    ) { fail(error) }
}

extension WebViewModel: WKUIDelegate {
    func webView(
        _ webView: WKWebView,
        createWebViewWith configuration: WKWebViewConfiguration,
        for navigationAction: WKNavigationAction,
        windowFeatures: WKWindowFeatures
    ) -> WKWebView? {
        guard navigationAction.targetFrame == nil,
              let url = navigationAction.request.url,
              let origin
        else { return nil }
        let decision = NavigationPolicy(origin: origin).decision(for: url, userInitiated: true)
        if decision == .allowInApp {
            webView.load(navigationAction.request)
        } else if decision == .openExternally {
            UIApplication.shared.open(url)
        } else {
            message = "The requested window was blocked."
        }
        return nil
    }
}
