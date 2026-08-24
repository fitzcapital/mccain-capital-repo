import SwiftUI

struct RootView: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        ZStack {
            Color(red: 0.025, green: 0.045, blue: 0.085).ignoresSafeArea()

            switch model.screenState {
            case .needsConfiguration:
                SetupView()
            case .locked:
                LockView()
            case .loading:
                WebWorkspaceView(showLoadingCover: true)
            case .content:
                WebWorkspaceView(showLoadingCover: false)
            case .unavailable(let detail):
                UnavailableView(detail: detail)
            }
        }
        .sheet(isPresented: $model.isShowingSettings) {
            SettingsView()
        }
        .alert("McCain Capital", isPresented: messageIsPresented) {
            Button("OK") { model.web.message = nil }
        } message: {
            Text(model.web.message ?? "")
        }
    }

    private var messageIsPresented: Binding<Bool> {
        Binding(
            get: { model.web.message != nil },
            set: { if !$0 { model.web.message = nil } }
        )
    }
}

private struct BrandHeader: View {
    let subtitle: String

    var body: some View {
        VStack(spacing: 10) {
            Image(systemName: "building.columns.fill")
                .font(.system(size: 42, weight: .semibold))
                .foregroundStyle(Color.accentColor)
            Text("McCain Capital")
                .font(.title.bold())
                .foregroundStyle(.white)
            Text(subtitle)
                .font(.subheadline)
                .multilineTextAlignment(.center)
                .foregroundStyle(.white.opacity(0.72))
        }
    }
}

private struct SetupView: View {
    @EnvironmentObject private var model: AppModel
    @State private var address = ""
    @State private var errorMessage: String?
    @State private var isSaving = false

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    BrandHeader(subtitle: "Private iPhone access through Tailscale")
                        .frame(maxWidth: .infinity)
                        .listRowBackground(Color.clear)
                }
                Section("Private app address") {
                    TextField("https://your-mac.your-tailnet.ts.net", text: $address)
                        .textInputAutocapitalization(.never)
                        .keyboardType(.URL)
                        .autocorrectionDisabled()
                        .accessibilityIdentifier("originField")
                    if let errorMessage {
                        Text(errorMessage).foregroundStyle(.red)
                    }
                    Text("Use the HTTPS address created by Tailscale Serve. HTTP addresses and IP addresses are intentionally blocked.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Section {
                    Button {
                        save()
                    } label: {
                        if isSaving { ProgressView() } else { Text("Connect securely") }
                    }
                    .disabled(isSaving)
                    .accessibilityIdentifier("connectButton")
                }
            }
            .scrollContentBackground(.hidden)
            .background(Color.clear)
        }
    }

    private func save() {
        isSaving = true
        errorMessage = nil
        Task {
            do {
                try await model.saveOrigin(address)
            } catch {
                errorMessage = error.localizedDescription
            }
            isSaving = false
        }
    }
}

private struct WebWorkspaceView: View {
    @EnvironmentObject private var model: AppModel
    let showLoadingCover: Bool

    var body: some View {
        VStack(spacing: 0) {
            ZStack(alignment: .top) {
                WebViewContainer(model: model.web)
                    .ignoresSafeArea(.container, edges: .bottom)
                if model.web.progress < 1 {
                    ProgressView(value: model.web.progress)
                        .progressViewStyle(.linear)
                        .tint(Color.accentColor)
                }
                if showLoadingCover {
                    Color(red: 0.025, green: 0.045, blue: 0.085)
                    VStack(spacing: 14) {
                        ProgressView().tint(.white)
                        Text("Opening your private workspace…")
                            .foregroundStyle(.white.opacity(0.8))
                    }
                }
            }
            WebToolbar()
        }
    }
}

private struct WebToolbar: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        HStack {
            Button { model.web.goBack() } label: { Image(systemName: "chevron.backward") }
                .disabled(!model.web.canGoBack)
                .accessibilityLabel("Back")
            Spacer()
            Button { model.web.goForward() } label: { Image(systemName: "chevron.forward") }
                .disabled(!model.web.canGoForward)
                .accessibilityLabel("Forward")
            Spacer()
            Button { model.retry() } label: { Image(systemName: "arrow.clockwise") }
                .accessibilityLabel("Refresh")
            Spacer()
            Button { model.isShowingSettings = true } label: { Image(systemName: "gearshape") }
                .accessibilityLabel("Settings")
        }
        .font(.title3.weight(.semibold))
        .padding(.horizontal, 28)
        .padding(.vertical, 11)
        .background(.ultraThinMaterial)
    }
}

private struct UnavailableView: View {
    @EnvironmentObject private var model: AppModel
    let detail: String

    var body: some View {
        VStack(spacing: 22) {
            BrandHeader(subtitle: "Your private Mac app is unavailable")
            Image(systemName: model.network.isConnected ? "network.slash" : "wifi.slash")
                .font(.system(size: 48))
                .foregroundStyle(.orange)
            Text(detail)
                .multilineTextAlignment(.center)
                .foregroundStyle(.white.opacity(0.82))
            VStack(alignment: .leading, spacing: 8) {
                Label("Confirm Tailscale is connected on this iPhone", systemImage: "1.circle")
                Label("Confirm the Mac is awake and online", systemImage: "2.circle")
                Label("Confirm McCain Capital is running on port 5001", systemImage: "3.circle")
            }
            .font(.subheadline)
            .foregroundStyle(.white.opacity(0.75))
            Button("Retry connection") { model.retry() }
                .buttonStyle(.borderedProminent)
                .accessibilityIdentifier("retryButton")
            Button("Connection settings") { model.isShowingSettings = true }
                .buttonStyle(.bordered)
        }
        .padding(28)
    }
}

private struct LockView: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        VStack(spacing: 24) {
            BrandHeader(subtitle: "Your financial workspace is covered")
            Image(systemName: "lock.shield.fill")
                .font(.system(size: 58))
                .foregroundStyle(Color.accentColor)
            if let message = model.lockMessage {
                Text(message).foregroundStyle(.orange).multilineTextAlignment(.center)
            }
            Button("Unlock with Face ID or passcode") {
                Task { await model.unlock() }
            }
            .buttonStyle(.borderedProminent)
            .accessibilityIdentifier("unlockButton")
        }
        .padding(28)
    }
}

private struct SettingsView: View {
    @EnvironmentObject private var model: AppModel
    @Environment(\.dismiss) private var dismiss
    @State private var address = ""
    @State private var lockEnabled = false
    @State private var errorMessage: String?
    @State private var showHostChangeConfirmation = false

    var body: some View {
        NavigationStack {
            Form {
                Section("Private Tailscale address") {
                    TextField("https://your-mac.your-tailnet.ts.net", text: $address)
                        .textInputAutocapitalization(.never)
                        .keyboardType(.URL)
                        .autocorrectionDisabled()
                    Text("Changing hosts clears website data for the old host after confirmation.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Section("Privacy lock") {
                    Toggle("Require Face ID or device passcode", isOn: $lockEnabled)
                    Text("This only covers the app on your iPhone. Flask login remains separate and authoritative.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                if let errorMessage {
                    Section { Text(errorMessage).foregroundStyle(.red) }
                }
                Section {
                    Link("Open Tailscale", destination: URL(string: "tailscale://")!)
                }
            }
            .navigationTitle("Settings")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { validateSave() }
                }
            }
            .onAppear {
                address = model.origin?.string ?? ""
                lockEnabled = model.appLockEnabled
            }
            .confirmationDialog(
                "Change private host?",
                isPresented: $showHostChangeConfirmation,
                titleVisibility: .visible
            ) {
                Button("Change host and clear old website data", role: .destructive) { save() }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("You may need to sign in to Flask again on the new host.")
            }
        }
    }

    private func validateSave() {
        do {
            let next = try AppOrigin(address)
            if let current = model.origin, current != next {
                showHostChangeConfirmation = true
            } else {
                save()
            }
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func save() {
        errorMessage = nil
        Task {
            do {
                try await model.saveOrigin(address)
                if lockEnabled != model.appLockEnabled {
                    try await model.setAppLock(enabled: lockEnabled)
                }
                dismiss()
            } catch {
                errorMessage = error.localizedDescription
            }
        }
    }
}
