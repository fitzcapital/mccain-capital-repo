import SwiftUI

enum GalaxyTheme {
    static let void = Color(red: 0.018, green: 0.028, blue: 0.070)
    static let navy = Color(red: 0.035, green: 0.065, blue: 0.145)
    static let surface = Color(red: 0.055, green: 0.090, blue: 0.180)
    static let cyan = Color(red: 0.18, green: 0.82, blue: 0.96)
    static let violet = Color(red: 0.48, green: 0.36, blue: 0.96)
    static let gold = Color(red: 0.95, green: 0.76, blue: 0.30)

    static var background: some View {
        ZStack {
            LinearGradient(colors: [void, navy, void], startPoint: .topLeading, endPoint: .bottomTrailing)
            RadialGradient(colors: [violet.opacity(0.22), .clear], center: .topTrailing, startRadius: 8, endRadius: 380)
            RadialGradient(colors: [cyan.opacity(0.12), .clear], center: .bottomLeading, startRadius: 5, endRadius: 320)
        }
    }
}
