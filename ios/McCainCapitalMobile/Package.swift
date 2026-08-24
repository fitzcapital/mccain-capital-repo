// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "McCainCapitalCore",
    platforms: [.macOS(.v13)],
    products: [
        .library(name: "McCainCapitalCore", targets: ["McCainCapitalCore"]),
        .executable(name: "McCainCapitalCoreChecks", targets: ["McCainCapitalCoreChecks"]),
    ],
    targets: [
        .target(
            name: "McCainCapitalCore",
            path: "McCainCapitalMobile/Core"
        ),
        .executableTarget(
            name: "McCainCapitalCoreChecks",
            dependencies: ["McCainCapitalCore"],
            path: "CoreChecks"
        ),
    ]
)
