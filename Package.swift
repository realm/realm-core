// swift-tools-version:5.0

import PackageDescription
import Foundation

let versionStr = "5.23.2"
let versionPieces = versionStr.split(separator: "-")
let versionCompontents = versionPieces[0].split(separator: ".")
let versionExtra = versionPieces.count > 1 ? versionPieces[1] : ""

let package = Package(
    name: "RealmCore",
    products: [
        .library(
            name: "RealmCore",
            targets: ["RealmCore"]),
    ],
    targets: [
        .target(
            name: "RealmCore",
            path: "src",
            exclude: [
                "realm/tools",
                "realm/parser",
                "realm/metrics",
                "realm/exec",
                "win32",
                "external"
            ],
            publicHeadersPath: ".",
            cxxSettings: [
                .headerSearchPath("src"),
                .define("REALM_NO_CONFIG"),
                .define("REALM_INSTALL_LIBEXECDIR", to: ""),
                .define("REALM_ENABLE_ASSERTIONS", to: "1"),
                .define("REALM_ENABLE_ENCRYPTION", to: "1"),

                .define("REALM_VERSION_MAJOR", to: String(versionCompontents[0])),
                .define("REALM_VERSION_MINOR", to: String(versionCompontents[1])),
                .define("REALM_VERSION_PATCH", to: String(versionCompontents[2])),
                .define("REALM_VERSION_EXTRA", to: "\"\(versionExtra)\""),
                .define("REALM_VERSION_STRING", to: "\"\(versionStr)\""),
            ]),
    ],
    cxxLanguageStandard: .cxx14
)
