// swift-tools-version:5.0

import PackageDescription
import Foundation

let versionStr = "10.1.2"
let versionPieces = versionStr.split(separator: "-")
let versionCompontents = versionPieces[0].split(separator: ".")
let versionExtra = versionPieces.count > 1 ? versionPieces[1] : ""

let package = Package(
    name: "RealmCore",
    platforms: [
        .macOS(.v10_10),
        .iOS(.v11),
        .tvOS(.v9),
        .watchOS(.v2)
    ],
    products: [
        .library(
            name: "RealmCore",
            targets: ["RealmCore"]),
        .library(
            name: "RealmCoreDynamic",
            type: .dynamic,
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
            ],
            sources: [
                "realm",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128_compare.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128_mul.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128_div.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128_add.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128_fma.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid64_to_bid128.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid_convert_data.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid_decimal_data.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid_decimal_globals.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid_from_int.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid_round.c"
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
    cxxLanguageStandard: .cxx1z
)
