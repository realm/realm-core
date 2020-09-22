// swift-tools-version:5.0

import PackageDescription
import Foundation

let versionStr = "10.0.0-beta.5"
let versionPieces = versionStr.split(separator: "-")
let versionCompontents = versionPieces[0].split(separator: ".")
let versionExtra = versionPieces.count > 1 ? versionPieces[1] : ""

let cxxSettings: [CXXSetting] = [
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
]

let syncServerSources = [
    "realm/sync/encrypt/encryption_transformer.cpp",
    "realm/sync/noinst/reopening_file_logger.cpp",
    "realm/sync/noinst/server_dir.cpp",
    "realm/sync/noinst/server_file_access_cache.cpp",
    "realm/sync/noinst/server_history.cpp",
    "realm/sync/noinst/server_legacy_migration.cpp",
    "realm/sync/noinst/vacuum.cpp",
    "realm/sync/access_control.cpp",
    "realm/sync/crypto_server_apple.mm",
    "realm/sync/metrics.cpp",
    "realm/sync/server_configuration.cpp",
    "realm/sync/server.cpp"
]

let package = Package(
    name: "RealmDatabase",
    products: [
        .library(
            name: "Storage",
            type: .static,
            targets: ["Storage"]),
        .library(
            name: "QueryParser",
            type: .static,
            targets: ["QueryParser"]),
        .library(
            name: "SyncClient",
            type: .static,
            targets: ["SyncClient"]),
        .library(
            name: "ObjectStore",
            type: .static,
            targets: ["ObjectStore"]),
    ],
    targets: [
        .target(
            name: "Storage",
            path: "src",
            exclude: [
                "realm/tools",
                "realm/parser",
                "realm/metrics",
                "realm/exec",
                "realm/object-store",
                "realm/sync",
                "external/pegtl",
                "win32",
                "realm/util/network.cpp",
                "realm/util/network_ssl.cpp",
                "realm/util/http.cpp",
                "realm/util/websocket.cpp"
            ],
            sources: [
                "realm",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128.c",
                "external/IntelRDFPMathLib20U2/LIBRARY/src/bid128_compare.c",
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
            cxxSettings: cxxSettings),
        .target(
            name: "QueryParser",
            dependencies: ["Storage"],
            path: "src",
            sources: ["realm/parser"],
            //publicHeadersPath: "realm/parser",
            cxxSettings: [
                .headerSearchPath("external/pegtl/include/tao")
            ] + cxxSettings),
        .target(
            name: "SyncClient",
            dependencies: ["Storage"],
            path: "src",
            exclude: [
                "realm/sync/encrypt",
                "realm/sync/inspector",
                "realm/sync/noinst/vacuum_command.cpp",
                "realm/sync/crypto_server_openssl.cpp",
                "realm/sync/dump_command.cpp",
                "realm/sync/hist_command.cpp",
                "realm/sync/print_changeset_command.cpp",
                "realm/sync/realm_upgrade.cpp",
                "realm/sync/server_command.cpp",
                "realm/sync/server_index.cpp",
                "realm/sync/server_index_command.cpp",
                "realm/sync/stat_command.cpp",
                "realm/sync/verify_server_file_command.cpp"
            ] + syncServerSources,
            sources: [
                "realm/sync",
                "realm/util/network.cpp",
                "realm/util/network_ssl.cpp",
                "realm/util/http.cpp",
                "realm/util/websocket.cpp"
            ],
            //publicHeadersPath: "realm/sync",
            cxxSettings: [
                .define("REALM_HAVE_SECURE_TRANSPORT", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .define("REALM_HAVE_OPENSSL", to: "1", .when(platforms: [.linux]))
            ] + cxxSettings,
            linkerSettings: [
                .linkedFramework("Security", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .linkedLibrary("z")
            ]),
        .target(
            name: "ObjectStore",
            dependencies: ["Storage", "SyncClient"],
            path: "src",
            exclude: [
                "realm/object-store/impl/epoll",
                "realm/object-store/impl/generic",
                "realm/object-store/impl/windows"
            ],
            sources: ["realm/object-store"],
            //publicHeadersPath: "realm/object-store",
            cxxSettings: [
                .define("REALM_ENABLE_SYNC", to: "1"),
                .headerSearchPath("realm/object-store")
            ] + cxxSettings)
    ],
    cxxLanguageStandard: .cxx1z
)
