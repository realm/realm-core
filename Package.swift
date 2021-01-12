// swift-tools-version:5.0

import PackageDescription
import Foundation

let versionStr = "10.3.3"
let versionPieces = versionStr.split(separator: "-")
let versionCompontents = versionPieces[0].split(separator: ".")
let versionExtra = versionPieces.count > 1 ? versionPieces[1] : ""

let cxxSettings: [CXXSetting] = [
    .headerSearchPath("src"),
    .define("REALM_DEBUG", .when(configuration: .debug)),
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
    "realm/sync/encrypt",
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

let syncCommandSources = [
    "realm/sync/apply_to_state_command.cpp",
    "realm/sync/encrypt/encryption_transformer_command.cpp",
    "realm/sync/inspector",
    "realm/sync/noinst/vacuum_command.cpp",
    "realm/sync/dump_command.cpp",
    "realm/sync/hist_command.cpp",
    "realm/sync/print_changeset_command.cpp",
    "realm/sync/realm_upgrade.cpp",
    "realm/sync/server_command.cpp",
    "realm/sync/server_index.cpp",
    "realm/sync/server_index_command.cpp",
    "realm/sync/stat_command.cpp",
    "realm/sync/verify_server_file_command.cpp"
]

let package = Package(
    name: "RealmDatabase",
    platforms: [
        .macOS(.v10_10),
        .iOS(.v11),
        .tvOS(.v9),
        .watchOS(.v2)
    ],
    products: [
        .library(
            name: "RealmStorage",
            targets: ["Storage"]),
        .library(
            name: "RealmQueryParser",
            targets: ["QueryParser"]),
        .library(
            name: "RealmSyncClient",
            targets: ["SyncClient"]),
        .library(
            name: "RealmObjectStore",
            targets: ["ObjectStore"]),
        .library(
            name: "RealmCapi",
            targets: ["Capi"]),
        .library(
            name: "RealmFFI",
            targets: ["FFI"]),
    ],
    targets: [
        .target(
            name: "Bid",
            path: "src/external/IntelRDFPMathLib20U2/LIBRARY/src",
            sources: [
                "bid128.c",
                "bid128_compare.c",
                "bid128_mul.c",
                "bid128_div.c",
                "bid128_add.c",
                "bid128_fma.c",
                "bid64_to_bid128.c",
                "bid_convert_data.c",
                "bid_decimal_data.c",
                "bid_decimal_globals.c",
                "bid_from_int.c",
                "bid_round.c"
            ],
            publicHeadersPath: "."
        ),
        .target(
            name: "Storage",
            dependencies: ["Bid"],
            path: "src",
            exclude: [
                "realm/tools",
                "realm/parser",
                "realm/metrics",
                "realm/exec",
                "realm/object-store",
                "realm/sync",
                "external",
                "win32",
                "realm/util/network.cpp",
                "realm/util/network_ssl.cpp",
                "realm/util/http.cpp",
                "realm/util/websocket.cpp",
                "realm/realm.h"
            ],
            sources: [
                "realm"
            ],
            publicHeadersPath: ".",
            cxxSettings: cxxSettings),
        .target(
            name: "QueryParser",
            dependencies: ["Storage"],
            path: "src",
            sources: ["realm/parser"],
            publicHeadersPath: "realm/parser",
            cxxSettings: [
                .headerSearchPath("external/pegtl/include/tao")
            ] + cxxSettings),
        .target(
            name: "SyncClient",
            dependencies: ["Storage"],
            path: "src",
            exclude: ([
                "realm/sync/crypto_server_openssl.cpp",
            ] + syncCommandSources + syncServerSources) as [String],
            sources: [
                "realm/sync",
                "realm/util/network.cpp",
                "realm/util/network_ssl.cpp",
                "realm/util/http.cpp",
                "realm/util/websocket.cpp"
            ],
            publicHeadersPath: "realm/sync",
            cxxSettings: [
                .define("REALM_HAVE_SECURE_TRANSPORT", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
            ] + cxxSettings,
            linkerSettings: [
                .linkedFramework("Security", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .linkedLibrary("z")
            ]),
        .target(
            name: "SyncServer",
            dependencies: ["SyncClient"],
            path: "src",
            exclude: ([
                "realm/sync/crypto_server_openssl.cpp",
            ] + syncCommandSources) as [String],
            sources: syncServerSources,
            publicHeadersPath: "realm/sync/impl", // hack
            cxxSettings: cxxSettings,
            linkerSettings: [
                .linkedFramework("Foundation", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
            ]),
        .target(
            name: "ObjectStore",
            dependencies: ["SyncClient", "QueryParser"],
            path: "src",
            exclude: [
                "realm/object-store/impl/epoll",
                "realm/object-store/impl/generic",
                "realm/object-store/impl/windows",
                "realm/object-store/c_api"
            ],
            sources: ["realm/object-store"],
            publicHeadersPath: "realm/object-store",
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath("realm/object-store")
            ] + cxxSettings) as [CXXSetting]),
        .target(
            name: "Capi",
            dependencies: ["ObjectStore"],
            path: "src",
            exclude: [
                "realm/object-store/c_api/realm.c"
            ],
            sources: ["realm/object-store/c_api"],
            publicHeadersPath: "realm/object-store/c_api",
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath("external/pegtl/include/tao")
            ] + cxxSettings) as [CXXSetting]),
        .target(
            name: "FFI",
            dependencies: ["Capi"],
            path: "src/swift"),
        .target(
            name: "ObjectStoreTests",
            dependencies: ["ObjectStore", "SyncServer"],
            path: "test/object-store",
            exclude: [
                "benchmarks",
                "notifications-fuzzer",
                "c_api"
            ],
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath("."),
                .headerSearchPath("../../external/catch/single_include")
            ] + cxxSettings) as [CXXSetting]),
        .target(
            name: "CapiTests",
            dependencies: ["Capi"],
            path: "test/object-store/c_api",
            exclude: [
                "benchmarks",
                "mongodb",
                "notifications-fuzzer",
                "sync",
                "util"
            ],
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath("../"),
                .headerSearchPath("../../../external/catch/single_include")
            ] + cxxSettings) as [CXXSetting])
    ],
    cxxLanguageStandard: .cxx1z
)
