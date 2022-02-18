// swift-tools-version:5.3

import PackageDescription
import Foundation

func readDependencies() -> [String: String] {
    let PWD = URL(fileURLWithPath: #filePath, isDirectory: false).deletingLastPathComponent()
    let dependencies = PWD.deletingLastPathComponent().appendingPathComponent("dependencies.list")
    let contents = try! String(contentsOfFile: dependencies.path)

    var map: [String: String] = [:]
    
    for line in contents.components(separatedBy: .newlines) {
        if line.starts(with: "#") || line.isEmpty { continue }
        
        let pair = line.split(separator: "=")
        map[String(pair[0])] = String(pair[1])
    }

    return map
}

let versionStr = readDependencies()["VERSION"]!
let versionPieces = versionStr.split(separator: "-")
let versionCompontents = versionPieces[0].split(separator: ".")
let versionExtra = versionPieces.count > 1 ? versionPieces[1] : ""

#if swift(>=5.5)
let applePlatforms: [PackageDescription.Platform] = [.macOS, .macCatalyst, .iOS, .tvOS, .watchOS]
#else
let applePlatforms: [PackageDescription.Platform] = [.macOS, .iOS, .tvOS, .watchOS]
#endif

var cxxSettings: [CXXSetting] = [
    .headerSearchPath("."),
    .define("REALM_DEBUG", .when(configuration: .debug)),
    .define("REALM_NO_CONFIG"),
    .define("REALM_ENABLE_ASSERTIONS", to: "1"),
    .define("REALM_ENABLE_ENCRYPTION", to: "1"),
    .define("REALM_ENABLE_SYNC", to: "1"),

    .define("REALM_VERSION_MAJOR", to: String(versionCompontents[0])),
    .define("REALM_VERSION_MINOR", to: String(versionCompontents[1])),
    .define("REALM_VERSION_PATCH", to: String(versionCompontents[2])),
    .define("REALM_VERSION_EXTRA", to: "\"\(versionExtra)\""),
    .define("REALM_VERSION_STRING", to: "\"\(versionStr)\""),
    .define("REALM_HAVE_SECURE_TRANSPORT", to: "1", .when(platforms: applePlatforms))
]

let package = Package(
    name: "RealmDatabaseTests",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6)
    ],
    dependencies: [
        .package(name: "RealmDatabase", path: "..")
    ],
    targets: [
        .target(
            name: "SyncServer",
            dependencies: [
                .product(name: "RealmCore", package: "RealmDatabase"),
            ],
            path: "util/sync-server",
            exclude: [
                "CMakeLists.txt",
                "crypto_server_openssl.cpp"
            ],
            publicHeadersPath: ".",
            cxxSettings: cxxSettings),
        .target(
            name: "ObjectStoreTestUtils",
            dependencies: [
                .product(name: "RealmCore", package: "RealmDatabase"),
                .target(name: "SyncServer")
            ],
            path: "object-store/util",
            exclude: [
                "baas_admin_api.hpp",
                "event_loop.hpp",
                "index_helpers.hpp",
                "test_file.hpp",
                "test_utils.hpp",
            ],
            publicHeadersPath: ".",
            cxxSettings: ([
                .headerSearchPath(".."),
                .headerSearchPath("../../external/catch/single_include"),
            ] + cxxSettings) as [CXXSetting]),
        .target(
            name: "ObjectStoreTests",
            dependencies: [
                .product(name: "RealmCore", package: "RealmDatabase"),
                .product(name: "RealmQueryParser", package: "RealmDatabase"),
                .target(name: "ObjectStoreTestUtils"),
            ],
            path: "object-store",
            exclude: [
                "CMakeLists.txt",
                "backup.cpp",
                "benchmarks",
                "c_api",
                "collection_fixtures.hpp",
                "notifications-fuzzer",
                "query.json",
                "sync-1.x.realm",
                "sync-metadata-v4.realm",
                "sync-metadata-v5.realm",
                "sync/session/session_util.hpp",
                "sync/sync_test_utils.hpp",
                "test_backup-olden-and-golden.realm",
                "util",
            ],
            cxxSettings: ([
                .headerSearchPath("."),
                .headerSearchPath("../external/catch/single_include"),
            ] + cxxSettings) as [CXXSetting],
            linkerSettings: [
                .linkedFramework("Foundation", .when(platforms: applePlatforms)),
                .linkedFramework("Security", .when(platforms: applePlatforms)),
            ]),
        .target(
            name: "CapiTests",
            dependencies: [
                .product(name: "RealmCapi", package: "RealmDatabase"),
                .target(name: "ObjectStoreTestUtils"),
            ],
            path: "object-store/c_api",
            cxxSettings: ([
                .headerSearchPath("../"),
                .headerSearchPath("../../external/catch/single_include")
            ] + cxxSettings) as [CXXSetting],
            linkerSettings: [
                .linkedFramework("Foundation", .when(platforms: applePlatforms)),
                .linkedFramework("Security", .when(platforms: applePlatforms)),
            ]),
    ],
    cxxLanguageStandard: .cxx1z
)
