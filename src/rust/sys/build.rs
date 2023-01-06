fn main() {
    let mut bridge = cxx_build::bridge("src/lib.rs");

    let bridge_dir = &std::env::var("CARGO_MANIFEST_DIR").unwrap();

    bridge
        .file("bridge.cpp")
        .flag_if_supported("-std=c++17")
        .include(bridge_dir);

    println!("cargo:rerun-if-changed={bridge_dir}/bridge.cpp");
    println!("cargo:rerun-if-changed={bridge_dir}/bridge.hpp");
    println!("cargo:rerun-if-changed={bridge_dir}/lib.rs");

    // Check if we are part of a CMake build, in which case we shouldn't try to
    // build Core.
    if let Ok(cmake_include_dirs) = std::env::var("REALM_RUST_CORE_INCLUDE_DIRECTORIES") {
        // We are part of a CMake build.
        //
        // This means:
        //
        // - We shouldn't attempt to compile Core. Instead, we compile and link
        //   against the Core headers/libraries that CMake is currently
        //   building.
        // - We shouldn't attempt to compile the bridge code (bridge.cpp).
        //   Instead, we invoke `cxx_build::bridge()` to get it to generate
        //   `lib.rs.h` etc., and then leave it to CMake to compile it.
        // - However, we still need to provide the right linker flags to
        //   `rustc`, so `cargo test` will work.

        // We need this to link tests running under CMake (CTest).
        let cmake_core_binary_dir = std::env::var("REALM_RUST_CORE_BINARY_DIR").unwrap();
        let cmake_core_binary_lib = std::env::var("REALM_RUST_CORE_BINARY_NAME").unwrap();
        let cmake_bridge_binary_dir = std::env::var("REALM_RUST_BRIDGE_BINARY_DIR").unwrap();
        let cmake_bridge_binary_lib = std::env::var("REALM_RUST_BRIDGE_BINARY_NAME").unwrap();

        let cmake_include_dirs = cmake_include_dirs.split(':');
        for include_dir in cmake_include_dirs {
            bridge.include(include_dir);
        }

        println!("cargo:rustc-link-search=native={cmake_core_binary_dir}");
        println!("cargo:rustc-link-lib=static={cmake_core_binary_lib}");
        println!("cargo:rustc-link-search=native={cmake_bridge_binary_dir}");
        println!("cargo:rustc-link-lib=static={cmake_bridge_binary_lib}");
    } else {
        // Running standalone. This builds Core via CMake, enabling standalone
        // `cargo test`.
        let root_dir = &format!("{bridge_dir}/../../..");
        let debug = std::env::var("PROFILE").unwrap_or(String::from("debug")) == "debug";
        let lib_postfix = if debug { "-dbg" } else { "" };

        let out_dir = std::env::var("OUT_DIR").unwrap();

        // Build Core. This is useful for running `cargo test` standalone.
        let dst = cmake::Config::new(root_dir)
            .build_target("install")
            .define("REALM_CARGO_BUILD", "ON")
            .define("CARGO_TARGET_DIR", format!("{out_dir}/../../../.."))
            .define(
                "CMAKE_BUILD_TYPE",
                if debug { "Debug" } else { "RelWithDebInfo" },
            )
            .build();

        let libnames = ["realm", "realm-rust-bridge"];

        println!("cargo:rustc-link-search=native={}/lib", dst.display());
        for libname in libnames {
            println!("cargo:rustc-link-lib=static={libname}{lib_postfix}");
        }
    }

    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("apple") {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
    }
}
