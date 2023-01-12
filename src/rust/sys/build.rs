fn main() {
    // let mut bridge = cxx_build::bridge("src/lib.rs");

    let bridge_dir = &std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let libnames = ["realm", "realm-rust-bridge"];
    let debug = std::env::var("PROFILE").unwrap_or(String::from("debug")) == "debug";
    let lib_postfix = if debug { "-dbg" } else { "" };

    // bridge
    //     .file("bridge.cpp")
    //     .flag_if_supported("-std=c++17")
    //     .include(bridge_dir);

    // println!("cargo:rerun-if-changed={bridge_dir}/bridge.cpp");
    // println!("cargo:rerun-if-changed={bridge_dir}/bridge.hpp");
    // println!("cargo:rerun-if-changed={bridge_dir}/lib.rs");

    // Check if we are part of a CMake build, in which case we shouldn't try to
    // build Core.
    let is_cmake_build = std::env::var("REALM_CMAKE_BUILD").unwrap_or("0".to_string());
    let is_cmake_build =
        ["1", "true", "yes", "on", "TRUE", "YES", "ON"].contains(&&*is_cmake_build);

    if is_cmake_build {
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

        let binary_dir =
            std::env::var("REALM_CMAKE_BINARY_DIR").expect("REALM_CMAKE_BINARY_DIR not set");

        println!("cargo:rustc-link-search=native={binary_dir}/src/realm");
        println!("cargo:rustc-link-search=native={binary_dir}/src/rust");

        for libname in libnames {
            println!("cargo:rustc-link-lib=static={libname}{lib_postfix}");
        }
    } else {
        // Running standalone. This builds Core via CMake, enabling standalone
        // `cargo test`.
        let root_dir = &format!("{bridge_dir}/../../..");

        let out_dir = std::env::var("OUT_DIR").unwrap();

        // Build Core. This is useful for running `cargo test` standalone.
        let dst = cmake::Config::new(root_dir)
            .build_target("RustBridge")
            .define("REALM_CARGO_BUILD", "ON")
            .define("CARGO_TARGET_DIR", format!("{out_dir}/../../../.."))
            .define(
                "CMAKE_BUILD_TYPE",
                if debug { "Debug" } else { "RelWithDebInfo" },
            )
            .build();

        println!(
            "cargo:rustc-link-search=native={}/build/src/realm",
            dst.display()
        );
        println!(
            "cargo:rustc-link-search=native={}/build/src/rust",
            dst.display()
        );
        for libname in libnames {
            println!("cargo:rustc-link-lib=static={libname}{lib_postfix}");
        }
    }

    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("apple") {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
    }
}
