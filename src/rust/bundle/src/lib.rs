/*!
 * Realm Rust bundle
 *
 * The purpose of this crate is to collect all the other Realm Rust crates into
 * a single static library that can be linked into C/C++ code.
 */

pub use realm_core as core;
pub use realm_query as query;
pub use realm_sys as sys;
