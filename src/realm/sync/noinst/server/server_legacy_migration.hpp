
#ifndef REALM_NOINST_SERVER_LEGACY_MIGRATION_HPP
#define REALM_NOINST_SERVER_LEGACY_MIGRATION_HPP

#include <string>

#include <realm/util/logger.hpp>

namespace realm {
namespace _impl {

/// If not already done, migrate legacy format server-side Realm files. This
/// migration step introduces stable identifiers, and discards all
/// client-specific state.
void ensure_legacy_migration_1(const std::string& realms_dir, const std::string& migration_dir, util::Logger&);

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_SERVER_LEGACY_MIGRATION_HPP
