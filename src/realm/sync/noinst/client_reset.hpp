
#ifndef REALM_NOINST_CLIENT_RESET_HPP
#define REALM_NOINST_CLIENT_RESET_HPP

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/object.hpp>

namespace realm {
namespace _impl {
namespace client_reset {

// transfer_group() transfers all tables, columns, objects and values from the src
// group to the dst group and deletes everything in the dst group that is absent in
// the src group. An update is only performed when a comparison shows that a
// change is needed. In this way, the continuous transaction history of changes
// is minimal.
//
// The result is that src group is unchanged and the dst group is equal to src
// when this function returns.
void transfer_group(const Transaction& tr_src, Transaction& tr_dst, util::Logger& logger);

// recover_schema() transfers all tables and columns that exist in src but not
// in dst into dst. Nothing is erased in dst.
void recover_schema(const Transaction& group_src, Transaction& group_dst, util::Logger& logger);

void remove_all_tables(Transaction& tr_dst, util::Logger& logger);

// preform_client_reset_diff() takes the Realm performs a client reset on
// the Realm in 'path_local' given the Realm 'path_remote' as the source of truth.
// Local changes in 'path_local' with client version greater than
// 'client_version' will be recovered as well as possible. The reset Realm will have its
// client file ident set to 'client_file_ident'. Recovered changesets will be produced
// with the new client file ident. The download progress in the Realm will be set to
// 'server_version', and the downloadable bytes set to 'downloadable_bytes'.
//
// The last argument, 'should_commit_remote' is for testing purposes. If true, a
// write transaction in 'path_remote' will be committed resulting in the two Realms
// having equal group content. This equality can be checked in tests. In production, it
// is unnecessary to commit in the remote Realm, since it will be discarded
// immediately after.
//
// The function returns the old version and the new version of the local Realm to
// be used to report the sync transaction to the user.
struct LocalVersionIDs {
    realm::VersionID old_version;
    realm::VersionID new_version;
};
LocalVersionIDs perform_client_reset_diff(const std::string& path_local, const util::Optional<std::string> path_fresh,
                                          const util::Optional<std::array<char, 64>>& encryption_key,
                                          sync::SaltedFileIdent client_file_ident, util::Logger& logger);

} // namespace client_reset
} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_RESET_HPP
