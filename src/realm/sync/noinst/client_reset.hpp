///////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_NOINST_CLIENT_RESET_HPP
#define REALM_NOINST_CLIENT_RESET_HPP

#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/protocol.hpp>

namespace realm {
namespace _impl {
namespace client_reset {

// The reset fails if there seems to be conflict between the
// instructions and state.
//
// After failure the processing stops and the client reset will
// drop all local changes.
//
// Failure is triggered by:
// 1. Destructive schema changes.
// 2. Creation of an already existing table with another type.
// 3. Creation of an already existing column with another type.
struct ClientResetFailed : public std::runtime_error {
    using std::runtime_error::runtime_error;
};


// transfer_group() transfers all tables, columns, objects and values from the src
// group to the dst group and deletes everything in the dst group that is absent in
// the src group. An update is only performed when a comparison shows that a
// change is needed. In this way, the continuous transaction history of changes
// is minimal.
//
// The result is that src group is unchanged and the dst group is equal to src
// when this function returns.
void transfer_group(const Transaction& tr_src, Transaction& tr_dst, util::Logger& logger);

void remove_all_tables(Transaction& tr_dst, util::Logger& logger);

// preform_client_reset_diff() takes the Realm performs a client reset on
// the Realm in 'path_local' given the Realm 'path_fresh' as the source of truth.
// If the fresh path is not provided, discard mode is assumed and all data in the local
// Realm is removed.
// If the fresh path is provided, the local Realm is changed such that its state is equal
// to the fresh Realm. Then the local Realm will have its client file ident set to
// 'client_file_ident'
//
// The function returns the old version and the new version of the local Realm to
// be used to report the sync transaction to the user.
struct LocalVersionIDs {
    realm::VersionID old_version;
    realm::VersionID new_version;
};
LocalVersionIDs perform_client_reset_diff(DB& db, DBRef db_remote, sync::SaltedFileIdent client_file_ident,
                                          util::Logger& logger);

} // namespace client_reset
} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_RESET_HPP
