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
#include <realm/sync/config.hpp>
#include <realm/sync/protocol.hpp>

#include <ostream>

namespace realm {

std::ostream& operator<<(std::ostream& os, const ClientResyncMode& mode);

namespace sync {
class SubscriptionStore;
}

namespace _impl::client_reset {

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

struct PendingReset {
    ClientResyncMode type;
    Timestamp time;
};
void remove_pending_client_resets(TransactionRef wt);
util::Optional<PendingReset> has_pending_reset(TransactionRef wt);
void track_reset(TransactionRef wt, ClientResyncMode mode);

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

LocalVersionIDs perform_client_reset_diff(DBRef db, DBRef db_remote, sync::SaltedFileIdent client_file_ident,
                                          util::Logger& logger, ClientResyncMode mode, bool recovery_is_allowed,
                                          bool* did_recover_out, sync::SubscriptionStore* sub_store,
                                          util::UniqueFunction<void(int64_t)> on_flx_version_complete);

namespace converters {

struct EmbeddedObjectConverter : std::enable_shared_from_this<EmbeddedObjectConverter> {
    void track(Obj e_src, Obj e_dst);
    void process_pending();

private:
    struct EmbeddedToCheck {
        Obj embedded_in_src;
        Obj embedded_in_dst;
    };
    std::vector<EmbeddedToCheck> embedded_pending;
};

struct InterRealmValueConverter {
    InterRealmValueConverter(ConstTableRef src_table, ColKey src_col, ConstTableRef dst_table, ColKey dst_col,
                             std::shared_ptr<EmbeddedObjectConverter> ec);
    void track_new_embedded(Obj src, Obj dst);
    struct ConversionResult {
        Mixed converted_value;
        bool requires_new_embedded_object = false;
        Obj src_embedded_to_check;
    };

    // convert `src` to the destination Realm and compare that value with `dst`
    // If `converted_src_out` is provided, it will be set to the converted src value
    int cmp_src_to_dst(Mixed src, Mixed dst, ConversionResult* converted_src_out = nullptr,
                       bool* did_update_out = nullptr);
    void copy_value(const Obj& src_obj, Obj& dst_obj, bool* update_out);

private:
    void copy_list(const Obj& src_obj, Obj& dst_obj, bool* update_out);
    void copy_set(const Obj& src_obj, Obj& dst_obj, bool* update_out);
    void copy_dictionary(const Obj& src_obj, Obj& dst_obj, bool* update_out);

    TableRef m_dst_link_table;
    ConstTableRef m_src_table;
    ConstTableRef m_dst_table;
    ColKey m_src_col;
    ColKey m_dst_col;
    TableRef m_opposite_of_src;
    TableRef m_opposite_of_dst;
    std::shared_ptr<EmbeddedObjectConverter> m_embedded_converter;
    bool m_is_embedded_link;
    const bool m_primitive_types_only;
};

struct InterRealmObjectConverter {
    InterRealmObjectConverter(ConstTableRef table_src, TableRef table_dst,
                              std::shared_ptr<EmbeddedObjectConverter> embedded_tracker);
    void copy(const Obj& src, Obj& dst, bool* update_out);

private:
    void populate_columns_from_table(ConstTableRef table_src, ConstTableRef table_dst);
    std::shared_ptr<EmbeddedObjectConverter> m_embedded_tracker;
    std::vector<InterRealmValueConverter> m_columns_cache;
};

} // namespace converters
} // namespace _impl::client_reset
} // namespace realm

#endif // REALM_NOINST_CLIENT_RESET_HPP
