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

#include <realm/transaction.hpp>
#include <realm/dictionary.hpp>
#include <realm/object_converter.hpp>
#include <realm/table_view.hpp>
#include <realm/set.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_recovery.hpp>
#include <realm/sync/noinst/pending_reset_store.hpp>
#include <realm/sync/subscriptions.hpp>

#include <realm/util/compression.hpp>

#include <algorithm>
#include <chrono>
#include <vector>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace realm {

std::ostream& operator<<(std::ostream& os, const ClientResyncMode& mode)
{
    switch (mode) {
        case ClientResyncMode::Manual:
            os << "Manual";
            break;
        case ClientResyncMode::DiscardLocal:
            os << "DiscardLocal";
            break;
        case ClientResyncMode::Recover:
            os << "Recover";
            break;
        case ClientResyncMode::RecoverOrDiscard:
            os << "RecoverOrDiscard";
            break;
    }
    return os;
}

namespace _impl::client_reset {

static inline bool should_skip_table(const Transaction& group, TableKey key)
{
    return !group.table_is_public(key);
}

void transfer_group(const Transaction& group_src, Transaction& group_dst, util::Logger& logger,
                    bool allow_schema_additions)
{
    logger.debug(util::LogCategory::reset,
                 "transfer_group, src size = %1, dst size = %2, allow_schema_additions = %3", group_src.size(),
                 group_dst.size(), allow_schema_additions);

    // Turn off the sync history tracking during state transfer since it will be thrown
    // away immediately after anyways. This reduces the memory footprint of a client reset.
    ClientReplication* client_repl = dynamic_cast<ClientReplication*>(group_dst.get_replication());
    REALM_ASSERT_RELEASE(client_repl);
    TempShortCircuitReplication sync_history_guard(*client_repl);

    // Find all tables in dst that should be removed.
    std::set<std::string> tables_to_remove;
    for (auto table_key : group_dst.get_table_keys()) {
        if (should_skip_table(group_dst, table_key))
            continue;
        StringData table_name = group_dst.get_table_name(table_key);
        logger.debug(util::LogCategory::reset, "key = %1, table_name = %2", table_key.value, table_name);
        ConstTableRef table_src = group_src.get_table(table_name);
        if (!table_src) {
            logger.debug(util::LogCategory::reset, "Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        // Check whether the table type is the same.
        TableRef table_dst = group_dst.get_table(table_key);
        auto pk_col_src = table_src->get_primary_key_column();
        auto pk_col_dst = table_dst->get_primary_key_column();
        bool has_pk_src = bool(pk_col_src);
        bool has_pk_dst = bool(pk_col_dst);
        if (has_pk_src != has_pk_dst) {
            throw ClientResetFailed(util::format("Client reset requires a primary key column in %1 table '%2'",
                                                 (has_pk_src ? "dest" : "source"), table_name));
        }
        if (!has_pk_src)
            continue;

        // Now the tables both have primary keys. Check type.
        if (pk_col_src.get_type() != pk_col_dst.get_type()) {
            throw ClientResetFailed(
                util::format("Client reset found incompatible primary key types (%1 vs %2) on '%3'",
                             pk_col_src.get_type(), pk_col_dst.get_type(), table_name));
        }
        // Check collection type, nullability etc. but having an index doesn't matter;
        ColumnAttrMask pk_col_src_attr = pk_col_src.get_attrs();
        ColumnAttrMask pk_col_dst_attr = pk_col_dst.get_attrs();
        pk_col_src_attr.reset(ColumnAttr::col_attr_Indexed);
        pk_col_dst_attr.reset(ColumnAttr::col_attr_Indexed);
        if (pk_col_src_attr != pk_col_dst_attr) {
            throw ClientResetFailed(
                util::format("Client reset found incompatible primary key attributes (%1 vs %2) on '%3'",
                             pk_col_src.value, pk_col_dst.value, table_name));
        }
        // Check name.
        StringData pk_col_name_src = table_src->get_column_name(pk_col_src);
        StringData pk_col_name_dst = table_dst->get_column_name(pk_col_dst);
        if (pk_col_name_src != pk_col_name_dst) {
            throw ClientResetFailed(
                util::format("Client reset requires equal pk column names but '%1' != '%2' on '%3'", pk_col_name_src,
                             pk_col_name_dst, table_name));
        }
        // The table survives.
        logger.debug(util::LogCategory::reset, "Table '%1' will remain", table_name);
    }

    // If there have been any tables marked for removal stop.
    // We consider two possible options for recovery:
    // 1: Remove the tables. But this will generate destructive schema
    //    schema changes that the local Realm cannot advance through.
    //    Since this action will fail down the line anyway, give up now.
    // 2: Keep the tables locally and ignore them. But the local app schema
    //    still has these classes and trying to modify anything in them will
    //    create sync instructions on tables that sync doesn't know about.
    // As an exception in recovery mode, we assume that the corresponding
    // additive schema changes will be part of the recovery upload. If they
    // are present, then the server can choose to allow them (if in dev mode).
    // If they are not present, then the server will emit an error the next time
    // a value is set on the unknown property.
    if (!allow_schema_additions && !tables_to_remove.empty()) {
        std::string names_list;
        for (const std::string& table_name : tables_to_remove) {
            names_list += Group::table_name_to_class_name(table_name);
            names_list += ", ";
        }
        if (names_list.size() > 2) {
            // remove the final ", "
            names_list = names_list.substr(0, names_list.size() - 2);
        }
        throw ClientResetFailed(
            util::format("Client reset cannot recover when classes have been removed: {%1}", names_list));
    }

    // Create new tables in dst if needed.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        auto pk_col_src = table_src->get_primary_key_column();
        TableRef table_dst = group_dst.get_table(table_name);
        if (!table_dst) {
            // Create the table.
            if (table_src->is_embedded()) {
                REALM_ASSERT(!pk_col_src);
                group_dst.add_table(table_name, Table::Type::Embedded);
            }
            else {
                REALM_ASSERT(pk_col_src); // a sync table will have a pk
                auto pk_col_src = table_src->get_primary_key_column();
                DataType pk_type = DataType(pk_col_src.get_type());
                StringData pk_col_name = table_src->get_column_name(pk_col_src);
                group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col_src.is_nullable(),
                                                     table_src->get_table_type());
            }
        }
    }

    // Now the class tables are identical.
    size_t num_tables;
    {
        size_t num_tables_src = 0;
        for (auto table_key : group_src.get_table_keys()) {
            if (!should_skip_table(group_src, table_key))
                ++num_tables_src;
        }
        size_t num_tables_dst = 0;
        for (auto table_key : group_dst.get_table_keys()) {
            if (!should_skip_table(group_dst, table_key))
                ++num_tables_dst;
        }
        REALM_ASSERT_EX(allow_schema_additions || num_tables_src == num_tables_dst, num_tables_src, num_tables_dst);
        num_tables = num_tables_src;
    }
    logger.debug(util::LogCategory::reset, "The number of tables is %1", num_tables);

    // Remove columns in dst if they are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        std::vector<std::string> columns_to_remove;
        for (ColKey col_key : table_dst->get_column_keys()) {
            StringData col_name = table_dst->get_column_name(col_key);
            ColKey col_key_src = table_src->get_column_key(col_name);
            if (!col_key_src) {
                columns_to_remove.push_back(col_name);
                continue;
            }
        }
        if (!allow_schema_additions && !columns_to_remove.empty()) {
            std::string columns_list;
            for (const std::string& col_name : columns_to_remove) {
                columns_list += col_name;
                columns_list += ", ";
            }
            throw ClientResetFailed(
                util::format("Client reset cannot recover when columns have been removed from '%1': {%2}", table_name,
                             columns_list));
        }
    }

    // Add columns in dst if present in src and absent in dst.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        for (ColKey col_key : table_src->get_column_keys()) {
            StringData col_name = table_src->get_column_name(col_key);
            ColKey col_key_dst = table_dst->get_column_key(col_name);
            if (!col_key_dst) {
                DataType col_type = table_src->get_column_type(col_key);
                bool nullable = col_key.is_nullable();
                auto search_index_type = table_src->search_index_type(col_key);
                logger.trace(util::LogCategory::reset,
                             "Create column, table = %1, column name = %2, "
                             " type = %3, nullable = %4, search_index = %5",
                             table_name, col_name, col_key.get_type(), nullable, search_index_type);
                ColKey col_key_dst;
                if (Table::is_link_type(col_key.get_type())) {
                    ConstTableRef target_src = table_src->get_link_target(col_key);
                    TableRef target_dst = group_dst.get_table(target_src->get_name());
                    if (col_key.is_list()) {
                        col_key_dst = table_dst->add_column_list(*target_dst, col_name);
                    }
                    else if (col_key.is_set()) {
                        col_key_dst = table_dst->add_column_set(*target_dst, col_name);
                    }
                    else if (col_key.is_dictionary()) {
                        DataType key_type = table_src->get_dictionary_key_type(col_key);
                        col_key_dst = table_dst->add_column_dictionary(*target_dst, col_name, key_type);
                    }
                    else {
                        REALM_ASSERT(!col_key.is_collection());
                        col_key_dst = table_dst->add_column(*target_dst, col_name);
                    }
                }
                else if (col_key.is_list()) {
                    col_key_dst = table_dst->add_column_list(col_type, col_name, nullable);
                }
                else if (col_key.is_set()) {
                    col_key_dst = table_dst->add_column_set(col_type, col_name, nullable);
                }
                else if (col_key.is_dictionary()) {
                    DataType key_type = table_src->get_dictionary_key_type(col_key);
                    col_key_dst = table_dst->add_column_dictionary(col_type, col_name, nullable, key_type);
                }
                else {
                    REALM_ASSERT(!col_key.is_collection());
                    col_key_dst = table_dst->add_column(col_type, col_name, nullable);
                }

                if (search_index_type != IndexType::None)
                    table_dst->add_search_index(col_key_dst, search_index_type);
            }
            else {
                // column preexists in dest, make sure the types match
                if (col_key.get_type() != col_key_dst.get_type()) {
                    throw ClientResetFailed(util::format(
                        "Incompatible column type change detected during client reset for '%1.%2' (%3 vs %4)",
                        table_name, col_name, col_key.get_type(), col_key_dst.get_type()));
                }
                ColumnAttrMask src_col_attrs = col_key.get_attrs();
                ColumnAttrMask dst_col_attrs = col_key_dst.get_attrs();
                src_col_attrs.reset(ColumnAttr::col_attr_Indexed);
                dst_col_attrs.reset(ColumnAttr::col_attr_Indexed);
                // make sure the attributes such as collection type, nullability etc. match
                // but index equality doesn't matter here.
                if (src_col_attrs != dst_col_attrs) {
                    throw ClientResetFailed(util::format(
                        "Incompatable column attribute change detected during client reset for '%1.%2' (%3 vs %4)",
                        table_name, col_name, col_key.value, col_key_dst.value));
                }
            }
        }
    }

    // Now the schemas are identical.

    // Remove objects in dst that are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        auto table_src = group_src.get_table(table_key);
        // There are no primary keys in embedded tables but this is ok, because
        // embedded objects are tied to the lifetime of top level objects.
        if (table_src->is_embedded())
            continue;
        StringData table_name = table_src->get_name();
        logger.debug(util::LogCategory::reset, "Removing objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);

        auto pk_col = table_dst->get_primary_key_column();
        REALM_ASSERT_DEBUG(pk_col); // sync realms always have a pk
        std::vector<std::pair<Mixed, ObjKey>> objects_to_remove;
        for (auto obj : *table_dst) {
            auto pk = obj.get_any(pk_col);
            if (!table_src->find_primary_key(pk)) {
                objects_to_remove.emplace_back(pk, obj.get_key());
            }
        }
        for (auto& pair : objects_to_remove) {
            logger.debug(util::LogCategory::reset, "  removing '%1'", pair.first);
            table_dst->remove_object(pair.second);
        }
    }

    // We must re-create any missing objects that are absent in dst before trying to copy
    // their properties because creating them may re-create any dangling links which would
    // otherwise cause inconsistencies when re-creating lists of links.
    for (auto table_key : group_src.get_table_keys()) {
        ConstTableRef table_src = group_src.get_table(table_key);
        auto table_name = table_src->get_name();
        if (should_skip_table(group_src, table_key) || table_src->is_embedded())
            continue;
        TableRef table_dst = group_dst.get_table(table_name);
        auto pk_col = table_src->get_primary_key_column();
        REALM_ASSERT(pk_col);
        logger.debug(util::LogCategory::reset,
                     "Creating missing objects for table '%1', number of rows = %2, "
                     "primary_key_col = %3, primary_key_type = %4",
                     table_name, table_src->size(), pk_col.get_index().val, pk_col.get_type());
        for (const Obj& src : *table_src) {
            bool created = false;
            table_dst->create_object_with_primary_key(src.get_primary_key(), &created);
            if (created) {
                logger.debug(util::LogCategory::reset, "   created %1", src.get_primary_key());
            }
        }
    }

    converters::EmbeddedObjectConverter embedded_tracker;
    // Now src and dst have identical schemas and all the top level objects are created.
    // What is left to do is to diff all properties of the existing objects.
    // Embedded objects are created on the fly.
    for (auto table_key : group_src.get_table_keys()) {
        if (should_skip_table(group_src, table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        // Embedded objects don't have a primary key, so they are handled
        // as a special case when they are encountered as a link value.
        if (table_src->is_embedded())
            continue;
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT_EX(allow_schema_additions || table_src->get_column_count() == table_dst->get_column_count(),
                        allow_schema_additions, table_src->get_column_count(), table_dst->get_column_count());
        auto pk_col = table_src->get_primary_key_column();
        REALM_ASSERT(pk_col);
        logger.debug(util::LogCategory::reset,
                     "Updating values for table '%1', number of rows = %2, "
                     "number of columns = %3, primary_key_col = %4, "
                     "primary_key_type = %5",
                     table_name, table_src->size(), table_src->get_column_count(), pk_col.get_index().val,
                     pk_col.get_type());

        converters::InterRealmObjectConverter converter(table_src, table_dst, &embedded_tracker);

        for (const Obj& src : *table_src) {
            auto src_pk = src.get_primary_key();
            // create the object - it should have been created above.
            auto dst = table_dst->get_object_with_primary_key(src_pk);
            REALM_ASSERT(dst);

            bool updated = false;
            converter.copy(src, dst, &updated);
            if (updated) {
                logger.debug(util::LogCategory::reset, "  updating %1", src_pk);
            }
        }
        embedded_tracker.process_pending();
    }
}

ClientResyncMode reset_precheck_guard(const TransactionRef& wt_local, ClientResyncMode mode,
                                      PendingReset::Action action, const std::optional<Status>& error,
                                      util::Logger& logger)
{
    if (auto previous_reset = sync::PendingResetStore::has_pending_reset(wt_local)) {
        logger.info(util::LogCategory::reset, "Found a previous %1", *previous_reset);
        if (action != previous_reset->action) {
            // IF a different client reset is being performed, cler the pending client reset and start over.
            logger.info(util::LogCategory::reset,
                        "New '%1' client reset of type: '%2' is incompatible - clearing previous reset", action,
                        mode);
            sync::PendingResetStore::clear_pending_reset(wt_local);
        }
        else {
            switch (previous_reset->mode) {
                case ClientResyncMode::Manual:
                    REALM_UNREACHABLE();
                case ClientResyncMode::DiscardLocal:
                    throw ClientResetFailed(util::format("A previous '%1' mode reset from %2 did not succeed, "
                                                         "giving up on '%3' mode to prevent a cycle",
                                                         previous_reset->mode, previous_reset->time, mode));
                case ClientResyncMode::Recover:
                    switch (mode) {
                        case ClientResyncMode::Recover:
                            throw ClientResetFailed(
                                util::format("A previous '%1' mode reset from %2 did not succeed, "
                                             "giving up on '%3' mode to prevent a cycle",
                                             previous_reset->mode, previous_reset->time, mode));
                        case ClientResyncMode::RecoverOrDiscard:
                            mode = ClientResyncMode::DiscardLocal;
                            logger.info(
                                util::LogCategory::reset,
                                "A previous '%1' mode reset from %2 downgrades this mode ('%3') to DiscardLocal",
                                previous_reset->mode, previous_reset->time, mode);
                            sync::PendingResetStore::clear_pending_reset(wt_local);
                            break;
                        case ClientResyncMode::DiscardLocal:
                            sync::PendingResetStore::clear_pending_reset(wt_local);
                            // previous mode Recover and this mode is Discard, this is not a cycle yet
                            break;
                        case ClientResyncMode::Manual:
                            REALM_UNREACHABLE();
                    }
                    break;
                case ClientResyncMode::RecoverOrDiscard:
                    throw ClientResetFailed(util::format("Unexpected previous '%1' mode reset from %2 did not "
                                                         "succeed, giving up on '%3' mode to prevent a cycle",
                                                         previous_reset->mode, previous_reset->time, mode));
            }
        }
    }
    if (action == PendingReset::Action::ClientResetNoRecovery) {
        if (mode == ClientResyncMode::Recover) {
            throw ClientResetFailed(
                "Client reset mode is set to 'Recover' but the server does not allow recovery for this client");
        }
        else if (mode == ClientResyncMode::RecoverOrDiscard) {
            logger.info(util::LogCategory::reset,
                        "Client reset in 'RecoverOrDiscard' is choosing 'DiscardLocal' because the server does not "
                        "permit recovery for this client");
            mode = ClientResyncMode::DiscardLocal;
        }
    }
    sync::PendingResetStore::track_reset(wt_local, mode, action, error);
    // Ensure we save the tracker object even if we encounter an error and roll
    // back the client reset later
    wt_local->commit_and_continue_writing();
    return mode;
}

bool perform_client_reset_diff(DB& db_local, sync::ClientReset& reset_config, sync::SaltedFileIdent client_file_ident,
                               util::Logger& logger, sync::SubscriptionStore* sub_store,
                               util::FunctionRef<void(int64_t)> on_flx_version_complete)
{
    DB& db_remote = *reset_config.fresh_copy;
    auto wt_local = db_local.start_write();
    auto actual_mode =
        reset_precheck_guard(wt_local, reset_config.mode, reset_config.action, reset_config.error, logger);
    bool recover_local_changes =
        actual_mode == ClientResyncMode::Recover || actual_mode == ClientResyncMode::RecoverOrDiscard;

    logger.info(util::LogCategory::reset,
                "Client reset: path_local = %1, client_file_ident = (ident: %2, salt: %3), "
                "remote_path = %4, requested_mode = %5, action = %6, actual_mode = %7, will_recover = %8, "
                "originating_error = %9",
                db_local.get_path(), client_file_ident.ident, client_file_ident.salt, db_remote.get_path(),
                reset_config.mode, reset_config.action, actual_mode, recover_local_changes, reset_config.error);

    auto& repl_local = dynamic_cast<ClientReplication&>(*db_local.get_replication());
    auto& history_local = repl_local.get_history();
    history_local.ensure_updated(wt_local->get_version());
    VersionID old_version_local = wt_local->get_version_of_current_transaction();

    auto& repl_remote = dynamic_cast<ClientReplication&>(*db_remote.get_replication());
    auto& history_remote = repl_remote.get_history();

    sync::SaltedVersion fresh_server_version = {0, 0};
    {
        SyncProgress remote_progress;
        sync::version_type remote_version_unused;
        SaltedFileIdent remote_ident_unused;
        history_remote.get_status(remote_version_unused, remote_ident_unused, remote_progress);
        fresh_server_version = remote_progress.latest_server_version;
    }

    TransactionRef tr_remote;
    std::vector<client_reset::RecoveredChange> recovered;
    if (recover_local_changes) {
        auto frozen_pre_local_state = db_local.start_frozen();
        auto local_changes = history_local.get_local_changes(wt_local->get_version());
        logger.info("Local changesets to recover: %1", local_changes.size());

        tr_remote = db_remote.start_write();
        recovered = process_recovered_changesets(*tr_remote, *frozen_pre_local_state, logger, local_changes);
    }
    else {
        tr_remote = db_remote.start_read();
    }

    // transform the local Realm such that all public tables become identical to the remote Realm
    transfer_group(*tr_remote, *wt_local, logger, false);

    // now that the state of the fresh and local Realms are identical,
    // reset the local sync history and steal the fresh Realm's ident
    history_local.set_history_adjustments(logger, wt_local->get_version(), client_file_ident, fresh_server_version,
                                          recovered);

    int64_t subscription_version = 0;
    if (sub_store) {
        if (recover_local_changes) {
            subscription_version = sub_store->mark_active_as_complete(*wt_local);
        }
        else {
            subscription_version = sub_store->set_active_as_latest(*wt_local);
        }
    }

    wt_local->commit_and_continue_as_read();
    on_flx_version_complete(subscription_version);

    VersionID new_version_local = wt_local->get_version_of_current_transaction();
    logger.info(util::LogCategory::reset,
                "perform_client_reset_diff is done: old_version = (version: %1, index: %2), "
                "new_version = (version: %3, index: %4)",
                old_version_local.version, old_version_local.index, new_version_local.version,
                new_version_local.index);

    return recover_local_changes;
}

} // namespace _impl::client_reset
} // namespace realm
