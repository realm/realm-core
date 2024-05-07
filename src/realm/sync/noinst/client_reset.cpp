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

} // namespace realm

namespace realm::_impl::client_reset {

std::ostream& operator<<(std::ostream& os, const PendingReset& pr)
{
    if (pr.action == ProtocolErrorInfo::Action::NoAction || pr.time.is_null()) {
        os << "empty pending client reset";
    }
    else if (pr.action != ProtocolErrorInfo::Action::ClientReset) {
        os << "pending '" << pr.action << "' client reset of type: '" << pr.mode << "' at: " << pr.time;
    }
    else {
        os << "pending client reset of type: '" << pr.mode << "' at: " << pr.time;
    }
    if (pr.error) {
        os << " for error: " << *pr.error;
    }
    return os;
}

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

// A table without a "class_" prefix will not generate sync instructions.
constexpr static std::string_view s_meta_reset_table_name("client_reset_metadata");
constexpr static std::string_view s_pk_col_name("id");
constexpr static std::string_view s_version_column_name("version");
constexpr static std::string_view s_timestamp_col_name("event_time");
constexpr static std::string_view s_reset_mode_col_name("type_of_reset");
constexpr static std::string_view s_reset_recovery_col_name("reset_recovery_allowed");
constexpr static std::string_view s_reset_action_col_name("reset_action");
constexpr static std::string_view s_reset_error_code_col_name("reset_error_code");
constexpr static std::string_view s_reset_error_msg_col_name("reset_error_msg");
constexpr int64_t s_metadata_version = 2;

void remove_pending_client_resets(Transaction& wt)
{
    if (auto table = wt.get_table(s_meta_reset_table_name); table && !table->is_empty()) {
        table->clear();
    }
}

int64_t from_reset_action(PendingReset::Action action)
{
    switch (action) {
        case PendingReset::Action::ClientReset:
            return 1;
        case PendingReset::Action::ClientResetNoRecovery:
            return 2;
        case PendingReset::Action::MigrateToFLX:
            return 3;
        case PendingReset::Action::RevertToPBS:
            return 4;
        default:
            throw ClientResetFailed(util::format("Unsupported client reset action: %1 for pending reset", action));
    }
}

PendingReset::Action to_reset_action(int64_t action)
{
    switch (action) {
        case 1:
            return PendingReset::Action::ClientReset;
        case 2:
            return PendingReset::Action::ClientResetNoRecovery;
        case 3:
            return PendingReset::Action::MigrateToFLX;
        case 4:
            return PendingReset::Action::RevertToPBS;
        default:
            return PendingReset::Action::NoAction;
    }
}

ClientResyncMode to_resync_mode(int64_t mode)
{
    // RecoverOrDiscard is treated as Recover and is not stored
    switch (mode) {
        case 0: // DiscardLocal
            return ClientResyncMode::DiscardLocal;
        case 1: // Recover
            return ClientResyncMode::Recover;
        default:
            throw ClientResetFailed(util::format("Unsupported client reset resync mode: %1 for pending reset", mode));
    }
}

int64_t from_resync_mode(ClientResyncMode mode)
{
    switch (mode) {
        case ClientResyncMode::DiscardLocal:
            return 0; // DiscardLocal
        case ClientResyncMode::RecoverOrDiscard:
            [[fallthrough]]; // RecoverOrDiscard is treated as Recover
        case ClientResyncMode::Recover:
            return 1; // Recover
        default:
            throw ClientResetFailed(util::format("Unsupported client reset resync mode: %1 for pending reset", mode));
    }
}

util::Optional<PendingReset> has_pending_reset(const Transaction& rt)
{
    ConstTableRef table = rt.get_table(s_meta_reset_table_name);
    if (!table || table->size() == 0) {
        return util::none;
    }
    if (table->size() > 1) {
        // this may happen if a future version of this code changes the format and expectations around reset metadata.
        throw ClientResetFailed(
            util::format("Previous client resets detected (%1) but only one is expected.", table->size()));
    }
    ColKey version_col = table->get_column_key(s_version_column_name);
    ColKey timestamp_col = table->get_column_key(s_timestamp_col_name);
    ColKey mode_col = table->get_column_key(s_reset_mode_col_name);
    Obj first = *table->begin();
    REALM_ASSERT(first);
    PendingReset pending;
    REALM_ASSERT(version_col);
    auto version = static_cast<int>(first.get<int64_t>(version_col));
    // Version 1 columns
    REALM_ASSERT(timestamp_col);
    REALM_ASSERT(mode_col);
    pending.time = first.get<Timestamp>(timestamp_col);
    pending.mode = to_resync_mode(first.get<int64_t>(mode_col));
    if (version == 0 || version > s_metadata_version) {
        throw ClientResetFailed(util::format("Unsupported client reset metadata version: %1 vs %2, from %3", version,
                                             s_metadata_version, pending.time));
    }
    // Version 2 columns
    if (version >= 2) {
        ColKey recovery_col = table->get_column_key(s_reset_recovery_col_name);
        ColKey action_col = table->get_column_key(s_reset_action_col_name);
        ColKey code_col = table->get_column_key(s_reset_error_code_col_name);
        ColKey msg_col = table->get_column_key(s_reset_error_msg_col_name);
        REALM_ASSERT(recovery_col);
        REALM_ASSERT(action_col);
        REALM_ASSERT(code_col);
        REALM_ASSERT(msg_col);
        pending.action = to_reset_action(first.get<int64_t>(action_col));
        auto code = first.get<int64_t>(code_col);
        if (code > 0) {
            pending.error = Status(static_cast<ErrorCodes::Error>(code), first.get<StringData>(msg_col));
        }
    }
    else {
        // Provide default action and recovery values if table version is 1
        pending.action = PendingReset::Action::ClientReset;
    }
    return pending;
}

static TableRef get_or_create_client_reset_table(Transaction& wt)
{
    TableRef table = wt.get_table(s_meta_reset_table_name);
    if (!table) {
        // Creating new table
        table = wt.add_table_with_primary_key(s_meta_reset_table_name, type_ObjectId, s_pk_col_name);
        REALM_ASSERT(table);
        table->add_column(type_Int, s_version_column_name);
        table->add_column(type_Timestamp, s_timestamp_col_name);
        table->add_column(type_Int, s_reset_mode_col_name);
        table->add_column(type_Bool, s_reset_recovery_col_name);
        table->add_column(type_Int, s_reset_action_col_name);
        table->add_column(type_Int, s_reset_error_code_col_name);
        table->add_column(type_String, s_reset_error_msg_col_name);
    }
    return table;
}

void track_reset(Transaction& wt, ClientResyncMode mode, PendingReset::Action action,
                 const std::optional<Status>& error)
{
    REALM_ASSERT(mode != ClientResyncMode::Manual);
    TableRef table = get_or_create_client_reset_table(wt);
    REALM_ASSERT(table);
    // Even if the table is being updated to V2, an existing entry will still throw an exception
    size_t table_size = table->size();
    if (table_size > 0) {
        // this may happen if a future version of this code changes the format and expectations around reset metadata.
        throw ClientResetFailed(
            util::format("Previous client resets detected (%1) but only one is expected.", table->size()));
    }
    ColKey version_col = table->get_column_key(s_version_column_name);
    ColKey timestamp_col = table->get_column_key(s_timestamp_col_name);
    ColKey mode_col = table->get_column_key(s_reset_mode_col_name);
    ColKey action_col = table->get_column_key(s_reset_action_col_name);
    ColKey code_col = table->get_column_key(s_reset_error_code_col_name);
    ColKey msg_col = table->get_column_key(s_reset_error_msg_col_name);
    // If the table is missing any columns, remove it and start over
    if (!(timestamp_col && mode_col && action_col && code_col && msg_col && version_col)) {
        wt.remove_table(s_meta_reset_table_name);
        track_reset(wt, mode, action, error); // try again
        return;
    }
    auto obj = table->create_object_with_primary_key(ObjectId::gen(),
                                                     {
                                                         {version_col, s_metadata_version},
                                                         {timestamp_col, Timestamp(std::chrono::system_clock::now())},
                                                         {mode_col, from_resync_mode(mode)},
                                                         {action_col, from_reset_action(action)},
                                                     });
    if (error) {
        obj.set(code_col, static_cast<int64_t>(error->code()));
        obj.set(msg_col, error->reason());
    }
    // Ensure we save the tracker object even if we encounter an error and roll
    // back the client reset later
    wt.commit_and_continue_writing();
}

ClientResyncMode reset_precheck_guard(Transaction& wt, ClientResyncMode mode, PendingReset::Action action,
                                      const std::optional<Status>& error, util::Logger& logger)
{
    if (auto previous_reset = has_pending_reset(wt)) {
        logger.info(util::LogCategory::reset, "Found a previous %1", previous_reset.value());
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
                        throw ClientResetFailed(util::format("A previous '%1' mode reset from %2 did not succeed, "
                                                             "giving up on '%3' mode to prevent a cycle",
                                                             previous_reset->mode, previous_reset->time, mode));
                    case ClientResyncMode::RecoverOrDiscard:
                        mode = ClientResyncMode::DiscardLocal;
                        logger.info(util::LogCategory::reset,
                                    "A previous '%1' mode reset from %2 downgrades this mode ('%3') to DiscardLocal",
                                    previous_reset->mode, previous_reset->time, mode);
                        remove_pending_client_resets(wt);
                        break;
                    case ClientResyncMode::DiscardLocal:
                        remove_pending_client_resets(wt);
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
    track_reset(wt, mode, action, error);
    return mode;
}

bool perform_client_reset_diff(DB& db_local, sync::ClientReset& reset_config, sync::SaltedFileIdent client_file_ident,
                               util::Logger& logger, sync::SubscriptionStore* sub_store,
                               util::FunctionRef<void(int64_t)> on_flx_version_complete)
{
    DB& db_remote = *reset_config.fresh_copy;
    auto wt_local = db_local.start_write();
    auto actual_mode =
        reset_precheck_guard(*wt_local, reset_config.mode, reset_config.action, reset_config.error, logger);
    bool recover_local_changes =
        actual_mode == ClientResyncMode::Recover || actual_mode == ClientResyncMode::RecoverOrDiscard;

    logger.info(util::LogCategory::reset,
                "Client reset: path_local = %1, client_file_ident = (ident: %2, salt: %3), "
                "remote_path = %4, requested_mode = %5, action = %6, actual_mode = %7, will_recover = %8, "
                "originating_error = %9",
                db_local.get_path(), client_file_ident.ident, client_file_ident.salt, db_remote.get_path(),
                reset_config.mode, reset_config.action, actual_mode, recover_local_changes, *reset_config.error);

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

} // namespace realm::_impl::client_reset
