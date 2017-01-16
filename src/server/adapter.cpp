////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include "adapter.hpp"
#include "object_store.hpp"
#include "object_schema.hpp"

#include <realm/sync/changeset_cooker.hpp>
#include <realm/impl/transact_log.hpp>
#include <realm/impl/input_stream.hpp>

using namespace realm;

class InstructionHander {
public:
    InstructionHander(Group &group) 
    : m_group(group)
    , m_schema(ObjectStore::schema_from_group(group)) {}

    Group &m_group;
    Schema m_schema;
    std::vector<Adapter::Instruction> parsed_instructions;
    size_t selected_table;
    std::string selected_object_type;
    ObjectSchema *selected_object_schema = nullptr;

    size_t selected_list_column;
    size_t selected_list_row;

    size_t selected_descriptor;
    std::string selected_descriptor_object_type;
    bool schema_changed = false;

    // No selection needed:
    bool select_table(size_t group_index, size_t, const size_t*)
    {
        selected_table = group_index;
        selected_object_type = ObjectStore::object_type_for_table_name(m_group.get_table_name(group_index));
        selected_object_schema = selected_object_type.size() ? &*m_schema.find(selected_object_type) : nullptr;
        return true;
    }
    bool select_descriptor(size_t, const size_t*)
    {
        // FIXME - caller to this is broken - for now just use last selected table
        selected_descriptor = selected_table;
        selected_descriptor_object_type = selected_object_type;
        return true;
    }
    bool select_link_list(size_t column_index, size_t row_index, size_t group_index)
    {
        selected_list_column = column_index;
        selected_list_row = row_index;
        return true;
    }
    bool insert_group_level_table(size_t group_index, size_t, StringData)
    {
        auto object_type = ObjectStore::object_type_for_table_name(m_group.get_table_name(group_index));
        if (object_type.size()) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::AddType,
                object_type
            });
            schema_changed = true;
        }
        return true;
    }
    bool erase_group_level_table(size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool rename_group_level_table(size_t, StringData)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool move_group_level_table(size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }

    // Must have table selected:
    bool insert_empty_rows(size_t row_index, size_t n_rows, size_t, bool)
    {
        REALM_ASSERT(n_rows == 1);
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::Insert,
                selected_object_type,
                row_index
            });
        }
        return true;
    }
    bool erase_rows(size_t row_index, size_t n_rows, size_t prior_num_rows, bool move_last_over)
    {
        REALM_ASSERT(n_rows == 1);
        REALM_ASSERT(move_last_over);
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::Delete,
                selected_object_type,
                row_index
            });
            parsed_instructions.emplace_back(Adapter::Instruction{
                selected_object_type,
                prior_num_rows-1,
                "ID",
                false,
                (int64_t)row_index
            });
        }
        return true;
    }
    bool swap_rows(size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool merge_rows(size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool clear_table()
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::Clear,
                selected_object_type
            });
        }
        return true;
    }
    bool set_int(size_t column_index, size_t row_index, int_fast64_t value, _impl::Instruction, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                (int64_t)value
            ));
        }
        return true;
    }
    bool add_int(size_t, size_t, int_fast64_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_bool(size_t column_index, size_t row_index, bool value, _impl::Instruction)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                value
            ));
        }
        return true;
    }
    bool set_float(size_t column_index, size_t row_index, float value, _impl::Instruction)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                value
            ));
        }
        return true;
    }
    bool set_double(size_t column_index, size_t row_index, double value, _impl::Instruction)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                value
            ));
        }
        return true;
    }
    bool set_string(size_t column_index, size_t row_index, StringData value, _impl::Instruction, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                value
            ));
        }
        return true;
    }
    bool set_binary(size_t column_index, size_t row_index, BinaryData value, _impl::Instruction)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                value
            ));
        }
        return true;
    }
    bool set_olddatetime(size_t, size_t, OldDateTime, _impl::Instruction)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_timestamp(size_t column_index, size_t row_index, Timestamp value, _impl::Instruction)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                false,
                value
            ));
        }
        return true;
    }
    bool set_table(size_t, size_t, _impl::Instruction)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_mixed(size_t, size_t, const Mixed&, _impl::Instruction)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_link(size_t column_index, size_t row_index, size_t link_index, size_t, _impl::Instruction)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                link_index == npos,
                (int64_t)link_index
            ));
        }
        return true;
    }
    bool set_null(size_t column_index, size_t row_index, _impl::Instruction, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                true,
                (int64_t)0
            ));
        }
        return true;
    }
    bool nullify_link(size_t column_index, size_t row_index, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction(
                selected_object_type,
                row_index,
                selected_object_schema->persisted_properties[column_index].name,
                true,
                (int64_t)0
            ));
        }
        return true;
    }
    bool insert_substring(size_t, size_t, size_t, StringData)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool erase_substring(size_t, size_t, size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool optimize_table()
    {
        return true;
    }

    // Must have descriptor selected:
    bool insert_link_column(size_t, DataType data_type, StringData prop_name, size_t target_table_idx, size_t)
    {
        if (selected_descriptor_object_type.size()) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                selected_descriptor_object_type,
                prop_name,
                (PropertyType)data_type,
                true,
                ObjectStore::object_type_for_table_name(m_group.get_table_name(target_table_idx))
            });
            schema_changed = true;
        }        
        return true;
    }
    bool insert_column(size_t, DataType data_type, StringData prop_name, bool nullable)
    {
        if (selected_descriptor_object_type.size()) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                selected_descriptor_object_type,
                prop_name,
                (PropertyType)data_type,
                nullable
            });
            schema_changed = true;
        }  
        return true;
    }
    bool erase_link_column(size_t, size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool erase_column(size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool rename_column(size_t, StringData)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool move_column(size_t, size_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool add_search_index(size_t)
    {
        return true;
    }
    bool remove_search_index(size_t)
    {
        return true;
    }
    bool set_link_type(size_t, LinkType)
    {
        REALM_ASSERT(0);
        return true;
    }

    // Must have linklist selected:
    bool link_list_set(size_t list_index, size_t value, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListSet,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name,
                value,
                list_index
            });
        }
        return true;
    }
    bool link_list_insert(size_t list_index, size_t value, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListInsert,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name,
                value,
                list_index
            });
        }
        return true;
    }
    bool link_list_move(size_t from_index, size_t to_index)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListMove,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name,
                to_index,
                from_index
            });
        }
        return true;
    }
    bool link_list_swap(size_t from_index, size_t to_index)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListSwap,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name,
                to_index,
                from_index
            });
        }
        return true;
    }
    bool link_list_erase(size_t list_index, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListNullify,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name,
                0,
                list_index
            });
        }
        return true;
    }
    bool link_list_nullify(size_t list_index, size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListNullify,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name,
                0,
                list_index
            });
        }
        return true;
    }
    bool link_list_clear(size_t)
    {
        if (selected_object_schema) {
            parsed_instructions.emplace_back(Adapter::Instruction{
                Adapter::Instruction::Type::ListClear,
                selected_object_type,
                selected_list_row,
                selected_object_schema->persisted_properties[selected_list_column].name
            });
        }
        return true;
    }

    void parse_complete()
    {
    }
};

Adapter::Adapter(std::function<void(std::string)> realm_changed,
                 std::string local_root_dir, std::string server_base_url,
                 std::shared_ptr<SyncUser> user)
: m_global_notifier(GlobalNotifier::shared_notifier(
    std::make_unique<Adapter::Callback>([=](auto info) { realm_changed(info.second); }),
                                        local_root_dir, server_base_url, user, 
                                        std::make_shared<sync::TrivialChangesetCooker>()))
{
    m_global_notifier->start();
}

std::vector<bool> Adapter::Callback::available(std::vector<GlobalNotifier::RealmInfo> realms,
                                               std::vector<bool> new_realms,
                                               bool all) {
    std::vector<bool> watch;
    for (auto realm : realms) {
        watch.push_back(true);
        m_realm_changed(realm);
    }
    return watch;
}

void Adapter::Callback::realm_changed(GlobalNotifier::ChangeNotification changes) {
    m_realm_changed(changes.realm_info);
}

Adapter::ChangeSet Adapter::current(std::string realm_path) {
    auto realm = Realm::make_shared_realm(m_global_notifier->get_config(realm_path));
    auto sync_history = static_cast<sync::SyncHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();
    auto version = progress.version;

    util::AppendBuffer<char> buffer;
    version = sync_history->fetch_next_cooked_changeset(version, buffer);
    if (version == 0) {
        return util::none;
    }

    InstructionHander handler(realm->read_group());
    _impl::SimpleInputStream stream(buffer.data(), buffer.size());
    _impl::TransactLogParser parser;
    parser.parse(stream, handler);

    return std::move(handler.parsed_instructions);
}

void Adapter::advance(std::string realm_path) {
    auto realm = Realm::make_shared_realm(m_global_notifier->get_config(realm_path));
    auto sync_history = static_cast<sync::SyncHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();

    util::AppendBuffer<char> buffer;
    auto version = sync_history->fetch_next_cooked_changeset(progress.version, buffer);

    if (version) {
        sync_history->set_cooked_progress({version, 0});
    }
    
    realm->invalidate();
}
