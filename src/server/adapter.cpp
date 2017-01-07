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
    ObjectSchema *selected_object_schema;

    // No selection needed:
    bool select_table(size_t group_index, size_t, const size_t*)
    {
        selected_table = group_index;
        selected_object_type = ObjectStore::object_type_for_table_name(m_group.get_table_name(selected_table));
        selected_object_schema = &*m_schema.find(selected_object_type);
        return true;
    }
    bool select_descriptor(size_t, const size_t*)
    {
        return true;
    }
    bool select_link_list(size_t, size_t, size_t)
    {
        return true;
    }
    bool insert_group_level_table(size_t, size_t, StringData)
    {
        return true;
    }
    bool erase_group_level_table(size_t, size_t)
    {
        return true;
    }
    bool rename_group_level_table(size_t, StringData)
    {
        return true;
    }
    bool move_group_level_table(size_t, size_t)
    {
        return true;
    }

    // Must have table selected:
    bool insert_empty_rows(size_t row_index, size_t n_rows, size_t, bool)
    {
        REALM_ASSERT(n_rows == 1);
        parsed_instructions.emplace_back(Adapter::Instruction{
            Adapter::Instruction::Type::Insertion,
            selected_object_type,
            row_index
        });
        return true;
    }
    bool erase_rows(size_t, size_t, size_t, bool)
    {
        return true;
    }
    bool swap_rows(size_t, size_t)
    {
        return true;
    }
    bool merge_rows(size_t, size_t)
    {
        return true;
    }
    bool clear_table()
    {
        return true;
    }
    bool set_int(size_t column_index, size_t row_index, int_fast64_t value, _impl::Instruction, size_t)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            (int64_t)value
        ));
        return true;
    }
    bool add_int(size_t, size_t, int_fast64_t)
    {
        return true;
    }
    bool set_bool(size_t column_index, size_t row_index, bool value, _impl::Instruction)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            value
        ));
        return true;
    }
    bool set_float(size_t column_index, size_t row_index, float value, _impl::Instruction)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            value
        ));
        return true;
    }
    bool set_double(size_t column_index, size_t row_index, double value, _impl::Instruction)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            value
        ));
        return true;
    }
    bool set_string(size_t column_index, size_t row_index, StringData value, _impl::Instruction, size_t)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            value
        ));
        return true;
    }
    bool set_binary(size_t column_index, size_t row_index, BinaryData value, _impl::Instruction)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            value
        ));
        return true;
    }
    bool set_olddatetime(size_t, size_t, OldDateTime, _impl::Instruction)
    {
        return true;
    }
    bool set_timestamp(size_t column_index, size_t row_index, Timestamp value, _impl::Instruction)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            false,
            value
        ));
        return true;
    }
    bool set_table(size_t, size_t, _impl::Instruction)
    {
        return true;
    }
    bool set_mixed(size_t, size_t, const Mixed&, _impl::Instruction)
    {
        return true;
    }
    bool set_link(size_t, size_t, size_t, size_t, _impl::Instruction)
    {
        return true;
    }
    bool set_null(size_t column_index, size_t row_index, _impl::Instruction, size_t)
    {
        parsed_instructions.emplace_back(Adapter::Instruction(
            selected_object_type,
            row_index,
            selected_object_schema->persisted_properties[column_index].name,
            true,
            (int64_t)0
        ));
        return true;
    }
    bool nullify_link(size_t, size_t, size_t)
    {
        return true;
    }
    bool insert_substring(size_t, size_t, size_t, StringData)
    {
        return true;
    }
    bool erase_substring(size_t, size_t, size_t, size_t)
    {
        return true;
    }
    bool optimize_table()
    {
        return true;
    }

    // Must have descriptor selected:
    bool insert_link_column(size_t, DataType, StringData, size_t, size_t)
    {
        return true;
    }
    bool insert_column(size_t, DataType, StringData, bool)
    {
        return true;
    }
    bool erase_link_column(size_t, size_t, size_t)
    {
        return true;
    }
    bool erase_column(size_t)
    {
        return true;
    }
    bool rename_column(size_t, StringData)
    {
        return true;
    }
    bool move_column(size_t, size_t)
    {
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
        return true;
    }

    // Must have linklist selected:
    bool link_list_set(size_t, size_t, size_t)
    {
        return true;
    }
    bool link_list_insert(size_t, size_t, size_t)
    {
        return true;
    }
    bool link_list_move(size_t, size_t)
    {
        return true;
    }
    bool link_list_swap(size_t, size_t)
    {
        return true;
    }
    bool link_list_erase(size_t, size_t)
    {
        return true;
    }
    bool link_list_nullify(size_t, size_t)
    {
        return true;
    }
    bool link_list_clear(size_t)
    {
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
    std::cout << "AVAILABLE" << std::endl;
    std::vector<bool> watch;
    for (auto realm : realms) {
        std::cout << realm.second << std::endl;
        watch.push_back(true);
        m_realm_changed(realm);
    }
    return watch;
}

void Adapter::Callback::realm_changed(GlobalNotifier::ChangeNotification changes) {
    std::cout << "CHANGED " << changes.get_path() << std::endl;
    m_realm_changed(changes.realm_info);
}

std::vector<Adapter::Instruction> Adapter::current(std::string realm_path) {
    auto realm = Realm::make_shared_realm(m_global_notifier->get_config(realm_path));
    auto sync_history = static_cast<sync::SyncHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();
    auto version = progress.version;


    util::AppendBuffer<char> buffer;
    version = sync_history->fetch_next_cooked_changeset(version, buffer);

    std::cout << "Processing " << realm_path << " " << version << std::endl;

    InstructionHander handler(realm->read_group());
    _impl::SimpleInputStream stream(buffer.data(), buffer.size());
    _impl::TransactLogParser parser;
    parser.parse(stream, handler);

    realm->invalidate();

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
