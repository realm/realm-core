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

class ChangesetCookerInstructionHander {
public:
    friend Adapter;

    ChangesetCookerInstructionHander(const Group &group)
    : m_group(group)
    , m_schema(ObjectStore::schema_from_group(m_group)) {
        for (size_t i = 0; i < m_group.size(); i++) {
            m_table_names[i] = m_group.get_table_name(i);
        }

        for (auto object_schema : m_schema) {
            for (auto &link_prop : object_schema.persisted_properties) {
                if (link_prop.type == PropertyType::Object || link_prop.type == PropertyType::Array) {
                    auto link_props = m_linking_properties.find(link_prop.object_type);
                    if (link_props == m_linking_properties.end()) {
                        LinkingProperties linking({{object_schema.name, link_prop}});
                        m_linking_properties.emplace(link_prop.object_type, std::move(linking));
                    }
                    else {
                        link_props->second.push_back({object_schema.name, link_prop});
                    }
                }
            }
        }
    }

    const Group &m_group;
    Schema m_schema;
    std::map<size_t, std::string> m_table_names;
    std::map<size_t, std::map<size_t, int64_t>> m_int_primaries;
    std::map<size_t, std::map<size_t, std::string>> m_string_primaries;
    std::map<size_t, std::map<size_t, size_t>> m_row_mapping;
    json json_instructions;

    using LinkingProperties = std::vector<std::pair<std::string, Property>>;
    std::map<std::string, LinkingProperties> m_linking_properties;

    size_t selected_table_index;
    ConstTableRef selected_table;
    ObjectSchema *selected_object_schema = nullptr;
    Property *selected_primary = nullptr;

    Property *list_property = nullptr;
    json list_parent_identity;

    ConstTableRef list_target_table;
    ObjectSchema *list_target_object_schema = nullptr;
    Property *list_target_primary = nullptr;

    json get_identity(size_t row, ConstTableRef &table, Property *primary_key) {
        if (primary_key) {
            if (primary_key->type == PropertyType::Int) {
                auto primaries = m_int_primaries.find(table->get_index_in_group());
                if (primaries != m_int_primaries.end()) {
                    auto primary = primaries->second.find(row);
                    if (primary != primaries->second.end()) {
                        return primary->second;
                    }
                }
                auto mappings = m_row_mapping.find(table->get_index_in_group());
                if (mappings != m_row_mapping.end()) {
                    auto mapping = mappings->second.find(row);
                    if (mapping != mappings->second.end()) {
                        return table->get_int(primary_key->table_column, mapping->second);
                    }
                }
                return table->get_int(primary_key->table_column, row); 
            }
            else if (primary_key->type == PropertyType::String) {
                auto primaries = m_string_primaries.find(table->get_index_in_group());
                if (primaries != m_string_primaries.end()) {
                    auto primary = primaries->second.find(row);
                    if (primary != primaries->second.end()) {
                        return primary->second;
                    }
                }
                auto mappings = m_row_mapping.find(table->get_index_in_group());
                if (mappings != m_row_mapping.end()) {
                    auto mapping = mappings->second.find(row);
                    if (mapping != mappings->second.end()) {
                        return table->get_string(primary_key->table_column, mapping->second);
                    }
                }
                return table->get_string(primary_key->table_column, row); 
            }
        }
        return row;
    }

    void select(std::string &name, ObjectSchema *&out_object_schema, ConstTableRef &out_table, Property *&out_primary) {
        out_object_schema = nullptr;
        out_primary = nullptr;
        out_table = ConstTableRef();

        std::string object_type = ObjectStore::object_type_for_table_name(name);
        if (object_type.size()) {
            auto object_schema = m_schema.find(object_type);
            if (object_schema != m_schema.end()) {
                out_object_schema = &*object_schema;
                out_table = ObjectStore::table_for_object_type(m_group, object_type);
                out_primary = out_object_schema->primary_key_property();
            }
        }
    }

    // No selection needed:
    bool select_table(size_t group_index, size_t levels, const size_t* path)
    {
        selected_table_index = group_index;
        select(m_table_names[group_index], selected_object_schema, selected_table, selected_primary);
        return true;
    }
    bool select_descriptor(size_t levels, const size_t* path)
    {
        return true;
    }
    bool select_link_list(size_t column_index, size_t row_index, size_t group_index)
    {
        REALM_ASSERT(selected_object_schema != nullptr);

        list_parent_identity = get_identity(row_index, selected_table, selected_primary);

        list_property = &selected_object_schema->persisted_properties[column_index];
        REALM_ASSERT(list_property->table_column == column_index);

        select(m_table_names[group_index], list_target_object_schema, list_target_table, list_target_primary);

        return true;
    }
    bool insert_group_level_table(size_t group_index, size_t num_tables, StringData name)
    {
        m_table_names[group_index] = name;
        std::string object_type = ObjectStore::object_type_for_table_name(m_table_names[group_index]);
        if (object_type.size()) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::AddType)},
                {"object_type", object_type}
            });
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
    bool insert_empty_rows(size_t row_index, size_t n_rows, size_t prior_num_rows, bool unordered)
    {
        REALM_ASSERT(n_rows == 1);
        if (selected_object_schema && !selected_primary) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::Insert)},
                {"object_type", selected_object_schema->name},
                {"identity", row_index}
            });
        }
        return true;
    }
    bool erase_rows(size_t row_index, size_t n_rows, size_t prior_num_rows, bool move_last_over)
    {
        REALM_ASSERT(n_rows == 1);
        REALM_ASSERT(move_last_over);
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::Delete)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)}
            });

            // change identity for objects with no primary key
            if (!selected_primary && row_index < prior_num_rows-1) {
                json_instructions.push_back({
                    {"type", Adapter::instruction_type_string(Adapter::InstructionType::ChangeIdentity)},
                    {"object_type", selected_object_schema->name},
                    {"identity", prior_num_rows-1},
                    {"new_identity", row_index}
                });
            }

            // update row mappings
            if (selected_primary) {
                if (selected_primary->type == PropertyType::Int) {
                    if (m_int_primaries[selected_table_index].count(prior_num_rows-1)) {
                        m_int_primaries[selected_table_index][row_index] = m_int_primaries[selected_table_index][prior_num_rows-1];
                        m_int_primaries[selected_table_index].erase(prior_num_rows-1);
                    }
                }
                else {
                    REALM_ASSERT(selected_primary->type == PropertyType::String);
                    if (m_string_primaries[selected_table_index].count(prior_num_rows-1)) {
                        m_string_primaries[selected_table_index][row_index] = m_string_primaries[selected_table_index][prior_num_rows-1];
                        m_string_primaries[selected_table_index].erase(prior_num_rows-1);
                    }
                } 
            }
            else {
                m_row_mapping[selected_table_index][row_index] = prior_num_rows-1;
            }
        }
        return true;
    }
    bool swap_rows(size_t, size_t)
    {
        if (selected_object_schema) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool merge_rows(size_t, size_t)
    {
        if (selected_object_schema) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool clear_table()
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::Clear)},
                {"object_type", selected_object_schema->name},
            });
        }
        return true;
    }
    bool set_int(size_t column_index, size_t row_index, int_fast64_t value, _impl::Instruction inst, size_t)
    {
        if (selected_object_schema) {
            if (selected_primary && selected_primary->table_column == column_index) {
                m_int_primaries[selected_table_index][row_index] = value;
                json_instructions.push_back({
                    {"type", Adapter::instruction_type_string(Adapter::InstructionType::Insert)},
                    {"object_type", selected_object_schema->name},
                    {"identity", value}
                });
            }
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"value", value}
            });
        }
        return true;
    }
    bool add_int(size_t, size_t, int_fast64_t)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_bool(size_t column_index, size_t row_index, bool value, _impl::Instruction inst)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"value", value}
            });
        }
        return true;
    }
    bool set_float(size_t column_index, size_t row_index, float value, _impl::Instruction inst)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"value", value}
            });
        }
        return true;
    }
    bool set_double(size_t column_index, size_t row_index, double value, _impl::Instruction inst)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"value", value}
            });
        }
        return true;
    }
    bool set_string(size_t column_index, size_t row_index, StringData value, _impl::Instruction inst, size_t)
    {
        if (selected_object_schema) {
            if (selected_primary && selected_primary->table_column == column_index) {
                m_string_primaries[selected_table_index][row_index] = value;
                json_instructions.push_back({
                    {"type", Adapter::instruction_type_string(Adapter::InstructionType::Insert)},
                    {"object_type", selected_object_schema->name},
                    {"identity", value}
                });
            }
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"value", value}
            });
        }
        return true;
    }
    bool set_binary(size_t column_index, size_t row_index, BinaryData value, _impl::Instruction inst)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"non_json_type", "data"},
                {"value", value}
            });
        }
        return true;
    }
    bool set_olddatetime(size_t, size_t, OldDateTime, _impl::Instruction inst)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_timestamp(size_t column_index, size_t row_index, Timestamp ts, _impl::Instruction inst)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"non_json_type", "date"},
                {"value", (int64_t)(ts.get_seconds() * 1000 + ts.get_nanoseconds() / 1000000)}
            });
        }
        return true;
    }
    bool set_table(size_t, size_t, _impl::Instruction)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_mixed(size_t, size_t, const Mixed&, _impl::Instruction inst)
    {
        REALM_ASSERT(0);
        return true;
    }
    bool set_null(size_t column_index, size_t row_index, _impl::Instruction inst, size_t)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"value", nullptr}
            });
        }
        return true;
    }
    bool set_link(size_t column_index, size_t row_index, size_t link_index, size_t target_group_level_ndx, _impl::Instruction inst)
    {
        if (selected_object_schema) {
            ObjectSchema *target_object_schema;
            ConstTableRef target_table;
            Property *target_primary;
            std::string table_name = m_group.get_table_name(target_group_level_ndx);
            select(table_name, target_object_schema, target_table, target_primary);

            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"object_identity", link_index == npos ? json(nullptr) : get_identity(link_index, target_table, target_primary)}
            });
        }
        return true;
    }
    bool nullify_link(size_t column_index, size_t row_index, size_t target_group_level_ndx)
    {
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)},
                {"property", selected_table->get_column_name(column_index)},
                {"object_identity", nullptr}
            });
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
    bool insert_link_column(size_t col_ndx, DataType data_type, StringData prop_name, size_t target_table_index, size_t backlink_col_ndx)
    {
        std::string object_type = ObjectStore::object_type_for_table_name(m_table_names[selected_table_index]);
        if (object_type.size()) { 
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::AddProperty)},
                {"object_type", object_type},
                {"property", prop_name},
                {"property_type", data_type == DataType::type_Link ? "object" : "list"},
                {"target_object_type", ObjectStore::object_type_for_table_name(m_table_names[target_table_index])}
            });
        }
        return true;
    }
    bool insert_column(size_t col_ndx, DataType data_type, StringData prop_name, bool nullable)
    {
        std::string object_type = ObjectStore::object_type_for_table_name(m_table_names[selected_table_index]);
        if (object_type.size()) { 
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::AddProperty)},
                {"object_type", object_type},
                {"property", prop_name},
                {"property_type", string_for_property_type((PropertyType)data_type)}
            });
            if (nullable) {
                json_instructions.back()["nullable"] = true;

            }
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
    bool move_column(size_t from, size_t to)
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
        REALM_ASSERT(0);    
        return true;
    }

    // Must have linklist selected:
    bool link_list_set(size_t list_index, size_t list_target_index, size_t prior_size)
    {
        if (list_property) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListSet)},
                {"object_type", selected_object_schema->name},
                {"property", list_property->name},
                {"identity", list_parent_identity},
                {"list_index", list_index},
                {"object_identity", get_identity(list_target_index, list_target_table, list_target_primary)}
            }); 
        } 
        return true;
    }
    bool link_list_insert(size_t list_index, size_t list_target_index, size_t prior_size)
    {
        if (list_property) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListInsert)},
                {"object_type", selected_object_schema->name},
                {"property", list_property->name},
                {"identity", list_parent_identity},
                {"list_index", list_index},
                {"object_identity", get_identity(list_target_index, list_target_table, list_target_primary)}
            }); 
        } 
        return true;
    }
    bool link_list_move(size_t from_index, size_t to_index)
    {
        if (list_property) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool link_list_swap(size_t from_index, size_t to_index)
    {
        if (list_property) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool link_list_erase(size_t list_index, size_t prior_size)
    {
        if (list_property) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListErase)},
                {"object_type", selected_object_schema->name},
                {"property", list_property->name},
                {"identity", list_parent_identity},
                {"list_index", list_index},
            }); 
        } 
        return true;
    }
    bool link_list_nullify(size_t list_index, size_t prior_size)
    {
        if (list_property) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListErase)},
                {"object_type", selected_object_schema->name},
                {"property", list_property->name},
                {"identity", list_parent_identity},
                {"list_index", list_index},
            }); 
        } 
        return true;
    }
    bool link_list_clear(size_t prior_size)
    {
        if (list_property) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListClear)},
                {"object_type", selected_object_schema->name},
                {"property", list_property->name},
                {"identity", list_parent_identity},
            }); 
        } 
        return true;
    }

    void parse_complete()
    {
    }
};

class ChangesetCooker: public realm::sync::SyncHistory::ChangesetCooker {
public:
    bool cook_changeset(const Group& group, const char* changeset,
                        std::size_t changeset_size,
                        util::AppendBuffer<char>& out_buffer) override {
        _impl::SimpleInputStream stream(changeset, changeset_size);
        _impl::TransactLogParser parser;
        ChangesetCookerInstructionHander cooker_handler(group);
        parser.parse(stream, cooker_handler);
        std::string out_string = cooker_handler.json_instructions.dump();
        out_buffer.append(out_string.c_str(), out_string.size()); // Throws
        return true;
    }
};


Adapter::Adapter(std::function<void(std::string)> realm_changed,
                 std::string local_root_dir, std::string server_base_url,
                 std::shared_ptr<SyncUser> user)
: m_global_notifier(GlobalNotifier::shared_notifier(
    std::make_unique<Adapter::Callback>([=](auto info) { realm_changed(info.second); }),
                                        local_root_dir, server_base_url, user, 
                                        std::make_shared<ChangesetCooker>()))
{
    m_global_notifier->start();
}

std::vector<bool> Adapter::Callback::available(std::vector<GlobalNotifier::RealmInfo> realms,
                                               std::vector<bool> new_realms,
                                               bool all) {
    std::vector<bool> watch;
    for (size_t i = 0; i < realms.size(); i++) {
        watch.push_back(true);
        if (!new_realms[i]) {
            m_realm_changed(realms[i]);
        }
    }
    return watch;
}

void Adapter::Callback::realm_changed(GlobalNotifier::ChangeNotification changes) {
    m_realm_changed(changes.realm_info);
}

util::Optional<Adapter::ChangeSet> Adapter::current(std::string realm_path) {
    auto realm = Realm::make_shared_realm(m_global_notifier->get_config(realm_path));
    auto sync_history = static_cast<sync::SyncHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();
    auto version = progress.version;

    util::AppendBuffer<char> buffer;
    version = sync_history->fetch_next_cooked_changeset(version, buffer);
    if (version == 0) {
        return util::none;
    }

    return ChangeSet(json::parse(std::string(buffer.data(), buffer.size())), realm);
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

SharedRealm Adapter::realm_at_path(std::string path) {
    return Realm::get_shared_realm(m_global_notifier->get_config(path));
}
