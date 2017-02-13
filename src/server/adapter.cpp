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

class ChangesetCookerInstructionHandler {
public:
    friend Adapter;

    ChangesetCookerInstructionHandler(const Group &group)
    : m_group(group)
    , m_schema(ObjectStore::schema_from_group(m_group)) {
        for (size_t i = 0; i < m_group.size(); i++) {
            m_table_names[i] = m_group.get_table_name(i);
        }
    }

    const Group &m_group;
    Schema m_schema;
    std::map<size_t, std::string> m_table_names;

    // primary caches consisting of primary keys set mid-changeset
    // these are updated to reflect inter-changeset row changes
    std::map<size_t, std::pair<std::string, std::string>> m_primary_key_properties;
    std::map<size_t, std::map<size_t, int64_t>> m_int_primaries;
    std::map<size_t, std::map<size_t, std::string>> m_string_primaries;

    // mapping inter-changetset rows to the index from the beginning of the changeset
    // used for primary key lookup
    std::map<size_t, std::map<size_t, size_t>> m_primary_key_lookup_row_mapping;

    json m_json_instructions;

    size_t m_selected_table_index;
    ConstTableRef m_selected_table;
    ObjectSchema *m_selected_object_schema = nullptr;
    Property *m_selected_primary = nullptr;
    bool m_selected_is_primary_key_table;

    Property *m_list_property = nullptr;
    json m_list_parent_identity;

    ConstTableRef m_list_target_table;
    ObjectSchema *m_list_target_object_schema = nullptr;
    Property *m_list_target_primary = nullptr;

    bool m_last_is_collapsable = false;

    void add_instruction(Adapter::InstructionType type, 
                         json &&inst = {}, bool 
                         collasable = false, 
                         util::Optional<std::string> object_type = util::none) {
        inst["type"] = Adapter::instruction_type_string(type);
        inst["object_type"] = object_type ? *object_type : m_selected_object_schema->name;
        m_json_instructions.push_back(std::move(inst));

        m_last_is_collapsable = collasable;
    }

    void add_insert_instruction(json &&identity) {
        add_instruction(Adapter::InstructionType::Insert, {
            {"identity", std::move(identity)},
            {"values", {}}
        }, true);
    }

    void add_set_instruction(size_t row, size_t column, json &&value) {
        json identity = get_identity(row, m_selected_table, m_selected_primary);

        // collapse values if inserting/setting values for the last object
        if (m_last_is_collapsable) {
            json &last = m_json_instructions.back();
            if (identity == last["identity"] && m_selected_object_schema->name == last["object_type"].get<std::string>()) {
                last["values"][m_selected_table->get_column_name(column)] = value;
                return;
            }
        }

        // if not collapsed create new
        add_instruction(Adapter::InstructionType::Set, {
            {"identity", std::move(identity)},
            {"values", {{m_selected_table->get_column_name(column), value}}}
        }, true);
    }

    void add_column_instruction(std::string object_type, std::string prop_name, json &&prop) {
        if (m_json_instructions.size()) {
            json &last = m_json_instructions.back();
            if (last["object_type"].get<std::string>() == object_type && (
                last["type"].get<std::string>() == "ADD_TYPE" || 
                last["type"].get<std::string>() == "ADD_PROPERTIES")) 
            {
                last["properties"][prop_name] = prop;
                return;
            }
        }

        add_instruction(Adapter::InstructionType::AddProperties, {{"properties", 
            {{prop_name, prop}}
        }}, false, object_type);
    }

    json get_identity(size_t row, ConstTableRef &table, Property *primary_key) {
        if (primary_key) {
            auto get_or_lookup_primary = [&](auto realm_primaries, auto get_primary) {
                auto primaries = realm_primaries.find(table->get_index_in_group());
                if (primaries != realm_primaries.end()) {
                    auto primary = primaries->second.find(row);
                    if (primary != primaries->second.end()) {
                        return primary->second;
                    }
                }
                auto mappings = m_primary_key_lookup_row_mapping.find(table->get_index_in_group());
                if (mappings != m_primary_key_lookup_row_mapping.end()) {
                    auto mapping = mappings->second.find(row);
                    if (mapping != mappings->second.end()) {
                        return get_primary(primary_key->table_column, mapping->second);
                    }
                }
                return get_primary(primary_key->table_column, row); 
            };

            if (primary_key->type == PropertyType::Int)
                return get_or_lookup_primary(m_int_primaries, [&](auto column, auto row) { 
                    return table->get_int(column, row); } );

            if (primary_key->type == PropertyType::String)
                return get_or_lookup_primary(m_string_primaries, [&](auto column, auto row) { 
                    return (std::string)table->get_string(column, row); } );
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
    bool select_table(size_t table_index, size_t levels, const size_t* path)
    {
        REALM_ASSERT(levels == 0);

        m_selected_table_index = table_index;
        select(m_table_names[table_index], m_selected_object_schema, m_selected_table, m_selected_primary);
        m_selected_is_primary_key_table = !m_selected_object_schema && m_table_names[table_index] == "pk";

        return true;
    }
    bool select_descriptor(size_t levels, const size_t* path)
    {
        return true;
    }
    bool select_link_list(size_t column_index, size_t row_index, size_t group_index)
    {
        REALM_ASSERT(m_selected_object_schema != nullptr);

        m_list_parent_identity = get_identity(row_index, m_selected_table, m_selected_primary);

        m_list_property = &m_selected_object_schema->persisted_properties[column_index];
        REALM_ASSERT(m_list_property->table_column == column_index);

        select(m_table_names[group_index], m_list_target_object_schema, m_list_target_table, m_list_target_primary);

        return true;
    }
    bool insert_group_level_table(size_t table_index, size_t num_tables, StringData name)
    {
        m_table_names[table_index] = name;
        std::string object_type = ObjectStore::object_type_for_table_name(m_table_names[table_index]);
        if (object_type.size()) {
            add_instruction(Adapter::InstructionType::AddType, {
                {"properties", {}}
            }, false, object_type);
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
        if (m_selected_object_schema) {
            add_insert_instruction(row_index);
        }
        return true;
    }
    bool erase_rows(size_t row_index, size_t n_rows, size_t prior_num_rows, bool move_last_over)
    {
        REALM_ASSERT(n_rows == 1);
        REALM_ASSERT(move_last_over);
        if (m_selected_object_schema) {
            add_instruction(Adapter::InstructionType::Delete, {
                {"identity", get_identity(row_index, m_selected_table, m_selected_primary)}
            });

            // handle move_last_over
            size_t old_row_index = prior_num_rows - 1;
            if (m_selected_primary) {
                auto &row_mapping = m_primary_key_lookup_row_mapping[m_selected_table_index];
                auto &int_primaries = m_int_primaries[m_selected_table_index];
                auto &string_primaries = m_string_primaries[m_selected_table_index];

                // invalidate caches
                if (row_mapping.count(row_index)) row_mapping.erase(row_index);
                if (int_primaries.count(row_index)) int_primaries.erase(row_index);
                if (string_primaries.count(row_index)) string_primaries.erase(row_index);

                // update caches for moved object
                if (row_index < old_row_index) {
                    auto old_row_iter = row_mapping.find(old_row_index);
                    if (old_row_iter != row_mapping.end()) {
                        row_mapping[row_index] = old_row_iter->second;
                        row_mapping.erase(old_row_iter);
                    }
                    else {
                        row_mapping[row_index] = old_row_index;
                    }

                    // update primary key caches
                    if (int_primaries.count(old_row_index)) {
                        int_primaries[row_index] = int_primaries[old_row_index];
                        int_primaries.erase(old_row_index);
                    }
                    if (string_primaries.count(old_row_index)) {
                        string_primaries[row_index] = string_primaries[old_row_index];
                        string_primaries.erase(old_row_index);
                    }
                }
            }
            else {
                if (row_index < old_row_index) {
                    // change identity for objects with no primary key
                    add_instruction(Adapter::InstructionType::ChangeIdentity, {
                        {"identity", old_row_index},
                        {"new_identity", row_index}
                    });
                }
            }
        }
        return true;
    }
    bool swap_rows(size_t row_index_1, size_t row_index_2)
    {
        if (m_selected_object_schema && !m_selected_primary) {
            add_instruction(Adapter::InstructionType::SwapIdentity, {
                {"identity_first", row_index_1},
                {"identity_second", row_index_2}
            });
        }
        return true;
    }
    bool merge_rows(size_t, size_t)
    {
        // It's ok to ignore this instruction because it only happens as a result of 
        // resolving a PK conflict, but for tables with primary keys we use the PK
        // instead of the row index for the identity
        if (m_selected_object_schema) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool clear_table()
    {
        if (m_selected_object_schema) {
            add_instruction(Adapter::InstructionType::Clear);
        }
        return true;
    }
    bool set_int(size_t column_index, size_t row_index, int_fast64_t value, _impl::Instruction inst, size_t)
    {
        if (m_selected_object_schema) {
            // overwrite identity of last addition if this is the primary key
            if (m_selected_primary && m_selected_primary->table_column == column_index) {
                m_int_primaries[m_selected_table_index][row_index] = value;

                auto &last = m_json_instructions.back();
                REALM_ASSERT(last["type"].get<std::string>() == "INSERT");
                last["identity"] = value;
                last["values"][m_selected_table->get_column_name(column_index)] = value;
            }
            else {
                add_set_instruction(row_index, column_index, value);
            }
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
        if (m_selected_object_schema) {
            add_set_instruction(row_index, column_index, value);
        }
        return true;
    }
    bool set_float(size_t column_index, size_t row_index, float value, _impl::Instruction inst)
    {
        if (m_selected_object_schema) {
            add_set_instruction(row_index, column_index, value);
        }
        return true;
    }
    bool set_double(size_t column_index, size_t row_index, double value, _impl::Instruction inst)
    {
        if (m_selected_object_schema) {
            add_set_instruction(row_index, column_index, value);
        }
        return true;
    }
    bool set_string(size_t column_index, size_t row_index, StringData value, _impl::Instruction inst, size_t)
    {
        if (m_selected_object_schema) {
            if (m_selected_primary && m_selected_primary->table_column == column_index) {
                m_string_primaries[m_selected_table_index][row_index] = value;
  
                auto &last = m_json_instructions.back();
                REALM_ASSERT(last["type"].get<std::string>() == "INSERT");
                last["identity"] = value;
                last["values"][m_selected_table->get_column_name(column_index)] = value;
            }
            else {
                add_set_instruction(row_index, column_index, value);
            }
        }
        else if (m_selected_is_primary_key_table) {
            auto &prop = m_primary_key_properties.emplace(std::make_pair(row_index, std::pair<std::string, std::string>())).first->second;
            if (column_index == 0) prop.first = value;
            else if (column_index == 1) prop.second = value;
        }
        return true;
    }
    bool set_binary(size_t column_index, size_t row_index, BinaryData value, _impl::Instruction inst)
    {
        if (m_selected_object_schema) {
            add_set_instruction(row_index, column_index, {"data", value});
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
        if (m_selected_object_schema) {
            int64_t value = ts.get_seconds() * 1000 + ts.get_nanoseconds() / 1000000;
            add_set_instruction(row_index, column_index, {"date", value});
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
        if (m_selected_object_schema) {
            add_set_instruction(row_index, column_index, nullptr);
        }
        return true;
    }
    bool set_link(size_t column_index, size_t row_index, size_t link_index, size_t target_group_level_ndx, _impl::Instruction inst)
    {
        if (m_selected_object_schema) {
            ObjectSchema *target_object_schema;
            ConstTableRef target_table;
            Property *target_primary;
            std::string table_name = m_group.get_table_name(target_group_level_ndx);
            select(table_name, target_object_schema, target_table, target_primary);

            json value = link_index == npos ? json(nullptr) : get_identity(link_index, target_table, target_primary);
            add_set_instruction(row_index, column_index, std::move(value));
        }
        return true;
    }
    bool nullify_link(size_t column_index, size_t row_index, size_t target_group_level_ndx)
    {
        if (m_selected_object_schema) {
            add_set_instruction(row_index, column_index, nullptr);
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
        std::string object_type = ObjectStore::object_type_for_table_name(m_table_names[m_selected_table_index]);
        if (object_type.size()) {
            add_column_instruction(object_type, prop_name, {
                {"type", data_type == DataType::type_Link ? "object" : "list"},
                {"object_type", ObjectStore::object_type_for_table_name(m_table_names[target_table_index])}
            });
        }
        return true;
    }
    bool insert_column(size_t col_ndx, DataType data_type, StringData prop_name, bool nullable)
    {
        std::string object_type = ObjectStore::object_type_for_table_name(m_table_names[m_selected_table_index]);
        if (object_type.size()) {
            add_column_instruction(object_type, prop_name, {
                {"type", string_for_property_type((PropertyType)data_type)},
                {"nullable", nullable}            
            });
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
    bool link_list_set(size_t list_index, size_t m_list_target_index, size_t prior_size)
    {
        if (m_list_property) {
            add_instruction(Adapter::InstructionType::ListSet, {
                {"identity", m_list_parent_identity},
                {"property", m_list_property->name},
                {"list_index", list_index},
                {"object_identity", get_identity(m_list_target_index, m_list_target_table, m_list_target_primary)}
            }); 
        } 
        return true;
    }
    bool link_list_insert(size_t list_index, size_t m_list_target_index, size_t prior_size)
    {
        if (m_list_property) {
            add_instruction(Adapter::InstructionType::ListInsert, {
                {"identity", m_list_parent_identity},
                {"property", m_list_property->name},
                {"list_index", list_index},
                {"object_identity", get_identity(m_list_target_index, m_list_target_table, m_list_target_primary)}
            }); 
        } 
        return true;
    }
    bool link_list_move(size_t from_index, size_t to_index)
    {
        if (m_list_property) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool link_list_swap(size_t from_index, size_t to_index)
    {
        if (m_list_property) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool link_list_erase(size_t list_index, size_t prior_size)
    {
        if (m_list_property) {
            add_instruction(Adapter::InstructionType::ListErase, {
                {"identity", m_list_parent_identity},
                {"property", m_list_property->name},
                {"list_index", list_index},
            }); 
        } 
        return true;
    }
    bool link_list_nullify(size_t list_index, size_t prior_size)
    {
        if (m_list_property) {
            add_instruction(Adapter::InstructionType::ListErase, {
                {"identity", m_list_parent_identity},
                {"property", m_list_property->name},
                {"list_index", list_index},
            }); 
        } 
        return true;
    }
    bool link_list_clear(size_t prior_size)
    {
        if (m_list_property) {
            add_instruction(Adapter::InstructionType::ListClear, {
                {"identity", m_list_parent_identity},
                {"property", m_list_property->name},
            }); 
        } 
        return true;
    }

    void append_primary_key_identifiers()
    {
        for (auto pk : m_primary_key_properties) {
            for (auto &json : m_json_instructions) {
                if (json["type"].get<std::string>() == Adapter::instruction_type_string(Adapter::InstructionType::AddType) &&
                    json["object_type"].get<std::string>() == pk.second.first) {
                    json["primary_key"] = pk.second.second;
                    break;
                }
            }
        }
    }
};

class ChangesetCooker: public realm::sync::SyncHistory::ChangesetCooker {
public:
    bool cook_changeset(const Group& group, const char* changeset,
                        std::size_t changeset_size,
                        util::AppendBuffer<char>& out_buffer) override {
        _impl::SimpleInputStream stream(changeset, changeset_size);
        _impl::TransactLogParser parser;
        ChangesetCookerInstructionHandler cooker_handler(group);
        parser.parse(stream, cooker_handler);
        cooker_handler.append_primary_key_identifiers();
        std::string out_string = cooker_handler.m_json_instructions.dump();
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

std::vector<bool> Adapter::Callback::available(std::vector<GlobalNotifier::RealmInfo> realms) {
    std::vector<bool> watch;
    for (size_t i = 0; i < realms.size(); i++) {
        watch.push_back(true);
    }
    return watch;
}

void Adapter::Callback::realm_changed(GlobalNotifier::ChangeNotification changes) {
    if (m_realm_changed) {
        m_realm_changed(changes.realm_info);
    }
}

util::Optional<Adapter::ChangeSet> Adapter::current(std::string realm_path) {
    auto realm = Realm::make_shared_realm(m_global_notifier->get_config(realm_path));

    REALM_ASSERT(dynamic_cast<sync::SyncHistory *>(realm->history()));
    auto sync_history = static_cast<sync::SyncHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();

    if (progress.changeset_index >= sync_history->get_num_cooked_changesets()) {
        return util::none;
    }

    util::AppendBuffer<char> buffer;
    sync_history->get_cooked_changeset(progress.changeset_index, buffer);
    return ChangeSet(json::parse(std::string(buffer.data(), buffer.size())), realm);
}

void Adapter::advance(std::string realm_path) {
    auto realm = Realm::make_shared_realm(m_global_notifier->get_config(realm_path));
    auto sync_history = static_cast<sync::SyncHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();
    if (progress.changeset_index < sync_history->get_num_cooked_changesets()) {
        sync_history->set_cooked_progress({progress.changeset_index + 1, 0});
    }
    realm->invalidate();
}

SharedRealm Adapter::realm_at_path(std::string path) {
    return Realm::get_shared_realm(m_global_notifier->get_config(path));
}
