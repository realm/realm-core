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
        m_table_names.reserve(m_group.size());
        for (size_t i = 0; i < m_group.size(); i++) {
            m_table_names.push_back(m_group.get_table_name(i));
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
    std::vector<std::string> m_table_names;

    json json_instructions;

    using LinkingProperties = std::vector<std::pair<std::string, Property>>;
    std::map<std::string, LinkingProperties> m_linking_properties;

    size_t selected_table_index;
    ConstTableRef selected_table;
    ObjectSchema *selected_object_schema = nullptr;
    Property *selected_primary = nullptr;

    size_t list_table_index;
    ConstTableRef list_table;
    ObjectSchema *list_object_schema = nullptr;
    Property *list_property = nullptr;
    Property *list_primary = nullptr;

    size_t list_row;

    ObjectSchema *list_target_object_schema;
    ConstTableRef list_target_table;
    Property *list_target_primary;

    json get_identity(size_t row, ConstTableRef &table, Property *primary_key) {
//        if (primary_key) {
//            if (primary_key->type == PropertyType::Int) {
//                return table->get_int(primary_key->table_column, row); 
//            }
//            else if (primary_key->type == PropertyType::String) {
//                return table->get_string(primary_key->table_column, row); 
//            }
//        }
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
        // FIXME - caller to this is broken - for now just use last selected table
        return true;
    }
    bool select_link_list(size_t column_index, size_t row_index, size_t group_index)
    {
        list_table_index = group_index;
        list_row = row_index;

        select(m_table_names[group_index], list_object_schema, list_table, list_primary);

        if (list_object_schema) {
            list_property = &list_object_schema->persisted_properties[column_index];
            REALM_ASSERT(list_property->table_column == column_index);
        }
        else {
            list_property = nullptr;
        }

        return true;
    }
    bool insert_group_level_table(size_t group_index, size_t num_tables, StringData name)
    {
        if (m_table_names.size() <= group_index) {
            m_table_names.resize(group_index + 1);
        }
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
        if (selected_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::Insert)},
                {"object_type", selected_object_schema->name},
                {"identity", get_identity(row_index, selected_table, selected_primary)}
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

            // // add instructions to nullify backlinks
            // auto table = ObjectStore::table_for_object_type(m_group, selected_object_type);
            // for (auto linking_object_property : m_linking_properties[selected_object_type]) {
            //     auto linking_table = ObjectStore::table_for_object_type(m_group, linking_object_property.first);
            //     size_t linking_table_index = linking_table->get_index_in_group();
            //     size_t linking_column = linking_object_property.second.table_column;
            //     size_t backlink_count = linking_table->get_backlink_count(row_index, *table, linking_column);
            //     for (size_t backlink_index = 0; backlink_index < backlink_count; backlink_index++) {
            //         size_t linking_row = table->get_backlink(row_index, *linking_table, linking_column, backlink_index);
            //         if (linking_object_property.second.type == PropertyType::Object) {
            //             REALM_ASSERT(linking_table->get_link(linking_column, linking_row) == row_index);

            //             json_instructions.push_back({
            //                 {"type", Adapter::instruction_type_string(Adapter::InstructionType::SetProperty)},
            //                 {"object_type", selected_object_type},
            //                 {"identity", get_identity(row_index)},
            //                 {"property", linking_object_property.second}
            //             });
            //             m_encoder.nullify_link(linking_column, linking_row, linking_table_index);
            //         }
            //         else {
            //             m_encoder.select_link_list(linking_column, linking_row, linking_table_index);

            //             auto link_view = linking_table->get_linklist(linking_column, linking_row);
            //             auto list_index = link_view->find(row_index);

            //             auto size = link_view->size();
            //             while (list_index != npos) {
            //                 link_list_nullify(list_index, size--);
            //                 list_index = link_view->find(row_index, list_index + 1);
            //             }
            //         }
            //     }
            // }
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
                {"object_identity", get_identity(link_index, target_table, target_primary)}
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
                {"property_type", string_for_property_type((PropertyType)data_type)},
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
    bool link_list_set(size_t list_index, size_t list_target_index, size_t prior_size)
    {
        if (list_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListSet)},
                {"object_type", list_object_schema->name},
                {"property", list_property->name},
                {"identity", get_identity(list_row, list_table, list_primary)},
                {"list_index", list_index},
                {"object_identity", get_identity(list_target_index, list_target_table, list_target_primary)}
            }); 
        } 
        return true;
    }
    bool link_list_insert(size_t list_index, size_t list_target_index, size_t prior_size)
    {
        if (list_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListInsert)},
                {"object_type", list_object_schema->name},
                {"property", list_property->name},
                {"identity", get_identity(list_row, list_table, list_primary)},
                {"list_index", list_index},
                {"object_identity", get_identity(list_target_index, list_target_table, list_target_primary)}
            }); 
        } 
        return true;
    }
    bool link_list_move(size_t from_index, size_t to_index)
    {
        if (list_object_schema) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool link_list_swap(size_t from_index, size_t to_index)
    {
        if (list_object_schema) {
            REALM_ASSERT(0);
        }
        return true;
    }
    bool link_list_erase(size_t list_index, size_t prior_size)
    {
        if (list_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListErase)},
                {"object_type", list_object_schema->name},
                {"property", list_property->name},
                {"identity", get_identity(list_row, list_table, list_primary)},
                {"list_index", list_index},
            }); 
        } 
        return true;
    }
    bool link_list_nullify(size_t list_index, size_t prior_size)
    {
        if (list_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListSet)},
                {"object_type", list_object_schema->name},
                {"property", list_property->name},
                {"identity", get_identity(list_row, list_table, list_primary)},
                {"list_index", list_index},
                {"object_identity", nullptr}
            }); 
        } 
        return true;
    }
    bool link_list_clear(size_t prior_size)
    {
        if (list_object_schema) {
            json_instructions.push_back({
                {"type", Adapter::instruction_type_string(Adapter::InstructionType::ListClear)},
                {"object_type", list_object_schema->name},
                {"property", list_property->name},
                {"identity", get_identity(list_row, list_table, list_primary)},
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
    std::cout << "AVAILABLE" << std::endl;
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
    std::cout << "CHANGED" << std::endl;
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
