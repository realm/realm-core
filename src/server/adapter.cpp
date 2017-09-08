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
#include <realm/sync/changeset_parser.hpp>
#include <realm/impl/transact_log.hpp>
#include <realm/impl/input_stream.hpp>

#include <unordered_map>
#include <unordered_set>

using namespace realm;

using ObjectID = realm::sync::ObjectID;
using Instruction = realm::sync::Instruction;

class ChangesetCookerInstructionHandler : public sync::InstructionHandler {
public:
    friend Adapter;

    ChangesetCookerInstructionHandler(const Group &group)
    : m_group(group)
    , m_schema(ObjectStore::schema_from_group(m_group))
    {
    }

    const Group &m_group;
    Schema m_schema;

    std::unordered_map<std::string, std::unordered_map<ObjectID, int64_t>> m_int_primaries;
    std::unordered_map<std::string, std::unordered_map<ObjectID, std::string>> m_string_primaries;
    std::unordered_map<std::string, std::unordered_set<ObjectID>> m_null_primaries;

    nlohmann::json m_json_instructions;

    std::string m_selected_table_name;
    ConstTableRef m_selected_table;
    ObjectSchema *m_selected_object_schema = nullptr;
    Property *m_selected_primary = nullptr;

    Property *m_list_property = nullptr;
    nlohmann::json m_list_parent_identity;

    ConstTableRef m_list_target_table;
    ObjectSchema *m_list_target_object_schema = nullptr;
    Property *m_list_target_primary = nullptr;

    bool m_last_is_collapsible = false;

    void add_instruction(Adapter::InstructionType type,
                         nlohmann::json &&inst = {}, bool collapsible = false,
                         util::Optional<std::string> object_type = util::none) {
        inst["type"] = Adapter::instruction_type_string(type);
        inst["object_type"] = object_type ? *object_type : m_selected_object_schema->name;
        m_json_instructions.push_back(std::move(inst));

        m_last_is_collapsible = collapsible;
    }

    void add_set_instruction(ObjectID row, StringData column, nlohmann::json &&value) {
        nlohmann::json identity = get_identity(row, *m_selected_table, m_selected_primary);

        // collapse values if inserting/setting values for the last object
        if (m_last_is_collapsible) {
            nlohmann::json &last = m_json_instructions.back();
            if (identity == last["identity"] && m_selected_object_schema->name == last["object_type"].get<std::string>()) {
                last["values"][column] = value;
                return;
            }
        }

        // if not collapsed create new
        add_instruction(Adapter::InstructionType::Set, {
            {"identity", std::move(identity)},
            {"values", {{column, value}}}
        }, true);
    }

    void add_column_instruction(std::string object_type, std::string prop_name, nlohmann::json &&prop) {
        if (m_json_instructions.size()) {
            nlohmann::json &last = m_json_instructions.back();
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

    nlohmann::json get_identity(ObjectID object_id, const Table& table, Property *primary_key) {
        if (primary_key) {
            std::string table_name = table.get_name();

            if (is_nullable(primary_key->type)) {
                auto& null_primaries = m_null_primaries[table_name];
                auto it = null_primaries.find(object_id);
                if (it != null_primaries.end())
                    return nullptr;
            }

            if (primary_key->type == PropertyType::Int) {
                auto& int_primaries = m_int_primaries[table_name];
                auto it = int_primaries.find(object_id);
                if (it != int_primaries.end()) {
                    return it->second;
                }

                size_t row = sync::row_for_object_id(m_group, table, object_id);
                REALM_ASSERT(row != npos);
                if (is_nullable(primary_key->type) && table.is_null(primary_key->table_column, row)) {
                    return nullptr;
                }
                return table.get_int(primary_key->table_column, row);
            }
            else if (primary_key->type == PropertyType::String) {
                auto& string_primaries = m_string_primaries[table_name];
                auto it = string_primaries.find(object_id);
                if (it != string_primaries.end()) {
                    return it->second;
                }

                size_t row = sync::row_for_object_id(m_group, table, object_id);
                REALM_ASSERT(row != npos);
                StringData value = table.get_string(primary_key->table_column, row);
                if (value.is_null())
                    return nullptr;
                return std::string(value);
            }
        }

        return object_id.to_string();
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
                if (!out_primary) {
                    throw std::runtime_error("Only object types with primary keys can be used in the adapter");
                }
            }
        }
    }

    std::unordered_map<uint32_t, sync::StringBufferRange> m_interned_strings;
    util::StringBuffer m_string_buffer;

    StringData get_string(sync::StringBufferRange range) const
    {
        return StringData{m_string_buffer.data() + range.offset, range.size};
    }

    StringData get_string(sync::InternString intern_string) const
    {
        auto it = m_interned_strings.find(intern_string.value);
        REALM_ASSERT(it != m_interned_strings.end());
        return get_string(it->second);
    }

    void set_intern_string(uint32_t index, sync::StringBufferRange range) override
    {
        m_interned_strings[index] = range;
    }

    sync::StringBufferRange add_string_range(StringData data) override
    {
        size_t offset = m_string_buffer.size();
        m_string_buffer.append(data.data(), data.size());
        return sync::StringBufferRange{uint32_t(offset), uint32_t(data.size())};
    }

    // No selection needed:
    void operator()(const Instruction::SelectTable& instr)
    {
        m_selected_table_name = get_string(instr.table);
        select(m_selected_table_name, m_selected_object_schema, m_selected_table, m_selected_primary);
    }

    void operator()(const Instruction::SelectContainer& instr)
    {
        REALM_ASSERT(m_selected_object_schema);

        m_list_parent_identity = get_identity(instr.object, *m_selected_table, m_selected_primary);

        size_t column_index = m_selected_table->get_column_index(get_string(instr.field));
        m_list_property = &m_selected_object_schema->persisted_properties[column_index];
        REALM_ASSERT(m_list_property->table_column == column_index);

        std::string link_target_table = get_string(instr.link_target_table);
        select(link_target_table, m_list_target_object_schema, m_list_target_table, m_list_target_primary);
    }

    void operator()(const Instruction::AddTable& instr)
    {
        std::string object_type = ObjectStore::object_type_for_table_name(get_string(instr.table));
        if (object_type.size()) {
            nlohmann::json dict = {{"properties", nullptr}};
            if (instr.has_primary_key) {
                dict["primary_key"] = get_string(instr.primary_key_field);
                dict["properties"][get_string(instr.primary_key_field)] = {
                    {"nullable", instr.primary_key_nullable},
                    {"type", string_for_property_type(PropertyType(instr.primary_key_type))}
                };
            }
            add_instruction(Adapter::InstructionType::AddType, std::move(dict),
                            false, object_type);
        }
    }

    void operator()(const Instruction::EraseTable&)
    {
        REALM_ASSERT(0);
    }

    // Must have table selected:
    void operator()(const Instruction::CreateObject& instr)
    {
        if (!m_selected_object_schema)
            return; // FIXME: Support objects without schemas

        nlohmann::json identity;
        nlohmann::json values;

        if (instr.has_primary_key) {
            if (instr.payload.type == type_Int) {
                identity = instr.payload.data.integer;
                m_int_primaries[m_selected_table_name][instr.object] = instr.payload.data.integer;
            }
            else if (instr.payload.type == type_String) {
                std::string value = get_string(instr.payload.data.str);
                identity = value;
                m_string_primaries[m_selected_table_name][instr.object] = value;
            }
            else if (instr.payload.is_null()) {
                identity = nullptr;
                m_null_primaries[m_selected_table_name].insert(instr.object);
            }
            else {
                REALM_TERMINATE("Non-integer/non-string primary keys not supported by adapter.");
            }

            values[m_selected_primary->name] = identity;
        }
        else {
            identity = instr.object.to_string(); // Use the stringified Object ID
        }
        add_instruction(Adapter::InstructionType::Insert, {
            {"identity", std::move(identity)},
            {"values", std::move(values)}
        }, true);
    }

    void operator()(const Instruction::EraseObject& instr)
    {
        if (!m_selected_object_schema)
            return; // FIXME: Support objects without schemas

        add_instruction(Adapter::InstructionType::Delete, {
            {"identity", get_identity(instr.object, *m_selected_table, m_selected_primary)}
        });

        if (m_selected_primary) {
            auto& int_primaries    = m_int_primaries[m_selected_table_name];
            auto& string_primaries = m_string_primaries[m_selected_table_name];
            auto& null_primaries   = m_null_primaries[m_selected_table_name];

            // invalidate caches
            int_primaries.erase(instr.object);
            string_primaries.erase(instr.object);
            null_primaries.erase(instr.object);
        }
    }

    void operator()(const Instruction::Set& instr)
    {
        if (!m_selected_object_schema)
            return; // FIXME: Support objects without schemas

        StringData field = get_string(instr.field);

        if (instr.payload.is_null()) {
            return add_set_instruction(instr.object, field, nullptr);
        }

        switch (instr.payload.type) {
            case type_Int:
                return add_set_instruction(instr.object, field, instr.payload.data.integer);
            case type_Bool:
                return add_set_instruction(instr.object, field, instr.payload.data.boolean);
            case type_Float:
                return add_set_instruction(instr.object, field, instr.payload.data.fnum);
            case type_Double:
                return add_set_instruction(instr.object, field, instr.payload.data.dnum);
            case type_String:
                return add_set_instruction(instr.object, field, std::string(get_string(instr.payload.data.str)));
            case type_Binary: {
                std::string string = get_string(instr.payload.data.str);
                return add_set_instruction(instr.object, field, {"data", string});
            }
            case type_Timestamp: {
                Timestamp ts = instr.payload.data.timestamp;
                int64_t value = ts.get_seconds() * 1000 + ts.get_nanoseconds() / 1000000;
                return add_set_instruction(instr.object, field, {"date", value});
            }
            case type_Link: {
                ObjectSchema *target_object_schema;
                ConstTableRef target_table;
                Property *target_primary;
                std::string table_name = get_string(instr.payload.data.link.target_table);
                select(table_name, target_object_schema, target_table, target_primary);
                nlohmann::json value = get_identity(instr.payload.data.link.target, *target_table, target_primary);
                return add_set_instruction(instr.object, field, std::move(value));
            }


            case type_Table:
            case type_Mixed:
            case type_LinkList:
            case type_OldDateTime:
                REALM_TERMINATE("Unsupported data type.");
        }
    }

    void operator()(const Instruction::AddInteger& instr)
    {
        // FIXME
        REALM_TERMINATE("AddInteger not supported by adapter.");
    }

    void operator()(const Instruction::InsertSubstring& instr)
    {
        // FIXME
        REALM_TERMINATE("InsertSubstring not supported by adapter.");
    }

    void operator()(const Instruction::EraseSubstring& instr)
    {
        // FIXME
        REALM_TERMINATE("EraseSubstring not supported by adapter.");
    }

    void operator()(const Instruction::ClearTable&)
    {
        add_instruction(Adapter::InstructionType::Clear);
    }

    void operator()(const Instruction::AddColumn& instr)
    {
        std::string object_type = ObjectStore::object_type_for_table_name(m_selected_table_name);
        if (object_type.size()) {
            if (instr.type == type_Link || instr.type == type_LinkList) {
                add_column_instruction(object_type, get_string(instr.field), {
                    {"type", (instr.type == type_Link ? "object" : "list")},
                    {"object_type", ObjectStore::object_type_for_table_name(get_string(instr.link_target_table))}
                });
            }
            else {
                add_column_instruction(object_type, get_string(instr.field), {
                    {"type", string_for_property_type(PropertyType(instr.type))},
                    {"nullable", instr.nullable}
                });
            }
        }
    }

    void operator()(const Instruction::EraseColumn& instr)
    {
        REALM_TERMINATE("EraseColumn not supported by adapter.");
    }

    // Must have linklist selected:
    void operator()(const Instruction::ContainerSet& instr)
    {
        if (!m_list_property)
            return; // FIXME

        // FIXME: Support arrays of primitives

        add_instruction(Adapter::InstructionType::ListSet, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property->name},
            {"list_index", instr.ndx},
            {"object_identity", get_identity(instr.payload.data.link.target, *m_list_target_table, m_list_target_primary)}
        });
    }

    void operator()(const Instruction::ContainerInsert& instr)
    {
        if (!m_list_property)
            return; // FIXME


        // FIXME: Support arrays of primitives

        add_instruction(Adapter::InstructionType::ListInsert, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property->name},
            {"list_index", instr.ndx},
            {"object_identity", get_identity(instr.payload.data.link.target, *m_list_target_table, m_list_target_primary)}
        });
    }

    void operator()(const Instruction::ContainerMove&)
    {
        if (!m_list_property)
            return; // FIXME

        REALM_TERMINATE("ContainerMove not supported by adapter.");
    }

    void operator()(const Instruction::ContainerSwap&)
    {
        if (!m_list_property)
            return; // FIXME

        REALM_TERMINATE("ContainerSwap not supported by adapter.");
    }

    void operator()(const Instruction::ContainerErase& instr)
    {
        if (!m_list_property)
            return; // FIXME

        add_instruction(Adapter::InstructionType::ListErase, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property->name},
            {"list_index", instr.ndx},
        });
    }

    void operator()(const Instruction::ContainerClear&)
    {
        if (!m_list_property)
            return; // FIXME

        add_instruction(Adapter::InstructionType::ListClear, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property->name},
        });
    }

    void operator()(const Instruction& instr) final override
    {
        instr.visit(*this);
    }
};

class ChangesetCooker: public realm::sync::ClientHistory::ChangesetCooker {
public:
    bool cook_changeset(const Group& group, const char* changeset,
                        std::size_t changeset_size,
                        util::AppendBuffer<char>& out_buffer) override {
        _impl::SimpleNoCopyInputStream stream_2(changeset, changeset_size);
        sync::Changeset c;
        sync::parse_changeset(stream_2, c);

        _impl::SimpleNoCopyInputStream stream(changeset, changeset_size);
        sync::ChangesetParser parser;
        ChangesetCookerInstructionHandler cooker_handler(group);
        parser.parse(stream, cooker_handler);
        std::string out_string = cooker_handler.m_json_instructions.dump();
        out_buffer.append(out_string.c_str(), out_string.size()); // Throws
        return true;
    }
};


Adapter::Adapter(std::function<void(std::string)> realm_changed,
                 std::string local_root_dir, std::string server_base_url,
                 std::shared_ptr<SyncUser> user, std::regex regex)
: m_global_notifier(GlobalNotifier::shared_notifier(
    std::make_unique<Adapter::Callback>([=](auto info) { realm_changed(info.second); }, regex),
                                        local_root_dir, server_base_url, user,
                                        std::make_shared<ChangesetCooker>()))
{
    m_global_notifier->start();
}

std::vector<bool> Adapter::Callback::available(std::vector<GlobalNotifier::RealmInfo> realms) {
    std::vector<bool> watch;
    for (size_t i = 0; i < realms.size(); i++) {
        watch.push_back(std::regex_match(realms[i].second, m_regex));
    }
    return watch;
}

void Adapter::Callback::realm_changed(GlobalNotifier::ChangeNotification changes) {
    if (m_realm_changed) {
        m_realm_changed(changes.realm_info);
    }
}

util::Optional<Adapter::ChangeSet> Adapter::current(std::string realm_path) {
    auto realm = realm::Realm::get_shared_realm(m_global_notifier->get_config(realm_path));

    REALM_ASSERT(dynamic_cast<sync::ClientHistory *>(realm->history()));
    auto sync_history = static_cast<sync::ClientHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();

    if (progress.changeset_index >= sync_history->get_num_cooked_changesets()) {
        return util::none;
    }

    util::AppendBuffer<char> buffer;
    sync_history->get_cooked_changeset(progress.changeset_index, buffer);
    return ChangeSet(nlohmann::json::parse(std::string(buffer.data(), buffer.size())), realm);
}

void Adapter::advance(std::string realm_path) {
    auto realm = realm::Realm::get_shared_realm(m_global_notifier->get_config(realm_path));
    auto sync_history = static_cast<sync::ClientHistory *>(realm->history());
    auto progress = sync_history->get_cooked_progress();
    if (progress.changeset_index < sync_history->get_num_cooked_changesets()) {
        progress.changeset_index++;
        sync_history->set_cooked_progress(progress);
    }
    realm->invalidate();
}

realm::Realm::Config Adapter::get_config(std::string path,
                                         util::Optional<std::string> realm_id,
                                         util::Optional<Schema> schema) {
    auto config = m_global_notifier->get_config(path, realm_id, std::move(schema));
    return config;
}
