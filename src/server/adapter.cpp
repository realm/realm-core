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

#include "admin_realm.hpp"
#include "impl/realm_coordinator.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"

#include <realm/sync/changeset_cooker.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/impl/transact_log.hpp>
#include <realm/impl/input_stream.hpp>
#include <realm/util/base64.hpp>

#include <json.hpp>
#include <unordered_map>
#include <unordered_set>

namespace {
// This needs to appear before `using namespace realm`. After that,
// realm::Schema's operator== becomes a candidate function to call for the
// comparison of array values within json. This occurs in a SFINAE context
// and so should simply be discarded as a candidate, but VC++ 2017 incorrectly
// considers it a hard error.
bool equals(nlohmann::json const& a, nlohmann::json const& b) {
    return a == b;
}
} // anonymous namespace

using namespace realm;

using ObjectID = sync::ObjectID;
using Instruction = sync::Instruction;

namespace {
static PropertyType from_core_type(DataType type)
{
    switch (type) {
        case type_Int:       return PropertyType::Int;
        case type_Float:     return PropertyType::Float;
        case type_Double:    return PropertyType::Double;
        case type_Bool:      return PropertyType::Bool;
        case type_String:    return PropertyType::String;
        case type_Binary:    return PropertyType::Data;
        case type_Timestamp: return PropertyType::Date;
        case type_Mixed:     return PropertyType::Any;
        case type_Link:      return PropertyType::Object | PropertyType::Nullable;
        case type_LinkList:  return PropertyType::Object | PropertyType::Array;
        case type_Table:     REALM_ASSERT(false && "Use ObjectSchema::from_core_type if subtables are a possibility");
        default: REALM_UNREACHABLE();
    }
}

class ChangesetCookerInstructionHandler final : public sync::InstructionHandler {
public:
    friend Adapter;

    ChangesetCookerInstructionHandler(const Group &group, util::Logger& logger, util::AppendBuffer<char>& out_buffer)
    : m_group(group)
    , m_table_info(m_group)
    , m_logger(logger)
    , m_out_buffer(out_buffer)
    {
    }

    const Group &m_group;
    sync::TableInfoCache m_table_info;
    util::Logger& m_logger;
    util::AppendBuffer<char>& m_out_buffer;
    std::unordered_map<std::string, ObjectSchema> m_schema;

    std::unordered_map<std::string, std::unordered_map<ObjectID, int64_t>> m_int_primaries;
    std::unordered_map<std::string, std::unordered_map<ObjectID, std::string>> m_string_primaries;
    std::unordered_map<std::string, std::unordered_set<ObjectID>> m_null_primaries;

    nlohmann::json m_pending_instruction = nullptr;

    std::string m_selected_object_type;
    ConstTableRef m_selected_table;
    ObjectSchema *m_selected_object_schema = nullptr;
    Property *m_selected_primary = nullptr;

    std::string m_list_property_name;
    nlohmann::json m_list_parent_identity;

    ConstTableRef m_list_target_table;
    ObjectSchema *m_list_target_object_schema = nullptr;
    Property *m_list_target_primary = nullptr;

    void flush() {
        if (m_pending_instruction.is_null())
            return;
        if (m_out_buffer.size())
            m_out_buffer.append(",", 1);
        else
            m_out_buffer.append("[", 1);
        auto str = m_pending_instruction.dump();
        m_out_buffer.append(str.data(), str.size());
        m_pending_instruction = nullptr;
    }

    bool finish() {
        flush();
        if (!m_out_buffer.size())
            return false;
        m_out_buffer.append("]", 1);
        return true;
    }

    void add_instruction(Adapter::InstructionType type,
                         nlohmann::json &&inst = {}, bool collapsible = false,
                         util::Optional<std::string> object_type = util::none) {
        if (!object_type && !m_selected_object_schema) {
            return; // FIXME: support objects without schemas
        }
        flush();

        inst["type"] = Adapter::instruction_type_string(type);
        inst["object_type"] = object_type ? *object_type : m_selected_object_schema->name;
        m_pending_instruction = std::move(inst);
        if (!collapsible)
            flush();
    }

    void add_set_instruction(ObjectID row, StringData column, nlohmann::json &&value) {
        nlohmann::json identity = get_identity(row, *m_selected_table, m_selected_primary);

        // collapse values if inserting/setting values for the last object
        if (!m_pending_instruction.is_null()) {
            nlohmann::json &last = m_pending_instruction;
            if (equals(identity, last["identity"]) && m_selected_object_schema && m_selected_object_schema->name == last["object_type"].get<std::string>()) {
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
        if (!m_pending_instruction.is_null()) {
            nlohmann::json &last = m_pending_instruction;
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
        }}, true, object_type);
    }

    nlohmann::json get_identity(ObjectID object_id, const Table& table, Property *primary_key) {
        if (primary_key) {
            std::string object_type = ObjectStore::object_type_for_table_name(table.get_name());

            if (is_nullable(primary_key->type)) {
                auto& null_primaries = m_null_primaries[object_type];
                auto it = null_primaries.find(object_id);
                if (it != null_primaries.end())
                    return nullptr;
            }

            if (primary_key->type == PropertyType::Int) {
                auto& int_primaries = m_int_primaries[object_type];
                auto it = int_primaries.find(object_id);
                if (it != int_primaries.end()) {
                    return it->second;
                }

                size_t row = sync::row_for_object_id(m_table_info, table, object_id);
                REALM_ASSERT(row != npos);
                if (is_nullable(primary_key->type) && table.is_null(primary_key->table_column, row)) {
                    return nullptr;
                }
                return table.get_int(primary_key->table_column, row);
            }
            else if (primary_key->type == PropertyType::String) {
                auto& string_primaries = m_string_primaries[object_type];
                auto it = string_primaries.find(object_id);
                if (it != string_primaries.end()) {
                    return it->second;
                }

                size_t row = sync::row_for_object_id(m_table_info, table, object_id);
                REALM_ASSERT(row != npos);
                StringData value = table.get_string(primary_key->table_column, row);
                if (value.is_null())
                    return nullptr;
                return std::string(value);
            }
        }

        return object_id.to_string();
    }

    void select(std::string const& object_type, ObjectSchema *&out_object_schema, ConstTableRef &out_table, Property *&out_primary) {
        out_object_schema = nullptr;
        out_primary = nullptr;
        out_table = ConstTableRef();

        if (object_type.empty()) {
            return;
        }
        auto it = m_schema.find(object_type);
        if (it == m_schema.end()) {
            out_table = ObjectStore::table_for_object_type(m_group, object_type);
            if (!out_table) {
                return;
            }
            it = m_schema.emplace(std::piecewise_construct,
                                  std::forward_as_tuple(object_type),
                                  std::forward_as_tuple(m_group, object_type, out_table->get_index_in_group())).first;
        }

        out_object_schema = &it->second;
        if (!out_table)
            out_table = ObjectStore::table_for_object_type(m_group, object_type);
        out_primary = it->second.primary_key_property();
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
        m_selected_object_type = get_string(instr.table);
        select(m_selected_object_type, m_selected_object_schema, m_selected_table, m_selected_primary);
    }

    void operator()(const Instruction::SelectField& instr)
    {
        REALM_ASSERT(m_selected_object_schema);

        m_list_parent_identity = get_identity(instr.object, *m_selected_table, m_selected_primary);
        m_list_property_name = get_string(instr.field);

        std::string link_target_table = get_string(instr.link_target_table);
        select(link_target_table, m_list_target_object_schema, m_list_target_table, m_list_target_primary);
    }

    void operator()(const Instruction::AddTable& instr)
    {
        std::string object_type = get_string(instr.table);
        if (object_type.size()) {
            nlohmann::json dict = {{"properties", nullptr}};
            if (instr.has_primary_key) {
                dict["primary_key"] = get_string(instr.primary_key_field);
                dict["properties"][get_string(instr.primary_key_field)] = {
                    {"nullable", instr.primary_key_nullable},
                    {"type", string_for_property_type(from_core_type(instr.primary_key_type))}
                };
            }
            add_instruction(Adapter::InstructionType::AddType, std::move(dict),
                            true, object_type);
        }
    }

    void operator()(const Instruction::EraseTable&)
    {
        REALM_ASSERT(0);
    }

    // Must have table selected:
    void operator()(const Instruction::CreateObject& instr)
    {
        if (!m_selected_object_schema) {
            m_logger.warn("Adapter: Ignoring CreateObject instruction with no object schema");
            return; // FIXME: Support objects without schemas
        }

        nlohmann::json identity;
        nlohmann::json values;

        if (instr.has_primary_key) {
            if (instr.payload.type == type_Int) {
                identity = instr.payload.data.integer;
                m_int_primaries[m_selected_object_type][instr.object] = instr.payload.data.integer;
            }
            else if (instr.payload.type == type_String) {
                std::string value = get_string(instr.payload.data.str);
                identity = value;
                m_string_primaries[m_selected_object_type][instr.object] = value;
            }
            else if (instr.payload.is_null()) {
                identity = nullptr;
                m_null_primaries[m_selected_object_type].insert(instr.object);
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
        if (!m_selected_object_schema) {
            m_logger.warn("Adapter: Ignoring EraseObject instruction with no object schema");
            return; // FIXME: Support objects without schemas
        }

        add_instruction(Adapter::InstructionType::Delete, {
            {"identity", get_identity(instr.object, *m_selected_table, m_selected_primary)}
        });

        if (m_selected_primary) {
            auto& int_primaries    = m_int_primaries[m_selected_object_type];
            auto& string_primaries = m_string_primaries[m_selected_object_type];
            auto& null_primaries   = m_null_primaries[m_selected_object_type];

            // invalidate caches
            int_primaries.erase(instr.object);
            string_primaries.erase(instr.object);
            null_primaries.erase(instr.object);
        }
    }

    void operator()(const Instruction::Set& instr)
    {
        if (!m_selected_object_schema) {
            m_logger.warn("Adapter: Ignoring Set instruction with no object schema");
            return; // FIXME: Support objects without schemas
        }

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
                StringData data = get_string(instr.payload.data.str);
                auto encoded_size = util::base64_encoded_size(data.size());
                std::vector<char> encoded_data(encoded_size + 1, '\0');
                util::base64_encode(data.data(), data.size(), encoded_data.data(), encoded_data.size());
                return add_set_instruction(instr.object, field, {"data64", encoded_data.data()});
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

    void operator()(const Instruction::AddInteger&)
    {
        // FIXME
        REALM_TERMINATE("AddInteger not supported by adapter.");
    }

    void operator()(const Instruction::InsertSubstring&)
    {
        // FIXME
        REALM_TERMINATE("InsertSubstring not supported by adapter.");
    }

    void operator()(const Instruction::EraseSubstring&)
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
        if (m_selected_object_type.size()) {
            if (instr.type == type_Link || instr.type == type_LinkList) {
                add_column_instruction(m_selected_object_type, get_string(instr.field), {
                    {"type", (instr.type == type_Link ? "object" : "list")},
                    {"object_type", get_string(instr.link_target_table)}
                });
            }
            else if (instr.type == type_Table) {
                // FIXME: Arrays of primitives are not yet supported.
            }
            else {
                add_column_instruction(m_selected_object_type, get_string(instr.field), {
                    {"type", string_for_property_type(from_core_type(instr.type))},
                    {"nullable", instr.nullable}
                });
            }
        }
    }

    void operator()(const Instruction::EraseColumn&)
    {
        REALM_TERMINATE("EraseColumn not supported by adapter.");
    }

    // Must have linklist selected:
    void operator()(const Instruction::ArraySet& instr)
    {
        if (!m_list_property_name.size()) {
            m_logger.warn("Adapter: Ignoring ArraySet instruction on unknown list property");
            return; // FIXME
        }

        // FIXME: Support arrays of primitives

        add_instruction(Adapter::InstructionType::ListSet, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property_name},
            {"list_index", instr.ndx},
            {"object_identity", get_identity(instr.payload.data.link.target, *m_list_target_table, m_list_target_primary)}
        });
    }

    void operator()(const Instruction::ArrayInsert& instr)
    {
        if (!m_list_property_name.size()) {
            m_logger.warn("Adapter: Ignoring ArrayInsert instruction on unknown list property");
            return; // FIXME
        }


        // FIXME: Support arrays of primitives

        add_instruction(Adapter::InstructionType::ListInsert, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property_name},
            {"list_index", instr.ndx},
            {"object_identity", get_identity(instr.payload.data.link.target, *m_list_target_table, m_list_target_primary)}
        });
    }

    void operator()(const Instruction::ArrayMove&)
    {
        if (!m_list_property_name.size())
            return; // FIXME

        REALM_TERMINATE("ArrayMove not supported by adapter.");
    }

    void operator()(const Instruction::ArraySwap&)
    {
        if (!m_list_property_name.size())
            return; // FIXME

        REALM_TERMINATE("ArraySwap not supported by adapter.");
    }

    void operator()(const Instruction::ArrayErase& instr)
    {
        if (!m_list_property_name.size())
            return; // FIXME

        add_instruction(Adapter::InstructionType::ListErase, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property_name},
            {"list_index", instr.ndx},
        });
    }

    void operator()(const Instruction::ArrayClear&)
    {
        if (!m_list_property_name.size())
            return; // FIXME

        add_instruction(Adapter::InstructionType::ListClear, {
            {"identity", m_list_parent_identity},
            {"property", m_list_property_name},
        });
    }

    void operator()(const Instruction& instr) final override
    {
        instr.visit(*this);
    }
};

class ChangesetCooker final : public sync::ClientHistory::ChangesetCooker {
public:
    ChangesetCooker(util::Logger& logger) : m_logger(logger) { }

    bool cook_changeset(const Group& group, const char* changeset,
                        std::size_t changeset_size,
                        util::AppendBuffer<char>& out_buffer) override {
        _impl::SimpleNoCopyInputStream stream(changeset, changeset_size);
        ChangesetCookerInstructionHandler cooker_handler(group, m_logger, out_buffer);
        sync::ChangesetParser().parse(stream, cooker_handler);
        return cooker_handler.finish();
    }

private:
    util::Logger& m_logger;
};

} // anonymous namespace

class Adapter::Impl final : public AdminRealmListener {
public:
    Impl(std::function<void(std::string)> realm_changed, std::regex regex,
         std::string local_root_dir, SyncConfig sync_config_template);

    Realm::Config get_config(StringData virtual_path, util::Optional<Schema> schema) const;

    using AdminRealmListener::start;

private:
    void register_realm(sync::ObjectID, StringData virtual_path) override;
    void unregister_realm(sync::ObjectID, StringData) override {}
    void error(std::exception_ptr) override {} // FIXME
    void download_complete() override {}

    const std::string m_server_base_url;
    std::shared_ptr<SyncUser> m_user;
    std::function<SyncBindSessionHandler> m_bind_callback;

    const std::unique_ptr<util::Logger> m_logger;
    const std::shared_ptr<ChangesetCooker> m_transformer;

    const std::function<void(std::string)> m_realm_changed;
    const std::regex m_regex;

    std::vector<std::shared_ptr<_impl::RealmCoordinator>> m_realms;

};

Adapter::Impl::Impl(std::function<void(std::string)> realm_changed, std::regex regex,
                    std::string local_root_dir, SyncConfig sync_config_template)
: AdminRealmListener(std::move(local_root_dir), std::move(sync_config_template))
, m_logger(SyncManager::shared().make_logger())
, m_transformer(std::make_shared<ChangesetCooker>(*m_logger))
, m_realm_changed(std::move(realm_changed))
, m_regex(std::move(regex))
{
}

Realm::Config Adapter::Impl::get_config(StringData virtual_path, util::Optional<Schema> schema) const {
    Realm::Config config = AdminRealmListener::get_config(virtual_path);
    if (schema) {
        config.schema = std::move(schema);
        config.schema_version = 0;
    }
    config.sync_config->transformer = m_transformer;
    return config;
}

void Adapter::Impl::register_realm(sync::ObjectID, StringData virtual_path) {
    std::string path = virtual_path;
    if (!std::regex_match(path, m_regex))
        return;

    auto coordinator = _impl::RealmCoordinator::get_coordinator(get_config(path, util::none));
    std::weak_ptr<Impl> weak_self = std::static_pointer_cast<Impl>(shared_from_this());
    coordinator->set_transaction_callback([path = std::move(path), weak_self = std::move(weak_self)](VersionID, VersionID) {
        if (auto self = weak_self.lock())
            self->m_realm_changed(path);
    });
    m_realms.push_back(coordinator);
}

Adapter::Adapter(std::function<void(std::string)> realm_changed, std::regex regex,
                 std::string local_root_dir, SyncConfig sync_config_template)
: m_impl(std::make_shared<Adapter::Impl>(std::move(realm_changed), std::move(regex),
                                         std::move(local_root_dir), std::move(sync_config_template)))
{
    m_impl->start();
}

util::Optional<util::AppendBuffer<char>> Adapter::current(std::string realm_path) {
    auto history = realm::sync::make_client_history(get_config(realm_path, util::none).path);
    SharedGroup sg(*history);

    auto progress = history->get_cooked_progress();
    if (progress.changeset_index >= history->get_num_cooked_changesets()) {
        return util::none;
    }

    util::AppendBuffer<char> buffer;
    history->get_cooked_changeset(progress.changeset_index, buffer);
    return buffer;
}

void Adapter::advance(std::string realm_path) {
    auto history = realm::sync::make_client_history(get_config(realm_path, util::none).path);
    SharedGroup sg(*history);

    auto progress = history->get_cooked_progress();
    if (progress.changeset_index < history->get_num_cooked_changesets()) {
        progress.changeset_index++;
        history->set_cooked_progress(progress);
    }
}

Realm::Config Adapter::get_config(std::string path, util::Optional<Schema> schema) {
    return m_impl->get_config(path, std::move(schema));
}
