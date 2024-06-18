/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/replication.hpp>

#include <realm/list.hpp>
#include <realm/path.hpp>
#include <iostream>

using namespace realm;
using namespace realm::util;
using LogLevel = util::Logger::Level;

const char* Replication::history_type_name(int type)
{
    switch (type) {
        case hist_None:
            return "None";
        case hist_OutOfRealm:
            return "Local out of Realm";
        case hist_InRealm:
            return "Local in-Realm";
        case hist_SyncClient:
            return "SyncClient";
        case hist_SyncServer:
            return "SyncServer";
        default:
            return "Unknown";
    }
}

void Replication::initialize(DB&)
{
    // Nothing needs to be done here
}

void Replication::do_initiate_transact(Group&, version_type, bool)
{
    char* data = m_stream.get_data();
    size_t size = m_stream.get_size();
    m_encoder.set_buffer(data, data + size);
    m_most_recently_created_object.clear();
}

Replication::version_type Replication::prepare_commit(version_type orig_version)
{
    char* data = m_stream.get_data();
    size_t size = m_encoder.write_position() - data;
    version_type new_version = prepare_changeset(data, size, orig_version); // Throws
    return new_version;
}

void Replication::add_class(TableKey table_key, StringData name, Table::Type type)
{
    if (auto logger = would_log(LogLevel::debug)) {
        if (type == Table::Type::Embedded) {
            logger->log(LogCategory::object, LogLevel::debug, "Add %1 class '%2'", type, name);
        }
        else {
            logger->log(LogCategory::object, LogLevel::debug, "Add class '%1'", name);
        }
    }
    unselect_all();
    m_encoder.insert_group_level_table(table_key); // Throws
}

void Replication::add_class_with_primary_key(TableKey tk, StringData name, DataType pk_type, StringData pk_name, bool,
                                             Table::Type table_type)
{
    if (auto logger = would_log(LogLevel::debug)) {
        logger->log(LogCategory::object, LogLevel::debug, "Add %1 class '%2' with primary key property '%3' of %4",
                    table_type, Group::table_name_to_class_name(name), pk_name, pk_type);
    }
    REALM_ASSERT(table_type != Table::Type::Embedded);
    unselect_all();
    m_encoder.insert_group_level_table(tk); // Throws
}

void Replication::erase_class(TableKey tk, StringData table_name, size_t)
{
    if (auto logger = would_log(LogLevel::debug)) {
        logger->log(LogCategory::object, LogLevel::debug, "Remove class '%1'",
                    Group::table_name_to_class_name(table_name));
    }
    unselect_all();
    m_encoder.erase_class(tk); // Throws
}

void Replication::insert_column(const Table* t, ColKey col_key, DataType type, StringData col_name,
                                Table* target_table)
{
    if (auto logger = would_log(LogLevel::debug)) {
        const char* collection_type = "";
        if (col_key.is_collection()) {
            if (col_key.is_list()) {
                collection_type = "list ";
            }
            else if (col_key.is_dictionary()) {
                collection_type = "dictionary ";
            }
            else {
                collection_type = "set ";
            }
        }
        if (target_table) {
            logger->log(LogCategory::object, LogLevel::debug, "On class '%1': Add property '%2' %3linking '%4'",
                        t->get_class_name(), col_name, collection_type, target_table->get_class_name());
        }
        else {
            logger->log(LogCategory::object, LogLevel::debug, "On class '%1': Add property '%2' %3of %4",
                        t->get_class_name(), col_name, collection_type, type);
        }
    }
    select_table(t);                  // Throws
    m_encoder.insert_column(col_key); // Throws
}

void Replication::erase_column(const Table* t, ColKey col_key)
{
    if (auto logger = would_log(LogLevel::debug)) {
        logger->log(LogCategory::object, LogLevel::debug, "On class '%1': Remove property '%2'", t->get_class_name(),
                    t->get_column_name(col_key));
    }
    select_table(t);                 // Throws
    m_encoder.erase_column(col_key); // Throws
}

void Replication::track_new_object(ObjKey key)
{
    m_selected_obj = key;
    m_selected_collection = CollectionId();
    m_newly_created_object = true;

    auto table_index = m_selected_table->get_index_in_group();
    if (table_index >= m_most_recently_created_object.size()) {
        if (table_index >= m_most_recently_created_object.capacity())
            m_most_recently_created_object.reserve(table_index * 2);
        m_most_recently_created_object.resize(table_index + 1);
    }
    m_most_recently_created_object[table_index] = m_selected_obj;
}

void Replication::create_object(const Table* t, GlobalKey id)
{
    if (auto logger = would_log(LogLevel::debug)) {
        logger->log(LogCategory::object, LogLevel::debug, "Create object '%1'", t->get_class_name());
    }
    select_table(t);                              // Throws
    m_encoder.create_object(id.get_local_key(0)); // Throws
    track_new_object(id.get_local_key(0));        // Throws
}

void Replication::create_object_with_primary_key(const Table* t, ObjKey key, Mixed pk)
{
    if (auto logger = would_log(LogLevel::debug)) {
        logger->log(LogCategory::object, LogLevel::debug, "Create object '%1' with primary key %2",
                    t->get_class_name(), pk);
    }
    select_table(t);              // Throws
    m_encoder.create_object(key); // Throws
    track_new_object(key);
}

void Replication::create_linked_object(const Table* t, ObjKey key)
{
    select_table(t);       // Throws
    track_new_object(key); // Throws
    // Does not need to encode anything as embedded tables can't be observed
}

void Replication::remove_object(const Table* t, ObjKey key)
{
    if (auto logger = would_log(LogLevel::debug)) {
        if (t->is_embedded()) {
            logger->log(LogCategory::object, LogLevel::debug, "Remove embedded object '%1'", t->get_class_name());
        }
        else if (t->get_primary_key_column()) {
            logger->log(LogCategory::object, LogLevel::debug, "Remove object '%1' with primary key %2",
                        t->get_class_name(), t->get_primary_key(key));
        }
        else {
            logger->log(LogCategory::object, LogLevel::debug, "Remove object '%1'[%2]", t->get_class_name(), key);
        }
    }
    select_table(t);              // Throws
    m_encoder.remove_object(key); // Throws
}

void Replication::do_select_table(const Table* table)
{
    m_encoder.select_table(table->get_key()); // Throws
    m_selected_table = table;
    m_selected_collection = CollectionId();
    m_selected_obj = ObjKey();
}

void Replication::do_select_obj(ObjKey key)
{
    m_selected_obj = key;
    m_selected_collection = CollectionId();

    auto table_index = m_selected_table->get_index_in_group();
    if (table_index < m_most_recently_created_object.size()) {
        m_newly_created_object = m_most_recently_created_object[table_index] == key;
    }
    else {
        m_newly_created_object = false;
    }

    if (auto logger = would_log(LogLevel::debug)) {
        auto class_name = m_selected_table->get_class_name();
        if (m_selected_table->get_primary_key_column()) {
            auto pk = m_selected_table->get_primary_key(key);
            logger->log(LogCategory::object, LogLevel::debug, "Mutating object '%1' with primary key %2", class_name,
                        pk);
        }
        else if (m_selected_table->is_embedded()) {
            auto obj = m_selected_table->get_object(key);
            logger->log(LogCategory::object, LogLevel::debug, "Mutating object '%1' with path '%2'", class_name,
                        obj.get_id());
        }
        else {
            logger->log(LogCategory::object, LogLevel::debug, "Mutating anonymous object '%1'[%2]", class_name, key);
        }
    }
}

void Replication::do_select_collection(const CollectionBase& coll)
{
    select_table(coll.get_table().unchecked_ptr());
    ColKey col_key = coll.get_col_key();
    ObjKey key = coll.get_owner_key();
    auto path = coll.get_stable_path();

    if (select_obj(key)) {
        m_encoder.select_collection(col_key, key, path); // Throws
    }
    m_selected_collection = CollectionId(coll.get_table()->get_key(), key, std::move(path));
}

void Replication::do_set(const Table* t, ColKey col_key, ObjKey key, _impl::Instruction variant)
{
    if (variant != _impl::Instruction::instr_SetDefault) {
        select_table(t); // Throws
        if (select_obj(key)) {
            m_encoder.modify_object(col_key, key); // Throws
        }
    }
}

void Replication::set(const Table* t, ColKey col_key, ObjKey key, Mixed value, _impl::Instruction variant)
{
    do_set(t, col_key, key, variant); // Throws
    if (auto logger = would_log(LogLevel::trace)) {
        if (col_key.get_type() == col_type_Link && value.is_type(type_Link)) {
            auto target_table = t->get_opposite_table(col_key);
            if (target_table->is_embedded()) {
                logger->log(LogCategory::object, LogLevel::trace, "   Creating embedded object '%1' in '%2'",
                            target_table->get_class_name(), t->get_column_name(col_key));
            }
            else if (target_table->get_primary_key_column()) {
                auto link = value.get<ObjKey>();
                auto pk = target_table->get_primary_key(link);
                logger->log(LogCategory::object, LogLevel::trace,
                            "   Linking object '%1' with primary key %2 from '%3'", target_table->get_class_name(),
                            pk, t->get_column_name(col_key));
            }
            else {
                logger->log(LogCategory::object, LogLevel::trace, "   Linking object '%1'[%2] from '%3'",
                            target_table->get_class_name(), key, t->get_column_name(col_key));
            }
        }
        else {
            logger->log(LogCategory::object, LogLevel::trace, "   Set '%1' to %2", t->get_column_name(col_key),
                        value.to_string(util::Logger::max_width_of_value));
        }
    }
}

void Replication::nullify_link(const Table* t, ColKey col_key, ObjKey key)
{
    select_table(t); // Throws
    if (select_obj(key)) {
        m_encoder.modify_object(col_key, key); // Throws
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Nullify '%1'", t->get_column_name(col_key));
    }
}

void Replication::add_int(const Table* t, ColKey col_key, ObjKey key, int_fast64_t value)
{
    do_set(t, col_key, key); // Throws
    if (auto logger = get_logger()) {
        logger->log(LogCategory::object, LogLevel::trace, "   Adding %1 to '%2'", value, t->get_column_name(col_key));
    }
}

Path Replication::get_prop_name(Path&& path) const
{
    auto col_key = path[0].get_col_key();
    auto prop_name = m_selected_table->get_column_name(col_key);
    path[0] = PathElement(prop_name);
    return std::move(path);
}

void Replication::log_collection_operation(const char* operation, const CollectionBase& collection, Mixed value,
                                           Mixed index) const
{
    auto logger = would_log(LogLevel::trace);
    if (REALM_LIKELY(!logger))
        return;

    auto path = collection.get_short_path();
    auto col_key = path[0].get_col_key();
    auto prop_name = m_selected_table->get_column_name(col_key);
    path[0] = PathElement(prop_name);
    std::string position;
    if (!index.is_null()) {
        position = util::format(" at position %1", index);
    }
    if (Table::is_link_type(col_key.get_type()) && value.is_type(type_Link)) {
        auto target_table = m_selected_table->get_opposite_table(col_key);
        if (target_table->is_embedded()) {
            logger->log(LogCategory::object, LogLevel::trace, "   %1 embedded object '%2' in %3%4 ", operation,
                        target_table->get_class_name(), path, position);
        }
        else if (target_table->get_primary_key_column()) {
            auto link = value.get<ObjKey>();
            auto pk = target_table->get_primary_key(link);
            logger->log(LogCategory::object, LogLevel::trace, "   %1 object '%2' with primary key %3 in %4%5",
                        operation, target_table->get_class_name(), pk, path, position);
        }
        else {
            auto link = value.get<ObjKey>();
            logger->log(LogCategory::object, LogLevel::trace, "   %1 object '%2'[%3] in %4%5", operation,
                        target_table->get_class_name(), link, path, position);
        }
    }
    else {
        logger->log(LogCategory::object, LogLevel::trace, "   %1 %2 in %3%4", operation,
                    value.to_string(util::Logger::max_width_of_value), path, position);
    }
}

void Replication::list_insert(const CollectionBase& list, size_t list_ndx, Mixed value, size_t)
{
    if (select_collection(list)) {                                   // Throws
        m_encoder.collection_insert(list.translate_index(list_ndx)); // Throws
    }
    log_collection_operation("Insert", list, value, int64_t(list_ndx));
}

void Replication::list_set(const CollectionBase& list, size_t list_ndx, Mixed value)
{
    if (select_collection(list)) {                                // Throws
        m_encoder.collection_set(list.translate_index(list_ndx)); // Throws
    }
    log_collection_operation("Set", list, value, int64_t(list_ndx));
}

void Replication::list_erase(const CollectionBase& list, size_t link_ndx)
{
    if (select_collection(list)) {                                  // Throws
        m_encoder.collection_erase(list.translate_index(link_ndx)); // Throws
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Erase '%1' at position %2",
                    get_prop_name(list.get_short_path()), link_ndx);
    }
}

void Replication::list_move(const CollectionBase& list, size_t from_link_ndx, size_t to_link_ndx)
{
    if (select_collection(list)) {                                                                         // Throws
        m_encoder.collection_move(list.translate_index(from_link_ndx), list.translate_index(to_link_ndx)); // Throws
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Move %1 to %2 in '%3'", from_link_ndx, to_link_ndx,
                    get_prop_name(list.get_short_path()));
    }
}

void Replication::set_insert(const CollectionBase& set, size_t set_ndx, Mixed value)
{
    Replication::list_insert(set, set_ndx, value, 0); // Throws
}

void Replication::set_erase(const CollectionBase& set, size_t set_ndx, Mixed)
{
    Replication::list_erase(set, set_ndx); // Throws
}

void Replication::set_clear(const CollectionBase& set)
{
    Replication::list_clear(set); // Throws
}

void Replication::list_clear(const CollectionBase& list)
{
    if (select_collection(list)) {               // Throws
        m_encoder.collection_clear(list.size()); // Throws
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Clear '%1'", get_prop_name(list.get_short_path()));
    }
}

void Replication::link_list_nullify(const Lst<ObjKey>& list, size_t link_ndx)
{
    if (select_collection(list)) { // Throws
        m_encoder.collection_erase(link_ndx);
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Nullify '%1' position %2",
                    m_selected_table->get_column_name(list.get_col_key()), link_ndx);
    }
}

void Replication::dictionary_insert(const CollectionBase& dict, size_t ndx, Mixed key, Mixed value)
{
    if (select_collection(dict)) { // Throws
        m_encoder.collection_insert(ndx);
    }
    log_collection_operation("Insert", dict, value, key);
}

void Replication::dictionary_set(const CollectionBase& dict, size_t ndx, Mixed key, Mixed value)
{
    if (select_collection(dict)) { // Throws
        m_encoder.collection_set(ndx);
    }
    log_collection_operation("Set", dict, value, key);
}

void Replication::dictionary_erase(const CollectionBase& dict, size_t ndx, Mixed key)
{
    if (select_collection(dict)) { // Throws
        m_encoder.collection_erase(ndx);
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Erase %1 from '%2'", key,
                    get_prop_name(dict.get_short_path()));
    }
}

void Replication::dictionary_clear(const CollectionBase& dict)
{
    if (select_collection(dict)) { // Throws
        m_encoder.collection_clear(dict.size());
    }
    if (auto logger = would_log(LogLevel::trace)) {
        logger->log(LogCategory::object, LogLevel::trace, "   Clear '%1'", get_prop_name(dict.get_short_path()));
    }
}
