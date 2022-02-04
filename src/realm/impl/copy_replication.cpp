/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#include "copy_replication.hpp"
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <realm/dictionary.hpp>

namespace realm::impl {

void CopyReplication::add_class(TableKey, StringData name, bool is_embedded)
{
    if (auto existing_table = m_tr->get_table(name)) {
        if (existing_table->is_embedded() != is_embedded)
            throw std::runtime_error(util::format("Incompatible class: %1", name));
        return;
    }
    if (is_embedded) {
        m_tr->add_embedded_table(name);
    }
    else {
        m_tr->add_table(name);
    }
}

void CopyReplication::add_class_with_primary_key(TableKey, StringData name, DataType type, StringData pk_name,
                                                 bool nullable)
{
    if (auto existing_table = m_tr->get_table(name)) {
        auto pk_col = existing_table->get_primary_key_column();
        if (DataType(pk_col.get_type()) != type || existing_table->get_column_name(pk_col) != pk_name)
            throw std::runtime_error(util::format("Incompatible class: %1", name));
        return;
    }
    m_tr->add_table_with_primary_key(name, type, pk_name, nullable);
}

void CopyReplication::insert_column(const Table* t, ColKey col_key, DataType type, StringData name, Table* dest)
{
    m_current.table = t;
    auto table = get_table_in_destination_realm();
    if (ColKey existing_key = table->get_column_key(name)) {
        if (existing_key.get_type() != col_key.get_type() || existing_key.get_attrs() != col_key.get_attrs())
            throw std::runtime_error(util::format("Incompatible property: %1::%2", t->get_name(), name));
        return;
    }
    if (dest) {
        auto target_table_name = dest->get_name();
        auto target = m_tr->get_table(target_table_name);
        if (col_key.is_list()) {
            table->add_column_list(*target, name);
        }
        else if (col_key.is_set()) {
            table->add_column_set(*target, name);
        }
        else if (col_key.is_dictionary()) {
            table->add_column_dictionary(*target, name);
        }
        else {
            table->add_column(*target, name);
        }
    }
    else {
        if (col_key.is_list()) {
            table->add_column_list(type, name, col_key.is_nullable());
        }
        else if (col_key.is_set()) {
            table->add_column_set(type, name, col_key.is_nullable());
        }
        else if (col_key.is_dictionary()) {
            auto key_type = t->get_dictionary_key_type(col_key);
            table->add_column_dictionary(type, name, col_key.is_nullable(), key_type);
        }
        else {
            auto new_col_key = table->add_column(type, name, col_key.is_nullable());
            if (t->has_search_index(col_key)) {
                table->add_search_index(new_col_key);
            }
        }
    }
}

void CopyReplication::create_object_with_primary_key(const Table* t, ObjKey key, Mixed primary_key)
{
    m_current.table = t;
    m_current.obj_key = key;
    m_current.obj_in_destination = get_table_in_destination_realm()->create_object_with_primary_key(primary_key);
}

void CopyReplication::set(const Table* t, ColKey col_key, ObjKey key, Mixed value, _impl::Instruction)
{
    sync(t, key);
    auto dest_col_key = get_colkey_in_destination_realm(col_key);
    if (value.is_type(type_Link, type_TypedLink)) {
        value = handle_link(col_key, value, [&](TableRef dest_target_table) {
            // Check if dest obj has embedded obj already
            Obj embedded;
            if (auto key = m_current.obj_in_destination.get<ObjKey>(dest_col_key)) {
                embedded = dest_target_table->get_object(key);
            }
            else {
                // If not, create one
                embedded = m_current.obj_in_destination.create_and_set_linked_object(dest_col_key);
            }
            m_current.obj_in_destination = embedded;
        });
        if (value.is_null())
            return;
    }
    m_current.obj_in_destination.set_any(dest_col_key, value);
}

void CopyReplication::list_clear(const CollectionBase& coll)
{
    sync(coll);
    auto dest_col_key = get_colkey_in_destination_realm(coll.get_col_key());
    m_current.obj_in_destination.get_listbase_ptr(dest_col_key)->clear();
}

void CopyReplication::list_insert(const CollectionBase& coll, size_t idx, Mixed value, size_t)
{
    ColKey col_key = coll.get_col_key();
    sync(coll);
    auto dest_col_key = get_colkey_in_destination_realm(col_key);
    auto list = m_current.obj_in_destination.get_listbase_ptr(dest_col_key);
    if (value.is_type(type_Link, type_TypedLink)) {
        value = handle_link(col_key, value, [&](TableRef) {
            auto link_list = m_current.obj_in_destination.get_linklist(dest_col_key);
            // We know that list has been cleared before inserting members, so there is
            // no former value to check.
            m_current.obj_in_destination = link_list.create_and_insert_linked_object(idx);
        });
        if (value.is_null())
            return;
    }
    list->insert_any(idx, value);
}

void CopyReplication::set_insert(const CollectionBase& coll, size_t, Mixed value)
{
    ColKey col_key = coll.get_col_key();
    sync(coll);
    auto dest_col_key = get_colkey_in_destination_realm(col_key);
    auto set = m_current.obj_in_destination.get_setbase_ptr(dest_col_key);
    if (value.is_type(type_Link, type_TypedLink)) {
        value = handle_link(col_key, value, [](TableRef) {});
        REALM_ASSERT(!value.is_null()); // We can't have set of embedded objects
    }
    set->insert_any(value);
}

void CopyReplication::dictionary_insert(const CollectionBase& coll, size_t, Mixed key, Mixed value)
{
    ColKey col_key = coll.get_col_key();
    sync(coll);
    auto dest_col_key = get_colkey_in_destination_realm(col_key);
    auto dict = m_current.obj_in_destination.get_dictionary(dest_col_key);
    if (value.is_type(type_Link, type_TypedLink)) {
        value = handle_link(col_key, value, [&](TableRef dest_target_table) {
            // Check if dictionary obj has embedded obj already
            auto tmp = dict.try_get(key);
            Obj embedded;
            if (tmp && tmp->is_type(type_TypedLink)) {
                ObjKey key = tmp->get<ObjKey>();
                embedded = dest_target_table->get_object(key);
            }
            else {
                // If not, create one
                embedded = dict.create_and_insert_linked_object(key);
            }
            m_current.obj_in_destination = embedded;
        });
        if (value.is_null())
            return;
    }
    dict.insert(key, value);
}

void CopyReplication::sync(const Table* t, ObjKey obj_key)
{
    if (t != m_current.table || obj_key != m_current.obj_key) {
        // Processing of embedded objects is always depth first, so if we have just finished
        // an embedded object, the state of the parent will be in the stack
        while (!m_states.empty()) {
            m_current = m_states.back();
            m_states.pop_back();
            if (t == m_current.table && obj_key == m_current.obj_key)
                return;
        }
        auto obj = t->get_object(obj_key);
        if (auto pk_col = t->get_primary_key_column()) {
            // Updating a primary object
            auto pk = obj.get_any(pk_col);
            m_current.table = t;
            m_current.obj_key = obj_key;
            m_current.obj_in_destination = get_table_in_destination_realm()->get_object_with_primary_key(pk);
        }
        else {
            // Updating an embedded object
            auto first = true;
            obj.traverse_path(
                [&](const Obj& o, ColKey ck, Mixed index) {
                    if (first) {
                        sync(o.get_table().unchecked_ptr(), o.get_key());
                        first = false;
                    }
                    auto dest_col_key = get_colkey_in_destination_realm(ck);
                    Obj src_obj;
                    if (dest_col_key.is_list()) {
                        src_obj = o.get_linklist(ck).get_object(size_t(index.get_int()));
                        m_current.obj_in_destination = m_current.obj_in_destination.get_linklist(dest_col_key)
                                                           .get_object(size_t(index.get_int()));
                    }
                    else if (dest_col_key.is_dictionary()) {
                        src_obj = o.get_dictionary(ck).get_object(index.get_string());
                        m_current.obj_in_destination =
                            m_current.obj_in_destination.get_dictionary(dest_col_key).get_object(index.get_string());
                    }
                    else {
                        src_obj = o.get_linked_object(ck);
                        m_current.obj_in_destination = m_current.obj_in_destination.get_linked_object(dest_col_key);
                    }
                    m_current.table = src_obj.get_table().unchecked_ptr();
                    m_current.obj_key = src_obj.get_key();
                },
                [](auto) {});
        }
    }
}

Mixed CopyReplication::handle_link(ColKey col_key, Mixed val, util::FunctionRef<void(TableRef)> create_embedded_func)
{
    auto dest_col_key = get_colkey_in_destination_realm(col_key);
    TableRef target_table;
    TableRef dest_target_table;
    auto dest_table = get_table_in_destination_realm();
    if (val.is_type(type_TypedLink)) {
        ObjLink link = val.get_link();
        target_table = m_current.table->get_parent_group()->get_table(link.get_table_key());
        dest_target_table = m_tr->get_table(target_table->get_name());
    }
    else {
        target_table = m_current.table->get_opposite_table(col_key);
        dest_target_table = dest_table->get_opposite_table(dest_col_key);
    }
    if (auto pk_col = target_table->get_primary_key_column()) {
        auto target_obj = target_table->get_object(val.get<ObjKey>());
        auto pk = target_obj.get_any(pk_col);
        auto obj_key = dest_target_table->get_objkey_from_primary_key(pk);
        val = Mixed(ObjLink(dest_target_table->get_key(), obj_key));
        return val;
    }
    else {
        REALM_ASSERT(dest_target_table->is_embedded());
        // Push state and switch to child which will be processed next
        m_states.push_back(m_current);
        m_current.obj_key = val.get<ObjKey>();
        m_current.table = target_table.unchecked_ptr();
        create_embedded_func(dest_target_table);
    }
    return {};
}

} // namespace realm::impl
