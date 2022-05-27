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

using namespace realm;
using namespace realm::util;

void Replication::initialize(DB&)
{
    // Nothing needs to be done here
}

void Replication::do_initiate_transact(Group&, version_type, bool)
{
    char* data = m_stream.get_data();
    size_t size = m_stream.get_size();
    m_encoder.set_buffer(data, data + size);
}

Replication::version_type Replication::prepare_commit(version_type orig_version)
{
    char* data = m_stream.get_data();
    size_t size = m_encoder.write_position() - data;
    version_type new_version = prepare_changeset(data, size, orig_version); // Throws
    return new_version;
}

void Replication::add_class(TableKey table_key, StringData, Table::Type)
{
    unselect_all();
    m_encoder.insert_group_level_table(table_key); // Throws
}

void Replication::add_class_with_primary_key(TableKey tk, StringData, DataType, StringData, bool,
                                             Table::Type table_type)
{
    REALM_ASSERT(table_type != Table::Type::Embedded);
    unselect_all();
    m_encoder.insert_group_level_table(tk); // Throws
}

void Replication::create_object(const Table* t, GlobalKey id)
{
    select_table(t);                              // Throws
    m_encoder.create_object(id.get_local_key(0)); // Throws
}

void Replication::create_object_with_primary_key(const Table* t, ObjKey key, Mixed)
{
    select_table(t);              // Throws
    m_encoder.create_object(key); // Throws
}

void Replication::do_select_table(const Table* table)
{
    m_encoder.select_table(table->get_key()); // Throws
    m_selected_table = table;
}

void Replication::do_select_collection(const CollectionBase& list)
{
    select_table(list.get_table().unchecked_ptr());
    ColKey col_key = list.get_col_key();
    ObjKey key = list.get_owner_key();

    m_encoder.select_collection(col_key, key); // Throws
    m_selected_list = CollectionId(list.get_table()->get_key(), key, col_key);
}

void Replication::list_clear(const CollectionBase& list)
{
    select_collection(list);           // Throws
    m_encoder.list_clear(list.size()); // Throws
}

void Replication::link_list_nullify(const Lst<ObjKey>& list, size_t link_ndx)
{
    select_collection(list);
    m_encoder.list_erase(link_ndx);
}

void Replication::dictionary_insert(const CollectionBase& dict, size_t ndx, Mixed key, Mixed)
{
    select_collection(dict);
    m_encoder.dictionary_insert(ndx, key);
}

void Replication::dictionary_set(const CollectionBase& dict, size_t ndx, Mixed key, Mixed)
{
    select_collection(dict);
    m_encoder.dictionary_set(ndx, key);
}

void Replication::dictionary_erase(const CollectionBase& dict, size_t ndx, Mixed key)
{
    select_collection(dict);
    m_encoder.dictionary_erase(ndx, key);
}
