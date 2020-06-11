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

#include <realm/global_key.hpp>
#include <realm/impl/transact_log.hpp>

namespace realm {
namespace _impl {

TransactLogConvenientEncoder::TransactLogConvenientEncoder(TransactLogStream& stream)
    : m_encoder(stream)
{
}

TransactLogConvenientEncoder::~TransactLogConvenientEncoder() {}

void TransactLogConvenientEncoder::add_class(TableKey table_key, StringData, bool)
{
    unselect_all();
    m_encoder.insert_group_level_table(table_key); // Throws
}

void TransactLogConvenientEncoder::add_class_with_primary_key(TableKey tk, StringData, DataType, StringData, bool)
{
    unselect_all();
    m_encoder.insert_group_level_table(tk); // Throws
}

void TransactLogConvenientEncoder::create_object(const Table* t, GlobalKey id)
{
    select_table(t);                              // Throws
    m_encoder.create_object(id.get_local_key(0)); // Throws
}

void TransactLogConvenientEncoder::create_object_with_primary_key(const Table* t, GlobalKey id, Mixed)
{
    select_table(t);                                                                       // Throws
    m_encoder.create_object(_impl::TableFriend::global_to_local_object_id_hashed(*t, id)); // Throws
}

bool TransactLogEncoder::select_table(TableKey key)
{
    size_t levels = 0;
    append_simple_instr(instr_SelectTable, levels, key.value); // Throws
    return true;
}

void TransactLogConvenientEncoder::do_select_table(const Table* table)
{
    m_encoder.select_table(table->get_key()); // Throws
    m_selected_table = table;
}

bool TransactLogEncoder::select_list(ColKey col_key, ObjKey key)
{
    append_simple_instr(instr_SelectList, col_key, key.value); // Throws
    return true;
}


void TransactLogConvenientEncoder::do_select_list(const CollectionBase& list)
{
    select_table(list.get_table().unchecked_ptr());
    ColKey col_key = list.get_col_key();
    ObjKey key = list.CollectionBase::get_key();

    m_encoder.select_list(col_key, key); // Throws
    m_selected_list = LinkListId(list.get_table()->get_key(), key, col_key);
}

void TransactLogConvenientEncoder::list_clear(const CollectionBase& list)
{
    select_list(list);                 // Throws
    m_encoder.list_clear(list.size()); // Throws
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw BadTransactLog();
}

} // namespace _impl
} // namespace realm
