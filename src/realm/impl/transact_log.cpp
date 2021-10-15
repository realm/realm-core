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
#include <realm/list.hpp>

namespace realm {
namespace _impl {


bool TransactLogEncoder::select_table(TableKey key)
{
    size_t levels = 0;
    append_simple_instr(instr_SelectTable, levels, key.value); // Throws
    return true;
}

bool TransactLogEncoder::select_collection(ColKey col_key, ObjKey key)
{
    append_simple_instr(instr_SelectList, col_key, key.value); // Throws
    return true;
}

/******************************** Dictionary *********************************/

bool TransactLogEncoder::dictionary_insert(size_t dict_ndx, Mixed key)
{
    REALM_ASSERT(key.get_type() == type_String);
    append_string_instr(instr_DictionaryInsert, key.get_string()); // Throws
    append_simple_instr(dict_ndx);
    return true;
}

bool TransactLogEncoder::dictionary_set(size_t dict_ndx, Mixed key)
{
    REALM_ASSERT(key.get_type() == type_String);
    append_string_instr(instr_DictionarySet, key.get_string()); // Throws
    append_simple_instr(dict_ndx);
    return true;
}

bool TransactLogEncoder::dictionary_erase(size_t ndx, Mixed key)
{
    REALM_ASSERT(key.get_type() == type_String);
    append_string_instr(instr_DictionaryErase, key.get_string()); // Throws
    append_simple_instr(ndx);
    return true;
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw BadTransactLog();
}

} // namespace _impl
} // namespace realm
