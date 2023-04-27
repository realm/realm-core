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

#include <realm/impl/transact_log.hpp>

namespace realm::_impl {


bool TransactLogEncoder::select_table(TableKey key)
{
    size_t levels = 0;
    append_simple_instr(instr_SelectTable, levels, key.value); // Throws
    return true;
}

bool TransactLogEncoder::select_collection(ColKey col_key, ObjKey key)
{
    append_simple_instr(instr_SelectCollection, col_key, key.value); // Throws
    return true;
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw Exception(ErrorCodes::BadChangeset, "Bad transaction log");
}

} // namespace realm::_impl
