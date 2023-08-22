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
#include <realm/util/overload.hpp>

namespace realm::_impl {


bool TransactLogEncoder::select_table(TableKey key)
{
    size_t levels = 0;
    append_simple_instr(instr_SelectTable, levels, key.value); // Throws
    return true;
}

bool TransactLogEncoder::select_collection(ColKey col_key, ObjKey key, const StablePath& path)
{
    auto path_size = path.size();
    if (path_size > 1) {
        append_simple_instr(instr_SelectCollectionByPath, col_key, key.value);
        append_simple_instr(path_size - 1);

        for (size_t n = 1; n < path_size; n++) {
            if (const auto int64_ptr = mpark::get_if<int64_t>(&path[n])) {
                append_simple_instr(*int64_ptr);
            }
            else if (const auto key_index_ptr = mpark::get_if<KeyIndex>(&path[n])) {
                append_simple_instr(0); // this is based solely on the fact that stable indices cannot be zero.
                char buffer[5];
                memcpy(buffer, key_index_ptr->get_string(), 4);
                buffer[4] = '\0';
                encode_string(StringData(buffer));
                append_simple_instr(key_index_ptr->get_key());
            }
        }
    }
    else {
        append_simple_instr(instr_SelectCollection, col_key, key.value); // Throws
    }
    return true;
}

void TransactLogEncoder::encode_string(StringData string)
{
    size_t max_required_bytes = max_enc_bytes_per_int + string.size();
    char* ptr = reserve(max_required_bytes); // Throws
    ptr = encode(ptr, size_t(string.size()));
    ptr = std::copy(string.data(), string.data() + string.size(), ptr);
    advance(ptr);
}

REALM_NORETURN
void TransactLogParser::parser_error() const
{
    throw Exception(ErrorCodes::BadChangeset, "Bad transaction log");
}

} // namespace realm::_impl
