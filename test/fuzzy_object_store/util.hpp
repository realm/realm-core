/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#include <string>

namespace {

struct State {
    std::string str;
    size_t pos;
};

struct EndOfFile {};

unsigned char get_next(State& s)
{
    if (s.pos == s.str.size()) {
        throw EndOfFile{};
    }
    char byte = s.str[s.pos];
    s.pos++;
    return byte;
}

enum INS {
    ADD_TABLE,
    REMOVE_TABLE,
    CREATE_OBJECT,
    RENAME_COLUMN,
    ADD_COLUMN,
    REMOVE_COLUMN,
    SET,
    REMOVE_OBJECT,
    REMOVE_RECURSIVE,
    ADD_COLUMN_LINK,
    ADD_COLUMN_LINK_LIST,
    CLEAR_TABLE,
    ADD_SEARCH_INDEX,
    REMOVE_SEARCH_INDEX,
    COMMIT,
    ROLLBACK,
    ADVANCE,
    MOVE_LAST_OVER,
    CLOSE_AND_REOPEN,
    GET_ALL_COLUMN_NAMES,
    CREATE_TABLE_VIEW,
    COMPACT,
    IS_NULL,
    ENUMERATE_COLUMN,

    ASYNC_WRITE,
    ASYNC_CANCEL,
    ASYNC_RUN,

    COUNT
};


} // namespace