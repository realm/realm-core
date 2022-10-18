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

#ifndef FUZZ_UTIL_HPP
#define FUZZ_UTIL_HPP

#include <string>

struct State {
    std::string str;
    size_t pos;
};

struct EndOfFile {};

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


#define TEST_FUZZ
// #ifdef TEST_FUZZ
//  Determines whether or not to run the shared group verify function
//  after each transaction. This will find errors earlier but is expensive.
#define REALM_VERIFY true

#if REALM_VERIFY
#define REALM_DO_IF_VERIFY(log, op)                                                                                  \
    do {                                                                                                             \
        log << #op << ";\n";                                                                                         \
        op;                                                                                                          \
    } while (false)
#else
#define REALM_DO_IF_VERIFY(log, owner)                                                                               \
    do {                                                                                                             \
    } while (false)
#endif

#endif