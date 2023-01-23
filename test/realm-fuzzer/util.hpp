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
    ADD_TABLE = 0,
    REMOVE_TABLE = 1,
    CREATE_OBJECT = 2,
    RENAME_COLUMN = 3,
    ADD_COLUMN = 4,
    REMOVE_COLUMN = 5,
    SET = 6,
    REMOVE_OBJECT = 7,
    REMOVE_RECURSIVE = 8,
    ADD_COLUMN_LINK = 9,
    ADD_COLUMN_LINK_LIST = 10,
    CLEAR_TABLE = 11,
    ADD_SEARCH_INDEX = 12,
    REMOVE_SEARCH_INDEX = 13,
    COMMIT = 14,
    ROLLBACK = 15,
    ADVANCE = 16,
    MOVE_LAST_OVER = 17,
    CLOSE_AND_REOPEN = 18,
    GET_ALL_COLUMN_NAMES = 19,
    CREATE_TABLE_VIEW = 20,
    COMPACT = 21,
    IS_NULL = 22,
    ENUMERATE_COLUMN = 23,
    ASYNC_WRITE = 24,
    ASYNC_CANCEL = 25,
    COUNT = 26
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