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

struct EndOfFile {
};

enum Instruction {
    Add_Table = 0,
    Remove_Table = 1,
    Create_Object = 2,
    Rename_Column = 3,
    Add_Column = 4,
    Remove_Column = 5,
    Set = 6,
    Remove_Object = 7,
    Remove_Recursive = 8,
    Add_Column_Link = 9,
    Add_Column_Link_List = 10,
    Clear_Table = 11,
    Add_Search_Index = 12,
    Remove_Search_Index = 13,
    Commit = 14,
    Rollback = 15,
    Advance = 16,
    Move_Last_Over = 17,
    Close_And_Reopen = 18,
    Get_All_Column_Names = 19,
    Create_Table_View = 20,
    Compact = 21,
    Is_Null = 22,
    Enumerate_Column = 23,
    Count = 24
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