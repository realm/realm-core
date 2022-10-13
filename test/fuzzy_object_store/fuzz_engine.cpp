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

#include "fuzz_engine.hpp"
#include "fuzz_configurator.hpp"
#include "fuzz_object.hpp"

#include <realm.hpp>
#include <realm/index_string.hpp>
#include <realm/object-store/shared_realm.hpp>

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

using namespace realm;
using namespace realm::util;

#define TEST_FUZZ
// #ifdef TEST_FUZZ
//  Determines whether or not to run the shared group verify function
//  after each transaction. This will find errors earlier but is expensive.
#define REALM_VERIFY true

#if REALM_VERIFY
#define REALM_DO_IF_VERIFY(log, op)                                                                                  \
    do {                                                                                                             \
        if (log)                                                                                                     \
            *log << #op << ";\n";                                                                                    \
        op;                                                                                                          \
    } while (false)
#else
#define REALM_DO_IF_VERIFY(log, owner)                                                                               \
    do {                                                                                                             \
    } while (false)
#endif

namespace {

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

// You can use this variable to make a conditional breakpoint if you know that
// a problem occurs after a certain amount of iterations.
int iteration = 0;

const size_t add_empty_row_max = REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1000;
const size_t max_tables = REALM_MAX_BPNODE_SIZE * 10;

// Max number of rows in a table. Overridden only by create_object() and only in the case where
// max_rows is not exceeded *prior* to executing add_empty_row.
const size_t max_rows = 100000;

} // anonymous namespace

int FuzzEngine::run_fuzzy_engine(int argc, const char* argv[])
{
    auto& instance = FuzzConfigurator::init(argc, argv);
    if (instance.logging) {
        instance.m_log << "Going to fuzz shared_realm ... \n";
    }
    auto logger = instance.logging ? &instance.m_log : nullptr;
    do_fuzz(instance.m_contents, instance.m_path, logger);
    return 0;
}

void FuzzEngine::do_fuzz(std::string& in, const std::string& path, std::ostream* log)
{
    column_index = table_index = 0;

    State s;
    s.str = in;
    s.pos = 0;

    // const bool use_encryption = false;
    const bool use_encryption = get_next(s) % 2 == 0;

    struct TestConfig : public Realm::Config {
        TestConfig(std::string local_path, bool use_encryption)
        {
            disable_sync_to_disk();
            path = local_path;
            schema_version = 0;
            if (use_encryption) {
                const char* key = get_encryption_key();
                const char* i = key;
                while (*i != '\0') {
                    encryption_key.push_back(*i);
                    i++;
                }
            }
        }
    };
    TestConfig config{path, use_encryption};


    if (log) {
        *log << "// Test case generated in " REALM_VER_CHUNK " on " << get_current_time_stamp() << ".\n";
        *log << "// REALM_MAX_BPNODE_SIZE is " << REALM_MAX_BPNODE_SIZE << "\n";
        *log << "// ----------------------------------------------------------------------\n";
        std::string printable_key;
        if (!use_encryption) {
            printable_key = "nullptr";
        }
        else {
            printable_key = std::string("\"") + config.encryption_key.data() + "\"";
        }
        *log << "const char* key = " << printable_key << ";\n";
        *log << "\n";
    }
    auto shared_realm = Realm::get_shared_realm(config);
    FuzzObject fuzz_object;
    std::vector<TableView> table_views;

    auto fetch_group = [shared_realm]() -> Group& {
        if (!shared_realm->is_in_transaction()) {
            shared_realm->begin_transaction();
        }
        return shared_realm->read_group();
    };

    try {
        for (;;) {
            char instr = get_next(s) % COUNT;
            iteration++;

            // This can help when debugging
            if (log) {
                *log << iteration << " ";
            }

            Group& group = fetch_group();

            if (instr == ADD_TABLE && group.size() < max_tables)
                fuzz_object.create_table(group, log);

            else if (instr == REMOVE_TABLE && group.size() > 0) {
                fuzz_object.remove_table(group, log);
            }
            else if (instr == CLEAR_TABLE && group.size() > 0) {
                fuzz_object.clear_table(group, log, s);
            }
            else if (instr == CREATE_OBJECT && group.size() > 0) {
                fuzz_object.create_object(group, log, s);
            }
            else if (instr == ADD_COLUMN && group.size() > 0) {
                fuzz_object.add_column(group, log, s);
            }
            else if (instr == REMOVE_COLUMN && group.size() > 0) {
                fuzz_object.remove_column(group, log, s);
            }
            else if (instr == GET_ALL_COLUMN_NAMES && group.size() > 0) {
                fuzz_object.get_all_column_names(group);
            }
            else if (instr == RENAME_COLUMN && group.size() > 0) {
                fuzz_object.rename_column(group, log, s);
            }
            else if (instr == ADD_SEARCH_INDEX && group.size() > 0) {
                fuzz_object.add_search_index(group, log, s);
            }
            else if (instr == REMOVE_SEARCH_INDEX && group.size() > 0) {
                fuzz_object.remove_search_index(group, log, s);
            }
            else if (instr == ADD_COLUMN_LINK && group.size() >= 1) {
                fuzz_object.add_column_link(group, log, s);
            }
            else if (instr == ADD_COLUMN_LINK_LIST && group.size() >= 2) {
                fuzz_object.add_column_link_list(group, log, s);
            }
            else if (instr == SET && group.size() > 0) {
                fuzz_object.set_obj(group, log, s);
            }
            else if (instr == REMOVE_OBJECT && group.size() > 0) {
                fuzz_object.remove_obj(group, log, s);
            }
            else if (instr == REMOVE_RECURSIVE && group.size() > 0) {
                fuzz_object.remove_recursive(group, log, s);
            }
            else if (instr == ENUMERATE_COLUMN && group.size() > 0) {
                fuzz_object.enumerate_column(group, log, s);
            }
            else if (instr == COMMIT) {
                fuzz_object.commit(shared_realm, log);
            }
            else if (instr == ROLLBACK) {
                fuzz_object.rollback(shared_realm, group, log);
            }
            else if (instr == ADVANCE) {
                fuzz_object.advance(group, log);
            }
            else if (instr == CLOSE_AND_REOPEN) {
                fuzz_object.close_and_reopen(shared_realm, log, config);
            }
            else if (instr == CREATE_TABLE_VIEW && group.size() > 0) {
                fuzz_object.create_table_view(group, log, s, table_views);
            }
            else if (instr == COMPACT) {
            }
            else if (instr == IS_NULL && group.size() > 0) {
                fuzz_object.check_null(group, log, s);
            }
            else if (instr == ASYNC_WRITE && group.size() > 0) {
                fuzz_object.async_write(shared_realm, log);
            }
            else if (instr == ASYNC_CANCEL) {
                fuzz_object.async_cancel(shared_realm, group, log, s);
            }
        }
    }
    catch (const EndOfFile&) {
    }
}

//    if (file_names_from_stdin) {
//        std::string file_name;
//
//        std::cin >> file_name;
//        while (std::cin) {
//            std::ifstream in(prefix + file_name, std::ios::in | std::ios::binary);
//            if (!in.is_open()) {
//                std::cerr << "Could not open file for reading: " << (prefix + file_name) << std::endl;
//            }
//            else {
//                std::cout << file_name << std::endl;
//                realm::test_util::RealmPathInfo test_context{name};
//                SHARED_GROUP_TEST_PATH(path);
//
//                std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
//                parse_and_apply_instructions(contents, path, log);
//            }
//
//            std::cin >> file_name;
//        }
//    }
//    else {

//    auto path = "./fuzz_test.realm.lock";
//        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
//    log = &fs;

//    }
