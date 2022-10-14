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
#include "util.hpp"

#include <ctime>
#include <cstdio>
#include <fstream>
#include <iostream>

using namespace realm;
const size_t max_tables = REALM_MAX_BPNODE_SIZE * 10;

int FuzzEngine::run(int argc, const char* argv[])
{
    try {
        FuzzObject fuzzer;
        FuzzConfigurator cnf(fuzzer, argc, argv);

        if (cnf.is_stdin_filename_enabled()) {
            run_loop(cnf);
        }
        else {
            do_fuzz(cnf);
        }
    }
    catch (const EndOfFile&) {
    }
    return 0;
}

void FuzzEngine::do_fuzz(FuzzConfigurator& cnf)
{
    const auto path = cnf.get_realm_path();
    auto log = cnf.get_logger();
    auto& state = cnf.get_state();
    auto& fuzzer = cnf.get_fuzzer();
    auto shared_realm = Realm::get_shared_realm(cnf.get_config());
    std::vector<TableView> table_views;

    auto fetch_group = [shared_realm]() -> Group& {
        if (!shared_realm->is_in_transaction()) {
            shared_realm->begin_transaction();
        }
        return shared_realm->read_group();
    };

    int iteration = 0;

    for (;;) {
        char instr = fuzzer.get_next_token(state) % COUNT;
        iteration++;

        if (log) {
            *log << iteration << " ";
            *log << "Do fuzz with command " << (int)instr << std::endl;
        }

        Group& group = fetch_group();

        if (instr == ADD_TABLE && group.size() < max_tables) {
            fuzzer.create_table(group, log);
        }
        else if (instr == REMOVE_TABLE && group.size() > 0) {
            fuzzer.remove_table(group, log, state);
        }
        else if (instr == CLEAR_TABLE && group.size() > 0) {
            fuzzer.clear_table(group, log, state);
        }
        else if (instr == CREATE_OBJECT && group.size() > 0) {
            fuzzer.create_object(group, log, state);
        }
        else if (instr == ADD_COLUMN && group.size() > 0) {
            fuzzer.add_column(group, log, state);
        }
        else if (instr == REMOVE_COLUMN && group.size() > 0) {
            fuzzer.remove_column(group, log, state);
        }
        else if (instr == GET_ALL_COLUMN_NAMES && group.size() > 0) {
            fuzzer.get_all_column_names(group);
        }
        else if (instr == RENAME_COLUMN && group.size() > 0) {
            fuzzer.rename_column(group, log, state);
        }
        else if (instr == ADD_SEARCH_INDEX && group.size() > 0) {
            fuzzer.add_search_index(group, log, state);
        }
        else if (instr == REMOVE_SEARCH_INDEX && group.size() > 0) {
            fuzzer.remove_search_index(group, log, state);
        }
        else if (instr == ADD_COLUMN_LINK && group.size() >= 1) {
            fuzzer.add_column_link(group, log, state);
        }
        else if (instr == ADD_COLUMN_LINK_LIST && group.size() >= 2) {
            fuzzer.add_column_link_list(group, log, state);
        }
        else if (instr == SET && group.size() > 0) {
            fuzzer.set_obj(group, log, state);
        }
        else if (instr == REMOVE_OBJECT && group.size() > 0) {
            fuzzer.remove_obj(group, log, state);
        }
        else if (instr == REMOVE_RECURSIVE && group.size() > 0) {
            fuzzer.remove_recursive(group, log, state);
        }
        else if (instr == ENUMERATE_COLUMN && group.size() > 0) {
            fuzzer.enumerate_column(group, log, state);
        }
        else if (instr == COMMIT) {
            fuzzer.commit(shared_realm, log);
        }
        else if (instr == ROLLBACK) {
            fuzzer.rollback(shared_realm, group, log);
        }
        // else if (instr == ADVANCE) {
        //     fuzzer.advance(shared_realm, group, log);
        // }
        // else if (instr == CLOSE_AND_REOPEN) {
        //      fuzzer.close_and_reopen(shared_realm, log, cnf.get_config());
        //  }
        else if (instr == CREATE_TABLE_VIEW && group.size() > 0) {
            fuzzer.create_table_view(group, log, state, table_views);
        }
        else if (instr == COMPACT) {
        }
        else if (instr == IS_NULL && group.size() > 0) {
            fuzzer.check_null(group, log, state);
        }
        else if (instr == ASYNC_WRITE && group.size() > 0) {
            fuzzer.async_write(shared_realm, log);
        }
        else if (instr == ASYNC_CANCEL) {
            fuzzer.async_cancel(shared_realm, group, log, state);
        }
    }
}

void FuzzEngine::run_loop(FuzzConfigurator& cnf)
{
    std::string file_name;
    std::cin >> file_name;
    while (std::cin) {
        std::ifstream in(cnf.get_prefix() + file_name, std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Could not open file for reading: " << (cnf.get_prefix() + file_name) << std::endl;
        }
        else {
            std::cout << file_name << std::endl;
            std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
            cnf.set_state(contents);
            do_fuzz(cnf);
            std::cin >> file_name;
        }
    }
}
