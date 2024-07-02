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

const char hex_digits[] = "0123456789abcdefgh";
char hex_buffer[3];

static const char* to_hex(char c)
{
    hex_buffer[0] = hex_digits[(c >> 4) & 0x0f];
    hex_buffer[1] = hex_digits[c & 0x0f];
    return hex_buffer;
}

int FuzzEngine::run_fuzzer(const std::string& input, const std::string& name, bool enable_logging,
                           const std::string& path)
{
    auto configure = [&](auto& fuzzer) {
        FuzzConfigurator cnf(fuzzer, input, false, name);
        if (enable_logging) {
            cnf.get_logger().enable_logging(path);
            cnf.print_cnf();
        }
        return cnf;
    };

    try {
        FuzzObject fuzzer;
        FuzzConfigurator cnf = configure(fuzzer);
        REALM_ASSERT(&fuzzer == &cnf.get_fuzzer());
        do_fuzz(cnf);
    }
    catch (const EndOfFile& e) {
        std::cout << "End of file" << std::endl;
    }
    catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
    }
    return 0;
}

void FuzzEngine::do_fuzz(FuzzConfigurator& cnf)
{
    const auto path = cnf.get_realm_path();
    auto& log = cnf.get_logger();
    auto& state = cnf.get_state();
    auto& fuzzer = cnf.get_fuzzer();
    auto shared_realm = Realm::get_shared_realm(cnf.get_config());
    std::vector<TableView> table_views;

    log << "Start fuzzing with state = ";
    for (auto c : state.str) {
        log << to_hex(c) << " ";
    }
    log << "\n";

    auto begin_write = [&log](SharedRealm shared_realm) -> Group& {
        log << "begin_write() - check : shared_realm->is_in_transaction()\n";
        if (!shared_realm->is_in_transaction() && !shared_realm->is_in_async_transaction()) {
            log << "begin_write() - open transaction : shared_realm->begin_transaction()\n";
            try {
                shared_realm->begin_transaction();
            }
            catch (std::exception& e) {
                log << e.what() << "\n";
                throw;
            }
        }
        log << "begin_write() - return shared_realm->read_group();\n";
        return shared_realm->read_group();
    };

    int iteration = 0;

    for (;;) {
        char instr = fuzzer.get_next_token(state) % Count;
        iteration++;
        log << "Iteration: " << iteration << ". fuzz with command: " << std::to_string(instr) << "\n";

        try {
            Group& group = begin_write(shared_realm);
            if (instr == Add_Table && group.size() < max_tables) {
                fuzzer.create_table(group, log);
            }
            else if (instr == Remove_Table && group.size() > 0) {
                fuzzer.remove_table(group, log, state);
            }
            else if (instr == Clear_Table && group.size() > 0) {
                fuzzer.clear_table(group, log, state);
            }
            else if (instr == Create_Object && group.size() > 0) {
                fuzzer.create_object(group, log, state);
            }
            else if (instr == Add_Column && group.size() > 0) {
                fuzzer.add_column(group, log, state);
            }
            else if (instr == Remove_Column && group.size() > 0) {
                fuzzer.remove_column(group, log, state);
            }
            else if (instr == Get_All_Column_Names && group.size() > 0) {
                fuzzer.get_all_column_names(group, log);
            }
            else if (instr == Rename_Column && group.size() > 0) {
                fuzzer.rename_column(group, log, state);
            }
            else if (instr == Add_Search_Index && group.size() > 0) {
                fuzzer.add_search_index(group, log, state);
            }
            else if (instr == Remove_Search_Index && group.size() > 0) {
                fuzzer.remove_search_index(group, log, state);
            }
            else if (instr == Add_Column_Link && group.size() >= 1) {
                fuzzer.add_column_link(group, log, state);
            }
            else if (instr == Add_Column_Link_List && group.size() >= 2) {
                fuzzer.add_column_link_list(group, log, state);
            }
            else if (instr == Instruction::Set && group.size() > 0) {
                fuzzer.set_obj(group, log, state);
            }
            else if (instr == Remove_Object && group.size() > 0) {
                fuzzer.remove_obj(group, log, state);
            }
            else if (instr == Remove_Recursive && group.size() > 0) {
                fuzzer.remove_recursive(group, log, state);
            }
            else if (instr == Enumerate_Column && group.size() > 0) {
                fuzzer.enumerate_column(group, log, state);
            }
            else if (instr == Commit) {
                fuzzer.commit(shared_realm, log);
            }
            else if (instr == Rollback) {
                fuzzer.rollback(shared_realm, group, log);
            }
            else if (instr == Advance) {
                fuzzer.advance(shared_realm, log);
            }
            else if (instr == Close_And_Reopen) {
                fuzzer.close_and_reopen(shared_realm, log, cnf.get_config());
            }
            else if (instr == Create_Table_View && group.size() > 0) {
                fuzzer.create_table_view(group, log, state, table_views);
            }
            else if (instr == Compact) {
            }
            else if (instr == Is_Null && group.size() > 0) {
                fuzzer.check_null(group, log, state);
            }
        }
        catch (const std::exception& e) {
            log << "\nException thrown during execution:\n" << e.what() << "\n";
        }
    }
}
