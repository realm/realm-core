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

#include "fuzz_configurator.hpp"

#include "fuzz_object.hpp"
#include "../util/test_path.hpp"

#include <realm/disable_sync_to_disk.hpp>
#include <realm/object-store/util/scheduler.hpp>

FuzzConfigurator::FuzzConfigurator(FuzzObject& fuzzer, const std::string& input, bool use_input_file,
                                   const std::string& name)
    : m_used_input_file(use_input_file)
    , m_fuzzer(fuzzer)
    , m_fuzz_name(name)
{
    realm::disable_sync_to_disk();
    init(input);
    setup_realm_config();
}

void FuzzConfigurator::setup_realm_config()
{
    m_config.path = m_path;
    m_config.schema_version = 0;
    m_config.scheduler = realm::util::Scheduler::make_dummy();
    if (m_use_encryption) {
        const char* key = m_fuzzer.get_encryption_key();
        if (key) {
            const char* i = key;
            while (*i != '\0') {
                m_config.encryption_key.push_back(*i);
                i++;
            }
        }
    }
}

const realm::Realm::Config& FuzzConfigurator::get_config() const
{
    return m_config;
}

FuzzObject& FuzzConfigurator::get_fuzzer()
{
    return m_fuzzer;
}

const std::string& FuzzConfigurator::get_realm_path() const
{
    return m_path;
}

FuzzLog& FuzzConfigurator::get_logger()
{
    return m_log;
}

State& FuzzConfigurator::get_state()
{
    return m_state;
}

void FuzzConfigurator::init(const std::string& input)
{
    std::string db_name = "fuzz-test";
    realm::test_util::RealmPathInfo test_context{db_name};
    SHARED_GROUP_TEST_PATH(path);
    m_path = path.c_str();
    if (m_used_input_file) {
        std::ifstream in(input, std::ios::in | std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "Could not open file for reading: " << input << "\n";
            throw;
        }
        std::string contents((std::istreambuf_iterator<char>(in)), (std::istreambuf_iterator<char>()));
        set_state(contents);
    }
    else {
        set_state(input);
    }
}

void FuzzConfigurator::set_state(const std::string& input)
{
    m_state = State{input, 0};
    m_use_encryption = m_fuzzer.get_next_token(m_state) % 2 == 0;
}

void FuzzConfigurator::print_cnf()
{
    m_log << "// Fuzzer: " << m_fuzz_name << "\n";
    m_log << "// Test case generated in " REALM_VER_CHUNK " on " << m_fuzzer.get_current_time_stamp() << ".\n";
    m_log << "// REALM_MAX_BPNODE_SIZE is " << REALM_MAX_BPNODE_SIZE << "\n";
    m_log << "// ----------------------------------------------------------------------\n";
    const auto& printable_key =
        !m_use_encryption ? "nullptr" : std::string("\"") + m_config.encryption_key.data() + "\"";
    m_log << "// const char* key = " << printable_key << ";\n";
    m_log << "\n";
}
